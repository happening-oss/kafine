-module(kafine_node_consumer_pause_batch_tests).
-include_lib("eunit/include/eunit.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(PARTITION_1, 61).
-define(PARTITION_2, 62).
-define(CALLBACK_STATE, ?MODULE).
-define(WAIT_TIMEOUT_MS, 2_000).

setup() ->
    meck:new(test_consumer_callback, [non_strict]),
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {ok, ?CALLBACK_STATE} end),
    meck:expect(test_consumer_callback, begin_record_batch, fun(_T, _P, _O, _Info, St) ->
        {ok, St}
    end),
    meck:expect(test_consumer_callback, handle_record, fun(_T, _P, _M, St) -> {ok, St} end),
    meck:expect(test_consumer_callback, end_record_batch, fun(_T, _P, _N, _Info, St) -> {ok, St} end),

    meck:expect(kafine_consumer, init_ack, fun(_Ref, _Topic, _Partition, _State) -> ok end),

    meck:new(kamock_list_offsets, [passthrough]),
    meck:new(kamock_fetch, [passthrough]),
    ok.

cleanup(_) ->
    meck:unload().

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun end_record_batch_pause/0,
        fun end_record_batch_pause_all/0
    ]}.

end_record_batch_pause() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            ?PARTITION_1 => #{offset => 10},
            ?PARTITION_2 => #{offset => 12}
        }
    }),

    % Pretend that there are some messages.
    MessageBuilder = fun(_T, _P, O) ->
        Key = iolist_to_binary(io_lib:format("key~B", [O])),
        #{key => Key}
    end,
    meck:expect(
        kamock_partition_data,
        make_partition_data,
        kamock_partition_data:batches(0, 14, 3, MessageBuilder)
    ),

    % Pause one of the partitions.
    meck:expect(test_consumer_callback, end_record_batch, fun
        (_T, _P = ?PARTITION_1, _M, _I, St) -> {pause, St};
        (_T, _P, _M, _I, St) -> {ok, St}
    end),

    {ok, Pid} = start_node_consumer(Broker, TopicPartitionStates),

    % Wait until we've caught up.
    meck:wait(
        test_consumer_callback,
        end_record_batch,
        ['_', ?PARTITION_1, 12, '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),
    meck:wait(
        test_consumer_callback,
        end_record_batch,
        ['_', ?PARTITION_2, 14, '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % We should see fetches to both partitions.
    ?assertMatch(
        [{?PARTITION_2, 12}, {?PARTITION_1, 10}],
        fetch_request_history()
    ),
    meck:reset(kamock_partition_data),

    % Produce some more messages.
    meck:expect(
        kamock_partition_data,
        make_partition_data,
        kamock_partition_data:batches(0, 18, 3, MessageBuilder)
    ),

    % First partition is paused; wait until second partition catches up.
    meck:wait(
        test_consumer_callback,
        end_record_batch,
        ['_', ?PARTITION_2, 18, '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % We should see fetches to only one partition.
    % There are two fetches because offset 14 falls into a batch, 15 is the next batch, then 18 is EOF.
    ?assertMatch(
        [{?PARTITION_2, 14}, {?PARTITION_2, 15}],
        fetch_request_history()
    ),
    meck:reset(kamock_partition_data),

    kafine_node_consumer:stop(Pid),
    cleanup_topic_partition_states(TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

end_record_batch_pause_all() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            ?PARTITION_1 => #{offset => 10},
            ?PARTITION_2 => #{offset => 12}
        }
    }),

    % Pretend that there are some messages.
    MessageBuilder = fun(_T, _P, O) ->
        Key = iolist_to_binary(io_lib:format("key~B", [O])),
        #{key => Key}
    end,
    meck:expect(
        kamock_partition_data,
        make_partition_data,
        kamock_partition_data:batches(0, 14, 3, MessageBuilder)
    ),

    % Pause all of the partitions.
    meck:expect(test_consumer_callback, end_record_batch, fun(_T, _P, _M, _I, St) -> {pause, St} end),

    {ok, Pid} = start_node_consumer(Broker, TopicPartitionStates),

    % Wait until we've caught up.
    meck:wait(
        test_consumer_callback,
        end_record_batch,
        ['_', ?PARTITION_1, 12, '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),
    meck:wait(
        test_consumer_callback,
        end_record_batch,
        ['_', ?PARTITION_2, 14, '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % We should see fetches to both partitions.
    ?assertMatch(
        [{?PARTITION_2, 12}, {?PARTITION_1, 10}],
        fetch_request_history()
    ),
    meck:reset(kamock_partition_data),

    % Produce some more messages.
    meck:expect(
        kamock_partition_data,
        make_partition_data,
        kamock_partition_data:batches(0, 18, 3, MessageBuilder)
    ),

    % We should be in the 'idle' state -- we've got nothing to do, so we don't bother issuing empty fetches.
    ?assertMatch({idle, _}, sys:get_state(Pid)),

    kafine_node_consumer:stop(Pid),
    cleanup_topic_partition_states(TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

init_topic_partition_states(InitStates) ->
    kafine_fetch_response_tests:init_topic_partition_states(InitStates).

cleanup_topic_partition_states(TopicPartitionStates) ->
    kafine_fetch_response_tests:cleanup_topic_partition_states(TopicPartitionStates).

start_node_consumer(Broker, TopicPartitionStates) ->
    kafine_node_consumer_tests:start_node_consumer(Broker, TopicPartitionStates).

fetch_request_history() ->
    lists:filtermap(
        fun
            ({_, {_, make_partition_data, [_, #{partition := P, fetch_offset := O}, _]}, _}) ->
                {true, {P, O}};
            (_) ->
                false
        end,
        meck:history(kamock_partition_data)
    ).
