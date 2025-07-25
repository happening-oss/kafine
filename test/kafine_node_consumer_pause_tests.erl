-module(kafine_node_consumer_pause_tests).
-include_lib("eunit/include/eunit.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(PARTITION_1, 61).
-define(PARTITION_2, 62).
-define(CALLBACK_STATE, {state, ?MODULE}).
-define(WAIT_TIMEOUT_MS, 2_000).

setup() ->
    kafine_node_consumer_tests:setup(?MODULE).

cleanup(_) ->
    meck:unload().

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun start_paused/0,
        fun pause/0,
        % TODO: pause at parity, rather than arbitrarily.
        fun pause_all/0
    ]}.

start_paused() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    % ConsumerCallback:init is called by kafine_consumer, not kafine_node_consumer, so this doesn't actually get
    % invoked. But maybe it's more readable if we show how you _would_ start paused.
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {pause, ?CALLBACK_STATE} end),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            ?PARTITION_1 => #{state => paused, offset => 10},
            ?PARTITION_2 => #{state => paused, offset => 12}
        }
    }),

    TelemetryRef = telemetry_test:attach_event_handlers(self(), [
        [kafine, node_consumer, idle],
        [kafine, node_consumer, continue]
    ]),

    {ok, Pid} = start_node_consumer(?CONSUMER_REF, Broker, TopicPartitionStates),

    receive
        {[kafine, node_consumer, idle], TelemetryRef, _, _} -> ok
    end,

    ?assertMatch({idle, _}, sys:get_state(Pid)),

    telemetry:detach(TelemetryRef),

    stop_node_consumer(Pid, TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

pause() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            ?PARTITION_1 => #{offset => 10},
            ?PARTITION_2 => #{offset => 12}
        }
    }),

    % Pretend that there are some messages.
    kafine_kamock:produce(0, 14),

    % Pause one of the partitions when it gets to a particular offset.
    meck:expect(test_consumer_callback, handle_record, fun
        (_T, _P = ?PARTITION_1, _M = #{offset := 11}, St) -> {pause, St};
        (_T, _P, _M, St) -> {ok, St}
    end),

    {ok, Pid} = start_node_consumer(?CONSUMER_REF, Broker, TopicPartitionStates),

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
        [
            {?PARTITION_1, 10},
            {?PARTITION_1, 11},
            {?PARTITION_2, 12},
            {?PARTITION_2, 13}
        ],
        fetch_request_history()
    ),
    meck:reset(kamock_partition_data),

    % Produce some more messages.
    kafine_kamock:produce(0, 18),

    % First partition is paused; wait until second partition catches up.
    meck:wait(
        test_consumer_callback,
        end_record_batch,
        ['_', ?PARTITION_2, 18, '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % We should see fetches to only one partition.
    ?assertMatch(
        [{?PARTITION_2, 14}, {?PARTITION_2, 15}, {?PARTITION_2, 16}, {?PARTITION_2, 17}],
        fetch_request_history()
    ),
    meck:reset(kamock_partition_data),

    stop_node_consumer(Pid, TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

pause_all() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            ?PARTITION_1 => #{offset => 10},
            ?PARTITION_2 => #{offset => 12}
        }
    }),

    % Pretend that there are some messages.
    kafine_kamock:produce(0, 14),

    % Pause both of the partitions when they get to a particular offset.
    meck:expect(test_consumer_callback, handle_record, fun
        (_T, _P, _M = #{offset := 13}, St) -> {pause, St};
        (_T, _P, _M, St) -> {ok, St}
    end),

    {ok, Pid} = start_node_consumer(?CONSUMER_REF, Broker, TopicPartitionStates),

    % Wait until we've caught up.
    meck:wait(
        test_consumer_callback,
        end_record_batch,
        ['_', ?PARTITION_1, 14, '_', '_'],
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
        [
            {?PARTITION_1, 10},
            {?PARTITION_1, 11},
            {?PARTITION_1, 12},
            {?PARTITION_1, 13},
            {?PARTITION_2, 12},
            {?PARTITION_2, 13}
        ],
        fetch_request_history()
    ),
    meck:reset(kamock_partition_data),

    % Produce some more messages.
    kafine_kamock:produce(0, 18),

    % We should be in the 'idle' state -- we've got nothing to do, so we don't bother issuing empty fetches.
    ?assertMatch({idle, _}, sys:get_state(Pid)),

    stop_node_consumer(Pid, TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

init_topic_partition_states(InitStates) ->
    kafine_fetch_response_tests:init_topic_partition_states(InitStates).

start_node_consumer(Ref, Broker, TopicPartitionStates) ->
    kafine_node_consumer_tests:start_node_consumer(Ref, Broker, TopicPartitionStates).

stop_node_consumer(Pid, TopicPartitionStates) ->
    kafine_node_consumer_tests:stop_node_consumer(Pid, TopicPartitionStates).

fetch_request_history() ->
    lists:sort(
        lists:filtermap(
            fun
                ({_, {_, make_partition_data, [_, #{partition := P, fetch_offset := O}, _]}, _}) ->
                    {true, {P, O}};
                (_) ->
                    false
            end,
            meck:history(kamock_partition_data)
        )
    ).
