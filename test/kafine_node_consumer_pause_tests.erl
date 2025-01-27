-module(kafine_node_consumer_pause_tests).
-include_lib("eunit/include/eunit.hrl").

-include("src/consumer/kafine_topic_partition_state.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(PARTITION_1, 61).
-define(PARTITION_2, 62).
-define(CALLBACK_STATE, ?MODULE).
-define(WAIT_TIMEOUT_MS, 2_000).
-define(CONNECTION_OPTIONS, #{}).

setup() ->
    meck:new(test_consumer_callback, [non_strict]),
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {ok, ?CALLBACK_STATE} end),
    meck:expect(test_consumer_callback, begin_record_batch, fun(_T, _P, _O, _Info, St) ->
        {ok, St}
    end),
    meck:expect(test_consumer_callback, handle_record, fun(_T, _P, _M, St) -> {ok, St} end),
    meck:expect(test_consumer_callback, end_record_batch, fun(_T, _P, _N, _Info, St) -> {ok, St} end),

    meck:new(kamock_list_offsets, [passthrough]),
    meck:new(kamock_fetch, [passthrough]),
    ok.

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
    TopicPartitionStates = #{
        TopicName => #{
            % The partition state is initially set from ConsumerCallback:init.
            ?PARTITION_1 => #topic_partition_state{
                state = paused, state_data = ?CALLBACK_STATE, offset = 10
            },
            ?PARTITION_2 => #topic_partition_state{
                state = paused, state_data = ?CALLBACK_STATE, offset = 12
            }
        }
    },

    TelemetryRef = telemetry_test:attach_event_handlers(self(), [
        [kafine, node_consumer, idle],
        [kafine, node_consumer, continue]
    ]),

    {ok, Pid} = start_node_consumer(Broker, TopicPartitionStates),

    receive
        {[kafine, node_consumer, idle], TelemetryRef, _, _} -> ok
    end,

    ?assertMatch({idle, _}, sys:get_state(Pid)),

    telemetry:detach(TelemetryRef),

    kafine_node_consumer:stop(Pid),
    kamock_broker:stop(Broker),
    ok.

pause() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = #{
        TopicName => #{
            ?PARTITION_1 => #topic_partition_state{state = active, offset = 10},
            ?PARTITION_2 => #topic_partition_state{state = active, offset = 12}
        }
    },

    % Pretend that there are some messages.
    mock_produce(0, 14),

    % Pause one of the partitions when it gets to a particular offset.
    meck:expect(test_consumer_callback, handle_record, fun
        (_T, _P = ?PARTITION_1, _M = #{offset := 11}, St) -> {pause, St};
        (_T, _P, _M, St) -> {ok, St}
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
        [{?PARTITION_2, 12}, {?PARTITION_1, 10}, {?PARTITION_2, 13}, {?PARTITION_1, 11}],
        fetch_request_history()
    ),
    meck:reset(kamock_partition_data),

    % Produce some more messages.
    mock_produce(0, 18),

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

    kafine_node_consumer:stop(Pid),
    kamock_broker:stop(Broker),
    ok.

pause_all() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = #{
        TopicName => #{
            ?PARTITION_1 => #topic_partition_state{state = active, offset = 10},
            ?PARTITION_2 => #topic_partition_state{state = active, offset = 12}
        }
    },

    % Pretend that there are some messages.
    mock_produce(0, 14),

    % Pause both of the partitions when they get to a particular offset.
    meck:expect(test_consumer_callback, handle_record, fun
        (_T, _P, _M = #{offset := 13}, St) -> {pause, St};
        (_T, _P, _M, St) -> {ok, St}
    end),

    {ok, Pid} = start_node_consumer(Broker, TopicPartitionStates),

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
            {?PARTITION_2, 12},
            {?PARTITION_1, 10},
            {?PARTITION_2, 13},
            {?PARTITION_1, 11},
            {?PARTITION_1, 12},
            {?PARTITION_1, 13}
        ],
        fetch_request_history()
    ),
    meck:reset(kamock_partition_data),

    % Produce some more messages.
    mock_produce(0, 18),

    % We should be in the 'idle' state -- we've got nothing to do, so we don't bother issuing empty fetches.
    ?assertMatch({idle, _}, sys:get_state(Pid)),

    kafine_node_consumer:stop(Pid),
    kamock_broker:stop(Broker),
    ok.

start_node_consumer(Broker, TopicPartitionStates) ->
    % validate_options is a helper function; we only call it because we're testing kafine_node_consumer directly.
    ConsumerOptions = kafine_consumer_options:validate_options(#{}),
    TopicNames = maps:keys(TopicPartitionStates),
    TopicOptions =
        #{TopicName => kafine_topic_options:validate_options(#{}) || TopicName <- TopicNames},
    {ok, Pid} = kafine_node_consumer:start_link(
        Broker,
        ?CONNECTION_OPTIONS,
        ConsumerOptions,
        {test_consumer_callback, undefined},
        self()
    ),
    ok = kafine_node_consumer:subscribe(Pid, TopicPartitionStates, TopicOptions),
    {ok, Pid}.

mock_produce(FirstOffset, LastOffset) ->
    meck:expect(
        kamock_list_offsets_partition_response,
        make_list_offsets_partition_response,
        kamock_list_offsets_partition_response:range(FirstOffset, LastOffset)
    ),

    % If we send an empty fetch, that's a bad thing; we should have gone idle.
    % Note that there's a race here, so this won't always trigger.
    meck:expect(
        kamock_fetch,
        handle_fetch_request,
        fun(FetchRequest = #{topics := Topics}, Env) ->
            ?assertNotEqual([], Topics),
            meck:passthrough([FetchRequest, Env])
        end
    ),

    meck:expect(
        kamock_fetchable_topic,
        make_fetchable_topic_response,
        fun(FetchableTopic = #{topic := _Topic, partitions := FetchPartitions}, Env) ->
            ?assertNotEqual([], FetchPartitions),
            meck:passthrough([FetchableTopic, Env])
        end
    ),

    MessageBuilder = fun(_T, _P, O) ->
        Key = iolist_to_binary(io_lib:format("key~B", [O])),
        #{key => Key}
    end,
    meck:expect(
        kamock_partition_data,
        make_partition_data,
        kamock_partition_data:range(FirstOffset, LastOffset, MessageBuilder)
    ),
    ok.

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
