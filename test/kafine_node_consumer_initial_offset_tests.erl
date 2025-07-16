-module(kafine_node_consumer_initial_offset_tests).
-include_lib("eunit/include/eunit.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(WAIT_TIMEOUT_MS, 2_000).
-define(PARTITION_1, 61).

setup() ->
    kafine_node_consumer_tests:setup(?MODULE).

cleanup(_) ->
    meck:unload().

kafine_node_consumer_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun earliest/0,
        fun latest/0,
        fun resume_paused/0,
        fun negative_in_range/0,
        fun negative_before_first/0,
        fun negative_before_zero/0,
        fun negative_one/0
    ]}.

setup_broker(Ref, FirstOffset, LastOffset) ->
    {ok, Broker} = kamock_broker:start(Ref),

    MessageBuilder = fun(_Topic, Partition, Offset) ->
        Key = iolist_to_binary(io_lib:format("key-~B-~B", [Partition, Offset])),
        Value = iolist_to_binary(io_lib:format("value-~B-~B", [Partition, Offset])),
        #{key => Key, value => Value}
    end,

    meck:expect(
        kamock_list_offsets_partition_response,
        make_list_offsets_partition_response,
        kamock_list_offsets_partition_response:range(FirstOffset, LastOffset)
    ),

    meck:expect(
        kamock_partition_data,
        make_partition_data,
        kamock_partition_data:range(FirstOffset, LastOffset, MessageBuilder)
    ),

    Broker.

%% We should be able to set the initial offset to 'earliest', which should result in a ListOffsets request, and _then_
%% the first Fetch.
earliest() ->
    Broker = setup_broker(?BROKER_REF, 6, 10),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            ?PARTITION_1 => #{offset => earliest}
        }
    }),
    {ok, Pid} = start_node_consumer(?CONSUMER_REF, Broker, TopicPartitionStates),

    % We should see a call to ListOffsets, then a Fetch from the earliest offset.
    meck:wait(kamock_list_offsets, handle_list_offsets_request, '_', ?WAIT_TIMEOUT_MS),
    IsFetchOffset = fun(ExpectedTopic, ExpectedPartition, ExpectedOffset) ->
        fun(#{topics := [#{topic := T, partitions := [#{partition := P, fetch_offset := O}]}]}) ->
            ExpectedTopic == T andalso ExpectedPartition == P andalso ExpectedOffset == O
        end
    end,
    meck:wait(
        kamock_fetch,
        handle_fetch_request,
        [meck:is(IsFetchOffset(TopicName, ?PARTITION_1, 6)), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % Wait for two calls to end_record_batch, since we should repeat.
    meck:wait(
        2,
        test_consumer_callback,
        end_record_batch,
        '_',
        ?WAIT_TIMEOUT_MS
    ),

    stop_node_consumer(Pid, TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

latest() ->
    Broker = setup_broker(?BROKER_REF, 6, 10),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            ?PARTITION_1 => #{offset => latest}
        }
    }),
    {ok, Pid} = start_node_consumer(?CONSUMER_REF, Broker, TopicPartitionStates),

    % We should see a call to ListOffsets, then a Fetch from the latest offset.
    meck:wait(kamock_list_offsets, handle_list_offsets_request, '_', ?WAIT_TIMEOUT_MS),
    IsFetchOffset = fun(ExpectedTopic, ExpectedPartition, ExpectedOffset) ->
        fun(#{topics := [#{topic := T, partitions := [#{partition := P, fetch_offset := O}]}]}) ->
            ExpectedTopic == T andalso ExpectedPartition == P andalso ExpectedOffset == O
        end
    end,
    meck:wait(
        kamock_fetch,
        handle_fetch_request,
        [meck:is(IsFetchOffset(TopicName, ?PARTITION_1, 10)), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % Wait for two calls to end_record_batch, since we should repeat.
    meck:wait(
        2,
        test_consumer_callback,
        end_record_batch,
        '_',
        ?WAIT_TIMEOUT_MS
    ),

    stop_node_consumer(Pid, TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

resume_paused() ->
    Broker = setup_broker(?BROKER_REF, 6, 10),

    TelemetryRef = telemetry_test:attach_event_handlers(self(), [
        [kafine, node_consumer, idle],
        [kafine, node_consumer, continue],
        [kafine, node_consumer, subscribe]
    ]),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            ?PARTITION_1 => #{state => paused, offset => latest}
        }
    }),
    {ok, Pid} = start_node_consumer(?CONSUMER_REF, Broker, TopicPartitionStates),

    % Wait for the idle/subscribe loop.
    receive
        {[kafine, node_consumer, idle], TelemetryRef, _, _} -> ok
    end,

    receive
        {[kafine, node_consumer, subscribe], TelemetryRef, #{partition_count := 1}, _Meta} -> ok
    end,

    % Then wait until we go idle again, because the subscription is still paused.
    receive
        {[kafine, node_consumer, idle], TelemetryRef, _, _} -> ok
    end,

    % We should NOT have seen a ListOffsets request yet.
    ?assertEqual(0, meck:num_calls(kamock_list_offsets, handle_list_offsets_request, '_')),

    ok = kafine_node_consumer:resume(Pid, TopicName, ?PARTITION_1),

    % We should see a call to ListOffsets, then a Fetch from the latest offset.
    meck:wait(kamock_list_offsets, handle_list_offsets_request, '_', ?WAIT_TIMEOUT_MS),
    IsFetchOffset = fun(ExpectedTopic, ExpectedPartition, ExpectedOffset) ->
        fun(#{topics := [#{topic := T, partitions := [#{partition := P, fetch_offset := O}]}]}) ->
            ExpectedTopic == T andalso ExpectedPartition == P andalso ExpectedOffset == O
        end
    end,
    meck:wait(
        kamock_fetch,
        handle_fetch_request,
        [meck:is(IsFetchOffset(TopicName, ?PARTITION_1, 10)), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % Wait for two calls to end_record_batch, since we should repeat.
    meck:wait(
        2,
        test_consumer_callback,
        end_record_batch,
        '_',
        ?WAIT_TIMEOUT_MS
    ),

    telemetry:detach(TelemetryRef),

    stop_node_consumer(Pid, TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

negative_in_range() ->
    % With messages 0..5, start with offset -3; we should see the last three messages.
    Broker = setup_broker(?BROKER_REF, 0, 5),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            ?PARTITION_1 => #{offset => -3}
        }
    }),
    {ok, Pid} = start_node_consumer(?CONSUMER_REF, Broker, TopicPartitionStates),

    % We should see a call to ListOffsets, then a Fetch from offset 2.
    meck:wait(kamock_list_offsets, handle_list_offsets_request, '_', ?WAIT_TIMEOUT_MS),
    IsFetchOffset = fun(ExpectedTopic, ExpectedPartition, ExpectedOffset) ->
        fun(#{topics := [#{topic := T, partitions := [#{partition := P, fetch_offset := O}]}]}) ->
            ExpectedTopic == T andalso ExpectedPartition == P andalso ExpectedOffset == O
        end
    end,

    meck:wait(
        kamock_fetch,
        handle_fetch_request,
        [meck:is(IsFetchOffset(TopicName, ?PARTITION_1, 2)), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % Wait for two calls to end_record_batch, since we should repeat.
    meck:wait(
        2,
        test_consumer_callback,
        end_record_batch,
        '_',
        ?WAIT_TIMEOUT_MS
    ),

    stop_node_consumer(Pid, TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

negative_before_first() ->
    % With messages 5..10, start with offset -7 (which would be offset 3, which is before the messages start).
    % We should see ListOffsets, Fetch 0, OFFSET_OUT_OF_RANGE, ListOffsets, Fetch 10.
    Broker = setup_broker(?BROKER_REF, 5, 10),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            ?PARTITION_1 => #{offset => -7}
        }
    }),
    {ok, Pid} = start_node_consumer(?CONSUMER_REF, Broker, TopicPartitionStates),

    % We should see a call to ListOffsets, then a Fetch from offset 3.
    meck:wait(kamock_list_offsets, handle_list_offsets_request, '_', ?WAIT_TIMEOUT_MS),
    IsFetchOffset = fun(ExpectedTopic, ExpectedPartition, ExpectedOffset) ->
        fun(#{topics := [#{topic := T, partitions := [#{partition := P, fetch_offset := O}]}]}) ->
            ExpectedTopic == T andalso ExpectedPartition == P andalso ExpectedOffset == O
        end
    end,

    meck:wait(
        kamock_fetch,
        handle_fetch_request,
        [meck:is(IsFetchOffset(TopicName, ?PARTITION_1, 3)), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % Then another ListOffsets.
    meck:wait(kamock_list_offsets, handle_list_offsets_request, '_', ?WAIT_TIMEOUT_MS),

    % Then another Fetch.
    meck:wait(
        kamock_fetch,
        handle_fetch_request,
        [meck:is(IsFetchOffset(TopicName, ?PARTITION_1, 10)), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % Wait for two calls to end_record_batch, since we should repeat.
    meck:wait(
        2,
        test_consumer_callback,
        end_record_batch,
        '_',
        ?WAIT_TIMEOUT_MS
    ),

    stop_node_consumer(Pid, TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

negative_before_zero() ->
    % With messages 0..5, start with offset -7; it should start from zero.
    Broker = setup_broker(?BROKER_REF, 0, 5),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            ?PARTITION_1 => #{offset => -7}
        }
    }),
    {ok, Pid} = start_node_consumer(?CONSUMER_REF, Broker, TopicPartitionStates),

    % We should see a call to ListOffsets, then a Fetch from offset 0.
    meck:wait(kamock_list_offsets, handle_list_offsets_request, '_', ?WAIT_TIMEOUT_MS),
    IsFetchOffset = fun(ExpectedTopic, ExpectedPartition, ExpectedOffset) ->
        fun(#{topics := [#{topic := T, partitions := [#{partition := P, fetch_offset := O}]}]}) ->
            ExpectedTopic == T andalso ExpectedPartition == P andalso ExpectedOffset == O
        end
    end,

    meck:wait(
        kamock_fetch,
        handle_fetch_request,
        [meck:is(IsFetchOffset(TopicName, ?PARTITION_1, 0)), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % Wait for two calls to end_record_batch, since we should repeat.
    meck:wait(
        2,
        test_consumer_callback,
        end_record_batch,
        '_',
        ?WAIT_TIMEOUT_MS
    ),

    stop_node_consumer(Pid, TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

negative_one() ->
    % Specific test for -1; last.
    Broker = setup_broker(?BROKER_REF, 0, 5),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            ?PARTITION_1 => #{offset => -1}
        }
    }),
    {ok, Pid} = start_node_consumer(?CONSUMER_REF, Broker, TopicPartitionStates),

    % We should see a call to ListOffsets, then a Fetch from offset 4.
    meck:wait(kamock_list_offsets, handle_list_offsets_request, '_', ?WAIT_TIMEOUT_MS),
    IsFetchOffset = fun(ExpectedTopic, ExpectedPartition, ExpectedOffset) ->
        fun(#{topics := [#{topic := T, partitions := [#{partition := P, fetch_offset := O}]}]}) ->
            ExpectedTopic == T andalso ExpectedPartition == P andalso ExpectedOffset == O
        end
    end,

    meck:wait(
        kamock_fetch,
        handle_fetch_request,
        [meck:is(IsFetchOffset(TopicName, ?PARTITION_1, 4)), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % Wait for two calls to end_record_batch, since we should repeat.
    meck:wait(
        2,
        test_consumer_callback,
        end_record_batch,
        '_',
        ?WAIT_TIMEOUT_MS
    ),

    stop_node_consumer(Pid, TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

init_topic_partition_states(InitStates) ->
    kafine_fetch_response_tests:init_topic_partition_states(InitStates).

start_node_consumer(Ref, Broker, TopicPartitionStates) ->
    kafine_node_consumer_tests:start_node_consumer(Ref, Broker, TopicPartitionStates).

stop_node_consumer(Pid, TopicPartitionStates) ->
    kafine_node_consumer_tests:stop_node_consumer(Pid, TopicPartitionStates).
