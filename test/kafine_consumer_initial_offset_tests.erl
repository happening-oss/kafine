-module(kafine_consumer_initial_offset_tests).
-include_lib("eunit/include/eunit.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(PARTITION_1, 1).
-define(CONNECTION_OPTIONS, kafine_connection_options:validate_options(#{})).
-define(CONSUMER_OPTIONS, kafine_consumer_options:validate_options(#{})).
-define(TOPIC_OPTIONS(InitialOffset),
    kafine_topic_options:validate_options(
        [?TOPIC_NAME], #{?TOPIC_NAME => #{initial_offset => InitialOffset}}
    )
).
-define(FETCHER_METADATA, #{}).
-define(CALLBACK_ARGS, undefined).
-define(WAIT_TIMEOUT_MS, 2_000).

setup() ->
    meck:new(test_consumer_callback, [non_strict]),
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {ok, dummy} end),
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
    TopicOptions = ?TOPIC_OPTIONS(earliest),
    {ok, Sup} = kafine_consumer_sup:start_link(
        ?CONSUMER_REF,
        Broker,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        [TopicName],
        TopicOptions,
        ?FETCHER_METADATA,
        [parallel_callback(?CONSUMER_REF, TopicOptions)]
    ),
    kafine_parallel_subscription_callback:subscribe_partitions(
        not_used, #{TopicName => [?PARTITION_1]}, ?CONSUMER_REF
    ),

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

    kafine_consumer_sup:stop(Sup),
    kamock_broker:stop(Broker),
    ok.

latest() ->
    Broker = setup_broker(?BROKER_REF, 6, 10),

    TopicName = ?TOPIC_NAME,
    TopicOptions = ?TOPIC_OPTIONS(latest),
    {ok, Sup} = kafine_consumer_sup:start_link(
        ?CONSUMER_REF,
        Broker,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        [TopicName],
        TopicOptions,
        ?FETCHER_METADATA,
        [parallel_callback(?CONSUMER_REF, TopicOptions)]
    ),
    kafine_parallel_subscription_callback:subscribe_partitions(
        not_used, #{TopicName => [?PARTITION_1]}, ?CONSUMER_REF
    ),

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

    kafine_consumer_sup:stop(Sup),
    kamock_broker:stop(Broker),
    ok.

resume_paused() ->
    Broker = setup_broker(?BROKER_REF, 6, 10),

    TelemetryRef = telemetry_test:attach_event_handlers(self(), [
        [kafine, fetcher, wait_for_job]
    ]),

    % start paused
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {pause, dummy} end),

    TopicName = ?TOPIC_NAME,
    TopicOptions = ?TOPIC_OPTIONS(latest),
    {ok, Sup} = kafine_consumer_sup:start_link(
        ?CONSUMER_REF,
        Broker,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        [TopicName],
        TopicOptions,
        ?FETCHER_METADATA,
        [parallel_callback(?CONSUMER_REF, TopicOptions)]
    ),
    kafine_parallel_subscription_callback:subscribe_partitions(
        not_used, #{TopicName => [?PARTITION_1]}, ?CONSUMER_REF
    ),

    % Wait for the fetcher to have requested a job that can't be fulfilled
    receive
        {[kafine, fetcher, wait_for_job], TelemetryRef, _, _} -> ok
    end,

    % We should NOT have seen a ListOffsets request yet.
    ?assertEqual(0, meck:num_calls(kamock_list_offsets, handle_list_offsets_request, '_')),

    ok = kafine_consumer:resume(?CONSUMER_REF, TopicName, ?PARTITION_1),

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

    kafine_consumer_sup:stop(Sup),
    kamock_broker:stop(Broker),
    ok.

negative_in_range() ->
    % With messages 0..5, start with offset -3; we should see the last three messages.
    Broker = setup_broker(?BROKER_REF, 0, 5),

    TopicName = ?TOPIC_NAME,
    TopicOptions = ?TOPIC_OPTIONS(-3),
    {ok, Sup} = kafine_consumer_sup:start_link(
        ?CONSUMER_REF,
        Broker,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        [TopicName],
        TopicOptions,
        ?FETCHER_METADATA,
        [parallel_callback(?CONSUMER_REF, TopicOptions)]
    ),
    kafine_parallel_subscription_callback:subscribe_partitions(
        not_used, #{TopicName => [?PARTITION_1]}, ?CONSUMER_REF
    ),

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

    kafine_consumer_sup:stop(Sup),
    kamock_broker:stop(Broker),
    ok.

negative_before_first() ->
    % With messages 5..10, start with offset -7 (which would be offset 3, which is before the messages start).
    % We should see ListOffsets, Fetch 0, OFFSET_OUT_OF_RANGE, ListOffsets, Fetch 10.
    Broker = setup_broker(?BROKER_REF, 5, 10),

    TopicName = ?TOPIC_NAME,
    TopicOptions = ?TOPIC_OPTIONS(-7),
    {ok, Sup} = kafine_consumer_sup:start_link(
        ?CONSUMER_REF,
        Broker,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        [TopicName],
        TopicOptions,
        ?FETCHER_METADATA,
        [parallel_callback(?CONSUMER_REF, TopicOptions)]
    ),
    kafine_parallel_subscription_callback:subscribe_partitions(
        not_used, #{TopicName => [?PARTITION_1]}, ?CONSUMER_REF
    ),

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

    kafine_consumer_sup:stop(Sup),
    kamock_broker:stop(Broker),
    ok.

negative_before_zero() ->
    % With messages 0..5, start with offset -7; it should start from zero.
    Broker = setup_broker(?BROKER_REF, 0, 5),

    TopicName = ?TOPIC_NAME,
    TopicOptions = ?TOPIC_OPTIONS(-7),
    {ok, Sup} = kafine_consumer_sup:start_link(
        ?CONSUMER_REF,
        Broker,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        [TopicName],
        TopicOptions,
        ?FETCHER_METADATA,
        [parallel_callback(?CONSUMER_REF, TopicOptions)]
    ),
    kafine_parallel_subscription_callback:subscribe_partitions(
        not_used, #{TopicName => [?PARTITION_1]}, ?CONSUMER_REF
    ),

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

    kafine_consumer_sup:stop(Sup),
    kamock_broker:stop(Broker),
    ok.

negative_one() ->
    % Specific test for -1; last.
    Broker = setup_broker(?BROKER_REF, 0, 5),

    TopicName = ?TOPIC_NAME,
    TopicOptions = ?TOPIC_OPTIONS(-1),
    {ok, Sup} = kafine_consumer_sup:start_link(
        ?CONSUMER_REF,
        Broker,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        [TopicName],
        TopicOptions,
        ?FETCHER_METADATA,
        [parallel_callback(?CONSUMER_REF, TopicOptions)]
    ),
    kafine_parallel_subscription_callback:subscribe_partitions(
        not_used, #{TopicName => [?PARTITION_1]}, ?CONSUMER_REF
    ),

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

    kafine_consumer_sup:stop(Sup),
    kamock_broker:stop(Broker),
    ok.

parallel_callback(Ref, TopicOptions) ->
    Options = kafine_parallel_subscription_callback:validate_options(
        #{
            topic_options => TopicOptions,
            callback_mod => test_consumer_callback,
            callback_arg => ?CALLBACK_ARGS,
            skip_empty_fetches => false
        }
    ),

    #{
        id => kafine_parallel_subscription,
        start => {kafine_parallel_subscription_impl, start_link, [Ref, Options]},
        restart => permanent,
        shutdown => 5000,
        type => supervisor,
        modules => [kafine_parallel_subscription_impl]
    }.
