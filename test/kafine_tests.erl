-module(kafine_tests).
%%% Tests for the high-level API in kafine.erl
-include_lib("eunit/include/eunit.hrl").

-define(CLUSTER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME_1, iolist_to_binary(io_lib:format("~s___~s_1_t", [?MODULE, ?FUNCTION_NAME]))).
-define(TOPIC_NAME_2, iolist_to_binary(io_lib:format("~s___~s_2_t", [?MODULE, ?FUNCTION_NAME]))).
-define(GROUP_ID, iolist_to_binary(io_lib:format("~s___~s_g", [?MODULE, ?FUNCTION_NAME]))).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF_1, {?MODULE, ?FUNCTION_NAME, 1}).
-define(CONSUMER_REF_2, {?MODULE, ?FUNCTION_NAME, 2}).
-define(CALLBACK_ARGS, undefined).
-define(CALLBACK_STATE, ?MODULE).
-define(CONNECTION_OPTIONS, #{}).
-define(CONSUMER_OPTIONS, #{}).
-define(SUBSCRIBER_OPTIONS, #{}).
-define(WAIT_TIMEOUT_MS, 2_000).

all_test_() ->
    {setup, fun setup/0, fun cleanup/1, [
        fun start_topic_consumer/0,
        fun start_group_consumer/0
    ]}.

setup() ->
    {ok, _} = application:ensure_all_started(kafine),

    meck:new(test_consumer_callback, [non_strict]),
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {ok, ?CALLBACK_STATE} end),
    meck:expect(test_consumer_callback, begin_record_batch, fun(_T, _P, _O, _Info, St) ->
        {ok, St}
    end),
    meck:expect(test_consumer_callback, handle_record, fun(_T, _P, _M, St) -> {ok, St} end),
    meck:expect(test_consumer_callback, end_record_batch, fun(_T, _P, _N, _Info, St) -> {ok, St} end),
    ok.

cleanup(_) ->
    application:stop(kafine),
    ok.

start_topic_consumer() ->
    {ok, _Cluster, _Brokers = [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF),

    FirstOffset = 0,
    LastOffset = 2,
    MessageBuilder = fun(T, Partition, Offset) ->
        MessageId = iolist_to_binary(
            io_lib:format("~s-~B-~B", [T, Partition, Offset])
        ),
        #{key => MessageId, value => MessageId}
    end,
    meck:expect(
        kamock_partition_data,
        make_partition_data,
        kamock_partition_data:range(FirstOffset, LastOffset, MessageBuilder)
    ),

    {ok, _Consumer} = kafine:start_topic_consumer(
        ?CONSUMER_REF,
        Bootstrap,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        ?SUBSCRIBER_OPTIONS,
        {test_consumer_callback, ?CALLBACK_ARGS},
        [?TOPIC_NAME_1, ?TOPIC_NAME_2],
        #{}
    ),

    % There should be 8 calls to test_consumer_callback:init; one for each topic and partition:
    meck:wait(8, test_consumer_callback, init, '_', ?WAIT_TIMEOUT_MS),

    % Then we should see a bunch of calls to begin_record_batch, handle_record, end_record_batch; we'll just check for
    % the handle_record calls.
    TopicCount = 2,
    PartitionCount = 4,
    ExpectedRecordCount = TopicCount * PartitionCount * 2,
    meck:wait(ExpectedRecordCount, test_consumer_callback, handle_record, '_', ?WAIT_TIMEOUT_MS),
    ok.

start_group_consumer() ->
    {ok, _Cluster, _Brokers = [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF),

    FirstOffset = 0,
    LastOffset = 2,
    MessageBuilder = fun(T, Partition, Offset) ->
        MessageId = iolist_to_binary(
            io_lib:format("~s-~B-~B", [T, Partition, Offset])
        ),
        #{key => MessageId, value => MessageId}
    end,
    meck:expect(
        kamock_partition_data,
        make_partition_data,
        kamock_partition_data:range(FirstOffset, LastOffset, MessageBuilder)
    ),

    {ok, _Consumer1} = kafine:start_group_consumer(
        ?CONSUMER_REF_1,
        Bootstrap,
        ?CONNECTION_OPTIONS,
        ?GROUP_ID,
        ?CONSUMER_OPTIONS,
        ?SUBSCRIBER_OPTIONS,
        {test_consumer_callback, ?CALLBACK_ARGS},
        [?TOPIC_NAME_1, ?TOPIC_NAME_2],
        #{}
    ),

    {ok, _Consumer2} = kafine:start_group_consumer(
        ?CONSUMER_REF_2,
        Bootstrap,
        ?CONNECTION_OPTIONS,
        ?GROUP_ID,
        ?CONSUMER_OPTIONS,
        ?SUBSCRIBER_OPTIONS,
        {test_consumer_callback, ?CALLBACK_ARGS},
        [?TOPIC_NAME_1, ?TOPIC_NAME_2],
        #{}
    ),

    % There should be 8 calls to test_consumer_callback:init; one for each topic and partition:
    meck:wait(8, test_consumer_callback, init, '_', ?WAIT_TIMEOUT_MS),

    % Then we should see a bunch of calls to begin_record_batch, handle_record, end_record_batch; we'll just check for
    % the handle_record calls.
    TopicCount = 2,
    PartitionCount = 4,
    ExpectedRecordCount = TopicCount * PartitionCount * 2,
    meck:wait(ExpectedRecordCount, test_consumer_callback, handle_record, '_', ?WAIT_TIMEOUT_MS),
    ok.
