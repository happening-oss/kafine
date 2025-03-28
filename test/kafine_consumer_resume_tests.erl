-module(kafine_consumer_resume_tests).
-include_lib("eunit/include/eunit.hrl").

-include("history_matchers.hrl").

-define(CLUSTER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(PARTITION_1, 1).
-define(PARTITION_2, 2).
-define(CALLBACK_ARGS, undefined).
-define(CALLBACK_STATE, {state, ?MODULE}).
-define(WAIT_TIMEOUT_MS, 2_000).

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
        fun start_paused_resume_later/0,
        fun resume_after_move/0
    ]}.

start_paused_resume_later() ->
    {ok, Cluster, [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF),

    % Pretend that there are some messages.
    kafine_kamock:produce(0, 4),

    % Start paused.
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {pause, ?CALLBACK_STATE} end),

    {ok, Consumer} = kafine_consumer:start_link(
        ?CONSUMER_REF,
        Bootstrap,
        #{},
        {test_consumer_callback, ?CALLBACK_ARGS},
        #{}
    ),

    Topic1 = ?TOPIC_NAME,
    ok = kafine_consumer:subscribe(Consumer, #{
        Topic1 => {#{}, #{?PARTITION_1 => 0, ?PARTITION_2 => 0}}
    }),

    % We're paused. The assertion for this is in kafine_consumer_pause_tests.

    % Resume.
    kafine_consumer:resume(Consumer, Topic1, ?PARTITION_1),

    meck:wait(2, test_consumer_callback, end_record_batch, '_', ?WAIT_TIMEOUT_MS),

    ?assertMatch(
        [
            ?init_callback(TopicName, ?PARTITION_1, ?CALLBACK_ARGS),
            ?init_callback(TopicName, ?PARTITION_2, ?CALLBACK_ARGS),
            ?begin_record_batch(TopicName, ?PARTITION_1, 0, 0, 4, 4),
            ?handle_record(TopicName, ?PARTITION_1, 0, _, _),
            ?end_record_batch(TopicName, ?PARTITION_1, 1, 0, 4, 4),
            ?begin_record_batch(TopicName, ?PARTITION_1, 1, 0, 4, 4),
            ?handle_record(TopicName, ?PARTITION_1, 1, _, _),
            ?end_record_batch(TopicName, ?PARTITION_1, 2, 0, 4, 4)
        ],
        meck:history(test_consumer_callback)
    ),

    kafine_consumer:stop(Consumer),
    kamock_cluster:stop(Cluster),
    ok.

resume_after_move() ->
    NodeIds = [101, 102, 103],
    {ok, Cluster, [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF, NodeIds),

    % Start with the partition on a particular broker.
    meck:expect(
        kamock_metadata_response_partition,
        make_metadata_response_partition,
        fun(PartitionIndex, _Env) ->
            kamock_metadata_response_partition:make_metadata_response_partition(
                PartitionIndex,
                101,
                NodeIds
            )
        end
    ),

    kafine_kamock:produce(0, 10),

    % Pause at some point.
    meck:expect(test_consumer_callback, handle_record, fun
        (_T, _P = ?PARTITION_1, _M = #{offset := 2}, St) -> {pause, St};
        (_T, _P, _M, St) -> {ok, St}
    end),

    {ok, Consumer} = kafine_consumer:start_link(
        ?CONSUMER_REF,
        Bootstrap,
        #{},
        {test_consumer_callback, ?CALLBACK_ARGS},
        #{}
    ),

    TopicName = ?TOPIC_NAME,
    Partitions = [?PARTITION_1],
    Subscription = #{
        TopicName => {#{}, #{P => 0 || P <- Partitions}}
    },

    ok = kafine_consumer:subscribe(Consumer, Subscription),

    meck:wait(
        test_consumer_callback, end_record_batch, ['_', ?PARTITION_1, 3, '_', '_'], ?WAIT_TIMEOUT_MS
    ),

    % We're paused, right?
    #{node_consumers := NodeConsumers} = kafine_consumer:info(Consumer),
    ?assertEqual(1, map_size(NodeConsumers)),
    maps:foreach(
        fun(_NodeId, Pid) ->
            ?assertMatch({idle, _}, sys:get_state(Pid))
        end,
        NodeConsumers
    ),

    % Move the partition while paused.
    meck:expect(
        kamock_metadata_response_partition,
        make_metadata_response_partition,
        fun(PartitionIndex, _Env) ->
            kamock_metadata_response_partition:make_metadata_response_partition(
                PartitionIndex,
                102,
                NodeIds
            )
        end
    ),

    % Resume.
    ok = kafine_consumer:resume(Consumer, TopicName, ?PARTITION_1),

    % We should follow the move.
    meck:wait(
        test_consumer_callback,
        end_record_batch,
        ['_', ?PARTITION_1, 10, '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),

    kafine_consumer:stop(Consumer),
    kamock_cluster:stop(Cluster),
    ok.
