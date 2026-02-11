-module(kafine_consumer_resume_tests).
-include_lib("eunit/include/eunit.hrl").

-include("history_matchers.hrl").
-include("assert_received.hrl").

-define(CLUSTER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(PARTITION_1, 1).
-define(PARTITION_2, 2).
-define(CONNECTION_OPTIONS, kafine_connection_options:validate_options(#{})).
-define(CONSUMER_OPTIONS, kafine_consumer_options:validate_options(#{})).
-define(TOPIC_OPTIONS, kafine_topic_options:validate_options([?TOPIC_NAME], #{})).
-define(FETCHER_METADATA, #{}).
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
    {foreach, spawn, fun setup/0, fun cleanup/1, [
        fun start_paused_resume_later/0,
        fun resume_after_move/0,
        fun resume_from_offset/0
    ]}.

start_paused_resume_later() ->
    telemetry_test:attach_event_handlers(self(), [
        [kafine, parallel_handler, pause],
        [kafine, parallel_handler, resume]
    ]),

    {ok, Cluster, [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF),

    Topic = ?TOPIC_NAME,

    % Pretend that there are some messages.
    kafine_kamock:produce(0, 4),

    % Start paused.
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {pause, ?CALLBACK_STATE} end),

    {ok, B} = kafine_bootstrap:start_link(?CONSUMER_REF, Bootstrap, ?CONNECTION_OPTIONS),
    {ok, M} = kafine_metadata_cache:start_link(?CONSUMER_REF),
    {ok, Sup} = kafine_fetcher_sup:start_link(
        ?CONSUMER_REF,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        ?TOPIC_OPTIONS,
        ?FETCHER_METADATA
    ),
    {ok, CB} = kafine_parallel_subscription_impl:start_link(
        ?CONSUMER_REF,
        kafine_parallel_subscription_callback:validate_options(
            #{
                topic_options => ?TOPIC_OPTIONS,
                callback_mod => test_consumer_callback,
                callback_arg => ?CALLBACK_ARGS
            }
        )
    ),

    {ok, S1} = kafine_parallel_subscription_callback:init(?CONSUMER_REF),
    {ok, _} = kafine_parallel_subscription_callback:subscribe_partitions(
        not_used,
        #{Topic => [?PARTITION_1, ?PARTITION_2]},
        S1
    ),

    ?assertReceived({[kafine, parallel_handler, pause], _, _, #{topic := Topic, partition := ?PARTITION_1}}),
    #{children := Children} = kafine_parallel_subscription_impl:info(CB),
    ?assertMatch(2, maps:size(Children)),
    maps:foreach(
        fun(_, Info) ->
            ?assertMatch(#{status := paused}, Info)
        end,
        Children
    ),

    % Resume.
    ok = kafine_consumer:resume(?CONSUMER_REF, Topic, ?PARTITION_1),

    ?assertReceived({[kafine, parallel_handler, resume], _, _, #{topic := Topic, partition := ?PARTITION_1}}),
    meck:wait(2, test_consumer_callback, end_record_batch, '_', ?WAIT_TIMEOUT_MS),

    #{children := Children2} = kafine_parallel_subscription_impl:info(CB),
    ?assertMatch(2, maps:size(Children2)),
    maps:foreach(
        fun
            (_, Info = #{partition := ?PARTITION_1}) -> ?assertMatch(#{status := active}, Info);
            (_, Info = #{partition := ?PARTITION_2}) -> ?assertMatch(#{status := paused}, Info)
        end,
        Children2
    ),

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

    kafine_parallel_subscription_impl:stop(CB),
    kafine_fetcher_sup:stop(Sup),
    kafine_metadata_cache:stop(M),
    kafine_bootstrap:stop(B),
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

    {ok, B} = kafine_bootstrap:start_link(?CONSUMER_REF, Bootstrap, ?CONNECTION_OPTIONS),
    {ok, M} = kafine_metadata_cache:start_link(?CONSUMER_REF),
    {ok, Sup} = kafine_fetcher_sup:start_link(
        ?CONSUMER_REF,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        ?TOPIC_OPTIONS,
        ?FETCHER_METADATA
    ),
    {ok, CB} = kafine_parallel_subscription_impl:start_link(
        ?CONSUMER_REF,
        kafine_parallel_subscription_callback:validate_options(
            #{
                topic_options => ?TOPIC_OPTIONS,
                callback_mod => test_consumer_callback,
                callback_arg => ?CALLBACK_ARGS
            }
        )
    ),

    Topic = ?TOPIC_NAME,
    Partitions = [?PARTITION_1],

    {ok, S1} = kafine_parallel_subscription_callback:init(?CONSUMER_REF),
    {ok, _} = kafine_parallel_subscription_callback:subscribe_partitions(
        not_used,
        #{Topic => Partitions},
        S1
    ),

    meck:wait(
        test_consumer_callback, end_record_batch, ['_', ?PARTITION_1, 3, '_', '_'], ?WAIT_TIMEOUT_MS
    ),

    % We're paused, right?
    #{children := Children} = kafine_parallel_subscription_impl:info(CB),
    ?assertEqual(1, maps:size(Children)),
    maps:foreach(
        fun(_, Info) ->
            ?assertMatch(#{status := paused}, Info)
        end,
        Children
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
    ok = kafine_consumer:resume(?CONSUMER_REF, Topic, ?PARTITION_1),

    % We should follow the move.
    meck:wait(
        test_consumer_callback,
        end_record_batch,
        ['_', ?PARTITION_1, 10, '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),

    kafine_parallel_subscription_impl:stop(CB),
    kafine_fetcher_sup:stop(Sup),
    kafine_metadata_cache:stop(M),
    kafine_bootstrap:stop(B),
    kamock_cluster:stop(Cluster),
    ok.

resume_from_offset() ->
    telemetry_test:attach_event_handlers(self(), [
        [kafine, parallel_handler, pause],
        [kafine, parallel_handler, resume]
    ]),

    {ok, Broker} = kamock_broker:start(?CLUSTER_REF),

    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {pause, ?CALLBACK_STATE} end),

    % Pretend that there are some messages.
    kafine_kamock:produce(0, 4),

    Topic = ?TOPIC_NAME,
    {ok, B} = kafine_bootstrap:start_link(?CONSUMER_REF, Broker, ?CONNECTION_OPTIONS),
    {ok, M} = kafine_metadata_cache:start_link(?CONSUMER_REF),
    {ok, Sup} = kafine_fetcher_sup:start_link(
        ?CONSUMER_REF,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        ?TOPIC_OPTIONS,
        ?FETCHER_METADATA
    ),
    {ok, CB} = kafine_parallel_subscription_impl:start_link(
        ?CONSUMER_REF,
        kafine_parallel_subscription_callback:validate_options(
            #{
                topic_options => ?TOPIC_OPTIONS,
                callback_mod => test_consumer_callback,
                callback_arg => ?CALLBACK_ARGS
            }
        )
    ),

    {ok, S1} = kafine_parallel_subscription_callback:init(?CONSUMER_REF),
    {ok, _} = kafine_parallel_subscription_callback:subscribe_partitions(
        not_used,
        #{Topic => [?PARTITION_1]},
        S1
    ),

    ?assertReceived({[kafine, parallel_handler, pause], _, _, #{topic := Topic, partition := ?PARTITION_1}}),

    ok = kafine_consumer:resume(?CONSUMER_REF, Topic, ?PARTITION_1, 2),

    % We should start seeing messages from partition 1 starting with offset 2
    ?assertReceived({[kafine, parallel_handler, resume], _, _, #{topic := Topic, partition := ?PARTITION_1}}),

    meck:wait(2, test_consumer_callback, end_record_batch, '_', ?WAIT_TIMEOUT_MS),

    ?assertMatch(
        [
            ?init_callback(TopicName, ?PARTITION_1, ?CALLBACK_ARGS),
            ?begin_record_batch(TopicName, ?PARTITION_1, 2, 0, 4, 4),
            ?handle_record(TopicName, ?PARTITION_1, 2, _, _),
            ?end_record_batch(TopicName, ?PARTITION_1, 3, 0, 4, 4),
            ?begin_record_batch(TopicName, ?PARTITION_1, 3, 0, 4, 4),
            ?handle_record(TopicName, ?PARTITION_1, 3, _, _),
            ?end_record_batch(TopicName, ?PARTITION_1, 4, 0, 4, 4)
        ],
        meck:history(test_consumer_callback)
    ),

    kafine_parallel_subscription_impl:stop(CB),
    kafine_fetcher_sup:stop(Sup),
    kafine_metadata_cache:stop(M),
    kafine_bootstrap:stop(B),
    kamock_broker:stop(Broker),
    ok.
