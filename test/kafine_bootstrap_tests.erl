-module(kafine_bootstrap_tests).

-include_lib("eunit/include/eunit.hrl").

-include("assert_meck.hrl").
-include("assert_received.hrl").

-define(REF, {?MODULE, ?FUNCTION_NAME}).
-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CLUSTER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONNECTION_OPTIONS, #{
    client_id => <<"test_client">>,
    backoff => kafine_backoff:fixed(50)
}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(TOPIC_NAME_2, iolist_to_binary(io_lib:format("~s___~s_t_2", [?MODULE, ?FUNCTION_NAME]))).
-define(WAIT_TIMEOUT_MS, 2_000).

setup() ->
    meck:new(kamock_metadata, [passthrough]).

cleanup(_) ->
    meck:unload().

kafine_node_metadata_test_() ->
    {foreach, spawn, fun setup/0, fun cleanup/1, [
        fun get_metadata/0,
        fun find_coordinator_returns_current_coordinator/0,
        fun reconnects_on_disconnect/0,
        fun applies_backoff_on_failed_reconnect/0,
        fun exits_if_backoff_exceeds_limit/0,
        fun get_metadata_during_backoff_returns_after_reconnect/0,
        fun find_coordinator_during_backoff_returns_after_reconnect/0
    ]}.

get_metadata() ->
    {ok, _, Brokers = [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF, [101, 102]),

    meck:expect(
        kamock_metadata_response_topic,
        make_metadata_response_topic,
        kamock_metadata_response_topic:partitions(2)
    ),

    {ok, Pid} = kafine_bootstrap:start_link(?REF, Bootstrap, ?CONNECTION_OPTIONS),

    Req = kafine_bootstrap:get_metadata(Pid, [?TOPIC_NAME, ?TOPIC_NAME_2]),
    {reply, Result} = gen_statem:receive_response(Req),

    ExpectedBrokers = lists:map(
        fun(Broker) -> maps:with([host, port, node_id], Broker) end, Brokers
    ),
    ExpectedTopicPartitionData = #{
        ?TOPIC_NAME => #{
            0 => #{leader => 101, replicas => [101, 102], isr => [101, 102]},
            1 => #{leader => 102, replicas => [102, 101], isr => [102, 101]}
        },
        ?TOPIC_NAME_2 => #{
            0 => #{leader => 101, replicas => [101, 102], isr => [101, 102]},
            1 => #{leader => 102, replicas => [102, 101], isr => [102, 101]}
        }
    },
    ExpectedResult = {ExpectedBrokers, ExpectedTopicPartitionData},

    ?assertEqual(ExpectedResult, Result).

find_coordinator_returns_current_coordinator() ->
    {ok, _, [Bootstrap, _, Coordinator, _]} = kamock_cluster:start(?CLUSTER_REF, [
        101, 102, 103, 104
    ]),

    {ok, _} = kafine_bootstrap:start_link(?REF, Bootstrap, ?CONNECTION_OPTIONS),

    meck:expect(
        kamock_find_coordinator,
        handle_find_coordinator_request,
        kamock_find_coordinator:return(Coordinator)
    ),

    Result = kafine_bootstrap:find_coordinator(?REF, <<"some_group">>),

    ?assertEqual({ok, maps:with([host, port, node_id], Coordinator)}, Result).

reconnects_on_disconnect() ->
    {ok, _, [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF, [101, 102]),

    telemetry_test:attach_event_handlers(self(), [
        [kafine, bootstrap, connected],
        [kafine, bootstrap, disconnected],
        [kamock, protocol, connected]
    ]),

    {ok, Pid} = kafine_bootstrap:start_link(?REF, Bootstrap, ?CONNECTION_OPTIONS),

    ?assertReceived({[kamock, protocol, connected], _, _, _}),
    ?assertReceived({[kafine, bootstrap, connected], _, _, _}),

    % Next request should cause the connection to drop
    meck:expect(
        kamock_metadata_response_topic,
        make_metadata_response_topic,
        ['_', '_'],
        meck:seq([
            meck:val(stop),
            kamock_metadata_response_topic:partitions(2)
        ])
    ),

    % Make a request. Connection should drop, be re-established, then the request should be retried
    ReqId = kafine_bootstrap:get_metadata(Pid, [?TOPIC_NAME]),
    {reply, {Brokers, TopicPartitionNodes}} = gen_statem:receive_response(ReqId),
    ?assertMatch([#{node_id := 101}, #{node_id := 102}], Brokers),
    ?assertEqual(
        #{
            ?TOPIC_NAME => #{
                0 => #{leader => 101, replicas => [101, 102], isr => [101, 102]},
                1 => #{leader => 102, replicas => [102, 101], isr => [102, 101]}
            }
        },
        TopicPartitionNodes
    ),

    % Make sure the disconnect+reconnect occurred
    ?assertReceived({[kafine, bootstrap, disconnected], _, _, _}),
    ?assertReceived({[kamock, protocol, connected], _, _, _}),
    ?assertReceived({[kafine, bootstrap, connected], _, _, _}).

applies_backoff_on_failed_reconnect() ->
    {ok, _, [Bootstrap = #{node_id := NodeId, port := Port} | _]} = kamock_cluster:start(
        ?CLUSTER_REF, [101, 102]
    ),

    telemetry_test:attach_event_handlers(self(), [
        [kafine, bootstrap, connected],
        [kafine, bootstrap, disconnected],
        [kafine, bootstrap, backoff],
        [kamock, protocol, connected]
    ]),

    meck:expect(kafine_backoff, init, fun(_) -> {backoff_state, 1} end),

    {ok, _} = kafine_bootstrap:start_link(?REF, Bootstrap, ?CONNECTION_OPTIONS),

    ?assertReceived({[kamock, protocol, connected], _, _, _}),
    ?assertReceived({[kafine, bootstrap, connected], _, _, _}),

    meck:reset(kafine_backoff),

    meck:expect(kafine_backoff, backoff, [
        {[{backoff_state, 1}], {50, {backoff_state, 2}}},
        {[{backoff_state, '_'}], {50, {backoff_state, 3}}}
    ]),

    % Stop the broker, dropping connections and failing future attempts
    kamock_broker:stop(Bootstrap),

    ?assertReceived({[kafine, bootstrap, disconnected], _, _, _}),

    % should repeatedly back off
    ?assertReceived({[kafine, bootstrap, backoff], _, _, _}),
    ?assertCalled(kafine_backoff, backoff, [{backoff_state, 1}]),

    ?assertReceived({[kafine, bootstrap, backoff], _, _, _}),
    ?assertCalled(kafine_backoff, backoff, [{backoff_state, 2}]),

    % Bring broker back up
    {ok, Bootstrap2 = #{node_id := NodeId}} = kamock_broker:start(?CLUSTER_REF, #{port => Port}),

    % should connect and reset backoff state
    ?assertReceived({[kamock, protocol, connected], _, _, _}),
    ?assertReceived({[kafine, bootstrap, connected], _, _, _}),
    ?assertWait(kafine_backoff, init, '_', ?WAIT_TIMEOUT_MS),

    ?assertMatch(#{active_connections := 1}, kamock_broker:info(Bootstrap2)).

exits_if_backoff_exceeds_limit() ->
    {ok, _, [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF, [101, 102]),

    telemetry_test:attach_event_handlers(self(), [
        [kafine, bootstrap, connected],
        [kafine, bootstrap, disconnected],
        [kafine, bootstrap, backoff],
        [kamock, protocol, connected]
    ]),

    meck:expect(kafine_backoff, init, fun(_) -> {backoff_state, 1} end),

    process_flag(trap_exit, true),
    {ok, Pid} = kafine_bootstrap:start_link(?REF, Bootstrap, ?CONNECTION_OPTIONS),

    ?assertReceived({[kamock, protocol, connected], _, _, _}),
    ?assertReceived({[kafine, bootstrap, connected], _, _, _}),

    % backoff once, then always return limit_exceeded
    meck:expect(kafine_backoff, backoff, [
        {[{backoff_state, 1}], {50, {backoff_state, 2}}},
        {[{backoff_state, '_'}], limit_exceeded}
    ]),

    % Stop the broker, dropping connections and failing future attempts
    kamock_broker:stop(Bootstrap),

    ?assertReceived({[kafine, bootstrap, disconnected], _, _, _}),

    % should back off, then exit after limit exceeded
    ?assertReceived({[kafine, bootstrap, backoff], _, _, _}),
    ?assertCalled(kafine_backoff, backoff, [{backoff_state, 1}]),

    ?assertReceived({'EXIT', Pid, backoff_limit_exceeded}),
    ?assertCalled(kafine_backoff, backoff, [{backoff_state, 2}]).

get_metadata_during_backoff_returns_after_reconnect() ->
    {ok, _, [Bootstrap = #{node_id := NodeId, port := Port} | _]} = kamock_cluster:start(
        ?CLUSTER_REF, [101, 102]
    ),

    meck:expect(
        kamock_metadata_response_topic,
        make_metadata_response_topic,
        kamock_metadata_response_topic:partitions(2)
    ),

    telemetry_test:attach_event_handlers(self(), [
        [kafine, bootstrap, connected],
        [kafine, bootstrap, disconnected],
        [kafine, bootstrap, backoff],
        [kamock, protocol, connected]
    ]),

    {ok, Pid} = kafine_bootstrap:start_link(?REF, Bootstrap, ?CONNECTION_OPTIONS),

    ?assertReceived({[kamock, protocol, connected], _, _, _}),
    ?assertReceived({[kafine, bootstrap, connected], _, _, _}),

    % Stop the broker, dropping connections and failing future attempts
    kamock_broker:stop(Bootstrap),

    ?assertReceived({[kafine, bootstrap, disconnected], _, _, _}),

    % wait for back off
    ?assertReceived({[kafine, bootstrap, backoff], _, _, _}),

    % request metadata
    Req = kafine_bootstrap:get_metadata(Pid, [?TOPIC_NAME, ?TOPIC_NAME_2]),

    % bring the broker back up
    kamock_broker:start(?BROKER_REF, #{node_id => NodeId, port => Port}),

    % Should receive the response
    {reply, Result} = gen_statem:receive_response(Req),

    ?assertMatch({[_ | _], #{}}, Result).

find_coordinator_during_backoff_returns_after_reconnect() ->
    {ok, _, [Bootstrap = #{node_id := NodeId, port := Port}, Coordinator | _]} = kamock_cluster:start(
        ?CLUSTER_REF, [101, 102]
    ),

    telemetry_test:attach_event_handlers(self(), [
        [kafine, bootstrap, connected],
        [kafine, bootstrap, disconnected],
        [kafine, bootstrap, backoff],
        [kamock, protocol, connected]
    ]),

    meck:expect(
        kamock_find_coordinator,
        handle_find_coordinator_request,
        kamock_find_coordinator:return(Coordinator)
    ),

    {ok, _} = kafine_bootstrap:start_link(?REF, Bootstrap, ?CONNECTION_OPTIONS),

    ?assertReceived({[kamock, protocol, connected], _, _, _}),
    ?assertReceived({[kafine, bootstrap, connected], _, _, _}),

    % Stop the broker, dropping connections and failing future attempts
    kamock_broker:stop(Bootstrap),

    ?assertReceived({[kafine, bootstrap, disconnected], _, _, _}),

    % wait for back off
    ?assertReceived({[kafine, bootstrap, backoff], _, _, _}),

    TestPid = self(),
    proc_lib:spawn_link(fun() ->
        TestPid ! finding_coordinator,
        Result = kafine_bootstrap:find_coordinator(?REF, <<"some_group">>),
        ?assertEqual({ok, maps:with([host, port, node_id], Coordinator)}, Result),
        TestPid ! found_coordinator
    end),

    ?assertReceived(finding_coordinator),

    % bring the broker back up
    kamock_broker:start(?BROKER_REF, #{node_id => NodeId, port => Port}),

    ?assertReceived(found_coordinator).
