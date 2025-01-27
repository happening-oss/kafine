-module(kafine_eager_rebalance_reconnect_tests).
-include_lib("eunit/include/eunit.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CALLBACK_STATE, ?MODULE).
-define(GROUP_ID, iolist_to_binary(io_lib:format("~s___~s_g", [?MODULE, ?FUNCTION_NAME]))).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(REBALANCE_REF, rebalance).
-define(HEARTBEAT_INTERVAL_MS, 30).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {with, [
            fun initial_connect/1,
            fun should_reconnect_if_connection_drops_as_follower/1,
            fun should_reconnect_if_connection_drops/1,
            fun should_reconnect_if_connection_drops_during_find_coordinator/1,
            fun should_reconnect_if_connection_drops_during_join_group/1,
            fun should_reconnect_if_connection_drops_during_sync_group/1,
            fun should_reconnect_if_connection_drops_during_join_group_as_follower/1,
            fun should_reconnect_if_connection_drops_during_sync_group_as_follower/1
        ]}
    ]}.

setup() ->
    meck:new(test_membership_callback, [non_strict]),
    meck:expect(test_membership_callback, init, fun(_) -> {ok, ?CALLBACK_STATE} end),
    meck:expect(test_membership_callback, subscribe_partitions, fun(_, _, St) -> {ok, St} end),
    meck:expect(test_membership_callback, unsubscribe_partitions, fun(St) -> {ok, St} end),

    meck:new(test_assignment_callback, [non_strict]),
    meck:expect(test_assignment_callback, init, fun(_) -> {ok, ?CALLBACK_STATE} end),
    meck:expect(test_assignment_callback, before_assignment, fun(_, _, St) -> {ok, St} end),
    meck:expect(test_assignment_callback, after_assignment, fun(_, _, St) -> {ok, St} end),

    meck:new(kafine_range_assignor, [passthrough]),

    meck:new(kamock_find_coordinator, [passthrough]),
    meck:new(kamock_join_group, [passthrough]),
    meck:new(kamock_sync_group, [passthrough]),
    meck:new(kamock_heartbeat, [passthrough]),
    meck:new(kamock_offset_commit, [passthrough]),

    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    Broker.

cleanup(Broker) ->
    meck:unload(),
    kamock_broker:stop(Broker),
    ok.

initial_connect(Broker) ->
    setup_as_leader(),
    TelemetryRef = attach_event_handlers(),

    GroupId = ?GROUP_ID,
    Topics = [?TOPIC_NAME],
    {ok, R} = kafine_eager_rebalance:start_link(
        ?REBALANCE_REF,
        Broker,
        #{},
        GroupId,
        #{
            heartbeat_interval_ms => ?HEARTBEAT_INTERVAL_MS,
            subscription_callback => {test_membership_callback, undefined},
            assignment_callback => {test_assignment_callback, undefined}
        },
        Topics
    ),

    receive
        {[kamock, protocol, connected], TelemetryRef, _, _} -> ok
    end,
    receive
        {[kafine, rebalance, leader], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,

    ?assertMatch(#{active_connections := 1}, kamock_broker:info(Broker)),

    kafine_eager_rebalance:stop(R),
    telemetry:detach(TelemetryRef),
    ok.

should_reconnect_if_connection_drops(Broker) ->
    setup_as_leader(),
    TelemetryRef = attach_event_handlers(),

    GroupId = ?GROUP_ID,
    Topics = [?TOPIC_NAME],
    {ok, R} = kafine_eager_rebalance:start_link(
        ?REBALANCE_REF,
        Broker,
        #{},
        GroupId,
        #{
            heartbeat_interval_ms => ?HEARTBEAT_INTERVAL_MS,
            subscription_callback => {test_membership_callback, undefined},
            assignment_callback => {test_assignment_callback, undefined}
        },
        Topics
    ),

    receive
        {[kamock, protocol, connected], TelemetryRef, _, _} -> ok
    end,
    receive
        {[kafine, rebalance, leader], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,
    ?assertMatch(#{active_connections := 1}, kamock_broker:info(Broker)),

    kill_kamock_connections(),

    receive
        {[kafine, rebalance, disconnected], TelemetryRef, _, _} -> ok
    end,
    receive
        {[kamock, protocol, connected], TelemetryRef, _, _} -> ok
    end,

    ?assertMatch(#{active_connections := 1}, kamock_broker:info(Broker)),

    kafine_eager_rebalance:stop(R),
    telemetry:detach(TelemetryRef),
    ok.

should_reconnect_if_connection_drops_as_follower(Broker) ->
    TelemetryRef = attach_event_handlers(),
    setup_as_follower(?TOPIC_NAME, [0, 1, 2, 3]),

    GroupId = ?GROUP_ID,
    Topics = [?TOPIC_NAME],
    {ok, R} = kafine_eager_rebalance:start_link(
        ?REBALANCE_REF,
        Broker,
        #{},
        GroupId,
        #{
            heartbeat_interval_ms => ?HEARTBEAT_INTERVAL_MS,
            subscription_callback => {test_membership_callback, undefined},
            assignment_callback => {test_assignment_callback, undefined}
        },
        Topics
    ),

    receive
        {[kamock, protocol, connected], TelemetryRef, _, _} -> ok
    end,
    receive
        {[kafine, rebalance, follower], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,
    ?assertMatch(#{active_connections := 1}, kamock_broker:info(Broker)),

    kill_kamock_connections(),

    receive
        {[kafine, rebalance, disconnected], TelemetryRef, _, _} -> ok
    end,
    receive
        {[kamock, protocol, connected], TelemetryRef, _, _} -> ok
    end,

    ?assertMatch(#{active_connections := 1}, kamock_broker:info(Broker)),

    kafine_eager_rebalance:stop(R),
    telemetry:detach(TelemetryRef),
    ok.

should_reconnect_if_connection_drops_during_find_coordinator(Broker) ->
    setup_as_leader(),
    meck:expect(
        kamock_find_coordinator,
        handle_find_coordinator_request,
        ['_', '_'],
        meck:raise(exit, killed_as_part_of_test_case)
    ),
    TelemetryRef = attach_event_handlers(),

    GroupId = ?GROUP_ID,
    Topics = [?TOPIC_NAME],
    {ok, R} = kafine_eager_rebalance:start_link(
        ?REBALANCE_REF,
        Broker,
        #{},
        GroupId,
        #{
            heartbeat_interval_ms => ?HEARTBEAT_INTERVAL_MS,
            subscription_callback => {test_membership_callback, undefined},
            assignment_callback => {test_assignment_callback, undefined}
        },
        Topics
    ),

    receive
        {[kamock, protocol, connected], TelemetryRef, _, _} -> ok
    end,
    receive
        {[kafine, rebalance, disconnected], TelemetryRef, _, _} -> ok
    end,
    meck:expect(kamock_find_coordinator, handle_find_coordinator_request, fun(Arg1, Arg2) ->
        meck:passthrough([Arg1, Arg2])
    end),
    receive
        {[kamock, protocol, connected], TelemetryRef, _, _} -> ok
    end,
    receive
        {[kafine, rebalance, leader], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,
    ?assertMatch(#{active_connections := 1}, kamock_broker:info(Broker)),

    kafine_eager_rebalance:stop(R),
    telemetry:detach(TelemetryRef),
    ok.

should_reconnect_if_connection_drops_during_join_group(Broker) ->
    setup_as_leader(),
    meck:expect(
        kamock_join_group,
        handle_join_group_request,
        ['_', '_'],
        meck:raise(exit, killed_as_part_of_test_case)
    ),
    TelemetryRef = attach_event_handlers(),

    GroupId = ?GROUP_ID,
    Topics = [?TOPIC_NAME],
    {ok, R} = kafine_eager_rebalance:start_link(
        ?REBALANCE_REF,
        Broker,
        #{},
        GroupId,
        #{
            heartbeat_interval_ms => ?HEARTBEAT_INTERVAL_MS,
            subscription_callback => {test_membership_callback, undefined},
            assignment_callback => {test_assignment_callback, undefined}
        },
        Topics
    ),

    receive
        {[kamock, protocol, connected], TelemetryRef, _, _} -> ok
    end,
    receive
        {[kafine, rebalance, disconnected], TelemetryRef, _, _} -> ok
    end,
    meck:expect(kamock_join_group, handle_join_group_request, fun(Arg1, Arg2) ->
        meck:passthrough([Arg1, Arg2])
    end),

    receive
        {[kamock, protocol, connected], TelemetryRef, _, _} -> ok
    end,
    receive
        {[kafine, rebalance, leader], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,
    ?assertMatch(#{active_connections := 1}, kamock_broker:info(Broker)),

    kafine_eager_rebalance:stop(R),
    telemetry:detach(TelemetryRef),
    ok.

should_reconnect_if_connection_drops_during_join_group_as_follower(Broker) ->
    setup_as_follower(?TOPIC_NAME, [0, 1, 2, 3]),

    meck:expect(
        kamock_join_group,
        handle_join_group_request,
        ['_', '_'],
        meck:raise(exit, killed_as_part_of_test_case)
    ),
    TelemetryRef = attach_event_handlers(),

    GroupId = ?GROUP_ID,
    Topics = [?TOPIC_NAME],
    {ok, R} = kafine_eager_rebalance:start_link(
        ?REBALANCE_REF,
        Broker,
        #{},
        GroupId,
        #{
            heartbeat_interval_ms => ?HEARTBEAT_INTERVAL_MS,
            subscription_callback => {test_membership_callback, undefined},
            assignment_callback => {test_assignment_callback, undefined}
        },
        Topics
    ),

    receive
        {[kamock, protocol, connected], TelemetryRef, _, _} -> ok
    end,
    receive
        {[kafine, rebalance, disconnected], TelemetryRef, _, _} -> ok
    end,
    meck:expect(
        kamock_join_group,
        handle_join_group_request,
        kamock_join_group:as_follower()
    ),
    receive
        {[kamock, protocol, connected], TelemetryRef, _, _} -> ok
    end,
    receive
        {[kafine, rebalance, follower], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,
    ?assertMatch(#{active_connections := 1}, kamock_broker:info(Broker)),

    kafine_eager_rebalance:stop(R),
    telemetry:detach(TelemetryRef),
    ok.

should_reconnect_if_connection_drops_during_sync_group(Broker) ->
    setup_as_leader(),
    meck:expect(
        kamock_sync_group,
        handle_sync_group_request,
        ['_', '_'],
        meck:raise(exit, killed_as_part_of_test_case)
    ),
    TelemetryRef = attach_event_handlers(),

    GroupId = ?GROUP_ID,
    Topics = [?TOPIC_NAME],
    {ok, R} = kafine_eager_rebalance:start_link(
        ?REBALANCE_REF,
        Broker,
        #{},
        GroupId,
        #{
            heartbeat_interval_ms => ?HEARTBEAT_INTERVAL_MS,
            subscription_callback => {test_membership_callback, undefined},
            assignment_callback => {test_assignment_callback, undefined}
        },
        Topics
    ),

    receive
        {[kamock, protocol, connected], TelemetryRef, _, _} -> ok
    end,
    receive
        {[kafine, rebalance, disconnected], TelemetryRef, _, _} -> ok
    end,
    meck:expect(kamock_sync_group, handle_sync_group_request, fun(Arg1, Arg2) ->
        meck:passthrough([Arg1, Arg2])
    end),
    receive
        {[kamock, protocol, connected], TelemetryRef, _, _} -> ok
    end,
    receive
        {[kafine, rebalance, leader], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,
    ?assertMatch(#{active_connections := 1}, kamock_broker:info(Broker)),

    kafine_eager_rebalance:stop(R),
    telemetry:detach(TelemetryRef),
    ok.

should_reconnect_if_connection_drops_during_sync_group_as_follower(Broker) ->
    Topic = ?TOPIC_NAME,
    Partitions = [0, 1, 2, 3],
    setup_as_follower(Topic, Partitions),
    meck:expect(
        kamock_sync_group,
        handle_sync_group_request,
        ['_', '_'],
        meck:raise(exit, killed_as_part_of_test_case)
    ),
    TelemetryRef = attach_event_handlers(),

    GroupId = ?GROUP_ID,
    Topics = [?TOPIC_NAME],
    {ok, R} = kafine_eager_rebalance:start_link(
        ?REBALANCE_REF,
        Broker,
        #{},
        GroupId,
        #{
            heartbeat_interval_ms => ?HEARTBEAT_INTERVAL_MS,
            subscription_callback => {test_membership_callback, undefined},
            assignment_callback => {test_assignment_callback, undefined}
        },
        Topics
    ),

    receive
        {[kamock, protocol, connected], TelemetryRef, _, _} -> ok
    end,
    receive
        {[kafine, rebalance, disconnected], TelemetryRef, _, _} -> ok
    end,
    meck:expect(
        kamock_sync_group,
        handle_sync_group_request,
        kamock_sync_group:assign([#{topic => Topic, partitions => Partitions}])
    ),
    receive
        {[kamock, protocol, connected], TelemetryRef, _, _} -> ok
    end,
    receive
        {[kafine, rebalance, follower], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,
    ?assertMatch(#{active_connections := 1}, kamock_broker:info(Broker)),

    kafine_eager_rebalance:stop(R),
    telemetry:detach(TelemetryRef),
    ok.

attach_event_handlers() ->
    telemetry_test:attach_event_handlers(self(), [
        [kafine, rebalance, leader],
        [kafine, rebalance, follower],
        [kamock, protocol, connected],
        [kafine, rebalance, disconnected]
    ]).

kill_kamock_connections() ->
    Connections = [
        Pid
     || {_Ref, _N, Sup} <- ranch_server:get_connections_sups(),
        {kamock_broker_protocol, Pid, _, _} <- supervisor:which_children(Sup)
    ],
    lists:foreach(
        fun(Pid) ->
            exit(Pid, kill)
        end,
        Connections
    ).

setup_as_leader() ->
    meck:expect(
        kamock_join_group,
        handle_join_group_request,
        fun(Arg1, Arg2) -> meck:passthrough([Arg1, Arg2]) end
    ),
    meck:expect(
        kamock_sync_group,
        handle_sync_group_request,
        fun(Arg1, Arg2) -> meck:passthrough([Arg1, Arg2]) end
    ).

setup_as_follower(Topic, Partitions) ->
    % If we're a follower, we'll get a JoinGroup response with leader /= member_id.
    meck:expect(
        kamock_join_group,
        handle_join_group_request,
        kamock_join_group:as_follower()
    ),
    meck:expect(
        kamock_sync_group,
        handle_sync_group_request,
        kamock_sync_group:assign([#{topic => Topic, partitions => Partitions}])
    ).
