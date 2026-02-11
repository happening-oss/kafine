-module(kafine_coordinator_move_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/error_code.hrl").
%-include_lib("kernel/include/logger.hrl").

-include("assert_meck.hrl").
-include("assert_received.hrl").

-define(REF, {?MODULE, ?FUNCTION_NAME}).
-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CLUSTER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(GROUP_ID, <<"test_group">>).
-define(CONNECTION_OPTIONS, #{
    client_id => <<"test_client">>,
    backoff => kafine_backoff:fixed(50)
}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(TOPIC_NAME_2, iolist_to_binary(io_lib:format("~s___~s_t_2", [?MODULE, ?FUNCTION_NAME]))).
-define(TOPICS, [?TOPIC_NAME, ?TOPIC_NAME_2]).
-define(MEMBERSHIP_OPTIONS,
    kafine_membership_options:validate_options(#{
        assignment_callback => {kafine_noop_assignment_callback, undefined},
        subscription_callback => {kafine_parallel_subscription_callback, #{}}
    })
).
-define(WAIT_TIMEOUT_MS, 2_000).

setup() ->
    meck:new(kafine_connection, [passthrough]),
    meck:new(kamock_find_coordinator, [passthrough]),
    meck:new(kamock_join_group, [passthrough]),
    meck:new(kamock_sync_group, [passthrough]),
    meck:new(kamock_heartbeat, [passthrough]),
    meck:new(kamock_leave_group, [passthrough]),
    meck:new(kamock_offset_fetch, [passthrough]).

cleanup(_) ->
    meck:unload().

kafine_node_metadata_test_() ->
    {foreach, spawn, fun setup/0, fun cleanup/1, [
        fun coordinator_move/0,
        fun coordinator_move_during_get_member_id/0,
        fun coordinator_move_during_join_group/0,
        fun coordinator_move_during_sync_group/0,
        fun coordinator_move_during_leave_group/0,
        fun coordinator_move_during_offset_fetch/0,
        fun coordinator_move_during_reconnect_backoff/0
    ]}.

coordinator_move() ->
    {ok, _, [#{node_id := NodeId1} = Broker1, #{node_id := NodeId2} = Broker2 | _]} =
        kamock_cluster:start(?CLUSTER_REF),

    set_coordinator(Broker1),

    {ok, _} = kafine_bootstrap:start_link(?REF, Broker1, ?CONNECTION_OPTIONS),

    {ok, Pid} = kafine_coordinator:start_link(
        ?REF, ?GROUP_ID, ?TOPICS, ?CONNECTION_OPTIONS, ?MEMBERSHIP_OPTIONS
    ),

    ReqId = kafine_coordinator:join_group(Pid, <<>>),
    {error, {member_id_required, MemberId}} = receive_response(ReqId),

    ReqId2 = kafine_coordinator:join_group(Pid, MemberId),
    {ok, #{generation_id := GenerationId, protocol_name := ProtocolName}} = receive_response(
        ReqId2
    ),

    Assignment = #{
        MemberId => #{
            assigned_partitions => #{?TOPIC_NAME => [0, 1, 2, 3], ?TOPIC_NAME_2 => [0, 1, 2, 3]},
            user_data => <<>>
        }
    },

    ReqId3 = kafine_coordinator:sync_group(Pid, MemberId, GenerationId, ProtocolName, Assignment),
    {ok, _} = receive_response(ReqId3),

    meck:reset(kamock_find_coordinator),
    meck:reset(kafine_connection),

    % Switch coordinator node
    set_coordinator(Broker2),

    % Heartbeat to old node
    ReqId4 = kafine_coordinator:heartbeat(Pid, MemberId, GenerationId),

    % kafine_coordinator should detect coordinator moved, find new coordinator and retry
    ?assertEqual(ok, receive_response(ReqId4)),
    ?assertCalled(kamock_find_coordinator, handle_find_coordinator_request, '_'),
    ?assertCalled(kafine_connection, start_link, [
        maps:with([node_id, host, port], Broker2), ?CONNECTION_OPTIONS
    ]),
    ?assertCalled(kamock_heartbeat, handle_heartbeat_request, ['_', #{node_id => NodeId1}]),
    ?assertCalled(kamock_heartbeat, handle_heartbeat_request, ['_', #{node_id => NodeId2}]).

coordinator_move_during_get_member_id() ->
    {ok, _, [#{node_id := NodeId1} = Broker1, #{node_id := NodeId2} = Broker2 | _]} =
        kamock_cluster:start(?CLUSTER_REF),

    set_coordinator(Broker1),

    {ok, _} = kafine_bootstrap:start_link(?REF, Broker1, ?CONNECTION_OPTIONS),

    {ok, Pid} = kafine_coordinator:start_link(
        ?REF, ?GROUP_ID, ?TOPICS, ?CONNECTION_OPTIONS, ?MEMBERSHIP_OPTIONS
    ),

    meck:wait(2, kafine_connection, start_link, [#{node_id => NodeId1}, '_'], ?WAIT_TIMEOUT_MS),

    meck:reset(kamock_find_coordinator),
    meck:reset(kafine_connection),

    % Switch coordinator node
    set_coordinator(Broker2),

    % getting member id (join group with empty member id) should detect coordinator moved, find
    % new coordinator and retry
    ReqId = kafine_coordinator:join_group(Pid, <<>>),
    ?assertMatch({error, {member_id_required, _}}, receive_response(ReqId)),
    ?assertCalled(kamock_find_coordinator, handle_find_coordinator_request, '_'),
    ?assertCalled(kafine_connection, start_link, [
        maps:with([node_id, host, port], Broker2), ?CONNECTION_OPTIONS
    ]),
    ?assertCalled(kamock_join_group, handle_join_group_request, [
        #{member_id => <<>>}, #{node_id => NodeId1}
    ]),
    ?assertCalled(kamock_join_group, handle_join_group_request, [
        #{member_id => <<>>}, #{node_id => NodeId2}
    ]).

coordinator_move_during_join_group() ->
    {ok, _, [#{node_id := NodeId1} = Broker1, #{node_id := NodeId2} = Broker2 | _]} =
        kamock_cluster:start(?CLUSTER_REF),

    set_coordinator(Broker1),

    {ok, _} = kafine_bootstrap:start_link(?REF, Broker1, ?CONNECTION_OPTIONS),

    {ok, Pid} = kafine_coordinator:start_link(
        ?REF, ?GROUP_ID, ?TOPICS, ?CONNECTION_OPTIONS, ?MEMBERSHIP_OPTIONS
    ),

    ReqId = kafine_coordinator:join_group(Pid, <<>>),
    {error, {member_id_required, MemberId}} = receive_response(ReqId),

    meck:reset(kamock_find_coordinator),
    meck:reset(kafine_connection),
    meck:reset(kamock_join_group),

    % Switch coordinator node
    set_coordinator(Broker2),

    ReqId2 = kafine_coordinator:join_group(Pid, MemberId),
    ?assertMatch({ok, _}, receive_response(ReqId2)),
    ?assertCalled(kamock_find_coordinator, handle_find_coordinator_request, '_'),
    ?assertCalled(kafine_connection, start_link, [
        maps:with([node_id, host, port], Broker2), ?CONNECTION_OPTIONS
    ]),
    ?assertCalled(kamock_join_group, handle_join_group_request, [
        #{member_id => MemberId}, #{node_id => NodeId1}
    ]),
    ?assertCalled(kamock_join_group, handle_join_group_request, [
        #{member_id => MemberId}, #{node_id => NodeId2}
    ]).

coordinator_move_during_sync_group() ->
    {ok, _, [#{node_id := NodeId1} = Broker1, #{node_id := NodeId2} = Broker2 | _]} =
        kamock_cluster:start(?CLUSTER_REF),

    set_coordinator(Broker1),

    {ok, _} = kafine_bootstrap:start_link(?REF, Broker1, ?CONNECTION_OPTIONS),

    {ok, Pid} = kafine_coordinator:start_link(
        ?REF, ?GROUP_ID, ?TOPICS, ?CONNECTION_OPTIONS, ?MEMBERSHIP_OPTIONS
    ),

    ReqId = kafine_coordinator:join_group(Pid, <<>>),
    {error, {member_id_required, MemberId}} = receive_response(ReqId),

    ReqId2 = kafine_coordinator:join_group(Pid, MemberId),
    {ok, #{generation_id := GenerationId, protocol_name := ProtocolName}} = receive_response(
        ReqId2
    ),

    meck:reset(kamock_find_coordinator),
    meck:reset(kafine_connection),

    % Switch coordinator node
    set_coordinator(Broker2),

    Assignment = #{
        MemberId => #{
            assigned_partitions => #{?TOPIC_NAME => [0, 1, 2, 3], ?TOPIC_NAME_2 => [0, 1, 2, 3]},
            user_data => <<>>
        }
    },

    ReqId3 = kafine_coordinator:sync_group(Pid, MemberId, GenerationId, ProtocolName, Assignment),
    ?assertMatch({ok, _}, receive_response(ReqId3)),
    ?assertCalled(kamock_find_coordinator, handle_find_coordinator_request, '_'),
    ?assertCalled(kafine_connection, start_link, [
        maps:with([node_id, host, port], Broker2), ?CONNECTION_OPTIONS
    ]),
    ?assertCalled(kamock_sync_group, handle_sync_group_request, [
        #{member_id => MemberId, generation_id => GenerationId}, #{node_id => NodeId1}
    ]),
    ?assertCalled(kamock_sync_group, handle_sync_group_request, [
        #{member_id => MemberId, generation_id => GenerationId}, #{node_id => NodeId2}
    ]).

coordinator_move_during_leave_group() ->
    {ok, _, [#{node_id := NodeId1} = Broker1, #{node_id := NodeId2} = Broker2 | _]} =
        kamock_cluster:start(?CLUSTER_REF),

    set_coordinator(Broker1),

    {ok, _} = kafine_bootstrap:start_link(?REF, Broker1, ?CONNECTION_OPTIONS),

    {ok, Pid} = kafine_coordinator:start_link(
        ?REF, ?GROUP_ID, ?TOPICS, ?CONNECTION_OPTIONS, ?MEMBERSHIP_OPTIONS
    ),

    ReqId = kafine_coordinator:join_group(Pid, <<>>),
    {error, {member_id_required, MemberId}} = receive_response(ReqId),

    ReqId2 = kafine_coordinator:join_group(Pid, MemberId),
    {ok, #{generation_id := GenerationId, protocol_name := ProtocolName}} = receive_response(
        ReqId2
    ),

    Assignment = #{
        MemberId => #{
            assigned_partitions => #{?TOPIC_NAME => [0, 1, 2, 3], ?TOPIC_NAME_2 => [0, 1, 2, 3]},
            user_data => <<>>
        }
    },

    ReqId3 = kafine_coordinator:sync_group(Pid, MemberId, GenerationId, ProtocolName, Assignment),
    {ok, _} = receive_response(ReqId3),

    meck:reset(kamock_find_coordinator),
    meck:reset(kafine_connection),

    % Switch coordinator node
    set_coordinator(Broker2),

    % Heartbeat to old node
    ReqId4 = kafine_coordinator:leave_group(Pid, MemberId),

    % kafine_coordinator should detect coordinator moved, find new coordinator and retry
    ?assertEqual(ok, receive_response(ReqId4)),
    ?assertCalled(kamock_find_coordinator, handle_find_coordinator_request, '_'),
    ?assertCalled(kafine_connection, start_link, [
        maps:with([node_id, host, port], Broker2), ?CONNECTION_OPTIONS
    ]),
    ?assertCalled(kamock_leave_group, handle_leave_group_request, ['_', #{node_id => NodeId1}]),
    ?assertCalled(kamock_leave_group, handle_leave_group_request, ['_', #{node_id => NodeId2}]).

coordinator_move_during_offset_fetch() ->
    {ok, _, [#{node_id := NodeId1} = Broker1, #{node_id := NodeId2} = Broker2 | _]} =
        kamock_cluster:start(?CLUSTER_REF),

    set_coordinator(Broker1),

    {ok, _} = kafine_bootstrap:start_link(?REF, Broker1, ?CONNECTION_OPTIONS),

    {ok, Pid} = kafine_coordinator:start_link(
        ?REF, ?GROUP_ID, ?TOPICS, ?CONNECTION_OPTIONS, ?MEMBERSHIP_OPTIONS
    ),

    ReqId = kafine_coordinator:join_group(Pid, <<>>),
    {error, {member_id_required, MemberId}} = receive_response(ReqId),

    ReqId2 = kafine_coordinator:join_group(Pid, MemberId),
    {ok, #{generation_id := GenerationId, protocol_name := ProtocolName}} = receive_response(
        ReqId2
    ),

    Assignment = #{
        MemberId => #{
            assigned_partitions => #{?TOPIC_NAME => [0, 1, 2, 3], ?TOPIC_NAME_2 => [0, 1, 2, 3]},
            user_data => <<>>
        }
    },

    ReqId3 = kafine_coordinator:sync_group(Pid, MemberId, GenerationId, ProtocolName, Assignment),
    {ok, _} = receive_response(ReqId3),

    meck:reset(kamock_find_coordinator),
    meck:reset(kafine_connection),

    % Switch coordinator node
    set_coordinator(Broker2),

    % Heartbeat to old node
    ReqId4 = kafine_coordinator:offset_fetch(
        Pid, #{?TOPIC_NAME => [0, 1]}, kafine_coordinator:reqids_new()
    ),

    % kafine_coordinator should detect coordinator moved, find new coordinator and retry
    ?assertEqual({ok, #{?TOPIC_NAME => #{0 => -1, 1 => -1}}}, receive_response(ReqId4)),
    ?assertCalled(kamock_find_coordinator, handle_find_coordinator_request, '_'),
    ?assertCalled(kafine_connection, start_link, [
        maps:with([node_id, host, port], Broker2), ?CONNECTION_OPTIONS
    ]),
    ?assertCalled(kamock_offset_fetch, handle_offset_fetch_request, ['_', #{node_id => NodeId1}]),
    ?assertCalled(kamock_offset_fetch, handle_offset_fetch_request, ['_', #{node_id => NodeId2}]).

coordinator_move_during_reconnect_backoff() ->
    telemetry_test:attach_event_handlers(self(), [
        [kafine, coordinator, connected],
        [kafine, coordinator, backoff]
    ]),

    {ok, Broker = #{node_id := NodeId}} = kamock_broker:start(?BROKER_REF),

    % Make the coordinator be a host that doesn't exist
    set_coordinator(#{node_id => NodeId + 1, host => <<"nowhere">>, port => 0}),

    {ok, _} = kafine_bootstrap:start_link(?REF, Broker, ?CONNECTION_OPTIONS),

    {ok, _} = kafine_coordinator:start_link(
        ?REF, ?GROUP_ID, ?TOPICS, ?CONNECTION_OPTIONS, ?MEMBERSHIP_OPTIONS
    ),

    ?assertReceived({[kafine, coordinator, backoff], _, _, _}),

    meck:reset(kamock_find_coordinator),
    meck:reset(kafine_connection),

    % Switch coordinator node
    set_coordinator(Broker),

    % kafine_coordinator should figure ot the coordinator has changed and connect to the new one
    ?assertReceived({[kafine, coordinator, connected], _, _, _}),
    ?assertCalled(kamock_find_coordinator, handle_find_coordinator_request, '_'),
    ?assertCalled(kafine_connection, start_link, [#{node_id => NodeId}, '_']).

set_coordinator(Broker = #{node_id := NodeId}) ->
    meck:expect(
        kamock_find_coordinator,
        handle_find_coordinator_request,
        kamock_find_coordinator:return(Broker)
    ),
    meck:expect(
        kamock_join_group,
        handle_join_group_request,
        [
            {['_', #{node_id => NodeId}], meck:passthrough()},
            {['_', '_'], kamock_join_group:return_error(?NOT_COORDINATOR)}
        ]
    ),
    meck:expect(
        kamock_sync_group,
        handle_sync_group_request,
        [
            {['_', #{node_id => NodeId}], meck:passthrough()},
            {['_', '_'], kamock_sync_group:return_error(?NOT_COORDINATOR)}
        ]
    ),
    meck:expect(
        kamock_heartbeat,
        handle_heartbeat_request,
        [
            {['_', #{node_id => NodeId}], meck:passthrough()},
            {['_', '_'], kamock_heartbeat:return_error(?NOT_COORDINATOR)}
        ]
    ),
    meck:expect(
        kamock_offset_fetch,
        handle_offset_fetch_request,
        [
            {['_', #{node_id => NodeId}], meck:passthrough()},
            {['_', '_'], kamock_offset_fetch:return_error(?NOT_COORDINATOR)}
        ]
    ),
    meck:expect(
        kamock_leave_group,
        handle_leave_group_request,
        [
            {['_', #{node_id => NodeId}], meck:passthrough()},
            {['_', '_'], kamock_leave_group:return_error(?NOT_COORDINATOR, [])}
        ]
    ).

receive_response(ReqIds) ->
    {Response, _} = do_receive_response(ReqIds),
    Response.

do_receive_response(ReqIds) ->
    receive
        Msg ->
            case kafine_coordinator:check_response(Msg, ReqIds) of
                {{reply, Response}, _, NewReqIds} ->
                    {Response, NewReqIds};
                {{error, {Reason, _}}, _, NewReqIds} ->
                    {{error, Reason}, NewReqIds};
                _ ->
                    receive_response(ReqIds)
            end
    after ?WAIT_TIMEOUT_MS ->
        error(timeout)
    end.
