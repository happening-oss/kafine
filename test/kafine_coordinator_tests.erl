-module(kafine_coordinator_tests).

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
    meck:new(kamock_metadata, [passthrough]),
    meck:new(kafine_backoff, [passthrough]).

cleanup(_) ->
    meck:unload().

kafine_node_metadata_test_() ->
    {foreach, spawn, fun setup/0, fun cleanup/1, [
        fun connects_to_coordinator_broker/0,
        fun handles_offset_fetch_request/0,
        fun handles_parallel_offset_fetch_requests/0,
        fun join_group_without_member_id/0,
        fun join_group_after_getting_member_id/0,
        fun sync_group_with_single_member/0,
        fun heartbeat_success/0,
        fun heartbeat_during_rebalance/0,
        fun leave_group/0,
        fun offset_commit/0,
        fun offset_commit_with_errors/0,
        fun reconnects_on_disconnect/0,
        fun applies_backoff_on_failed_reconnect/0
    ]}.

connects_to_coordinator_broker() ->
    {ok, _, [Bootstrap, _, Coordinator, _]} = kamock_cluster:start(?CLUSTER_REF, [
        101, 102, 103, 104
    ]),
    {ok, _} = kafine_bootstrap:start_link(?REF, Bootstrap, ?CONNECTION_OPTIONS),
    meck:expect(
        kamock_find_coordinator,
        handle_find_coordinator_request,
        fun(_Req = #{correlation_id := CorrelationId}, _Env) ->
            kamock_find_coordinator:make_find_coordinator_response(CorrelationId, Coordinator)
        end
    ),

    {ok, _} = kafine_coordinator:start_link(
        ?REF, ?GROUP_ID, ?TOPICS, ?CONNECTION_OPTIONS, ?MEMBERSHIP_OPTIONS
    ),

    ExpectedCoordinator = maps:with([host, port, node_id], Coordinator),
    ?assertWait(
        kafine_connection, start_link, [ExpectedCoordinator, ?CONNECTION_OPTIONS], ?WAIT_TIMEOUT_MS
    ).

handles_offset_fetch_request() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, _} = kafine_bootstrap:start_link(?REF, Broker, ?CONNECTION_OPTIONS),

    {ok, Pid} = kafine_coordinator:start_link(
        ?REF, ?GROUP_ID, ?TOPICS, ?CONNECTION_OPTIONS, ?MEMBERSHIP_OPTIONS
    ),

    meck:wait(2, kafine_connection, start_link, 2, ?WAIT_TIMEOUT_MS),

    CommittedOffsets = #{
        ?TOPIC_NAME => #{0 => 12, 1 => 34},
        ?TOPIC_NAME_2 => #{2 => 56}
    },

    configure_offsets(CommittedOffsets),

    ReqIds0 = kafine_coordinator:reqids_new(),
    ReqIds1 = kafine_coordinator:offset_fetch(
        Pid, #{?TOPIC_NAME => [0, 1], ?TOPIC_NAME_2 => [2]}, ReqIds0
    ),
    {ok, Result} = receive_response(ReqIds1),

    ?assertEqual(CommittedOffsets, Result).

handles_parallel_offset_fetch_requests() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, _} = kafine_bootstrap:start_link(?REF, Broker, ?CONNECTION_OPTIONS),

    {ok, Pid} = kafine_coordinator:start_link(
        ?REF, ?GROUP_ID, ?TOPICS, ?CONNECTION_OPTIONS, ?MEMBERSHIP_OPTIONS
    ),

    meck:wait(2, kafine_connection, start_link, 2, ?WAIT_TIMEOUT_MS),

    CommittedOffsets = #{
        ?TOPIC_NAME => #{0 => 12, 1 => 34},
        ?TOPIC_NAME_2 => #{2 => 56}
    },

    configure_offsets(CommittedOffsets),

    ReqIds0 = kafine_coordinator:reqids_new(),
    ReqIds1 = kafine_coordinator:offset_fetch(Pid, #{?TOPIC_NAME => [0]}, ReqIds0),
    ReqIds2 = kafine_coordinator:offset_fetch(Pid, #{?TOPIC_NAME => [1]}, ReqIds1),
    ReqIds3 = kafine_coordinator:offset_fetch(Pid, #{?TOPIC_NAME_2 => [2]}, ReqIds2),
    Results = receive_responses(ReqIds3),

    ?assert(lists:member({ok, #{?TOPIC_NAME => #{0 => 12}}}, Results)),
    ?assert(lists:member({ok, #{?TOPIC_NAME => #{1 => 34}}}, Results)),
    ?assert(lists:member({ok, #{?TOPIC_NAME_2 => #{2 => 56}}}, Results)).

join_group_without_member_id() ->
    {ok, _, [Broker | _]} = kamock_cluster:start(?CLUSTER_REF),
    {ok, _} = kafine_bootstrap:start_link(?REF, Broker, ?CONNECTION_OPTIONS),

    {ok, Pid} = kafine_coordinator:start_link(
        ?REF, ?GROUP_ID, ?TOPICS, ?CONNECTION_OPTIONS, ?MEMBERSHIP_OPTIONS
    ),

    ReqId = kafine_coordinator:join_group(Pid, <<>>),
    ?assertMatch({error, {member_id_required, _MemberId}}, receive_response(ReqId)).

join_group_after_getting_member_id() ->
    {ok, _, [Broker | _]} = kamock_cluster:start(?CLUSTER_REF),
    {ok, _} = kafine_bootstrap:start_link(?REF, Broker, ?CONNECTION_OPTIONS),

    {ok, Pid} = kafine_coordinator:start_link(
        ?REF, ?GROUP_ID, ?TOPICS, ?CONNECTION_OPTIONS, ?MEMBERSHIP_OPTIONS
    ),

    ReqId = kafine_coordinator:join_group(Pid, <<>>),
    {error, {member_id_required, MemberId}} = receive_response(ReqId),

    ExpectedMember = #{
        member_id => MemberId,
        metadata => #{
            topics => ?TOPICS,
            user_data => <<>>
        },
        group_instance_id => null
    },

    ReqId2 = kafine_coordinator:join_group(Pid, MemberId),
    ?assertMatch(
        {ok, #{generation_id := _, leader := _, members := [ExpectedMember], protocol_name := _}},
        receive_response(ReqId2)
    ).

sync_group_with_single_member() ->
    {ok, _, [Broker | _]} = kamock_cluster:start(?CLUSTER_REF),
    {ok, _} = kafine_bootstrap:start_link(?REF, Broker, ?CONNECTION_OPTIONS),

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
    ?assertEqual(
        {ok, #{
            user_data => <<>>,
            assigned_partitions => #{?TOPIC_NAME => [0, 1, 2, 3], ?TOPIC_NAME_2 => [0, 1, 2, 3]}
        }},
        receive_response(ReqId3)
    ).

heartbeat_success() ->
    {ok, _, [Broker | _]} = kamock_cluster:start(?CLUSTER_REF),
    {ok, _} = kafine_bootstrap:start_link(?REF, Broker, ?CONNECTION_OPTIONS),

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

    ReqId4 = kafine_coordinator:heartbeat(Pid, MemberId, GenerationId),
    ?assertEqual(ok, receive_response(ReqId4)).

heartbeat_during_rebalance() ->
    {ok, _, [Broker | _]} = kamock_cluster:start(?CLUSTER_REF),
    {ok, _} = kafine_bootstrap:start_link(?REF, Broker, ?CONNECTION_OPTIONS),

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

    meck:new(kamock_heartbeat, [passthrough]),
    meck:expect(kamock_heartbeat, handle_heartbeat_request, fun(
        #{correlation_id := CorrelationId}, _Env
    ) ->
        #{
            correlation_id => CorrelationId,
            throttle_time_ms => 0,
            error_code => ?REBALANCE_IN_PROGRESS
        }
    end),

    % heartbeat should give us a rebalance_in_progress error
    ReqId4 = kafine_coordinator:heartbeat(Pid, MemberId, GenerationId),
    ?assertEqual({error, rebalance_in_progress}, receive_response(ReqId4)).

leave_group() ->
    {ok, _, [Broker | _]} = kamock_cluster:start(?CLUSTER_REF),
    {ok, _} = kafine_bootstrap:start_link(?REF, Broker, ?CONNECTION_OPTIONS),

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

    meck:new(kamock_leave_group, [passthrough]),

    % leave group should succeed
    ReqId4 = kafine_coordinator:leave_group(Pid, MemberId),
    ?assertEqual(ok, receive_response(ReqId4)),

    ?assertCalled(
        kamock_leave_group,
        handle_leave_group_request,
        [
            meck:is(fun(#{group_id := G, members := [#{member_id := M}]}) ->
                G =:= ?GROUP_ID andalso M =:= MemberId
            end),
            '_'
        ]
    ).

offset_commit() ->
    {ok, _, [Broker | _]} = kamock_cluster:start(?CLUSTER_REF),
    {ok, _} = kafine_bootstrap:start_link(?REF, Broker, ?CONNECTION_OPTIONS),

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

    OffsetsToCommit = #{
        ?TOPIC_NAME => #{0 => 100, 1 => 200},
        ?TOPIC_NAME_2 => #{2 => 300}
    },

    ExpectedResult = #{
        ?TOPIC_NAME => #{0 => ok, 1 => ok},
        ?TOPIC_NAME_2 => #{2 => ok}
    },

    ReqIds = kafine_coordinator:offset_commit(
        Pid,
        MemberId,
        GenerationId,
        OffsetsToCommit,
        test_offset_commit,
        kafine_coordinator:reqids_new()
    ),
    ?assertEqual(
        [{ok, ExpectedResult, 0}],
        receive_responses(ReqIds)
    ).

offset_commit_with_errors() ->
    {ok, _, [Broker | _]} = kamock_cluster:start(?CLUSTER_REF),
    {ok, _} = kafine_bootstrap:start_link(?REF, Broker, ?CONNECTION_OPTIONS),

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

    OffsetsToCommit = #{
        ?TOPIC_NAME => #{0 => 100, 1 => 200},
        ?TOPIC_NAME_2 => #{2 => 300}
    },

    ThrottleTimeMs = 42,

    meck:new(kamock_offset_commit, [passthrough]),
    meck:expect(
        kamock_offset_commit,
        handle_offset_commit_request,
        fun(#{correlation_id := CorrelationId, topics := Topics}, _Env) ->
            #{
                correlation_id => CorrelationId,
                throttle_time_ms => ThrottleTimeMs,
                topics => [
                    #{
                        name => Topic,
                        partitions => [
                            #{
                                partition_index => PartitionIndex,
                                error_code =>
                                    case PartitionIndex of
                                        1 -> ?INVALID_REQUEST;
                                        _ -> ?NONE
                                    end
                            }
                         || #{partition_index := PartitionIndex} <- Partitions
                        ]
                    }
                 || #{name := Topic, partitions := Partitions} <- Topics
                ]
            }
        end
    ),

    ExpectedResult = #{
        ?TOPIC_NAME => #{0 => ok, 1 => {kafka_error, ?INVALID_REQUEST}},
        ?TOPIC_NAME_2 => #{2 => ok}
    },

    ReqIds = kafine_coordinator:offset_commit(
        Pid,
        MemberId,
        GenerationId,
        OffsetsToCommit,
        test_offset_commit,
        kafine_coordinator:reqids_new()
    ),
    ?assertEqual(
        [{ok, ExpectedResult, ThrottleTimeMs}],
        receive_responses(ReqIds)
    ).

reconnects_on_disconnect() ->
    {ok, _, [Broker | _]} = kamock_cluster:start(?CLUSTER_REF),
    {ok, _} = kafine_bootstrap:start_link(?REF, Broker, ?CONNECTION_OPTIONS),

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

    telemetry_test:attach_event_handlers(self(), [
        [kafine, coordinator, connected],
        [kafine, coordinator, disconnected],
        [kamock, protocol, connected]
    ]),

    % Drop connection on next heartbeat request, go back to normal afterwards
    meck:expect(
        kamock_heartbeat,
        handle_heartbeat_request,
        ['_', '_'],
        meck:seq([meck:val(stop), meck:passthrough()])
    ),

    ReqId4 = kafine_coordinator:heartbeat(Pid, MemberId, GenerationId),

    ?assertReceived({[kafine, coordinator, disconnected], _, _, _}),
    ?assertReceived({[kamock, protocol, connected], _, _, _}),
    ?assertReceived({[kafine, coordinator, connected], _, _, _}),

    ok = receive_response(ReqId4),
    ?assertNotCalled(kafine_backoff, backoff, '_'),

    % kamock should have seen 2 heartbeat requests
    meck:wait(2, kamock_heartbeat, handle_heartbeat_request, '_', 0),

    kafine_coordinator:stop(Pid),
    kamock_broker:stop(Broker).

applies_backoff_on_failed_reconnect() ->
    {ok, _, [Bootstrap, Coordinator = #{port := Port} | _]} = kamock_cluster:start(?CLUSTER_REF),
    meck:expect(
        kamock_find_coordinator,
        handle_find_coordinator_request,
        kamock_find_coordinator:return(Coordinator)
    ),
    meck:expect(kafine_backoff, init, fun(_) -> {backoff_state, 1} end),
    meck:expect(kafine_backoff, reset, fun(State) -> State end),

    {ok, _} = kafine_bootstrap:start_link(?REF, Bootstrap, ?CONNECTION_OPTIONS),

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

    telemetry_test:attach_event_handlers(self(), [
        [kafine, coordinator, connected],
        [kafine, coordinator, disconnected],
        [kafine, coordinator, backoff],
        [kamock, protocol, connected]
    ]),

    meck:expect(kafine_backoff, init, fun(_) -> {backoff_state, 1} end),
    meck:expect(kafine_backoff, reset, fun(State) -> State end),
    meck:expect(kafine_backoff, backoff, [
        {[{backoff_state, 1}], {50, {backoff_state, 2}}},
        {[{backoff_state, '_'}], {50, {backoff_state, 3}}}
    ]),

    % Stop the broker, dropping connections and failing future attempts
    kamock_broker:stop(Coordinator),

    ?assertReceived({[kafine, coordinator, disconnected], _, _, _}),

    % should repeatedly back off
    ?assertReceived({[kafine, coordinator, backoff], _, _, _}),
    ?assertCalled(kafine_backoff, backoff, [{backoff_state, 1}]),

    ?assertReceived({[kafine, coordinator, backoff], _, _, _}),
    ?assertCalled(kafine_backoff, backoff, [{backoff_state, 2}]),

    % Bring broker back up
    {ok, Coordinator2} = kamock_broker:start(?BROKER_REF, #{port => Port}),

    % should connect and reset backoff state
    ?assertReceived({[kamock, protocol, connected], _, _, _}),
    ?assertReceived({[kafine, coordinator, connected], _, _, _}),
    ?assertCalled(kafine_backoff, reset, [{backoff_state, 3}]),

    kafine_coordinator:stop(Pid),
    kamock_broker:stop(Coordinator2),
    kamock_broker:stop(Bootstrap).

configure_offsets(Offsets) ->
    meck:expect(
        kamock_offset_fetch_response_partition,
        make_offset_fetch_response_partition,
        fun(Topic, Partition, _) ->
            #{
                partition_index => Partition,
                committed_offset => kafine_maps:get([Topic, Partition], Offsets, -1),
                metadata => <<>>,
                error_code => 0
            }
        end
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

receive_responses(ReqIds) ->
    receive_responses(ReqIds, []).

receive_responses(ReqIds, Acc) ->
    case kafine_coordinator:reqids_size(ReqIds) of
        0 ->
            Acc;
        _ ->
            {Response, NewReqIds} = do_receive_response(ReqIds),
            receive_responses(NewReqIds, [Response | Acc])
    end.
