-module(kafine_eager_rebalance_tests).
-include_lib("eunit/include/eunit.hrl").

-include_lib("kafcod/include/error_code.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CLUSTER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(GROUP_ID, iolist_to_binary(io_lib:format("~s___~s_g", [?MODULE, ?FUNCTION_NAME]))).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(WAIT_TIMEOUT_MS, 2_000).
-define(CALLBACK_STATE, undefined).
-define(REBALANCE_REF, {?MODULE, ?FUNCTION_NAME}).
-define(GENERATION_ID_1, 1).
-define(GENERATION_ID_2, 2).

% Usually 3s, but we want something quicker for the tests.
-define(HEARTBEAT_INTERVAL_MS, 30).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun join_new_group_as_leader/0,
        fun join_new_group_as_follower/0,
        fun coordinator_not_available/0,
        fun leader_with_new_member/0,
        fun follower_with_new_member/0,
        fun not_coordinator/0,
        fun offset_commit/0
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

    % We'll forward to the range assignor, but we want to make sure that we actually pay attention to the configuration.
    meck:new(test_assignor, [non_strict]),
    meck:expect(test_assignor, name, fun() -> <<"test">> end),
    meck:expect(test_assignor, metadata, fun(Topics) -> kafine_range_assignor:metadata(Topics) end),
    meck:expect(test_assignor, assign, fun(Members, TopicPartitions, AssignmentUserData) ->
        kafine_range_assignor:assign(Members, TopicPartitions, AssignmentUserData)
    end),

    meck:new(kafine_range_assignor, [passthrough]),

    meck:new(kamock_find_coordinator, [passthrough]),
    meck:new(kamock_join_group, [passthrough]),
    meck:new(kamock_sync_group, [passthrough]),
    meck:new(kamock_heartbeat, [passthrough]),
    meck:new(kamock_offset_commit, [passthrough]),
    ok.

cleanup(_) ->
    meck:unload(),
    ok.

join_new_group_as_leader() ->
    TelemetryRef = telemetry_test:attach_event_handlers(self(), [
        [kafine, rebalance, join_group],
        [kafine, rebalance, leader]
    ]),

    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    % The default for the mock broker is for single-member-as-leader, so we don't need to do anything to set that up.

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
            assignment_callback => {test_assignment_callback, undefined},
            assignor => test_assignor
        },
        Topics
    ),

    % Wait for JoinGroup w/member_id (see KIP-394).
    meck:wait(
        kamock_join_group,
        handle_join_group_request,
        [meck:is(has_member_id()), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % Wait for the 'join_group' event.
    receive
        {[kafine, rebalance, join_group], TelemetryRef, _Measurements = #{}, Metadata} ->
            ?assertMatch(#{group_id := GroupId}, Metadata)
    end,

    % Did we revoke assignments?
    ?assert(meck:called(test_membership_callback, unsubscribe_partitions, '_')),

    % Wait until we're the leader.
    % TODO: If we have multiple group members in the same test, how do we tell them apart here?
    % TODO: We pass extra telemetry metadata in the options, and it'll come back out later. Useful for other things, too.
    receive
        {[kafine, rebalance, leader], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,

    % Was the assignor called?
    ?assert(meck:called(test_assignor, assign, '_')),

    % Wait for two heartbeat requests (to make sure the timeouts aren't broken).
    meck:wait(2, kamock_heartbeat, handle_heartbeat_request, '_', ?WAIT_TIMEOUT_MS),

    ?assertMatch({leader, _}, sys:get_state(R)),

    % Did we get some partitions to start?
    ?assert(
        meck:called(test_membership_callback, subscribe_partitions, [
            '_',
            meck:is(fun(_Assignment1) ->
                true
            end),
            '_'
        ])
    ),

    telemetry:detach(TelemetryRef),

    kafine_eager_rebalance:stop(R),
    kamock_broker:stop(Broker),
    ok.

has_member_id() ->
    fun(#{member_id := MemberId}) -> MemberId /= <<>> end.

join_new_group_as_follower() ->
    TelemetryRef = telemetry_test:attach_event_handlers(self(), [
        [kafine, rebalance, join_group],
        [kafine, rebalance, follower]
    ]),

    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    LeaderId = kamock_join_group:generate_member_id(<<"leader">>),
    setup_as_follower(LeaderId, ?TOPIC_NAME, [0, 1, 2, 3]),

    GroupId = ?GROUP_ID,
    Topics = [?TOPIC_NAME],
    {ok, R} = kafine_eager_rebalance:start_link(
        ?REBALANCE_REF,
        Broker,
        #{},
        ?GROUP_ID,
        #{
            heartbeat_interval_ms => ?HEARTBEAT_INTERVAL_MS,
            subscription_callback => {test_membership_callback, undefined},
            assignment_callback => {test_assignment_callback, undefined}
        },
        Topics
    ),

    % Wait until we're the follower
    receive
        {[kafine, rebalance, follower], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,

    % Wait for two hearbeat requests (to make sure the timeouts aren't broken).
    meck:wait(2, kamock_heartbeat, handle_heartbeat_request, '_', ?WAIT_TIMEOUT_MS),

    ?assertMatch({follower, _}, sys:get_state(R)),

    ?assert(meck:called(test_membership_callback, subscribe_partitions, '_')),

    telemetry:detach(TelemetryRef),

    kafine_eager_rebalance:stop(R),
    kamock_broker:stop(Broker),
    ok.

setup_as_follower(LeaderId, Topic, Partitions) ->
    % If we're a follower, we'll get a JoinGroup response with leader /= member_id.
    meck:expect(
        kamock_join_group,
        handle_join_group_request,
        kamock_join_group:as_follower(LeaderId)
    ),
    meck:expect(
        kamock_sync_group,
        handle_sync_group_request,
        kamock_sync_group:assign([#{topic => Topic, partitions => Partitions}])
    ).

coordinator_not_available() ->
    setup_coordinator_not_available(),

    TelemetryRef = telemetry_test:attach_event_handlers(self(), [
        [kafine, rebalance, join_group],
        [kafine, rebalance, leader]
    ]),

    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    % Default is that we're the leader.

    GroupId = ?GROUP_ID,
    Topics = [?TOPIC_NAME],
    {ok, R} = kafine_eager_rebalance:start_link(
        ?REBALANCE_REF,
        Broker,
        #{},
        ?GROUP_ID,
        #{
            heartbeat_interval_ms => ?HEARTBEAT_INTERVAL_MS,
            subscription_callback => {test_membership_callback, undefined},
            assignment_callback => {test_assignment_callback, undefined}
        },
        Topics
    ),

    receive
        {[kafine, rebalance, leader], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,

    ?assertMatch({leader, _}, sys:get_state(R)),

    kafine_eager_rebalance:stop(R),
    kamock_broker:stop(Broker),
    ok.

setup_coordinator_not_available() ->
    % If the cluster has only just started up, there won't be a coordinator yet.
    meck:expect(
        kamock_find_coordinator,
        handle_find_coordinator_request,
        ['_', '_'],
        meck:seq(
            [
                % On the first few calls, return ?COORDINATOR_NOT_AVAILABLE
                meck:seq(
                    [
                        fun(_FindCoordinatorRequest = #{correlation_id := CorrelationId}, _Env) ->
                            kamock_find_coordinator:make_find_coordinator_error(
                                CorrelationId,
                                ?COORDINATOR_NOT_AVAILABLE,
                                <<"The coordinator is not available.">>
                            )
                        end
                     || _ <- lists:seq(1, 3)
                    ]
                ),
                % Then respond with the default.
                meck:passthrough()
            ]
        )
    ).

leader_with_new_member() ->
    TelemetryRef = telemetry_test:attach_event_handlers(self(), [
        [kafine, rebalance, join_group],
        [kafine, rebalance, leader]
    ]),

    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    meck:expect(
        kamock_join_group,
        handle_join_group_request,
        kamock_join_group:as_leader(?GENERATION_ID_1)
    ),

    % TODO: There's quite a lot of shared setup here; can we jump start to 'leader' somehow?
    GroupId = ?GROUP_ID,
    Topics = [?TOPIC_NAME],
    {ok, R} = kafine_eager_rebalance:start_link(
        ?REBALANCE_REF,
        Broker,
        #{},
        ?GROUP_ID,
        #{
            heartbeat_interval_ms => ?HEARTBEAT_INTERVAL_MS,
            subscription_callback => {test_membership_callback, undefined},
            assignment_callback => {test_assignment_callback, undefined}
        },
        Topics
    ),

    receive
        {[kafine, rebalance, leader], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,

    #{member_id := MemberId} =
        meck:capture(last, kamock_join_group, handle_join_group_request, '_', 1),

    % Clear history; we'll check it later.
    meck:reset(kamock_join_group),

    % Wait for a heartbeat message.
    meck:wait(kamock_heartbeat, handle_heartbeat_request, '_', ?WAIT_TIMEOUT_MS),

    meck:expect(
        kamock_join_group,
        handle_join_group_request,
        kamock_join_group:as_leader(?GENERATION_ID_2)
    ),

    % Trigger a rebalance.
    meck:expect(
        kamock_heartbeat,
        handle_heartbeat_request,
        kamock_heartbeat:expect_generation_id(?GENERATION_ID_2)
    ),

    % Wait for (another) JoinGroup request.
    meck:wait(
        kamock_join_group,
        handle_join_group_request,
        [meck:is(has_member_id()), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % Aside: the mock broker should probably increment the generation ID, but that's a mock broker problem, not ours.

    % Re-joining re-uses the member ID; we should only see one JoinGroup request, and it should have a member ID.
    ?assertMatch(
        [
            {_, {kamock_join_group, handle_join_group_request, [#{member_id := MemberId}, _]}, _}
        ],
        lists:filter(
            fun
                ({_, {_, handle_join_group_request, _}, _}) -> true;
                (_) -> false
            end,
            meck:history(kamock_join_group)
        )
    ),

    telemetry:detach(TelemetryRef),

    kafine_eager_rebalance:stop(R),
    kamock_broker:stop(Broker),
    ok.

follower_with_new_member() ->
    TelemetryRef = telemetry_test:attach_event_handlers(self(), [
        [kafine, rebalance, join_group],
        [kafine, rebalance, follower]
    ]),

    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    LeaderId = kamock_join_group:generate_member_id(<<"leader">>),
    setup_as_follower(LeaderId, ?TOPIC_NAME, [0, 1, 2, 3]),

    GroupId = ?GROUP_ID,
    Topics = [?TOPIC_NAME],
    {ok, R} = kafine_eager_rebalance:start_link(
        ?REBALANCE_REF,
        Broker,
        #{},
        ?GROUP_ID,
        #{
            heartbeat_interval_ms => ?HEARTBEAT_INTERVAL_MS,
            subscription_callback => {test_membership_callback, undefined},
            assignment_callback => {test_assignment_callback, undefined}
        },
        Topics
    ),

    % Wait until we're the follower
    receive
        {[kafine, rebalance, follower], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,

    #{member_id := MemberId} =
        meck:capture(last, kamock_join_group, handle_join_group_request, '_', 1),

    % Clear history; we'll check it later.
    meck:reset(kamock_join_group),

    % Wait for a heartbeat message.
    meck:wait(kamock_heartbeat, handle_heartbeat_request, '_', ?WAIT_TIMEOUT_MS),

    % Start a rebalance.
    meck:expect(
        kamock_heartbeat,
        handle_heartbeat_request,
        fun(_HeartbeatRequest = #{correlation_id := CorrelationId}, _Env) ->
            #{
                correlation_id => CorrelationId,
                throttle_time_ms => 0,
                error_code => ?REBALANCE_IN_PROGRESS
            }
        end
    ),

    % Wait for (another) JoinGroup request.
    meck:wait(
        kamock_join_group,
        handle_join_group_request,
        [meck:is(has_member_id()), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % Aside: the mock broker should probably increment the generation ID, but that's a mock broker problem, not ours.

    % Re-joining re-uses the member ID; we should only see one JoinGroup request, and it should have a member ID.
    ?assertMatch(
        [
            {_, {kamock_join_group, handle_join_group_request, [#{member_id := MemberId}, _]}, _}
        ],
        meck:history(kamock_join_group)
    ),

    telemetry:detach(TelemetryRef),

    kafine_eager_rebalance:stop(R),
    kamock_broker:stop(Broker),
    ok.

not_coordinator() ->
    TelemetryRef = telemetry_test:attach_event_handlers(self(), [
        [kafine, rebalance, join_group],
        [kafine, rebalance, leader]
    ]),

    % The coordinator's just another broker; we'll start a mock cluster.
    % We'll connect to the first broker; use the second as the coordinator.
    {ok, Cluster, Brokers} = kamock_cluster:start(?CLUSTER_REF),
    [Bootstrap, Coordinator | _] = Brokers,

    % Name's kinda misleading; it matches the other setup_TEST functions, but what it actually *does* is
    % setup where the coordinator is, so it'd be better named setup_coordinator.
    setup_not_coordinator(Coordinator),

    GroupId = ?GROUP_ID,
    Topics = [?TOPIC_NAME],
    {ok, R} = kafine_eager_rebalance:start_link(
        ?REBALANCE_REF,
        Bootstrap,
        #{},
        ?GROUP_ID,
        #{
            heartbeat_interval_ms => ?HEARTBEAT_INTERVAL_MS,
            subscription_callback => {test_membership_callback, undefined},
            assignment_callback => {test_assignment_callback, undefined}
        },
        Topics
    ),

    receive
        {[kafine, rebalance, leader], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,

    kafine_eager_rebalance:stop(R),
    kamock_cluster:stop(Cluster),
    ok.

setup_not_coordinator(Coordinator = #{node_id := CoordinatorId}) ->
    % FindCoordinator, if we're not the coordinator, redirects. Otherwise, passthrough.
    meck:expect(
        kamock_find_coordinator,
        handle_find_coordinator_request,
        fun
            (
                _FindCoordinatorRequest = #{correlation_id := CorrelationId},
                _Env = #{node_id := NodeId}
            ) when NodeId /= CoordinatorId ->
                % Not the coordinator; redirect.
                kamock_find_coordinator:make_find_coordinator_response(
                    CorrelationId, Coordinator
                );
            (FindCoordinatorRequest, Env) ->
                % Otherwise; passthrough.
                meck:passthrough([FindCoordinatorRequest, Env])
        end
    ),

    meck:expect(
        kamock_join_group,
        handle_join_group_request,
        fun
            (
                _JoinGroupRequest = #{correlation_id := CorrelationId},
                _Env = #{node_id := NodeId}
            ) when NodeId /= CoordinatorId ->
                % Not the coordinator; return an error.
                kamock_join_group:make_error(CorrelationId, ?NOT_COORDINATOR);
            (JoinGroupRequest, Env) ->
                % Otherwise; passthrough.
                meck:passthrough([JoinGroupRequest, Env])
        end
    ).

offset_commit() ->
    % Start mock broker
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    % Start membership
    TopicName = ?TOPIC_NAME,
    GroupId = ?GROUP_ID,
    Partition = 0,
    CommitOffset = 1,
    Ref = ?REBALANCE_REF,
    {ok, R} = kafine_eager_rebalance:start_link(
        Ref,
        Broker,
        #{},
        GroupId,
        #{
            subscription_callback => {test_membership_callback, undefined},
            assignment_callback => {test_assignment_callback, undefined}
        },
        [TopicName]
    ),
    % Commit offset 1 on partition 0
    Offsets = #{TopicName => #{Partition => CommitOffset}},
    % Check that the request went through without error
    #{
        topics := [
            #{name := TopicName, partitions := [#{error_code := ?NONE, partition_index := 0}]}
        ]
    } = kafine_eager_rebalance:offset_commit(Ref, Offsets),
    % We expect the broker to receive the above as a request
    meck:wait(
        kamock_offset_commit,
        handle_offset_commit_request,
        [meck:is(expected_offset_commit_request(TopicName, Partition, CommitOffset)), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    kafine_eager_rebalance:stop(R),
    kamock_broker:stop(Broker),
    ok.

expected_offset_commit_request(TopicName, Partition, CommitOffset) ->
    fun(
        #{
            topics := [
                #{
                    name := T,
                    partitions :=
                        [
                            #{
                                partition_index := P,
                                committed_offset := CO,
                                committed_metadata := <<>>
                            }
                        ]
                }
            ]
        }
    ) when T =:= TopicName, P =:= Partition, CO =:= CommitOffset ->
        true
    end.
