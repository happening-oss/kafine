-module(kafine_eager_rebalance_assignment_tests).
-include_lib("eunit/include/eunit.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(GROUP_ID, iolist_to_binary(io_lib:format("~s___~s_g", [?MODULE, ?FUNCTION_NAME]))).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(CALLBACK_STATE, undefined).
-define(REBALANCE_REF_1, rebalance_1).
-define(REBALANCE_REF_2, rebalance_2).
-define(HEARTBEAT_INTERVAL_MS, 30).
-define(GROUP_GENERATION_1, 1).
-define(GROUP_GENERATION_2, 2).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun call_assignment_callbacks_after_join_leader/0,
        fun call_assignments_callbacks_after_join_follower/0,
        fun call_assignment_callbacks_after_rebalance/0,
        fun assignment_callbacks_are_called_from_long_lived_process/0
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

    ok.

cleanup(_) ->
    meck:unload(),
    ok.

call_assignment_callbacks_after_join_leader() ->
    _TelemetryRef = telemetry_test:attach_event_handlers(self(), [
        [kafine, rebalance, leader]
    ]),

    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    GroupId = ?GROUP_ID,
    Topics = [?TOPIC_NAME],
    {ok, R} = kafine_eager_rebalance:start_link(
        ?REBALANCE_REF_1,
        Broker,
        #{},
        GroupId,
        #{
            heartbeat_interval_ms => ?HEARTBEAT_INTERVAL_MS,
            subscription_callback => {test_membership_callback, ?CALLBACK_STATE},
            assignment_callback => {test_assignment_callback, ?CALLBACK_STATE},
            assignor => kafine_range_assignor
        },
        Topics
    ),

    % Wait for rebalance to complete
    receive
        {[kafine, rebalance, leader], _, _, _} -> ok
    end,

    % Check we call the subscription callback
    ?assert(
        meck:called(
            test_membership_callback,
            subscribe_partitions,
            [
                '_',
                meck:is(is_assignment(#{?TOPIC_NAME => [0, 1, 2, 3]})),
                '_'
            ],
            '_'
        )
    ),

    ExpectedAdditional = #{?TOPIC_NAME => [0, 1, 2, 3]},

    ?assertEqual(
        [
            init,
            % Initial revoke_assignments at start of join_group
            {before_assignment, #{}, #{}},
            {after_assignment, #{}, #{}},
            % After sync group we get new topics
            {before_assignment, ExpectedAdditional, #{}},
            {after_assignment, ExpectedAdditional, #{}}
        ],
        filter_history(meck:history(test_assignment_callback))
    ),

    kafine_eager_rebalance:stop(R),
    kamock_broker:stop(Broker),
    ok.

call_assignments_callbacks_after_join_follower() ->
    _TelemetryRef = telemetry_test:attach_event_handlers(self(), [
        [kafine, rebalance, follower]
    ]),

    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    LeaderId = kamock_join_group:generate_member_id(<<"leader">>),
    setup_as_follower(LeaderId, ?TOPIC_NAME, [0, 1, 2, 3]),

    GroupId = ?GROUP_ID,
    Topics = [?TOPIC_NAME],
    {ok, R} = kafine_eager_rebalance:start_link(
        ?REBALANCE_REF_1,
        Broker,
        #{},
        GroupId,
        #{
            heartbeat_interval_ms => ?HEARTBEAT_INTERVAL_MS,
            subscription_callback => {test_membership_callback, ?CALLBACK_STATE},
            assignment_callback => {test_assignment_callback, ?CALLBACK_STATE},
            assignor => kafine_range_assignor
        },
        Topics
    ),

    % Wait for rebalance to complete
    receive
        {[kafine, rebalance, follower], _, _, _} -> ok
    end,

    % Check we call the subscription callback
    ?assert(
        meck:called(
            test_membership_callback,
            subscribe_partitions,
            [
                '_',
                meck:is(is_assignment(#{?TOPIC_NAME => [0, 1, 2, 3]})),
                '_'
            ],
            '_'
        )
    ),

    ExpectedAdditional = #{?TOPIC_NAME => [0, 1, 2, 3]},

    ?assertEqual(
        [
            init,
            % Initial revoke_assignments at start of join_group
            {before_assignment, #{}, #{}},
            {after_assignment, #{}, #{}},
            % After sync group we get new topics
            {before_assignment, ExpectedAdditional, #{}},
            {after_assignment, ExpectedAdditional, #{}}
        ],
        filter_history(meck:history(test_assignment_callback))
    ),

    kafine_eager_rebalance:stop(R),
    kamock_broker:stop(Broker),
    ok.

call_assignment_callbacks_after_rebalance() ->
    TelemetryRef = telemetry_test:attach_event_handlers(self(), [
        [kafine, rebalance, leader],
        [kafine, rebalance, follower]
    ]),

    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    % member-1 will be the leader
    HasMemberId = fun(MemberId) -> fun(#{member_id := M}) -> M =:= MemberId end end,
    HasNoMemberId = HasMemberId(<<>>),
    meck:expect(kamock_join_group, handle_join_group_request, [
        % Use the client ID as the member ID.
        {[meck:is(HasNoMemberId), '_'], fun(
            #{correlation_id := CorrelationId, client_id := ClientId}, _
        ) ->
            kamock_join_group:member_id_required(CorrelationId, ClientId)
        end},

        % We've got a member ID, assign the leader and follower appropriately.
        {
            [meck:is(HasMemberId(<<"member-1">>)), '_'],
            kamock_join_group:as_leader(?GROUP_GENERATION_1)
        },
        {
            [meck:is(HasMemberId(<<"member-2">>)), '_'],
            kamock_join_group:as_follower(<<"member-1">>, ?GROUP_GENERATION_1)
        }
    ]),

    % Start one group member,
    GroupId = ?GROUP_ID,
    Topics = [?TOPIC_NAME],
    {ok, FirstGroupMember} = kafine_eager_rebalance:start_link(
        ?REBALANCE_REF_1,
        Broker,
        #{client_id => <<"member-1">>},
        GroupId,
        #{
            heartbeat_interval_ms => ?HEARTBEAT_INTERVAL_MS,
            subscription_callback => {test_membership_callback, ?CALLBACK_STATE},
            assignment_callback => {test_assignment_callback, ?CALLBACK_STATE},
            assignor => kafine_range_assignor
        },
        Topics
    ),

    % Wait for initial rebalance to complete
    receive
        {[kafine, rebalance, leader], TelemetryRef, _, _} -> ok
    end,

    AllTopics = #{?TOPIC_NAME => [0, 1, 2, 3]},

    ?assertEqual(
        [
            init,
            % Initial revoke_assignments at start of join_group
            {before_assignment, #{}, #{}},
            {after_assignment, #{}, #{}},
            % After sync group we get new topics
            {before_assignment, AllTopics, #{}},
            {after_assignment, AllTopics, #{}}
        ],
        filter_history(meck:history(test_assignment_callback, FirstGroupMember))
    ),

    meck:reset(test_assignment_callback),

    % Assign each member 2 partitions
    AssignedPartitions1 = [#{topic => ?TOPIC_NAME, partitions => [0, 1]}],
    AssignedPartitions2 = [#{topic => ?TOPIC_NAME, partitions => [2, 3]}],

    meck:expect(kamock_join_group, handle_join_group_request, [
        % Use the client ID as the member ID.
        {[meck:is(HasNoMemberId), '_'], fun(
            #{correlation_id := CorrelationId, client_id := ClientId}, _
        ) ->
            kamock_join_group:member_id_required(CorrelationId, ClientId)
        end},

        % We've got a member ID, assign the leader and follower appropriately.
        {
            [meck:is(HasMemberId(<<"member-1">>)), '_'],
            kamock_join_group:as_leader(?GROUP_GENERATION_2)
        },
        {
            [meck:is(HasMemberId(<<"member-2">>)), '_'],
            kamock_join_group:as_follower(<<"member-1">>, ?GROUP_GENERATION_2)
        }
    ]),
    meck:expect(kamock_sync_group, handle_sync_group_request, [
        {
            [meck:is(HasMemberId(<<"member-1">>)), '_'],
            kamock_sync_group:assign(AssignedPartitions1, <<>>)
        },
        {
            [meck:is(HasMemberId(<<"member-2">>)), '_'],
            kamock_sync_group:assign(AssignedPartitions2, <<>>)
        }
    ]),

    % Start second consumer member
    {ok, SecondGroupMember} = kafine_eager_rebalance:start_link(
        ?REBALANCE_REF_2,
        Broker,
        #{client_id => <<"member-2">>},
        GroupId,
        #{
            heartbeat_interval_ms => ?HEARTBEAT_INTERVAL_MS,
            subscription_callback => {test_membership_callback, ?CALLBACK_STATE},
            assignment_callback => {test_assignment_callback, ?CALLBACK_STATE},
            assignor => kafine_range_assignor
        },
        Topics
    ),

    meck:expect(
        kamock_heartbeat,
        handle_heartbeat_request,
        kamock_heartbeat:expect_generation_id(?GROUP_GENERATION_2)
    ),

    % Wait until they've both joined.
    receive
        {[kafine, rebalance, leader], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,
    receive
        {[kafine, rebalance, follower], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,

    ReassignedPartitions = #{?TOPIC_NAME => [2, 3]},
    KeptPartitions = #{?TOPIC_NAME => [0, 1]},

    ?assertEqual(
        [
            % Revoke all assignments at start of join_group
            {before_assignment, #{}, AllTopics},
            {after_assignment, #{}, AllTopics},
            % After sync group we get new topics
            {before_assignment, KeptPartitions, #{}},
            {after_assignment, KeptPartitions, #{}}
        ],
        filter_history(meck:history(test_assignment_callback, FirstGroupMember))
    ),

    ?assertEqual(
        [
            init,
            % Initial revoke_assignments at start of join_group
            {before_assignment, #{}, #{}},
            {after_assignment, #{}, #{}},
            % After sync group we get new topics
            {before_assignment, ReassignedPartitions, #{}},
            {after_assignment, ReassignedPartitions, #{}}
        ],
        filter_history(meck:history(test_assignment_callback, SecondGroupMember))
    ),

    kafine_eager_rebalance:stop(FirstGroupMember),
    kafine_eager_rebalance:stop(SecondGroupMember),
    kamock_broker:stop(Broker),
    ok.

% This test is to keep track of which process is calling the
% assignment callbacks. This process may end up owning ETS
% tables so its important to keep it alive longer than
% any fetchers that may be using these tables
assignment_callbacks_are_called_from_long_lived_process() ->
    _TelemetryRef = telemetry_test:attach_event_handlers(self(), [
        [kafine, rebalance, leader]
    ]),

    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    GroupId = ?GROUP_ID,
    Topics = [?TOPIC_NAME],
    {ok, RebalancerPid} = kafine_eager_rebalance:start_link(
        ?REBALANCE_REF_1,
        Broker,
        #{},
        GroupId,
        #{
            heartbeat_interval_ms => ?HEARTBEAT_INTERVAL_MS,
            subscription_callback => {test_membership_callback, ?CALLBACK_STATE},
            assignment_callback => {test_assignment_callback, ?CALLBACK_STATE},
            assignor => kafine_range_assignor
        },
        Topics
    ),

    % Wait for rebalance to complete
    receive
        {[kafine, rebalance, leader], _, _, _} -> ok
    end,

    ?assertMatch(
        [
            init,
            % Initial revoke_assignments at start of join_group
            {before_assignment, _, _},
            {after_assignment, _, _},
            % After sync group we get new topics
            {before_assignment, _, _},
            {after_assignment, _, _}
        ],
        filter_history(meck:history(test_assignment_callback, RebalancerPid))
    ),

    kafine_eager_rebalance:stop(RebalancerPid),
    kamock_broker:stop(Broker),
    ok.

is_assignment(Assignment) ->
    fun(P) when P =:= Assignment ->
        true
    end.

setup_as_follower(LeaderId, Topic, Partitions) ->
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

filter_history(History) ->
    Fun = fun
        ({_, {_, init, _}, _}) ->
            init;
        ({_, {_, Func, [NewAssignment, OldAssignment, _]}, _}) ->
            {Func, NewAssignment, OldAssignment}
    end,
    lists:map(Fun, History).
