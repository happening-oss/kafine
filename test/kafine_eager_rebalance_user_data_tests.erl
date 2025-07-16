-module(kafine_eager_rebalance_user_data_tests).
-include_lib("eunit/include/eunit.hrl").
-include("assert_meck.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(GROUP_ID, iolist_to_binary(io_lib:format("~s___~s_g", [?MODULE, ?FUNCTION_NAME]))).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(CALLBACK_STATE, undefined).

-define(GROUP_GENERATION_1, 1).
-define(GROUP_GENERATION_2, 2).

% Usually 3s, but we want something quicker for the tests.
-define(HEARTBEAT_INTERVAL_MS, 30).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun assignment_user_data_is_preserved/0
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

    meck:new(test_assignor, [non_strict]),
    meck:expect(test_assignor, name, fun() -> <<"test">> end),
    meck:expect(test_assignor, metadata, fun(Topics) ->
        #{
            topics => Topics,
            % Confusingly named; this user data is anything the member wants to tell the leader.
            user_data => <<>>
        }
    end),
    meck:expect(test_assignor, assign, fun(_Members, _TopicPartitions, _AssignmentUserData) ->
        #{}
    end),
    ok.

cleanup(_) ->
    meck:unload(),
    ok.

assignment_user_data_is_preserved() ->
    % If the leader gives some user data to a member, and that member is later elected leader, then that user data is
    % passed to the assignor. This allows for stickiness to be communicated between members.
    GroupId = ?GROUP_ID,
    TopicName = ?TOPIC_NAME,
    Topics = [TopicName],

    TelemetryRef = telemetry_test:attach_event_handlers(self(), [
        [kafine, rebalance, join_group],
        [kafine, rebalance, leader],
        [kafine, rebalance, follower]
    ]),

    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    % Wait for both members to join, then tell one of them that they're the leader.
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

    % The leader can pass user data in each assignment. It could vary by member, or the leader could pass the same thing
    % to each member.
    AssignmentUserData = <<"assignment-user-data">>,

    % It gets a little bit confusing here. We tell the assignor to return some assignments and user data, and then we
    % completely ignore it when mocking the SyncGroup response.
    meck:expect(test_assignor, assign, fun(_Members, _TopicPartitions, _AssignmentUserData) ->
        #{
            <<"member-1">> => #{
                assigned_partitions => #{TopicName => [0, 1]}, user_data => AssignmentUserData
            },
            <<"member-2">> => #{
                assigned_partitions => #{TopicName => [2, 3]}, user_data => AssignmentUserData
            }
        }
    end),

    % Once they're both joined, assign some partitions.
    AssignedPartitions1 = [#{topic => ?TOPIC_NAME, partitions => [0, 1]}],
    AssignedPartitions2 = [#{topic => ?TOPIC_NAME, partitions => [2, 3]}],
    meck:expect(kamock_sync_group, handle_sync_group_request, [
        {
            % Note: we don't assert that the SyncGroup request from the leader contains the userdata returned from the
            % assignor, because that would require (1) decomposing the request; (2) copy-pasting this below, but
            % reversed. Ain't nobody got time for that.
            [meck:is(HasMemberId(<<"member-1">>)), '_'],
            kamock_sync_group:assign(AssignedPartitions1, AssignmentUserData)
        },
        {
            [meck:is(HasMemberId(<<"member-2">>)), '_'],
            kamock_sync_group:assign(AssignedPartitions2, AssignmentUserData)
        }
    ]),

    % We're gonna need two members.
    {ok, R1} = kafine_eager_rebalance:start_link(
        make_ref(),
        Broker,
        #{client_id => <<"member-1">>},
        GroupId,
        #{
            heartbeat_interval_ms => ?HEARTBEAT_INTERVAL_MS,
            subscription_callback => {test_membership_callback, undefined},
            assignment_callback => {test_assignment_callback, undefined},
            assignors => [test_assignor]
        },
        Topics
    ),

    {ok, R2} = kafine_eager_rebalance:start_link(
        make_ref(),
        Broker,
        #{client_id => <<"member-2">>},
        GroupId,
        #{
            heartbeat_interval_ms => ?HEARTBEAT_INTERVAL_MS,
            subscription_callback => {test_membership_callback, undefined},
            assignment_callback => {test_assignment_callback, undefined},
            assignors => [test_assignor]
        },
        Topics
    ),

    % Wait until they've both joined.
    receive
        {[kafine, rebalance, leader], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,
    receive
        {[kafine, rebalance, follower], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,

    % The assignor should be called with the initial user data being empty.
    ?assertCalled(test_assignor, assign, ['_', '_', <<>>]),

    meck:reset(test_assignor),
    meck:reset(test_membership_callback),

    % We're going to trigger a rebalance and swap the leader. Set that up here.

    % Wait for both members to join, then tell one of them that they're the leader.
    meck:expect(kamock_join_group, handle_join_group_request, [
        % Use the client ID as the member ID.
        {[meck:is(HasNoMemberId), '_'], fun(
            #{correlation_id := CorrelationId, client_id := ClientId}, _
        ) ->
            kamock_join_group:member_id_required(CorrelationId, ClientId)
        end},

        % We've got a member ID, assign the leader and follower appropriately (reverse from above).
        {
            [meck:is(HasMemberId(<<"member-2">>)), '_'],
            kamock_join_group:as_leader(?GROUP_GENERATION_2)
        },
        {
            [meck:is(HasMemberId(<<"member-1">>)), '_'],
            kamock_join_group:as_follower(<<"member-2">>, ?GROUP_GENERATION_2)
        }
    ]),

    % There's no need to swap the assignments, so we'll leave the previous SyncGroup mock alone.

    % Trigger the rebalance.
    meck:expect(
        kamock_heartbeat,
        handle_heartbeat_request,
        kamock_heartbeat:expect_generation_id(?GROUP_GENERATION_2)
    ),

    % Wait until they've both re-joined.
    receive
        {[kafine, rebalance, leader], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,
    receive
        {[kafine, rebalance, follower], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,

    % The (new) leader should have called the assignor with the shared user data.
    ?assertCalled(test_assignor, assign, ['_', '_', AssignmentUserData]),

    telemetry:detach(TelemetryRef),

    kafine_eager_rebalance:stop(R1),
    kafine_eager_rebalance:stop(R2),
    kamock_broker:stop(Broker),
    ok.
