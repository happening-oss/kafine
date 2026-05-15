-module(kafine_eager_rebalance_owned_partitions_tests).
-include_lib("eunit/include/eunit.hrl").
-include("assert_meck.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(GROUP_ID, iolist_to_binary(io_lib:format("~s___~s_g", [?MODULE, ?FUNCTION_NAME]))).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(CALLBACK_STATE, {state, ?MODULE}).
-define(REBALANCE_REF, {?MODULE, ?FUNCTION_NAME}).
-define(GROUP_GENERATION_1, 1).
-define(GROUP_GENERATION_2, 2).

% Usually 3s, but we want something quicker for the tests.
-define(HEARTBEAT_INTERVAL_MS, 30).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun join_new_group_as_leader/0
    ]}.

setup() ->
    meck:new(test_subscription_callback, [non_strict]),
    meck:expect(test_subscription_callback, init, fun(_) -> {ok, ?CALLBACK_STATE} end),
    meck:expect(test_subscription_callback, subscribe_partitions, fun(_, _, St) -> {ok, St} end),
    meck:expect(test_subscription_callback, unsubscribe_partitions, fun(St) -> {ok, St} end),

    meck:new(test_assignment_callback, [non_strict]),
    meck:expect(test_assignment_callback, init, fun(_) -> {ok, ?CALLBACK_STATE} end),
    meck:expect(test_assignment_callback, before_assignment, fun(_, _, St) -> {ok, St} end),
    meck:expect(test_assignment_callback, after_assignment, fun(_, _, St) -> {ok, St} end),

    % We'll forward to the range assignor, but we want to make sure that we actually pay attention to the configuration.
    meck:new(test_assignor, [non_strict]),
    meck:expect(test_assignor, name, fun() -> <<"test">> end),
    meck:expect(test_assignor, assign, fun kafine_range_assignor:assign/3),

    % One of the tests requires multiple assignors; we provide a second one here.
    meck:new(test_assignor2, [non_strict]),
    meck:expect(test_assignor2, name, fun() -> <<"test2">> end),
    meck:expect(test_assignor2, assign, fun kafine_range_assignor:assign/3),

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

    GroupId = ?GROUP_ID,
    Topics = [?TOPIC_NAME],
    MembershipOptions = kafine_membership_options:validate_options(#{
        heartbeat_interval_ms => ?HEARTBEAT_INTERVAL_MS,
        subscription_callback => {test_subscription_callback, undefined},
        assignment_callback => {test_assignment_callback, undefined},
        assignors => [test_assignor]
    }),

    {ok, B} = kafine_bootstrap:start_link(?REBALANCE_REF, Broker, #{}),
    {ok, M} = kafine_metadata_cache:start_link(?REBALANCE_REF),
    {ok, C} = kafine_coordinator:start_link(
        ?REBALANCE_REF, GroupId, Topics, #{}, MembershipOptions
    ),

    meck:expect(
        kamock_join_group,
        handle_join_group_request,
        kamock_join_group:as_leader(?GROUP_GENERATION_1)
    ),

    {ok, R} = kafine_eager_rebalance:start_link(?REBALANCE_REF, Topics, GroupId, MembershipOptions),

    % Wait until we've joined.
    receive
        {[kafine, rebalance, leader], TelemetryRef, #{}, #{
            group_id := GroupId, generation_id := ?GROUP_GENERATION_1
        }} ->
            ok
    end,

    % Was the assignor called? Did it get given any owned_partitions?
    ?assertCalled(test_assignor, assign, [meck:is(has_owned_partitions([])), '_', '_']),

    meck:reset(test_assignor),

    meck:expect(
        kamock_join_group,
        handle_join_group_request,
        kamock_join_group:as_leader(?GROUP_GENERATION_2)
    ),

    % Trigger a rebalance
    meck:expect(
        kamock_heartbeat,
        handle_heartbeat_request,
        kamock_heartbeat:expect_generation_id(?GROUP_GENERATION_2)
    ),

    % Wait until we've rejoined
    receive
        {[kafine, rebalance, leader], TelemetryRef, #{}, #{
            group_id := GroupId, generation_id := ?GROUP_GENERATION_2
        }} ->
            ok
    end,

    % Was the assignor called? Did it get given any owned_partitions?
    ?assertCalled(test_assignor, assign, [
        % The assignor gets called with Kafka-format owned-partitions.
        meck:is(has_owned_partitions([#{topic => ?TOPIC_NAME, partitions => [0, 1, 2, 3]}])),
        '_',
        '_'
    ]),

    telemetry:detach(TelemetryRef),

    kafine_eager_rebalance:stop(R),
    kafine_coordinator:stop(C),
    kafine_metadata_cache:stop(M),
    kafine_bootstrap:stop(B),
    kamock_broker:stop(Broker),
    ok.

has_owned_partitions(ExpectedOwnedPartitions) when is_list(ExpectedOwnedPartitions) ->
    % Expect a single member with the given owned_partitions.
    fun([_Member = #{member_id := _, metadata := SubscriptionMetadata}]) ->
        #{owned_partitions := OwnedPartitions} = SubscriptionMetadata,
        OwnedPartitions == ExpectedOwnedPartitions
    end.
