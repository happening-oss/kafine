-module(kafine_eager_rebalance).

-feature(maybe_expr, enable).

%%% Implementation of https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal
%%%
%%% It's named "eager" to distinguish it from the newer, "cooperative" rebalance protocol (which we don't support yet).

-export([
    start_link/4,
    stop/1,

    whereis/1,
    info/1,

    offset_commit/2
]).
-behaviour(gen_statem).
-export([
    init/1,
    callback_mode/0,
    handle_event/4,
    terminate/3
]).
-export_type([
    ref/0
]).

-include_lib("kernel/include/logger.hrl").

-type ref() :: any().

-spec start_link(
    Ref :: ref(),
    Topics :: [kafine:topic()],
    GroupId :: binary(),
    MembershipOptions :: kafine:membership_options()
) -> gen_statem:start_ret().

start_link(Ref, Topics, GroupId, MembershipOptions) ->
    gen_statem:start_link(
        via(Ref),
        ?MODULE,
        [Ref, Topics, GroupId, MembershipOptions],
        start_options()
    ).

stop(Pid) ->
    gen_statem:stop(Pid).

start_options() -> [{debug, kafine_trace:debug_options(#{mfa => {?MODULE, handle_event, 4}})}].

-spec info(RefOrPid :: ref() | pid()) ->
    #{
        state := dynamic(),
        topics := [kafine:topic()],
        group_id := binary(),
        member_id := binary(),
        generation_id := integer(),
        role := role(),
        membership_options := kafine:membership_options(),
        subscription_callback := {module(), dynamic()},
        assignment_callback := {module(), dynamic()},
        assignment := #{
            user_data := kafine_assignor:user_data(),
            assigned_partitions := #{kafine:topic() => [non_neg_integer()]}
        }
    }.

info(Pid) when is_pid(Pid) ->
    gen_statem:call(Pid, info);
info(Ref) ->
    gen_statem:call(via(Ref), info).

id(Ref) -> {?MODULE, Ref}.

via(Ref) ->
    kafine_via:via(id(Ref)).

-spec whereis(Ref :: ref()) -> pid() | undefined.

whereis(Ref) ->
    kafine_via:whereis_name(id(Ref)).

-spec offset_commit(
    Ref :: ref(), Offsets :: #{kafine:topic() => #{kafine:partition() => kafine:offset()}}
) ->
    {
        ok,
        kafine_topic_partition_data:t(ok | {kafka_error, integer()}),
        ThrottleTimeMs :: non_neg_integer()
    }
    | {error, dynamic()}.

offset_commit(Ref, Offsets) ->
    gen_statem:call(via(Ref), {offset_commit, Offsets}).

-type role() :: leader | follower | unknown.

-record(state, {
    ref :: ref(),
    topics :: [kafine:topic()],
    group_id :: binary(),
    % The broker will give us a member_id later. See KIP-394.
    member_id = <<>> :: binary(),
    generation_id = -1 :: integer(),
    role = unknown :: role(),

    membership_options :: kafine:membership_options(),
    subscription_callback :: {module(), term()},
    assignment_callback :: {module(), term()},
    assignment :: #{
        user_data => kafine_assignor:user_data(),
        assigned_partitions => #{kafine:topic() => [non_neg_integer()]}
    },
    rebalance_req_id = kafine_coordinator:reqids_new() :: kafine_coordinator:req_ids(),
    offset_commit_req_ids = kafine_coordinator:reqids_new() :: kafine_coordinator:req_ids(),
    rebalance_span = undefined :: kafine_telemetry:span() | undefined
}).

init([
    Ref,
    Topics,
    GroupId,
    MembershipOptions = #{
        subscription_callback := {SubscriptionCallback, MembershipArgs},
        assignment_callback := {AssignmentCallback, AssignmentArgs}
    }
]) ->
    % We trap exits so that we can leave the group on death.
    process_flag(trap_exit, true),
    Metadata = #{ref => Ref, group_id => GroupId, member_id => <<>>},
    logger:set_process_metadata(Metadata),
    kafine_proc_lib:set_label({?MODULE, {Ref, GroupId}}),

    {ok, SubscriptionState} = SubscriptionCallback:init(MembershipArgs),
    {ok, AssignmentState} = AssignmentCallback:init(AssignmentArgs),

    StateData = #state{
        ref = Ref,
        topics = Topics,
        group_id = GroupId,
        membership_options = MembershipOptions,
        subscription_callback = {SubscriptionCallback, SubscriptionState},
        assignment_callback = {AssignmentCallback, AssignmentState},
        assignment = #{user_data => <<>>, assigned_partitions => #{}}
    },

    {ok, get_member_id, StateData}.

callback_mode() ->
    [handle_event_function, state_enter].

handle_event(
    enter,
    _,
    get_member_id,
    StateData = #state{ref = Ref}
) ->
    ReqId = kafine_coordinator:join_group(Ref, <<>>),
    {keep_state, StateData#state{rebalance_req_id = ReqId}};
handle_event(
    internal,
    {response, {error, {member_id_required, MemberId}}},
    get_member_id,
    StateData = #state{
        ref = Ref,
        group_id = GroupId
    }
) ->
    logger:update_process_metadata(#{member_id => MemberId}),
    Span = kafine_telemetry:start_span(
        [kafine, rebalance],
        #{ref => Ref, group_id => GroupId, member_id => MemberId}
    ),
    NewStateData = StateData#state{member_id = MemberId, rebalance_span = Span},
    {next_state, join_group, NewStateData};
handle_event(
    enter,
    _,
    join_group,
    StateData = #state{
        ref = Ref,
        member_id = MemberId,
        assignment = Assignment = #{assigned_partitions := TopicPartitions},
        subscription_callback = {SubscriptionCallback, SubscriptionState0},
        assignment_callback = {AssignmentCallback, AssignmentState0}
    }
) ->
    % From https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb/
    %      (go to #incremental-cooperative-rebalancing-protocol)
    %
    % ... "each member is required to revoke all of its owned partitions before sending a JoinGroup request"
    {ok, SubscriptionState, AssignmentState} = kafine_assignment:revoke_assignments(
        TopicPartitions,
        {SubscriptionCallback, SubscriptionState0},
        {AssignmentCallback, AssignmentState0}
    ),

    % Second join request now that member id is set
    % This is expected; see KIP-394.
    ReqId = kafine_coordinator:join_group(Ref, MemberId),
    StateData2 = StateData#state{
        subscription_callback = {SubscriptionCallback, SubscriptionState},
        assignment_callback = {AssignmentCallback, AssignmentState},
        assignment = Assignment#{assigned_partitions => kafine_topic_partitions:new()},
        rebalance_req_id = ReqId
    },

    {keep_state, StateData2};
handle_event(
    internal,
    {response,
        {ok, #{
            generation_id := GenerationId,
            leader := LeaderId,
            protocol_name := ProtocolName,
            members := Members
        }}},
    join_group,
    StateData = #state{
        ref = Ref,
        topics = Topics,
        group_id = GroupId,
        member_id = MemberId,
        membership_options = #{
            assignors := Assignors
        },
        assignment = #{user_data := ExistingAssignmentUserData}
    }
) ->
    telemetry:execute([kafine, rebalance, join_group], #{}, #{
        ref => Ref, protocol_name => ProtocolName, group_id => GroupId
    }),

    StateData2 = StateData#state{generation_id = GenerationId},

    case LeaderId =:= MemberId of
        true ->
            ?LOG_INFO("We are the leader with member ID ~p", [MemberId]),
            ?LOG_DEBUG("Members = ~p", [Members]),

            {value, Assignor} = lists:search(
                fun(Assignor) ->
                    Assignor:name() == ProtocolName
                end,
                Assignors
            ),

            TopicPartitionInfo = kafine_metadata_cache:partitions(Ref, Topics),
            TopicPartitions = maps:map(
                fun(_Topic, Partitions) -> maps:keys(Partitions) end, TopicPartitionInfo
            ),
            ?LOG_DEBUG("TopicPartitions = ~p", [TopicPartitions]),

            Assignments = Assignor:assign(Members, TopicPartitions, ExistingAssignmentUserData),
            ?LOG_DEBUG("Assignments = ~p", [Assignments]),

            {next_state, {sync_group, ProtocolName, Assignments}, StateData2#state{role = leader}};
        false ->
            ?LOG_INFO("We are a follower with member ID ~p", [MemberId]),

            {next_state, {sync_group, ProtocolName, #{}}, StateData2#state{role = follower}}
    end;
handle_event(
    enter,
    _,
    {sync_group, ProtocolName, Assignments},
    StateData = #state{
        ref = Ref,
        member_id = MemberId,
        generation_id = GenerationId
    }
) ->
    ReqId = kafine_coordinator:sync_group(Ref, MemberId, GenerationId, ProtocolName, Assignments),
    {keep_state, StateData#state{rebalance_req_id = ReqId}};
handle_event(
    internal,
    {response, {ok, Assignment}},
    {sync_group, _, _},
    StateData = #state{
        ref = Ref,
        group_id = GroupId,
        member_id = MemberId,
        role = Role,
        subscription_callback = {SubscriptionCallback, SubscriptionState0},
        assignment_callback = {AssignmentCallback, AssignmentState0},
        assignment = #{assigned_partitions := PreviousTopicPartitionAssignments},
        rebalance_span = Span
    }
) ->
    #{assigned_partitions := TopicPartitionAssignments} = Assignment,
    ?LOG_INFO("Assignment = ~p", [Assignment]),

    {ok, SubscriptionState, AssignmentState} = kafine_assignment:handle_assignment(
        TopicPartitionAssignments,
        PreviousTopicPartitionAssignments,
        undefined,
        {AssignmentCallback, AssignmentState0},
        {SubscriptionCallback, SubscriptionState0}
    ),

    kafine_telemetry:stop_span([kafine, rebalance], Span),
    telemetry:execute([kafine, rebalance, Role], #{}, #{
        ref => Ref,
        member_id => MemberId,
        group_id => GroupId,
        assignment => TopicPartitionAssignments,
        previous_assignment => PreviousTopicPartitionAssignments
    }),

    StateData2 = StateData#state{
        subscription_callback = {SubscriptionCallback, SubscriptionState},
        assignment_callback = {AssignmentCallback, AssignmentState},
        assignment = Assignment,
        rebalance_span = undefined
    },

    {next_state, ready, StateData2};
handle_event(
    enter,
    _,
    ready,
    _StateData = #state{
        role = Role,
        membership_options = #{heartbeat_interval_ms := HeartbeatIntervalMs}
    }
) ->
    ?LOG_INFO("Rebalanced as ~p, start heartbeating at ~p", [Role, HeartbeatIntervalMs]),
    {keep_state_and_data, {state_timeout, HeartbeatIntervalMs, heartbeat}};
handle_event(
    state_timeout,
    heartbeat,
    ready,
    StateData = #state{
        ref = Ref,
        member_id = MemberId,
        generation_id = GenerationId
    }
) ->
    ReqId = kafine_coordinator:heartbeat(Ref, MemberId, GenerationId),
    {keep_state, StateData#state{rebalance_req_id = ReqId}};
handle_event(
    internal,
    {response, ok},
    ready,
    _StateData = #state{
        membership_options = #{heartbeat_interval_ms := HeartbeatIntervalMs}
    }
) ->
    % Heartbeat response
    {keep_state_and_data, {state_timeout, HeartbeatIntervalMs, heartbeat}};
handle_event(
    internal,
    {response, {error, rebalance_in_progress}},
    ready,
    StateData = #state{
        ref = Ref,
        group_id = GroupId,
        member_id = MemberId
    }
) ->
    ?LOG_NOTICE("Group rebalance in progress; re-joining group"),

    Span = kafine_telemetry:start_span(
        [kafine, rebalance],
        #{ref => Ref, group_id => GroupId, member_id => MemberId}
    ),

    NewStateData = StateData#state{role = unknown, rebalance_span = Span},
    {next_state, join_group, NewStateData};
handle_event(
    {call, From},
    {offset_commit, Offsets},
    ready,
    StateData = #state{
        ref = Ref,
        member_id = MemberId,
        generation_id = GenerationId,
        offset_commit_req_ids = ReqIds
    }
) ->
    NewReqIds = kafine_coordinator:offset_commit(
        Ref, MemberId, GenerationId, Offsets, From, ReqIds
    ),
    {keep_state, StateData#state{offset_commit_req_ids = NewReqIds}};
handle_event(
    internal,
    {offset_commit_response, From, Result},
    ready,
    _StateData
) ->
    {keep_state_and_data, {reply, From, Result}};
handle_event(
    {call, From},
    {offset_commit, _Offsets},
    _State,
    _StateData
) ->
    {keep_state_and_data, {reply, From, {error, not_joined}}};
handle_event(
    {call, From},
    info,
    State,
    #state{
        topics = Topics,
        group_id = GroupId,
        member_id = MemberId,
        generation_id = GenerationId,
        role = Role,
        membership_options = MembershipOptions,
        subscription_callback = SubscriptionCallback,
        assignment_callback = AssignmentCallback,
        assignment = Assignment
    }
) ->
    Info = #{
        state => State,
        topics => Topics,
        group_id => GroupId,
        member_id => MemberId,
        generation_id => GenerationId,
        role => Role,
        membership_options => MembershipOptions,
        subscription_callback => SubscriptionCallback,
        assignment_callback => AssignmentCallback,
        assignment => Assignment
    },
    {keep_state_and_data, {reply, From, Info}};
handle_event(
    info,
    Msg,
    _State,
    StateData
) ->
    maybe
        no_response ?= check_rebalance_response(Msg, StateData),
        no_response ?= check_offset_commit_response(Msg, StateData),
        keep_state_and_data
    else
        Response -> Response
    end.

check_rebalance_response(Msg, StateData = #state{rebalance_req_id = RebalanceReqId}) ->
    case kafine_coordinator:check_response(Msg, RebalanceReqId) of
        {{reply, Reply}, _, _} ->
            {keep_state, StateData#state{rebalance_req_id = kafine_coordinator:reqids_new()},
                {next_event, internal, {response, Reply}}};
        {{error, {Reason, _}}, _, _} ->
            {keep_state, StateData#state{rebalance_req_id = kafine_coordinator:reqids_new()},
                {next_event, internal, {response, {error, Reason}}}};
        _ ->
            no_response
    end.

check_offset_commit_response(Msg, StateData = #state{offset_commit_req_ids = OffsetCommitReqIds}) ->
    case kafine_coordinator:check_response(Msg, OffsetCommitReqIds) of
        {{reply, Reply}, From, NewReqIds} ->
            {keep_state, StateData#state{offset_commit_req_ids = NewReqIds},
                {next_event, internal, {offset_commit_response, From, Reply}}};
        {{error, {Reason, _}}, From, NewReqIds} ->
            {keep_state, StateData#state{offset_commit_req_ids = NewReqIds},
                {next_event, internal, {offset_commit_response, From, {error, Reason}}}};
        _ ->
            no_response
    end.

terminate(
    _Reason,
    _State,
    _StateData = #state{
        ref = Ref,
        member_id = MemberId
    }
) ->
    case MemberId of
        <<>> ->
            ok;
        _ ->
            ?LOG_INFO("Leaving group"),
            kafine_coordinator:leave_group(Ref, MemberId)
    end,
    ok.
