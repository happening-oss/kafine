-module(kafine_eager_rebalance).

%%% Implementation of https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal
%%%
%%% It's named "eager" to distinguish it from the newer, "cooperative" rebalance protocol (which we don't support yet).

-export([
    start_link/6,
    stop/1,
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

-include_lib("kafcod/include/error_code.hrl").
-include_lib("kafcod/include/coordinator_type.hrl").
-include_lib("kafcod/include/api_key.hrl").

-define(PROTOCOL_TYPE, <<"consumer">>).

-type ref() :: any().

start_link(Ref, Bootstrap, ConnectionOptions, GroupId, MembershipOptions, Topics) ->
    gen_statem:start_link(
        via(Ref),
        ?MODULE,
        [
            Ref,
            Bootstrap,
            ConnectionOptions,
            GroupId,
            kafine_membership_options:validate_options(MembershipOptions),
            Topics
        ],
        start_options()
    ).

stop(Pid) ->
    gen_statem:stop(Pid).

start_options() -> [{debug, kafine_trace:debug_options(#{mfa => {?MODULE, handle_event, 4}})}].

-spec via(Ref :: ref()) -> {via, kafine_via, {module(), ref()}}.

via(Ref) ->
    {via, kafine_via, {?MODULE, Ref}}.

-spec offset_commit(
    Ref :: ref(), Offsets :: #{kafine:topic() => #{kafine:partition() => kafine:offset()}}
) -> offset_commit_response:offset_commit_response_3().

offset_commit(Ref, Offsets) ->
    gen_statem:call(via(Ref), {offset_commit, Offsets}).

% We use complex states; this is the one for when we've just been elected as the leader, and we're in the
% Metadata/SyncGroup stage, when we need to hold some data between steps.
-record(leader_sync, {
    assignor,
    members,
    protocol_name
}).

-record(state, {
    broker :: kafine:broker(),
    group_id :: binary(),
    generation_id :: integer(),
    member_id :: binary(),
    topics :: [kafine:topic()],

    assignors :: [module()],
    connection :: kafine:connection() | undefined,
    connection_options :: kafine:connection_options(),
    membership_options :: kafine:membership_options(),
    subscription_callback :: {module(), term()},
    assignment_callback :: {module(), term()},
    assignment :: #{
        user_data => kafine_assignor:user_data(),
        assigned_partitions => #{Topic :: binary() => [non_neg_integer()]}
    },
    req_ids :: kafine_connection:request_id_collection()
}).

init([
    Ref,
    Bootstrap,
    ConnectionOptions,
    GroupId,
    MembershipOptions = #{
        subscription_callback := {SubscriptionCallback, MembershipArgs},
        assignment_callback := {AssignmentCallback, AssignmentArgs},
        assignors := Assignors
    },
    Topics
]) ->
    % We trap exits so that we can leave the group on death.
    process_flag(trap_exit, true),
    kafine_proc_lib:set_label({?MODULE, Ref, GroupId}),

    {ok, SubscriptionState} = SubscriptionCallback:init(MembershipArgs),
    {ok, AssignmentState} = AssignmentCallback:init(AssignmentArgs),

    StateData = #state{
        broker = Bootstrap,
        group_id = GroupId,
        generation_id = -1,
        % The broker will give us a member_id later. See KIP-394.
        member_id = <<>>,
        topics = Topics,
        assignors = Assignors,
        connection = undefined,
        connection_options = ConnectionOptions,
        membership_options = MembershipOptions,
        subscription_callback = {SubscriptionCallback, SubscriptionState},
        assignment_callback = {AssignmentCallback, AssignmentState},
        assignment = #{user_data => <<>>, assigned_partitions => #{}},
        req_ids = kafine_connection:reqids_new()
    },

    {ok, init, StateData, [
        {next_event, internal, connect}
    ]}.

callback_mode() ->
    [handle_event_function].

handle_event(
    internal,
    connect,
    init,
    StateData0
) ->
    StateData = do_connect(StateData0),
    {keep_state, StateData, [{next_event, internal, find_coordinator}]};
handle_event(
    internal,
    connect,
    find_coordinator,
    StateData0
) ->
    StateData = do_connect(StateData0),
    {keep_state, StateData, [{next_event, internal, find_coordinator}]};
handle_event(
    internal,
    connect,
    State,
    StateData0
) when is_record(State, leader_sync); State =:= follower_sync; State =:= join_group ->
    StateData = do_connect(StateData0),
    {next_state, find_coordinator, StateData, [{next_event, internal, join_group}]};
handle_event(
    internal,
    connect,
    State,
    StateData0
) when State =:= follower; State =:= leader ->
    StateData = do_connect(StateData0),
    {keep_state, StateData, []};
handle_event(
    internal,
    find_coordinator,
    _State,
    StateData = #state{group_id = GroupId, connection = Connection, req_ids = ReqIds}
) ->
    FindCoordinatorRequest = #{
        key_type => ?COORDINATOR_TYPE_GROUP,
        key => GroupId
    },
    ReqIds2 = kafine_connection:send_request(
        Connection,
        fun find_coordinator_request:encode_find_coordinator_request_3/1,
        FindCoordinatorRequest,
        fun find_coordinator_response:decode_find_coordinator_response_3/1,
        find_coordinator,
        ReqIds,
        kafine_request_telemetry:request_labels(?FIND_COORDINATOR, 3, GroupId)
    ),
    {next_state, find_coordinator, StateData#state{req_ids = ReqIds2}};
handle_event(
    internal,
    join_group,
    _State,
    StateData = #state{
        group_id = GroupId,
        member_id = MemberId,
        topics = Topics,
        assignors = Assignors,
        membership_options = MembershipOptions,
        connection = Connection,
        req_ids = ReqIds
    }
) when MemberId =:= <<>> ->
    % MemberId is initially empty so this first join group will return a MEMBER_ID_REQUIRED error
    % which sets our member id to a coordinator chosen value.
    % This is expected; see KIP-394.
    JoinGroupRequest = build_join_request(Topics, Assignors, MemberId, GroupId, MembershipOptions),

    ReqIds2 = kafine_connection:send_request(
        Connection,
        fun join_group_request:encode_join_group_request_7/1,
        JoinGroupRequest,
        fun join_group_response:decode_join_group_response_7/1,
        join_group,
        ReqIds,
        kafine_request_telemetry:request_labels(?JOIN_GROUP, 7, GroupId)
    ),
    {next_state, join_group, StateData#state{
        req_ids = ReqIds2
    }};
handle_event(
    internal,
    join_group,
    _State,
    StateData = #state{
        group_id = GroupId,
        member_id = MemberId,
        topics = Topics,
        assignors = Assignors,
        connection = Connection,
        req_ids = ReqIds,
        assignment = Assignments,
        membership_options = MembershipOptions,
        subscription_callback = {SubscriptionCallback, SubscriptionState0},
        assignment_callback = {AssignmentCallback, AssignmentState0}
    }
) ->
    % From https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb/
    %      (go to #incremental-cooperative-rebalancing-protocol)
    %
    % ... "each member is required to revoke all of its owned partitions before sending a JoinGroup request"
    {ok, SubscriptionState, AssignmentState} = kafine_assignment:revoke_assignments(
        Assignments,
        {SubscriptionCallback, SubscriptionState0},
        {AssignmentCallback, AssignmentState0}
    ),

    % Second join request now that member id is set
    % This is expected; see KIP-394.
    JoinGroupRequest = build_join_request(Topics, Assignors, MemberId, GroupId, MembershipOptions),

    ReqIds2 = kafine_connection:send_request(
        Connection,
        fun join_group_request:encode_join_group_request_7/1,
        JoinGroupRequest,
        fun join_group_response:decode_join_group_response_7/1,
        join_group,
        ReqIds,
        kafine_request_telemetry:request_labels(?JOIN_GROUP, 7, GroupId)
    ),
    {next_state, join_group, StateData#state{
        req_ids = ReqIds2,
        subscription_callback = {SubscriptionCallback, SubscriptionState},
        assignment_callback = {AssignmentCallback, AssignmentState},
        assignment = Assignments#{assigned_partitions => #{}}
    }};
handle_event(
    internal,
    {metadata, Topics},
    _State,
    StateData = #state{connection = Connection, req_ids = ReqIds}
) ->
    MetadataRequest = #{
        topics => [#{name => T} || T <- Topics],
        allow_auto_topic_creation => false,
        include_cluster_authorized_operations => false,
        include_topic_authorized_operations => false
    },

    ReqIds2 = kafine_connection:send_request(
        Connection,
        fun metadata_request:encode_metadata_request_9/1,
        MetadataRequest,
        fun metadata_response:decode_metadata_response_9/1,
        metadata,
        ReqIds,
        kafine_request_telemetry:request_labels(?METADATA, 9)
    ),
    {keep_state, StateData#state{req_ids = ReqIds2}};
handle_event(
    internal,
    {sync_group, {ProtocolName, Assignments}},
    _State,
    StateData = #state{
        group_id = GroupId,
        generation_id = GenerationId,
        member_id = MemberId,
        connection = Connection,
        req_ids = ReqIds
    }
) ->
    SyncGroupRequest = #{
        group_id => GroupId,
        generation_id => GenerationId,
        member_id => MemberId,
        group_instance_id => null,
        protocol_type => ?PROTOCOL_TYPE,
        protocol_name => ProtocolName,
        assignments => kafcod_consumer_protocol:encode_assignments(Assignments)
    },

    ReqIds2 = kafine_connection:send_request(
        Connection,
        fun sync_group_request:encode_sync_group_request_5/1,
        SyncGroupRequest,
        fun sync_group_response:decode_sync_group_response_5/1,
        sync_group,
        ReqIds,
        kafine_request_telemetry:request_labels(?SYNC_GROUP, 5, GroupId)
    ),
    {keep_state, StateData#state{req_ids = ReqIds2}};
handle_event(info, Info, State, StateData = #state{req_ids = ReqIds}) ->
    % We can't tell the difference between send_request responses and normal info messages, so we have to check them
    % first.
    check_response(kafine_connection:check_response(Info, ReqIds), Info, State, StateData);
handle_event(
    state_timeout,
    heartbeat,
    _State,
    StateData = #state{
        group_id = GroupId,
        generation_id = GenerationId,
        member_id = MemberId,
        connection = Connection,
        req_ids = ReqIds
    }
) ->
    HeartbeatRequest = #{
        group_id => GroupId,
        generation_id => GenerationId,
        member_id => MemberId,
        group_instance_id => null
    },
    ReqIds2 = kafine_connection:send_request(
        Connection,
        fun heartbeat_request:encode_heartbeat_request_4/1,
        HeartbeatRequest,
        fun heartbeat_response:decode_heartbeat_response_4/1,
        heartbeat,
        ReqIds,
        kafine_request_telemetry:request_labels(?HEARTBEAT, 4, GroupId)
    ),
    {keep_state, StateData#state{req_ids = ReqIds2}};
handle_event(
    {call, From},
    {offset_commit, Offsets},
    _State,
    _StateData = #state{
        connection = Connection,
        generation_id = GenerationId,
        group_id = GroupId,
        member_id = MemberId
    }
) ->
    OffsetCommitRequest = #{
        group_id => GroupId,
        generation_id_or_member_epoch => GenerationId,
        member_id => MemberId,
        retention_time_ms => -1,
        topics => convert_offset_commit(Offsets)
    },
    {ok, Response} = kafine_connection:call(
        Connection,
        fun offset_commit_request:encode_offset_commit_request_3/1,
        OffsetCommitRequest,
        fun offset_commit_response:decode_offset_commit_response_3/1,
        kafine_request_telemetry:request_labels(?OFFSET_COMMIT, 3, GroupId)
    ),
    % TODO: Ideally, we wouldn't return the Kafka response -- because it couples the caller to the wire representation.
    {keep_state_and_data, {reply, From, Response}}.

convert_offset_commit(Offsets) ->
    maps:fold(
        fun(Topic, PartitionOffsets, Acc) ->
            [
                #{
                    name => Topic,
                    partitions => maps:fold(
                        fun(PartitionIndex, CommittedOffset, Acc2) ->
                            [
                                #{
                                    partition_index => PartitionIndex,
                                    committed_offset => CommittedOffset,
                                    % Yes, committed_metadata is nullable, but when you OffsetFetch a brand-new group,
                                    % you get an empty binary, so we'll do that.
                                    committed_metadata => <<>>
                                }
                                | Acc2
                            ]
                        end,
                        [],
                        PartitionOffsets
                    )
                }
                | Acc
            ]
        end,
        [],
        Offsets
    ).

check_response(_Result = {Response, Label, ReqIds2}, _Info, State, StateData) ->
    % Important: Because we manipulate StateData here, you MUST NOT use keep_state_and_data.
    handle_response(Response, Label, State, StateData#state{req_ids = ReqIds2});
check_response(_Other, Info, State, StateData) ->
    % Normal info message. Ignored.
    handle_info(Info, State, StateData).

handle_info(
    {'EXIT', Connection, _Reason},
    _State,
    StateData = #state{connection = Connection}
) ->
    ?LOG_WARNING("Connection closed; reconnecting"),
    telemetry:execute([kafine, rebalance, disconnected], #{}),
    {keep_state, StateData, [{next_event, internal, connect}]};
handle_info(_Info, _State, _StateData) ->
    % Normal info message; ignore it.
    keep_state_and_data.

do_connect(StateData = #state{broker = Broker, connection_options = ConnectionOptions}) ->
    {ok, Connection} = kafine_connection:start_link(Broker, ConnectionOptions),
    telemetry:execute([kafine, rebalance, connected], #{}),
    StateData#state{connection = Connection}.

handle_response(
    {ok, FindCoordinatorResponse}, find_coordinator, State = find_coordinator, StateData
) ->
    handle_find_coordinator_response(FindCoordinatorResponse, State, StateData);
handle_response({ok, JoinGroupResponse}, join_group, State = join_group, StateData) ->
    handle_join_group_response(JoinGroupResponse, State, StateData);
handle_response({ok, MetadataResponse}, metadata, State, StateData) ->
    handle_metadata_response(MetadataResponse, State, StateData);
handle_response({ok, SyncGroupResponse}, sync_group, State, StateData) ->
    handle_sync_group_response(SyncGroupResponse, State, StateData);
handle_response({ok, HeartbeatResponse}, heartbeat, State, StateData) ->
    handle_heartbeat_response(HeartbeatResponse, State, StateData);
handle_response({error, {closed, _PreviousConnection}}, _, _, StateData) ->
    % This response comes from before a disconnect event; by this point we have already
    % reconnected and made new requests.
    % Alteratively this response has arrived before the 'EXIT' event
    {keep_state, StateData, []}.

handle_find_coordinator_response(#{error_code := ErrorCode}, _State, StateData) when
    ErrorCode == ?COORDINATOR_LOAD_IN_PROGRESS; ErrorCode == ?COORDINATOR_NOT_AVAILABLE
->
    % TODO: We might want a short delay here.
    ?LOG_WARNING("Coordinator not available; retrying"),
    {repeat_state, StateData, [{next_event, internal, find_coordinator}]};
handle_find_coordinator_response(
    #{error_code := ?NONE, host := Host, port := Port, node_id := NodeId},
    _State,
    StateData = #state{
        broker = #{host := Host, port := Port},
        group_id = GroupId
    }
) ->
    ?LOG_INFO("Coordinator for group '~s' is node ~B, at ~s:~B, reusing current connection", [
        GroupId, NodeId, Host, Port
    ]),
    {keep_state, StateData, [{next_event, internal, join_group}]};
handle_find_coordinator_response(
    #{error_code := ?NONE, host := Host, port := Port, node_id := NodeId},
    _State,
    StateData = #state{
        group_id = GroupId,
        connection = Connection,
        connection_options = ConnectionOptions
    }
) ->
    ?LOG_INFO("Coordinator for group '~s' is node ~B, at ~s:~B", [
        GroupId, NodeId, Host, Port
    ]),
    kafine_connection:stop(Connection),

    Coordinator = #{host => Host, port => Port, node_id => NodeId},
    {ok, CoordinatorConnection} = kafine_connection:start_link(Coordinator, ConnectionOptions),
    StateData2 = StateData#state{connection = CoordinatorConnection, broker = Coordinator},
    {keep_state, StateData2, [{next_event, internal, join_group}]}.

handle_join_group_response(
    #{error_code := ?NOT_COORDINATOR}, _State, StateData
) ->
    % Coordinator moved; find out where it went.
    {keep_state, StateData, [{next_event, internal, find_coordinator}]};
handle_join_group_response(
    #{error_code := ?MEMBER_ID_REQUIRED, member_id := MemberId}, _State, StateData
) ->
    % Expected; see KIP-394.
    {keep_state, StateData#state{member_id = MemberId}, [{next_event, internal, join_group}]};
handle_join_group_response(
    #{
        error_code := ?NONE,
        generation_id := GenerationId,
        leader := LeaderId,
        members := Members0,
        protocol_name := ProtocolName
    },
    _State,
    StateData = #state{
        group_id = GroupId,
        member_id = MemberId,
        assignors = Assignors
    }
) when LeaderId =:= MemberId ->
    ?LOG_INFO("We are the leader with member ID ~p", [MemberId]),

    telemetry:execute([kafine, rebalance, join_group], #{}, #{
        protocol_name => ProtocolName, group_id => GroupId
    }),

    {value, Assignor} = lists:search(
        fun(Assignor) ->
            Assignor:name() == ProtocolName
        end,
        Assignors
    ),

    % Decode the member metadata into something the assignor expects.
    Members = lists:map(
        fun(M = #{metadata := Metadata}) ->
            M#{metadata := kafcod_consumer_protocol:decode_metadata(Metadata)}
        end,
        Members0
    ),
    ?LOG_DEBUG("Members = ~p", [Members]),

    Topics = lists:uniq(
        lists:foldl(
            fun(#{metadata := #{topics := Topics}}, Acc) ->
                Acc ++ Topics
            end,
            [],
            Members
        )
    ),
    ?LOG_DEBUG("Topics = ~p", [Topics]),

    % Hold onto some stuff over the Metadata call.
    StateData2 = StateData#state{
        generation_id = GenerationId
    },

    % Get the metadata for those topics. We use a composite state record, because we only need this information briefly.
    State2 = #leader_sync{assignor = Assignor, members = Members, protocol_name = ProtocolName},
    {next_state, State2, StateData2, [{next_event, internal, {metadata, Topics}}]};
handle_join_group_response(
    #{
        error_code := ?NONE,
        leader := LeaderId,
        protocol_type := _,
        protocol_name := ProtocolName,
        generation_id := GenerationId
    },
    _State,
    StateData = #state{member_id = MemberId}
) when LeaderId /= MemberId ->
    ?LOG_INFO("We are a follower with member ID ~p", [MemberId]),
    {next_state, follower_sync, StateData#state{generation_id = GenerationId}, [
        {next_event, internal, {sync_group, {ProtocolName, #{}}}}
    ]}.

handle_metadata_response(
    _MetadataResponse = #{topics := TopicPartitions0},
    _State = #leader_sync{
        assignor = Assignor,
        members = Members,
        protocol_name = ProtocolName
    },
    StateData = #state{
        assignment = #{user_data := ExistingAssignmentUserData}
    }
) ->
    % We want [#{name := topic(), partitions := [non_neg_integer()]}]
    TopicPartitions = lists:map(
        fun(#{name := T, partitions := Ps0, error_code := ?NONE}) ->
            % We don't need to sort the partitions. It makes debugging easier, though.
            Ps = lists:sort(
                lists:map(fun(#{error_code := ?NONE, partition_index := P}) -> P end, Ps0)
            ),
            #{name => T, partitions => Ps}
        end,
        TopicPartitions0
    ),
    ?LOG_DEBUG("TopicPartitions = ~p", [TopicPartitions]),

    % Ask the assignor to assign the partitions to the members. It may take advantage of previous user data
    % (for stickiness, e.g.).
    Assignments = Assignor:assign(Members, TopicPartitions, ExistingAssignmentUserData),
    ?LOG_DEBUG("Assignments = ~p", [Assignments]),
    {keep_state, StateData, [{next_event, internal, {sync_group, {ProtocolName, Assignments}}}]}.

handle_sync_group_response(
    _SyncGroupResponse = #{error_code := ?NONE, assignment := Assignment0},
    _State = #leader_sync{},
    StateData = #state{
        connection = Connection,
        group_id = GroupId,
        member_id = MemberId,
        subscription_callback = {SubscriptionCallback, SubscriptionState0},
        assignment_callback = {AssignmentCallback, AssignmentState0},
        membership_options = #{heartbeat_interval_ms := HeartbeatIntervalMs},
        assignment = #{assigned_partitions := PreviousTopicPartitionAssignments}
    }
) ->
    ?LOG_INFO("We are the leader with member ID ~p; synced group ~s; start heartbeating at ~p", [
        MemberId, GroupId, HeartbeatIntervalMs
    ]),

    Assignment = kafcod_consumer_protocol:decode_assignment(Assignment0),
    #{assigned_partitions := TopicPartitionAssignments} = Assignment,
    ?LOG_INFO("Assignment = ~p", [Assignment]),

    {ok, SubscriptionState, AssignmentState} = kafine_assignment:handle_assignment(
        TopicPartitionAssignments,
        PreviousTopicPartitionAssignments,
        Connection,
        {AssignmentCallback, AssignmentState0},
        {SubscriptionCallback, SubscriptionState0}
    ),

    StateData2 = StateData#state{
        subscription_callback = {SubscriptionCallback, SubscriptionState},
        assignment_callback = {AssignmentCallback, AssignmentState},
        assignment = Assignment
    },

    telemetry:execute([kafine, rebalance, leader], #{}, #{
        group_id => GroupId,
        member_id => MemberId,
        assignment => TopicPartitionAssignments,
        previous_assignment => PreviousTopicPartitionAssignments
    }),

    % Start heartbeating.
    {next_state, leader, StateData2, [{state_timeout, HeartbeatIntervalMs, heartbeat}]};
handle_sync_group_response(
    _SyncGroupResponse = #{error_code := ?NONE, assignment := Assignment0},
    _State = follower_sync,
    StateData = #state{
        connection = Connection,
        group_id = GroupId,
        member_id = MemberId,
        subscription_callback = {SubscriptionCallback, SubscriptionState0},
        assignment_callback = {AssignmentCallback, AssignmentState0},
        membership_options = #{heartbeat_interval_ms := HeartbeatIntervalMs},
        assignment = #{assigned_partitions := PreviousTopicPartitionAssignments}
    }
) ->
    ?LOG_INFO("We are a follower with member ID ~p; synced group ~s; start heartbeating at ~p", [
        MemberId, GroupId, HeartbeatIntervalMs
    ]),

    Assignment = kafcod_consumer_protocol:decode_assignment(Assignment0),
    #{assigned_partitions := TopicPartitionAssignments} = Assignment,
    ?LOG_INFO("Assignment = ~p", [Assignment]),

    {ok, SubscriptionState, AssignmentState} = kafine_assignment:handle_assignment(
        TopicPartitionAssignments,
        PreviousTopicPartitionAssignments,
        Connection,
        {AssignmentCallback, AssignmentState0},
        {SubscriptionCallback, SubscriptionState0}
    ),

    StateData2 = StateData#state{
        subscription_callback = {SubscriptionCallback, SubscriptionState},
        assignment_callback = {AssignmentCallback, AssignmentState},
        assignment = Assignment
    },

    telemetry:execute([kafine, rebalance, follower], #{}, #{
        member_id => MemberId,
        group_id => GroupId,
        assignment => TopicPartitionAssignments,
        previous_assignment => PreviousTopicPartitionAssignments
    }),

    % Start heartbeating.
    {next_state, follower, StateData2, [{state_timeout, HeartbeatIntervalMs, heartbeat}]}.

handle_heartbeat_response(
    _HeartbeatResponse = #{error_code := ?NONE},
    _State,
    StateData = #state{membership_options = #{heartbeat_interval_ms := HeartbeatIntervalMs}}
) ->
    % Continue heartbeating.
    {keep_state, StateData, [{state_timeout, HeartbeatIntervalMs, heartbeat}]};
handle_heartbeat_response(
    _HeartbeatResponse = #{error_code := ?REBALANCE_IN_PROGRESS},
    _State,
    StateData = #state{group_id = GroupId, member_id = MemberId}
) ->
    % A member has left the group, or a new member wants to join. We need to re-join the group ourselves.
    telemetry:execute([kafine, rebalance, triggered], #{}, #{
        group_id => GroupId, member_id => MemberId
    }),
    ?LOG_NOTICE("Group rebalance in progress; re-joining group"),
    {keep_state, StateData, [{next_event, internal, join_group}]}.

terminate(
    _Reason,
    _State,
    _StateData = #state{connection = undefined}
) ->
    ok;
terminate(
    _Reason,
    _State,
    _StateData = #state{
        connection = Connection,
        group_id = GroupId,
        member_id = MemberId,
        assignment = Assignments,
        subscription_callback = {SubscriptionCallback, SubscriptionState0},
        assignment_callback = {AssignmentCallback, AssignmentState0}
    }
) ->
    leave_group(Connection, GroupId, MemberId),
    kafine_connection:stop(Connection),
    % do revoke after leave_group as revoke_assignments might
    % fail if consumer is no_proc and at least this way progress
    % can be made by other consumers
    kafine_assignment:revoke_assignments(
        Assignments,
        {SubscriptionCallback, SubscriptionState0},
        {AssignmentCallback, AssignmentState0}
    ),
    ok.

leave_group(Connection, GroupId, MemberId) when
    is_pid(Connection),
    is_binary(GroupId),
    is_binary(MemberId)
->
    ?LOG_INFO("Leaving group ~s", [GroupId]),
    LeaveGroupRequest = #{
        group_id => GroupId,
        members => [
            #{
                member_id => MemberId,
                group_instance_id => null
            }
        ]
    },
    kafine_connection:call(
        Connection,
        fun leave_group_request:encode_leave_group_request_4/1,
        LeaveGroupRequest,
        fun leave_group_response:decode_leave_group_response_4/1,
        kafine_request_telemetry:request_labels(?LEAVE_GROUP, 4, GroupId)
    ).

build_join_request(
    Topics,
    Assignors,
    MemberId,
    GroupId,
    _Options = #{
        session_timeout_ms := SessionTimeoutMs,
        rebalance_timeout_ms := RebalanceTimeoutMs
    }
) ->
    % Note that, because of the extra member-id-required round-trip, the assignor is called twice.
    % If this becomes a problem, we can stash 'Protocols' (or just the metadata) in our state instead.
    Protocols = lists:map(
        fun(Assignor) ->
            #{
                name => Assignor:name(),
                metadata => kafcod_consumer_protocol:encode_metadata(Assignor:metadata(Topics))
            }
        end,
        Assignors
    ),

    #{
        session_timeout_ms => SessionTimeoutMs,
        rebalance_timeout_ms => RebalanceTimeoutMs,
        protocol_type => ?PROTOCOL_TYPE,
        protocols => Protocols,
        member_id => MemberId,
        group_instance_id => null,
        group_id => GroupId
    }.
