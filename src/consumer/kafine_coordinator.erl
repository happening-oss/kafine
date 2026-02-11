-module(kafine_coordinator).

-behaviour(gen_statem).

-export([
    start_link/5,
    stop/1,

    whereis/1,
    info/1
]).

-export([
    exists/1,

    reqids_new/0,
    reqids_size/1,
    check_response/2,

    offset_fetch/3,
    offset_commit/6,

    join_group/2,
    sync_group/5,
    leave_group/2,
    heartbeat/3
]).

-export([
    callback_mode/0,
    init/1,
    handle_event/4,
    terminate/3
]).

-export_type([
    req_ids/0,
    join_group_response/0
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("kafcod/include/api_key.hrl").
-include_lib("kafcod/include/error_code.hrl").
-include("../kafine_eqwalizer.hrl").

-define(PROTOCOL_TYPE, <<"consumer">>).
-define(DEFAULT_CONNECT_RETRY_DELAY_MS, 100).
% Official kafka client uses rebalance_timeout_ms + 5s as join group timeout
-define(JOIN_GROUP_TIMEOUT_LAPSE, 5_000).

-type ref() :: term().

id(Ref) -> {?MODULE, Ref}.

via(Ref) ->
    kafine_via:via(id(Ref)).

-spec whereis(Ref :: ref()) -> pid() | undefined.

whereis(Ref) ->
    kafine_via:whereis_name(id(Ref)).

-spec start_link(
    Ref :: ref(),
    GroupId :: binary(),
    Topics :: [kafine:topic()],
    ConnectionOptions :: kafine:connection_options(),
    MembershipOptions :: kafine:membership_options()
) ->
    gen_statem:start_ret().

start_link(Ref, GroupId, Topics, ConnectionOptions, MembershipOptions) ->
    ConnectionOptions1 = kafine_connection_options:validate_options(ConnectionOptions),
    gen_statem:start_link(
        via(Ref),
        ?MODULE,
        [Ref, GroupId, Topics, ConnectionOptions1, MembershipOptions],
        start_options()
    ).

start_options() ->
    [{debug, kafine_trace:debug_options(#{mfa => {?MODULE, handle_event, 4}})}].

-spec stop(RefOrPid :: ref() | pid()) -> ok.

stop(Pid) when is_pid(Pid) ->
    gen_statem:stop(Pid);
stop(Ref) ->
    gen_statem:stop(via(Ref)).

-spec info(RefOrPid :: ref() | pid()) ->
    #{
        state := dynamic(),
        group_id := binary(),
        topics := [kafine:topic()],
        connection_options := kafine:connection_options(),
        membership_options := kafine:membership_options(),
        broker := kafine:broker() | undefined,
        connection := pid() | undefined,
        pending_requests := [{dynamic(), gen_statem:from()}]
    }.

info(RefOrPid) ->
    call(RefOrPid, info).

-spec exists(Ref :: ref()) -> boolean().

exists(Ref) ->
    case kafine_via:whereis_name(id(Ref)) of
        undefined -> false;
        _Pid -> true
    end.

-opaque req_ids() :: gen_statem:request_id_collection().

-spec reqids_new() -> req_ids().

reqids_new() -> gen_statem:reqids_new().

-spec reqids_size(ReqIds :: req_ids()) -> non_neg_integer().

reqids_size(ReqIds) -> gen_statem:reqids_size(ReqIds).

-type offset_fetch_response() ::
    {ok, kafine_topic_partition_data:t(kafine:offset())} | {error, dynamic()}.
-type offset_commit_response() ::
    {
        ok,
        kafine_topic_partition_data:t(ok | {kafka_error, integer()}),
        ThrottleTimeMs :: non_neg_integer()
    }
    | {error, dynamic()}.
-type join_group_response_body() :: #{
    generation_id := integer(),
    leader := binary(),
    members := [
        #{
            member_id := binary(),
            metadata := #{topics := [kafine:topic()], user_data := binary()}
        }
    ],
    protocol_name := binary()
}.
-type join_group_error() :: {member_id_required, MemberId :: binary()} | dynamic().
-type join_group_response() ::
    {ok, join_group_response_body()} | {error, Reason :: join_group_error()}.
-type sync_group_response() :: {ok, kafine_topic_partitions:t()} | {error, dynamic()}.
-type leave_group_response() :: ok | {error, dynamic()}.
-type heartbeat_response() :: ok | {error, rebalance_in_progress} | {error, dynamic()}.
-type response() ::
    offset_fetch_response()
    | offset_commit_response()
    | join_group_response()
    | sync_group_response()
    | leave_group_response()
    | heartbeat_response().

-spec check_response(Msg :: term(), ReqIds :: req_ids()) ->
    no_request | no_reply | {Response, Label :: dynamic(), NewReqIds :: req_ids()}
when
    Response :: {reply, response()} | {error, {Reason :: dynamic(), gen_statem:server_ref()}}.

check_response(Msg, ReqIds) ->
    ?DYNAMIC_CAST(gen_statem:check_response(Msg, ReqIds, true)).

-spec offset_fetch(
    RefOrPid :: ref() | pid(),
    TopicPartitions :: kafine_topic_partitions:t(),
    ReqIds :: req_ids()
) -> req_ids().

offset_fetch(RefOrPid, TopicPartitions, ReqIds) ->
    send_request(RefOrPid, {offset_fetch, TopicPartitions}, TopicPartitions, ReqIds).

-spec offset_commit(
    RefOrPid :: ref() | pid(),
    MemberId :: binary(),
    GenerationId :: integer(),
    Offsets :: kafine_topic_partition_data:t(kafine:offset()),
    Label :: term(),
    ReqIds :: req_ids()
) -> req_ids().

offset_commit(RefOrPid, MemberId, GenerationId, Offsets, Label, ReqIds) ->
    send_request(RefOrPid, {offset_commit, MemberId, GenerationId, Offsets}, Label, ReqIds).

-spec join_group(RefOrPid :: ref() | pid(), MemberId :: binary()) -> req_ids().
join_group(RefOrPid, MemberId) ->
    send_request(RefOrPid, {join_group, MemberId}).

-spec sync_group(
    RefOrPid :: ref() | pid(),
    MemberId :: binary(),
    GenerationId :: integer(),
    ProtocolName :: binary(),
    Assignments :: #{binary() => kafine_topic_partitions:t()}
) -> req_ids().
sync_group(RefOrPid, MemberId, GenerationId, ProtocolName, Assignments) ->
    send_request(RefOrPid, {sync_group, MemberId, GenerationId, ProtocolName, Assignments}).

-spec leave_group(RefOrPid :: ref() | pid(), MemberId :: binary()) -> req_ids().
leave_group(RefOrPid, MemberId) ->
    send_request(RefOrPid, {leave_group, MemberId}).

-spec heartbeat(RefOrPid :: ref() | pid(), MemberId :: binary(), GenerationId :: integer()) ->
    req_ids().
heartbeat(RefOrPid, MemberId, GenerationId) ->
    send_request(RefOrPid, {heartbeat, MemberId, GenerationId}).

send_request(RefOrPid, Request) ->
    send_request(RefOrPid, Request, undefined, reqids_new()).

send_request(Pid, Request, Label, ReqIds) when is_pid(Pid) ->
    gen_statem:send_request(Pid, Request, Label, ReqIds);
send_request(Ref, Request, Label, ReqIds) ->
    gen_statem:send_request(via(Ref), Request, Label, ReqIds).

call(Pid, Request) when is_pid(Pid) ->
    gen_statem:call(Pid, Request);
call(Ref, Request) ->
    gen_statem:call(via(Ref), Request).

-record(data, {
    ref :: ref(),
    group_id :: binary(),
    topics :: [kafine:topic()],
    connections_options :: kafine:connection_options(),
    membership_options :: kafine:membership_options(),
    broker :: kafine:broker() | undefined,
    connection = undefined :: kafine:connection() | undefined,
    backoff_state = undefined :: kafine_backoff:state() | undefined,
    req_ids = kafine_connection:reqids_new() :: kafine_connection:request_id_collection(),
    pending_requests = [] :: [{Request :: term(), From :: gen_statem:from()}]
}).

callback_mode() -> handle_event_function.

init([Ref, GroupId, Topics, ConnectionOptions = #{backoff := BackoffConfig}, MembershipOptions]) ->
    process_flag(trap_exit, true),
    Metadata = #{ref => Ref, group_id => GroupId},
    logger:set_process_metadata(Metadata),
    kafine_proc_lib:set_label({?MODULE, Ref}),
    Data = #data{
        ref = Ref,
        group_id = GroupId,
        topics = Topics,
        connections_options = ConnectionOptions,
        membership_options = MembershipOptions,
        backoff_state = kafine_backoff:init(BackoffConfig)
    },
    {ok, find_coordinator, Data, [{next_event, internal, find_coordinator}]}.

handle_event(
    EventType,
    find_coordinator,
    find_coordinator,
    Data = #data{
        ref = Ref,
        group_id = GroupId,
        broker = LastBroker,
        backoff_state = BackoffState
    }
) when EventType =:= internal; EventType =:= state_timeout ->
    case kafine_bootstrap:find_coordinator(Ref, GroupId) of
        {ok, NewBroker} when NewBroker =/= LastBroker ->
            NewData = Data#data{
                backoff_state = kafine_backoff:reset(BackoffState),
                broker = NewBroker
            },
            {next_state, disconnected, NewData, {next_event, internal, connect}};
        {ok, LastBroker} ->
            {next_state, disconnected, Data, {next_event, internal, connect}};
        {error, {kafka_error, Reason}} when
            Reason =:= ?COORDINATOR_LOAD_IN_PROGRESS; Reason =:= ?COORDINATOR_NOT_AVAILABLE
        ->
            ?LOG_INFO("Coordinator not available, retrying..."),
            {keep_state_and_data,
                {state_timeout, ?DEFAULT_CONNECT_RETRY_DELAY_MS, find_coordinator}}
    end;
handle_event(
    _,
    connect,
    disconnected,
    Data = #data{
        ref = Ref,
        broker = Broker,
        connections_options = ConnectionOptions,
        group_id = GroupId,
        backoff_state = BackoffState
    }
) ->
    ?LOG_DEBUG("Connecting to coordinator broker: ~p", [Broker]),

    case kafine_connection:start_link(Broker, ConnectionOptions) of
        {ok, Connection} ->
            telemetry:execute([kafine, coordinator, connected], #{}, #{
                ref => Ref, group_id => GroupId
            }),
            NewData = Data#data{
                connection = Connection,
                backoff_state = kafine_backoff:reset(BackoffState)
            },
            {next_state, connected, NewData, {next_event, internal, process_pending}};
        {error, Reason} ->
            ?LOG_WARNING("Failed to establish connection to broker: ~p", [Reason]),
            {DelayMs, NewBackoffState} = kafine_backoff:backoff(BackoffState),
            telemetry:execute(
                [kafine, coordinator, backoff],
                #{delay_ms => DelayMs},
                #{ref => Ref, reason => Reason}
            ),
            % If we failed to connect the coordinator, we should retry find_coordinator. This avoids
            % us looping forever if the cluster moves the coordinator after we queried it.
            NewData = Data#data{
                backoff_state = NewBackoffState,
                broker = undefined
            },
            {next_state, backoff, NewData, {state_timeout, DelayMs, find_coordinator}}
    end;
handle_event(
    state_timeout,
    find_coordinator,
    backoff,
    Data
) ->
    {next_state, find_coordinator, Data, {next_event, internal, find_coordinator}};
handle_event(
    internal,
    process_pending,
    connected,
    #data{pending_requests = []}
) ->
    keep_state_and_data;
handle_event(
    internal,
    process_pending,
    connected,
    Data = #data{
        pending_requests = [{Request, From} | Rest]
    }
) ->
    NewData = handle_request(Request, From, Data#data{pending_requests = Rest}),
    {keep_state, NewData, [{next_event, internal, process_pending}]};
handle_event(
    {call, From},
    info,
    State,
    #data{
        group_id = GroupId,
        topics = Topics,
        connections_options = ConnectionOptions,
        membership_options = MembershipOptions,
        broker = Broker,
        connection = Connection,
        pending_requests = PendingRequests
    }
) ->
    Info = #{
        state => State,
        group_id => GroupId,
        topics => Topics,
        connection_options => ConnectionOptions,
        membership_options => MembershipOptions,
        broker => Broker,
        connection => Connection,
        pending_requests => PendingRequests
    },
    {keep_state_and_data, {reply, From, Info}};
handle_event(
    {call, From},
    Request,
    connected,
    Data
) ->
    NewData = handle_request(Request, From, Data),
    {keep_state, NewData};
handle_event(
    {call, From},
    Request,
    _State,
    Data = #data{pending_requests = PendingRequests}
) ->
    {keep_state, Data#data{pending_requests = PendingRequests ++ [{Request, From}]}};
handle_event(
    info,
    {'EXIT', Connection, Reason},
    connected,
    Data = #data{
        ref = Ref,
        connection = Connection,
        group_id = GroupId,
        req_ids = ReqIds,
        pending_requests = PendingRequests
    }
) ->
    ?LOG_INFO("Connection to coordinator broker lost: ~p", [Reason]),
    telemetry:execute(
        [kafine, coordinator, disconnected],
        #{},
        #{ref => Ref, group_id => GroupId}
    ),
    InFlightRequests = lists:map(
        fun({_, Request}) -> Request end,
        kafine_connection:reqids_to_list(ReqIds)
    ),
    {next_state, disconnected,
        Data#data{
            connection = undefined,
            req_ids = kafine_connection:reqids_new(),
            pending_requests = PendingRequests ++ InFlightRequests
        },
        {next_event, internal, connect}};
handle_event(
    info,
    {'EXIT', Pid, Reason},
    _,
    _Data
) ->
    ?LOG_INFO("Received exit from ~p: ~p - Shutting down", [Pid, Reason]),
    {stop, Reason};
handle_event(
    info,
    Msg,
    _State,
    Data = #data{
        req_ids = ReqIds,
        pending_requests = PendingRequests
    }
) ->
    case kafine_connection:check_response(Msg, ReqIds) of
        Result when Result =:= no_request; Result =:= no_reply ->
            ?LOG_WARNING("Unexpected message ~p", [Msg]),
            keep_state_and_data;
        {{ok, Response}, {Request, From}, NewReqIds} ->
            handle_response(Response, Request, From, Data#data{req_ids = NewReqIds});
        {{error, {closed, _}}, Request, NewReqIds} ->
            % Connection closed, add the original request to pending_requests so we can retry when
            % we reconnect
            {keep_state, Data#data{
                req_ids = NewReqIds, pending_requests = [Request | PendingRequests]
            }};
        {Error = {error, _Reason}, {_Request, From}, NewReqIds} ->
            gen_statem:reply(From, Error),
            {keep_state, Data#data{req_ids = NewReqIds}}
    end.

handle_request(
    Request = {offset_fetch, TopicPartitions},
    From,
    Data = #data{
        group_id = GroupId,
        connection = Connection,
        req_ids = ReqIds
    }
) ->
    OffsetFetchRequest = #{
        group_id => GroupId,
        topics => [
            #{name => Topic, partition_indexes => Partitions}
         || {Topic, Partitions} <- kafine_topic_partitions:to_list(TopicPartitions)
        ]
    },

    NewReqIds =
        kafine_connection:send_request(
            Connection,
            fun offset_fetch_request:encode_offset_fetch_request_4/1,
            OffsetFetchRequest,
            fun offset_fetch_response:decode_offset_fetch_response_4/1,
            {Request, From},
            ReqIds,
            request_metadata(?OFFSET_FETCH, 4, GroupId)
        ),

    Data#data{req_ids = NewReqIds};
handle_request(
    Request = {offset_commit, MemberId, GenerationId, Offsets},
    From,
    Data = #data{
        group_id = GroupId,
        connection = Connection,
        req_ids = ReqIds
    }
) ->
    RequestTopics = kafine_topic_partition_lists:from_topic_partition_data(
        fun(_Topic, Partition, Offset) ->
            #{
                partition_index => Partition,
                committed_offset => Offset,
                % Yes, committed_metadata is nullable, but when you OffsetFetch a brand-new group,
                % you get an empty binary, so we'll do that.
                committed_metadata => <<>>
            }
        end,
        Offsets
    ),

    OffsetCommitRequest = #{
        group_id => GroupId,
        generation_id_or_member_epoch => GenerationId,
        member_id => MemberId,
        retention_time_ms => -1,
        topics => RequestTopics
    },

    NewReqIds = kafine_connection:send_request(
        Connection,
        fun offset_commit_request:encode_offset_commit_request_3/1,
        OffsetCommitRequest,
        fun offset_commit_response:decode_offset_commit_response_3/1,
        {Request, From},
        ReqIds,
        request_metadata(?OFFSET_COMMIT, 3, GroupId)
    ),

    Data#data{req_ids = NewReqIds};
handle_request(
    Request = {join_group, MemberId},
    From,
    Data = #data{
        connection = Connection,
        req_ids = ReqIds,
        group_id = GroupId,
        topics = Topics,
        membership_options = #{
            assignors := Assignors,
            session_timeout_ms := SessionTimeoutMs,
            rebalance_timeout_ms := RebalanceTimeoutMs
        }
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

    JoinGroupRequest = #{
        session_timeout_ms => SessionTimeoutMs,
        rebalance_timeout_ms => RebalanceTimeoutMs,
        protocol_type => ?PROTOCOL_TYPE,
        protocols => Protocols,
        member_id => MemberId,
        group_instance_id => null,
        group_id => GroupId
    },

    NewReqIds = kafine_connection:send_request(
        Connection,
        fun join_group_request:encode_join_group_request_7/1,
        JoinGroupRequest,
        fun join_group_response:decode_join_group_response_7/1,
        {Request, From},
        ReqIds,
        request_metadata(?JOIN_GROUP, 7, GroupId),
        RebalanceTimeoutMs + ?JOIN_GROUP_TIMEOUT_LAPSE
    ),

    Data#data{req_ids = NewReqIds};
handle_request(
    Request = {sync_group, MemberId, GenerationId, ProtocolName, Assignments},
    From,
    Data = #data{
        connection = Connection,
        req_ids = ReqIds,
        group_id = GroupId
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
        {Request, From},
        ReqIds,
        request_metadata(?SYNC_GROUP, 5, GroupId)
    ),

    Data#data{req_ids = ReqIds2};
handle_request(
    Request = {heartbeat, MemberId, GenerationId},
    From,
    Data = #data{
        connection = Connection,
        req_ids = ReqIds,
        group_id = GroupId
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
        {Request, From},
        ReqIds,
        request_metadata(?HEARTBEAT, 4, GroupId)
    ),

    Data#data{req_ids = ReqIds2};
handle_request(
    Request = {leave_group, MemberId},
    From,
    Data = #data{
        connection = Connection,
        req_ids = ReqIds,
        group_id = GroupId
    }
) ->
    LeaveGroupRequest = #{
        group_id => GroupId,
        members => [
            #{
                member_id => MemberId,
                group_instance_id => null
            }
        ]
    },

    ReqIds2 =
        kafine_connection:send_request(
            Connection,
            fun leave_group_request:encode_leave_group_request_4/1,
            LeaveGroupRequest,
            fun leave_group_response:decode_leave_group_response_4/1,
            {Request, From},
            ReqIds,
            request_metadata(?LEAVE_GROUP, 4, GroupId)
        ),

    Data#data{req_ids = ReqIds2}.

handle_response(
    #{error_code := ?NOT_COORDINATOR},
    Request,
    From,
    Data = #data{
        broker = #{node_id := NodeId},
        connection = Connection,
        pending_requests = PendingRequests
    }
) ->
    % NOTE: error_code := ?NOT_COORDINATOR doesn't catch coordinator moved during offset commit.
    % That's actually good, because retrying those would be dangerous due to the possibility of
    % there being multiple overlapping offset_commits in flight.
    ?LOG_INFO("Coordinator moved from node ~p, finding new coordinator and retrying", [NodeId]),
    % Disconnect from the stale coordinator
    kafine_connection:stop(Connection),
    % Consume the exit message this should produce
    receive
        {'EXIT', Connection, _Reason} -> ok
    after 0 ->
        ok
    end,
    % find the new coordinator
    NewData = Data#data{
        broker = undefined,
        connection = undefined,
        % Ensure this request is retried once we reconnect
        pending_requests = [{Request, From} | PendingRequests]
    },
    {next_state, find_coordinator, NewData, {next_event, internal, find_coordinator}};
handle_response(Response, {offset_fetch, _}, From, Data) ->
    NewData = handle_offset_fetch_response(Response, From, Data),
    {keep_state, NewData};
handle_response(Response, {offset_commit, _, _, _}, From, Data) ->
    NewData = handle_offset_commit_response(Response, From, Data),
    {keep_state, NewData};
handle_response(Response, {join_group, _}, From, Data) ->
    NewData = handle_join_group_response(Response, From, Data),
    {keep_state, NewData};
handle_response(Response, {sync_group, _, _, _, _}, From, Data) ->
    NewData = handle_sync_group_response(Response, From, Data),
    {keep_state, NewData};
handle_response(Response, {heartbeat, _, _}, From, Data) ->
    NewData = handle_heartbeat_response(Response, From, Data),
    {keep_state, NewData};
handle_response(Response, {leave_group, _}, From, Data) ->
    NewData = handle_leave_group_response(Response, From, Data),
    {keep_state, NewData}.

handle_offset_fetch_response(#{topics := Response}, From, Data) ->
    Result =
        kafine_topic_partition_lists:to_topic_partition_data(
            fun(_Topic, #{partition_index := PartitionIndex, committed_offset := CommittedOffset}) ->
                {PartitionIndex, CommittedOffset}
            end,
            Response
        ),

    gen_statem:reply(From, {ok, Result}),
    Data.

handle_offset_commit_response(
    #{topics := TopicResults, throttle_time_ms := ThrottleTimeMs}, From, Data
) ->
    Result = kafine_topic_partition_lists:to_topic_partition_data(
        fun(_Topic, #{partition_index := Partition, error_code := ErrorCode}) ->
            case ErrorCode of
                ?NONE -> {Partition, ok};
                _ -> {Partition, {kafka_error, ErrorCode}}
            end
        end,
        TopicResults
    ),

    gen_statem:reply(From, {ok, Result, ThrottleTimeMs}),
    Data.

handle_join_group_response(
    #{error_code := ?MEMBER_ID_REQUIRED, member_id := MemberId}, From, Data
) ->
    gen_statem:reply(From, {error, {member_id_required, MemberId}}),
    Data;
handle_join_group_response(
    #{
        error_code := ?NONE,
        generation_id := GenerationId,
        leader := Leader,
        members := Members,
        protocol_name := ProtocolName
    },
    From,
    Data
) ->
    Result =
        #{
            generation_id => GenerationId,
            leader => Leader,
            members => [
                Member#{metadata := kafcod_consumer_protocol:decode_metadata(Metadata)}
             || Member = #{metadata := Metadata} <- Members
            ],
            protocol_name => ProtocolName
        },
    gen_statem:reply(From, {ok, Result}),
    Data.

handle_sync_group_response(#{error_code := ?NONE, assignment := Assignment}, From, Data) ->
    Result = kafcod_consumer_protocol:decode_assignment(Assignment),
    gen_statem:reply(From, {ok, Result}),
    Data.

handle_heartbeat_response(#{error_code := ?NONE}, From, Data) ->
    gen_statem:reply(From, ok),
    Data;
handle_heartbeat_response(#{error_code := ?REBALANCE_IN_PROGRESS}, From, Data) ->
    gen_statem:reply(From, {error, rebalance_in_progress}),
    Data.

handle_leave_group_response(#{error_code := ?NONE}, From, Data) ->
    gen_statem:reply(From, ok),
    Data.

terminate(_Reason, _State, #data{connection = Connection}) when Connection =/= undefined ->
    kafine_connection:stop(Connection),
    ok;
terminate(_Reason, _State, _Data) ->
    ok.

request_metadata(ApiKey, ApiVersion, GroupId) ->
    #{api_key => ApiKey, api_version => ApiVersion, group_id => GroupId}.
