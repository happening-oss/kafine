-module(kafine_node_fetcher).
-export([
    start_link/6,
    stop/1,
    info/1,

    reqids_new/0,
    check_response/2,
    job/4
]).
-behaviour(gen_statem).
-export([
    callback_mode/0,
    init/1,
    handle_event/4,
    terminate/3
]).
-export_type([
    request_id_collection/0
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("kafcod/include/api_key.hrl").
-include("../kafine_eqwalizer.hrl").

% How long to wait after a connection drops before reconnecting. This should be very short, we want
% to retry pretty close to immediately. But we don't retry immediately, primarily for the sake of
% not reconnecting before we've failed an in flight request
-define(INITIAL_RECONNECT_DELAY_MS, 1).

-spec start_link(
    Ref :: kafine:consumer_ref(),
    ConnectionOptions :: kafine:connection_options(),
    ConsumerOptions :: kafine:consumer_options(),
    Owner :: pid(),
    Broker :: kafine:broker(),
    Metadata :: telemetry:event_metadata()
) -> gen_statem:start_ret().

start_link(Ref, ConnectionOptions, ConsumerOptions, Owner, Broker, Metadata) ->
    ConnectionOptions1 = kafine_connection_options:validate_options(ConnectionOptions),
    gen_statem:start_link(
        ?MODULE,
        [Ref, ConnectionOptions1, ConsumerOptions, Owner, Broker, Metadata],
        start_options()
    ).

start_options() ->
    [
        {spawn_opt, [
            % Disable the old heap for this process. This prevents holding references to large
            % binaries after we're done with them
            {fullsweep_after, 0}
        ]},
        {debug, kafine_trace:debug_options(#{mfa => {?MODULE, handle_event, 4}})}
    ].

stop(Pid) when is_pid(Pid) ->
    monitor(process, Pid),
    exit(Pid, normal),
    receive
        {'DOWN', _, process, Pid, _} -> ok
    end.

-spec info(Pid :: pid()) ->
    #{
        state := dynamic(),
        node_id := kafine:node_id(),
        owner := pid(),
        broker := kafine:broker(),
        connection_options := kafine:connection_options(),
        consumer_options := kafine:consumer_options(),
        connection := pid() | undefined
    }.

info(Pid) when is_pid(Pid) ->
    gen_statem:call(Pid, info).

-type request_id_collection() :: gen_statem:request_id_collection().

-spec reqids_new() -> request_id_collection().

reqids_new() ->
    gen_statem:reqids_new().

-spec check_response(Msg :: term(), ReqIds :: request_id_collection()) -> Result when
    Result :: {Response, Label, NewReqIds} | no_request | no_reply,
    Response :: {reply, Reply} | {error, {Reason :: term(), gen_statem:server_ref()}},
    Reply ::
        {ok, kafine_topic_partition_data:t(kafine_fetch:partition_result())}
        | {ok, kafine_topic_partition_data:t(kafine_list_offsets:partition_result())}
        | {error, term()},
    Label :: term(),
    NewReqIds :: request_id_collection().

check_response(Msg, ReqIds) ->
    ?DYNAMIC_CAST(gen_statem:check_response(Msg, ReqIds, true)).

-type job() ::
    {
        list_offsets,
        kafine_topic_partition_data:t(kafine:offset_timestamp())
    }
    | {
        fetch,
        kafine_topic_partition_data:t({kafine:offset(), module(), any()})
    }.

-spec job(Pid :: pid(), Job :: job(), Label :: term(), ReqIdCollection :: request_id_collection()) ->
    request_id_collection().

job(Pid, Job, Label, ReqIdCollection) ->
    gen_statem:send_request(Pid, {job, Job}, Label, ReqIdCollection).

-record(state, {
    ref :: kafine:consumer_ref(),
    owner :: pid(),
    broker :: kafine:broker(),
    connection_options :: kafine:connection_options(),
    consumer_options :: kafine:consumer_options(),
    connection = undefined :: pid() | undefined,
    connect_backoff_state :: kafine_backoff:state(),
    request_backoff_state = undefined :: kafine_backoff:state() | undefined,
    req_ids = kafine_connection:reqids_new() :: kafine_connection:request_id_collection(),
    request_job_span = undefined :: kafine_telemetry:span() | undefined,
    metadata :: telemetry:event_metadata()
}).

callback_mode() -> [handle_event_function, state_enter].

init([
    Ref,
    ConnectionOptions = #{backoff := BackoffConfig},
    ConsumerOptions,
    Owner,
    Broker = #{node_id := NodeId},
    Metadata
]) ->
    process_flag(trap_exit, true),
    Metadata2 = maps:merge(#{ref => Ref, node_id => NodeId}, Metadata),
    logger:set_process_metadata(Metadata2),
    kafine_proc_lib:set_label({?MODULE, {Ref, NodeId}}),

    % In case we've restarted, update the node fetcher with our pid
    kafine_fetcher:set_node_fetcher(Owner, Broker, self()),

    State = #state{
        ref = Ref,
        owner = Owner,
        broker = Broker,
        connection_options = ConnectionOptions,
        consumer_options = ConsumerOptions,
        connect_backoff_state = kafine_backoff:init(BackoffConfig),
        metadata = Metadata2
    },
    {ok, disconnected, State, {next_event, internal, connect}}.

handle_event(
    _,
    connect,
    disconnected,
    StateData = #state{
        broker = Broker = #{node_id := NodeId},
        connection_options = ConnectionOptions = #{backoff := BackoffConfig},
        metadata = Metadata
    }
) ->
    case kafine_connection:start_link(Broker, ConnectionOptions) of
        {ok, Connection} ->
            telemetry:execute([kafine, node_fetcher, connected], #{}, Metadata),
            NewState = StateData#state{
                connection = Connection,
                connect_backoff_state = kafine_backoff:init(BackoffConfig)
            },
            {next_state, request_job, NewState};
        {error, Reason} ->
            % We retry indefinitely unless told otherwise. In the event the node is permanently
            % gone, the group coordinator will rebalance and remove this node. Note the assumption
            % is made here that new broker instance => new node id, which is important because we
            % rely on this process being recreated with an updated host-port. Double-check this
            % assumption holds, especially if we're going to support static consumer group
            % membership.
            ?LOG_WARNING("Failed to connect to broker ~p at ~s: ~p, backing off", [
                NodeId, format_broker(Broker), Reason
            ]),
            {next_state, {connect_backoff, Reason}, StateData}
    end;
handle_event(
    enter,
    _,
    {connect_backoff, Reason},
    StateData = #state{
        broker = #{node_id := NodeId},
        connect_backoff_state = BackoffState,
        metadata = Metadata
    }
) ->
    case kafine_backoff:backoff(BackoffState) of
        limit_exceeded ->
            ?LOG_ERROR(
                "Backoff limit exceeded when attempting to connect to broker ~B",
                [NodeId]
            ),
            {stop, backoff_limit_exceeded, StateData};
        {DelayMs, NewBackoffState} ->
            telemetry:execute(
                [kafine, node_fetcher, connect_backoff],
                #{delay_ms => DelayMs},
                Metadata#{reason => Reason}
            ),
            NewState = StateData#state{connect_backoff_state = NewBackoffState},
            {keep_state, NewState, {state_timeout, DelayMs, expired}}
    end;
handle_event(
    state_timeout,
    expired,
    {connect_backoff, _Reason},
    StateData
) ->
    {next_state, disconnected, StateData, {next_event, internal, connect}};
handle_event(
    info,
    {'EXIT', Connection, Reason},
    _,
    StateData = #state{
        connection = Connection,
        metadata = Metadata
    }
) ->
    ?LOG_DEBUG("Connection exited with reason ~p, reconnecting", [Reason]),
    telemetry:execute([kafine, node_fetcher, disconnected], #{}, Metadata),
    {next_state, disconnected, StateData#state{connection = undefined}};
handle_event(
    enter,
    _,
    disconnected,
    _StateData
) ->
    % we don't retry immediately, primarily for the sake of not reconnecting before we've failed an
    % in flight request
    {keep_state_and_data, {state_timeout, ?INITIAL_RECONNECT_DELAY_MS, connect}};
handle_event(
    enter,
    _,
    request_job,
    StateData = #state{
        ref = Ref,
        owner = Owner,
        broker = #{node_id := NodeId}
    }
) ->
    Span = kafine_telemetry:start_span([kafine, node_fetcher, request_job], #{
        ref => Ref, node_id => NodeId
    }),
    kafine_fetcher:request_job(Owner, NodeId, self()),
    {keep_state, StateData#state{request_job_span = Span}};
handle_event(
    {call, From},
    {job, {JobType, _}},
    _,
    #state{connection = undefined}
) ->
    ?LOG_DEBUG("Failing ~p job while disconnected", [JobType]),
    {keep_state_and_data, {reply, From, {error, not_connected}}};
handle_event(
    {call, From},
    {job, {JobType, JobBody}},
    request_job,
    StateData = #state{request_job_span = RequestJobSpan}
) ->
    kafine_telemetry:stop_span([kafine, node_fetcher, request_job], RequestJobSpan),
    {next_state, {JobType, JobBody, From}, StateData#state{
        request_job_span = undefined, request_backoff_state = undefined
    }};
handle_event(
    enter,
    _,
    State = {list_offsets, TopicPartitionOffsets, _From},
    StateData = #state{
        consumer_options = #{isolation_level := IsolationLevel},
        connection = Connection,
        req_ids = ReqIds,
        metadata = Metadata
    }
) ->
    ?LOG_DEBUG("Listing offsets:~n~p", [TopicPartitionOffsets]),
    telemetry:execute([kafine, node_fetcher, list_offsets], #{}, Metadata),
    ListOffsetsRequest = kafine_list_offsets:build_request(
        TopicPartitionOffsets, IsolationLevel
    ),
    ReqIds2 = kafine_connection:send_request(
        Connection,
        fun list_offsets_request:encode_list_offsets_request_5/1,
        ListOffsetsRequest,
        fun list_offsets_response:decode_list_offsets_response_5/1,
        State,
        ReqIds,
        request_metadata(?LIST_OFFSETS, 5, Metadata)
    ),
    {keep_state, StateData#state{req_ids = ReqIds2}};
handle_event(
    enter,
    _,
    State = {fetch, FetchInfo, _From},
    StateData = #state{
        connection = Connection,
        metadata = Metadata,
        consumer_options = ConsumerOptions,
        req_ids = ReqIds
    }
) ->
    % FetchInfo is #{Topic => #{Partition => {Offset, Mod, ModState}}}
    ?LOG_DEBUG("Fetching: ~p", [FetchInfo]),
    FetchMeasurements = #{
        fetching => kafine_topic_partition_data:topic_partitions(FetchInfo)
    },
    telemetry:execute([kafine, node_fetcher, fetch], FetchMeasurements, Metadata),
    FetchRequest = kafine_fetch:build_request(FetchInfo, ConsumerOptions),
    ReqIds2 = kafine_connection:send_request(
        Connection,
        % Wireshark only supports up to v11.
        fun fetch_request:encode_fetch_request_11/1,
        FetchRequest,
        fun fetch_response:decode_fetch_response_11/1,
        State,
        ReqIds,
        request_metadata(?FETCH, 11, Metadata)
    ),
    {keep_state, StateData#state{req_ids = ReqIds2}};
handle_event(
    state_timeout,
    backoff_complete,
    {request_backoff, Job},
    StateData
) ->
    % The backoff complete timeout is only set when we're backing off a request, so we know that if
    % we get this event, we should retry the request
    {next_state, Job, StateData};
handle_event(
    info,
    Info,
    State,
    StateData = #state{req_ids = ReqIds}
) ->
    check_broker_response(kafine_connection:check_response(Info, ReqIds), Info, State, StateData);
handle_event(
    {call, From},
    info,
    State,
    #state{
        owner = Owner,
        broker = Broker = #{node_id := NodeId},
        connection_options = ConnectionOptions,
        consumer_options = ConsumerOptions,
        connection = Connection
    }
) ->
    Info = #{
        state => State,
        node_id => NodeId,
        owner => Owner,
        broker => Broker,
        connection_options => ConnectionOptions,
        consumer_options => ConsumerOptions,
        connection => Connection
    },
    {keep_state_and_data, {reply, From, Info}};
handle_event(
    enter,
    _,
    _,
    _
) ->
    % Catch-all handler for states that don't need to do anything at state enter
    keep_state_and_data.

check_broker_response(_Result = {Response, Label, ReqIds2}, _Info, _State, StateData) ->
    handle_response(Response, Label, StateData#state{req_ids = ReqIds2});
check_broker_response(_Other, Info, State, StateData) ->
    handle_info(Info, State, StateData).

handle_info(Info, _State, _StateData) ->
    % Normal info message; ignore it.
    ?LOG_WARNING("Ignoring ~p", [Info]),
    keep_state_and_data.

handle_response(
    {ok, ListOffsetsResponse},
    {list_offsets, RequestedOffsets, From},
    StateData
) ->
    ?LOG_DEBUG("List offsets response:~n~p", [ListOffsetsResponse]),
    Result = kafine_list_offsets:handle_response(ListOffsetsResponse, RequestedOffsets),
    {next_state, request_job, StateData, {reply, From, Result}};
handle_response(
    {ok, FetchResponse},
    Job = {fetch, FetchInfo, From},
    StateData
) ->
    ?LOG_DEBUG("Fetch response: ~p", [FetchResponse]),
    case kafine_fetch:handle_response(FetchResponse, FetchInfo) of
        Result = {ok, _} ->
            {next_state, request_job, StateData, {reply, From, Result}};
        {error, {kafka_error, ErrorCode}} ->
            handle_error_response(ErrorCode, Job, StateData)
    end;
handle_response(
    {error, {closed, _}},
    {JobType, _, From},
    _StateData
) ->
    ?LOG_DEBUG("Connection closed during ~p", [JobType]),
    {keep_state_and_data, {reply, From, {error, closed}}}.

handle_error_response(
    ErrorCode,
    Job = {JobType, _, _},
    StateData = #state{
        consumer_options = #{retry_backoff := RetryBackoff},
        request_backoff_state = BackoffState,
        metadata = Metadata
    }
) ->
    case kafcod_error:is_retriable(ErrorCode) of
        true ->
            case backoff(BackoffState, RetryBackoff) of
                {DelayMs, NewBackoffState} ->
                    ?LOG_DEBUG("Top-level error ~B for ~p job, retrying after ~B ms", [
                        ErrorCode, JobType, DelayMs
                    ]),
                    telemetry:execute(
                        [kafine, node_fetcher, request_backoff],
                        #{job_type => JobType, delay_ms => DelayMs, error_code => ErrorCode},
                        Metadata
                    ),
                    NewStateData = StateData#state{request_backoff_state = NewBackoffState},
                    {next_state, {request_backoff, Job}, NewStateData,
                        {state_timeout, DelayMs, backoff_complete}};
                limit_exceeded ->
                    ?LOG_ERROR("Retry limit hit with error ~B for ~p job, node fetcher exiting", [
                        ErrorCode, JobType
                    ]),
                    {stop, {kafka_error, ErrorCode}, StateData}
            end;
        false ->
            ?LOG_ERROR("Non retryable error ~B for ~p job, node fetcher exiting", [
                ErrorCode, JobType
            ]),
            {stop, {kafka_error, ErrorCode}, StateData}
    end.

backoff(undefined, RetryBackoff) ->
    BackoffState = kafine_backoff:init(RetryBackoff),
    backoff(BackoffState, RetryBackoff);
backoff(BackoffState, _RetryBackoff) ->
    kafine_backoff:backoff(BackoffState).

terminate(_Reason, _State, #state{connection = Connection}) when Connection =:= undefined ->
    ok;
terminate(_Reason, _State, #state{connection = Connection}) ->
    kafine_connection:stop(Connection).

request_metadata(ApiKey, ApiVersion, Metadata) ->
    Metadata#{api_key => ApiKey, api_version => ApiVersion}.

format_broker(#{host := Host, port := Port}) ->
    iolist_to_binary(io_lib:format("~s:~B", [Host, Port])).
