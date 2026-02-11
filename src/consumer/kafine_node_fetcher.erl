-module(kafine_node_fetcher).
-export([
    start_link/7,
    stop/1,
    info/1,

    job/2
]).
-behaviour(gen_statem).
-export([
    callback_mode/0,
    init/1,
    handle_event/4,
    terminate/3
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("kafcod/include/api_key.hrl").

-spec start_link(
    Ref :: kafine:consumer_ref(),
    ConnectionOptions :: kafine:connection_options(),
    ConsumerOptions :: kafine:consumer_options(),
    TopicOptions :: #{kafine:topic() => kafine:topic_options()},
    Owner :: pid(),
    Broker :: kafine:broker(),
    Metadata :: telemetry:event_metadata()
) -> gen_statem:start_ret().

start_link(Ref, ConnectionOptions, ConsumerOptions, TopicOptions, Owner, Broker, Metadata) ->
    ConnectionOptions1 = kafine_connection_options:validate_options(ConnectionOptions),
    gen_statem:start_link(
        ?MODULE,
        [Ref, ConnectionOptions1, ConsumerOptions, TopicOptions, Owner, Broker, Metadata],
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
        topic_options := #{kafine:topic() => kafine:topic_options()},
        connection := pid() | undefined
    }.

info(Pid) when is_pid(Pid) ->
    gen_statem:call(Pid, info).

-type job() ::
    {
        kafine_fetcher:job_id(),
        list_offsets,
        kafine_topic_partition_data:t(kafine:offset_timestamp())
    }
    | {
        kafine_fetcher:job_id(),
        fetch,
        kafine_topic_partition_data:t({kafine:offset(), module(), any()})
    }.

-spec job(Pid :: pid(), Job :: job()) -> ok.

job(Pid, Job) ->
    gen_statem:cast(Pid, {job, Job}).

-record(state, {
    ref :: kafine:consumer_ref(),
    owner :: pid(),
    broker :: kafine:broker(),
    connection_options :: kafine:connection_options(),
    consumer_options :: kafine:consumer_options(),
    topic_options :: #{kafine:topic() => kafine:topic_options()},
    connection = undefined :: pid() | undefined,
    backoff_state :: kafine_backoff:state(),
    req_ids = kafine_connection:reqids_new() :: kafine_connection:request_id_collection(),
    request_job_span = undefined :: kafine_telemetry:span() | undefined,
    metadata :: telemetry:event_metadata()
}).

callback_mode() -> handle_event_function.

init([
    Ref,
    ConnectionOptions = #{backoff := BackoffConfig},
    ConsumerOptions,
    TopicOptions,
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
        topic_options = TopicOptions,
        consumer_options = ConsumerOptions,
        backoff_state = kafine_backoff:init(BackoffConfig),
        metadata = Metadata2
    },
    {ok, init, State, [{next_event, internal, connect}]}.

handle_event(
    _,
    connect,
    _,
    StateData = #state{
        broker = Broker = #{node_id := NodeId},
        connection_options = ConnectionOptions,
        backoff_state = BackoffState,
        metadata = Metadata
    }
) ->
    case kafine_connection:start_link(Broker, ConnectionOptions) of
        {ok, Connection} ->
            telemetry:execute([kafine, node_fetcher, connected], #{}, Metadata),
            NewState = StateData#state{
                connection = Connection,
                backoff_state = kafine_backoff:reset(BackoffState)
            },
            {next_state, request_job, NewState, {next_event, internal, request_job}};
        {error, Reason} ->
            % We retry indefinitely. In the event the node is permanently gone, the group
            % coordinator will rebalance and remove this node. Note the assumption is made here
            % that new broker instance => new node id, which is important because we rely on this
            % process being recreated with an updated host-port. Double-check this assumption
            % holds, especially if we're going to support static consumer group membership.
            ?LOG_WARNING("Failed to connect to broker ~p: ~p, backing off", [NodeId, Reason]),
            {DelayMs, NewBackoffState} = kafine_backoff:backoff(BackoffState),
            telemetry:execute(
                [kafine, node_fetcher, backoff],
                #{delay_ms => DelayMs},
                Metadata#{reason => Reason}
            ),
            NewState = StateData#state{backoff_state = NewBackoffState},
            {next_state, backoff, NewState, {state_timeout, DelayMs, connect}}
    end;
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
    % TODO: backoff strategy
    telemetry:execute([kafine, node_fetcher, disconnected], #{}, Metadata),
    {keep_state, StateData#state{connection = undefined, req_ids = kafine_connection:reqids_new()},
        {next_event, internal, connect}};
handle_event(
    internal,
    request_job,
    _,
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
handle_event(info, {job, JobType, _Data}, request_job, #state{connection = undefined}) ->
    ?LOG_DEBUG("Ignoring ~p job while disconnected", [JobType]),
    keep_state_and_data;
handle_event(
    cast,
    {job, Job = {_, list_offsets, TopicPartitionOffsets}},
    request_job,
    StateData = #state{
        consumer_options = #{isolation_level := IsolationLevel},
        connection = Connection,
        req_ids = ReqIds,
        request_job_span = RequestJobSpan,
        metadata = Metadata
    }
) ->
    ?LOG_DEBUG("Listing offsets:~n~p", [TopicPartitionOffsets]),
    kafine_telemetry:stop_span([kafine, node_fetcher, request_job], RequestJobSpan),
    telemetry:execute([kafine, node_fetcher, list_offsets], #{}, Metadata),
    ListOffsetsRequest = kafine_list_offsets:build_request(
        TopicPartitionOffsets, IsolationLevel
    ),
    ReqIds2 = kafine_connection:send_request(
        Connection,
        fun list_offsets_request:encode_list_offsets_request_5/1,
        ListOffsetsRequest,
        fun list_offsets_response:decode_list_offsets_response_5/1,
        Job,
        ReqIds,
        request_metadata(?LIST_OFFSETS, 5)
    ),
    {next_state, list_offsets, StateData#state{req_ids = ReqIds2, request_job_span = undefined}};
handle_event(
    cast,
    {job, Job = {_, fetch, FetchInfo}},
    request_job,
    StateData = #state{
        connection = Connection,
        metadata = Metadata,
        consumer_options = ConsumerOptions,
        req_ids = ReqIds,
        request_job_span = RequestJobSpan
    }
) ->
    % FetchInfo is #{Topic => #{Partition => {Offset, Mod, ModState}}}
    ?LOG_DEBUG("Fetching: ~p", [FetchInfo]),
    kafine_telemetry:stop_span([kafine, node_fetcher, request_job], RequestJobSpan, #{}, Metadata),
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
        Job,
        ReqIds,
        request_metadata(?FETCH, 11)
    ),
    {next_state, fetch, StateData#state{req_ids = ReqIds2, request_job_span = undefined}};
handle_event(
    info,
    Info,
    State,
    StateData = #state{req_ids = ReqIds}
) ->
    check_response(kafine_connection:check_response(Info, ReqIds), Info, State, StateData);
handle_event(
    {call, From},
    info,
    State,
    #state{
        owner = Owner,
        broker = Broker = #{node_id := NodeId},
        connection_options = ConnectionOptions,
        consumer_options = ConsumerOptions,
        topic_options = TopicOptions,
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
        topic_options => TopicOptions,
        connection => Connection
    },
    {keep_state_and_data, {reply, From, Info}}.

check_response(_Result = {Response, Label, ReqIds2}, _Info, State, StateData) ->
    handle_response(Response, Label, State, StateData#state{req_ids = ReqIds2});
check_response(_Other, Info, State, StateData) ->
    handle_info(Info, State, StateData).

handle_info(Info, _State, _StateData) ->
    % Normal info message; ignore it.
    ?LOG_WARNING("Ignoring ~p", [Info]),
    keep_state_and_data.

handle_response(
    {ok, ListOffsetsResponse},
    {JobId, list_offsets, RequestedOffsets},
    _State = list_offsets,
    StateData = #state{
        broker = #{node_id := NodeId},
        owner = Owner
    }
) ->
    ?LOG_DEBUG("List offsets response:~n~p", [ListOffsetsResponse]),
    kafine_list_offsets:handle_response(
        ListOffsetsResponse, RequestedOffsets, JobId, NodeId, Owner
    ),
    {next_state, request_job, StateData, [{next_event, internal, request_job}]};
handle_response(
    {ok, FetchResponse},
    {JobId, fetch, FetchInfo},
    _State = fetch,
    StateData = #state{
        broker = #{node_id := NodeId},
        owner = Owner,
        topic_options = TopicOptions
    }
) ->
    ?LOG_DEBUG("Fetch response: ~p", [FetchResponse]),
    kafine_fetch:handle_response(FetchResponse, FetchInfo, JobId, NodeId, TopicOptions, Owner),
    {next_state, request_job, StateData, [{next_event, internal, request_job}]}.

terminate(_Reason, _State, #state{connection = Connection}) when Connection =:= undefined ->
    ok;
terminate(_Reason, _State, #state{connection = Connection}) ->
    kafine_connection:stop(Connection).

request_metadata(ApiKey, ApiVersion) ->
    #{api_key => ApiKey, api_version => ApiVersion}.
