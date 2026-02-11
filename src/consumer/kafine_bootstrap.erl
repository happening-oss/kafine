-module(kafine_bootstrap).

-behaviour(gen_statem).

-export([
    start_link/3,
    stop/1,

    info/1
]).

-export([
    get_metadata/2,
    find_coordinator/2,

    check_response/2
]).

-export([
    callback_mode/0,
    init/1,
    handle_event/4,
    terminate/3
]).

-export_type([request_id/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("kafcod/include/api_key.hrl").
-include_lib("kafcod/include/coordinator_type.hrl").
-include_lib("kafcod/include/error_code.hrl").
-include("../kafine_eqwalizer.hrl").

-type ref() :: term().

-type request_id() :: gen_statem:request_id().

id(Ref) -> {?MODULE, Ref}.

via(Ref) ->
    kafine_via:via(id(Ref)).

-spec start_link(
    Ref :: ref(), Broker :: kafine:broker(), ConnectionOptions :: kafine:connection_options()
) ->
    gen_statem:start_ret().

start_link(Ref, Broker, ConnectionOptions) ->
    ConnectionOptions1 = kafine_connection_options:validate_options(ConnectionOptions),
    gen_statem:start_link(
        via(Ref),
        ?MODULE,
        [Ref, Broker, ConnectionOptions1],
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
        state := term(),
        broker := kafine:broker(),
        connection_options := kafine:connection_options(),
        connection := pid() | undefined,
        pending_requests := [{dynamic(), gen_statem:from()}]
    }.

info(RefOrPid) ->
    call(RefOrPid, info).

-spec get_metadata(RefOrPid :: ref() | pid(), Topics :: [kafine:topic()]) ->
    request_id().

get_metadata(RefOrPid, Topics) ->
    send_request(RefOrPid, {get_metadata, Topics}).

-spec find_coordinator(RefOrPid :: ref() | pid(), GroupId :: binary()) ->
    {ok, kafine:broker()} | {error, {kafka_error, non_neg_integer()}}.

find_coordinator(RefOrPid, GroupId) ->
    call(RefOrPid, {find_coordinator, GroupId}).

-type topic_partition_metadata() ::
    #{leader := kafine:node_id(), replicas := [kafine:node_id()], isr := [kafine:node_id()]}.
-type metadata_response() ::
    {
        Brokers :: [kafine:broker()],
        TopicPartitionNodes :: kafine_topic_partition_data:t(topic_partition_metadata())
    }.
-type response() :: metadata_response().

-spec check_response(Msg :: term(), ReqId :: gen_statem:request_id()) ->
    {reply, response()} | {error, {dynamic(), gen_statem:server_ref()}} | no_reply.

check_response(Msg, ReqId) ->
    ?DYNAMIC_CAST(gen_statem:check_response(Msg, ReqId)).

-spec call(RefOrPid :: ref() | pid(), Request :: term()) -> dynamic().

call(Pid, Request) when is_pid(Pid) ->
    gen_statem:call(Pid, Request);
call(Ref, Request) ->
    gen_statem:call(via(Ref), Request).

-spec send_request(RefOrPid :: ref() | pid(), Request :: term()) -> gen_statem:request_id().

send_request(RefOrPid, Request) when is_pid(RefOrPid) ->
    gen_statem:send_request(RefOrPid, Request);
send_request(Ref, Request) ->
    gen_statem:send_request(via(Ref), Request).

-type request() ::
    {get_metadata, kafine_topic_partitions:t()} | {find_coordinator, binary()}.

-record(data, {
    ref :: ref(),
    broker :: kafine:broker(),
    connections_options :: kafine:connection_options(),
    backoff_state :: kafine_backoff:state(),
    connection = undefined :: kafine:connection() | undefined,
    pending_requests = [] :: [
        {Request :: request(), From :: gen_statem:from()}
    ]
}).

callback_mode() -> handle_event_function.

init([Ref, Broker, ConnectionOptions = #{backoff := BackoffConfig}]) ->
    process_flag(trap_exit, true),
    Metadata = #{ref => Ref},
    logger:set_process_metadata(Metadata),
    kafine_proc_lib:set_label({?MODULE, Ref}),
    Data = #data{
        ref = Ref,
        broker = Broker,
        connections_options = ConnectionOptions,
        backoff_state = kafine_backoff:init(BackoffConfig)
    },
    {ok, disconnected, Data, [{next_event, internal, connect}]}.

handle_event(
    _,
    connect,
    _,
    Data = #data{
        ref = Ref,
        broker = Broker,
        connections_options = ConnectionOptions,
        backoff_state = BackoffState
    }
) ->
    ?LOG_DEBUG("Connecting to bootstrap broker: ~p", [Broker]),

    case kafine_connection:start_link(Broker, ConnectionOptions) of
        {ok, Connection} ->
            telemetry:execute([kafine, bootstrap, connected], #{}, #{ref => Ref}),
            NewData = Data#data{
                connection = Connection,
                backoff_state = kafine_backoff:reset(BackoffState)
            },
            {next_state, connected, NewData, {next_event, internal, process_pending}};
        {error, Reason} ->
            ?LOG_WARNING("Failed to establish connection to bootstrap broker: ~p", [Reason]),
            {DelayMs, NewBackoffState} = kafine_backoff:backoff(BackoffState),
            telemetry:execute(
                [kafine, bootstrap, backoff],
                #{delay_ms => DelayMs},
                #{ref => Ref, reason => Reason}
            ),
            NewData = Data#data{backoff_state = NewBackoffState},
            {next_state, backoff, NewData, {state_timeout, DelayMs, connect}}
    end;
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
    try
        handle_request(Request, From, Data),
        {keep_state, Data#data{pending_requests = Rest}, [{next_event, internal, process_pending}]}
    catch
        exit:{closed, _} ->
            ?LOG_INFO("Connection lost during request"),
            % Don't handle the disconnect here, handle the 'EXIT' message
            keep_state_and_data
    end;
handle_event(
    {call, From},
    info,
    State,
    #data{
        broker = Broker,
        connections_options = ConnectionOptions,
        connection = Connection,
        pending_requests = PendingRequests
    }
) ->
    Info = #{
        state => State,
        broker => Broker,
        connection_options => ConnectionOptions,
        connection => Connection,
        pending_requests => PendingRequests
    },
    {keep_state_and_data, {reply, From, Info}};
handle_event(
    {call, From},
    Request,
    connected,
    Data = #data{
        pending_requests = PendingRequests
    }
) ->
    try
        handle_request(Request, From, Data),
        keep_state_and_data
    catch
        exit:{closed, _} ->
            ?LOG_INFO("Connection lost during request"),
            % Don't handle the disconnect here, handle the 'EXIT' message
            {keep_state, Data#data{pending_requests = PendingRequests ++ [{Request, From}]}}
    end;
handle_event(
    {call, From},
    Request,
    disconnected,
    Data = #data{pending_requests = PendingRequests}
) ->
    {keep_state, Data#data{pending_requests = PendingRequests ++ [{Request, From}]}};
handle_event(
    info,
    {'EXIT', Connection, Reason},
    connected,
    Data = #data{
        ref = Ref,
        connection = Connection
    }
) ->
    ?LOG_INFO("Connection to bootstrap broker lost: ~p", [Reason]),
    telemetry:execute([kafine, bootstrap, disconnected], #{reason => Reason}, #{ref => Ref}),
    {next_state, disconnected, Data#data{connection = undefined}, [{next_event, internal, connect}]};
handle_event(
    info,
    {'EXIT', Pid, Reason},
    _,
    _Data
) ->
    ?LOG_INFO("Received exit from ~p: ~p - Shutting down", [Pid, Reason]),
    {stop, Reason}.

handle_request({get_metadata, Topics}, From, #data{connection = Connection}) ->
    Result = do_get_metadata(Connection, Topics),
    gen_statem:reply(From, Result);
handle_request({find_coordinator, GroupId}, From, #data{connection = Connection}) ->
    Result = do_find_coordinator(Connection, GroupId),
    gen_statem:reply(From, Result).

do_get_metadata(Connection, Topics) ->
    {ok, MetadataResponse} = fetch_metadata(Connection, Topics),

    #{brokers := Brokers0, topics := TopicsMetadata} = MetadataResponse,

    Brokers = lists:map(fun(Broker) -> maps:with([host, port, node_id], Broker) end, Brokers0),

    TopicPartitionMetadata =
        kafine_topic_partition_lists:to_topic_partition_data(
            fun(
                _Topic,
                #{
                    partition_index := Partition,
                    error_code := ?NONE,
                    leader_id := Leader,
                    replica_nodes := Replicas,
                    isr_nodes := Isr
                }
            ) ->
                {Partition, #{leader => Leader, replicas => Replicas, isr => Isr}}
            end,
            TopicsMetadata
        ),

    {Brokers, TopicPartitionMetadata}.

do_find_coordinator(Connection, GroupId) ->
    FindCoordinatorRequest = #{
        key_type => ?COORDINATOR_TYPE_GROUP,
        key => GroupId
    },

    {ok, FindCoordinatorResponse} = kafine_connection:call(
        Connection,
        fun find_coordinator_request:encode_find_coordinator_request_3/1,
        FindCoordinatorRequest,
        fun find_coordinator_response:decode_find_coordinator_response_3/1,
        request_metadata(?FIND_COORDINATOR, 3, GroupId)
    ),

    case FindCoordinatorResponse of
        #{error_code := ?NONE, node_id := NodeId, host := Host, port := Port} ->
            {ok, #{host => Host, port => Port, node_id => NodeId}};
        #{error_code := ErrorCode} ->
            {error, {kafka_error, ErrorCode}}
    end.

fetch_metadata(Connection, Topics) ->
    MetadataRequest = #{
        allow_auto_topic_creation => false,
        include_cluster_authorized_operations => false,
        include_topic_authorized_operations => false,
        topics => [#{name => Name} || Name <- Topics]
    },
    kafine_connection:call(
        Connection,
        fun metadata_request:encode_metadata_request_9/1,
        MetadataRequest,
        fun metadata_response:decode_metadata_response_9/1,
        request_metadata(?METADATA, 9)
    ).

terminate(_Reason, _State, #data{connection = Connection}) when Connection =/= undefined ->
    try
        kafine_connection:stop(Connection)
    after
        ok
    end;
terminate(_Reason, _State, _Data) ->
    ok.

request_metadata(ApiKey, ApiVersion) ->
    #{api_key => ApiKey, api_version => ApiVersion}.

request_metadata(ApiKey, ApiVersion, GroupId) ->
    #{api_key => ApiKey, api_version => ApiVersion, group_id => GroupId}.
