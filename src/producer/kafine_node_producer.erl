-module(kafine_node_producer).
-moduledoc false.
-export([
    start_link/5,
    stop/1,
    info/1,

    reqids_new/0,
    produce/4,
    check_response/2
]).
-behaviour(gen_statem).
-export([
    init/1,
    callback_mode/0,
    handle_event/4,
    terminate/3
]).
-export_type([
    start_ret/0,
    request_id_collection/0,
    info/0
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("kafcod/include/api_key.hrl").

-type request_id_collection() :: gen_statem:request_id_collection().
-type start_ret() :: gen_statem:start_ret().
-spec start_link(
    Ref :: term(),
    ConnectionOptions :: kafine:connection_options(),
    ProducerOptions :: kafine:producer_options(),
    Owner :: pid(),
    Broker :: kafine:broker()
) ->
    start_ret().

start_link(
    Ref,
    ConnectionOptions,
    ProducerOptions,
    Owner,
    Broker = #{host := _, port := _, node_id := _}
) ->
    gen_statem:start_link(
        ?MODULE,
        [
            Ref,
            kafine_connection_options:validate_options(ConnectionOptions),
            kafine_producer_options:validate_options(ProducerOptions),
            Owner,
            Broker
        ],
        start_options()
    ).

start_options() -> [{debug, kafine_trace:debug_options(#{mfa => {?MODULE, handle_event, 4}})}].

stop(Pid) ->
    gen_statem:stop(Pid).

-type info() :: #{
    state := dynamic(),
    node_id := kafine:node_id(),
    broker := kafine:broker(),
    connection_options := kafine:connection_options(),
    producer_options := kafine:producer_options(),
    connection := kafine:connection() | undefined
}.

-spec info(Pid :: pid()) -> info().

info(Pid) when is_pid(Pid) ->
    gen_statem:call(Pid, info).

-spec produce(
    Pid :: pid(),
    Batch :: kafine_topic_partition_data:t([kafine_producer:message()]),
    Label :: any(),
    ReqIdCollection :: request_id_collection()
) ->
    request_id_collection().

produce(Pid, Batch, Label, ReqIdCollection) ->
    send_request(Pid, {produce, Batch}, Label, ReqIdCollection).

reqids_new() ->
    gen_statem:reqids_new().

send_request(Pid, Request, Label, ReqIdCollection) ->
    gen_statem:send_request(Pid, Request, Label, ReqIdCollection).

-spec check_response(Msg, ReqIdCollection) -> Result when
    Msg :: term(),
    ReqIdCollection :: request_id_collection(),
    Result ::
        {Response, Label, ReqIdCollection2}
        | no_request
        | no_reply,
    Response ::
        {ok, Decoded :: map()}
        | {error, {Reason :: term(), gen_statem:server_ref()}}
        | ProduceBatchResponse,
    ProduceBatchResponse :: kafine_topic_partition_data:t(ok | {error, {kafka_error, integer()}}),
    Label :: term(),
    ReqIdCollection2 :: request_id_collection().

check_response(Msg, ReqIdCollection) ->
    check_response(gen_statem:check_response(Msg, ReqIdCollection, true)).

check_response({{reply, Response}, Label, ReqIdCollection2}) ->
    {Response, Label, ReqIdCollection2};
check_response({{error, Reason}, Label, ReqIdCollection2}) ->
    {{error, Reason}, Label, ReqIdCollection2};
check_response(Result) when Result == no_request; Result == no_reply ->
    Result.

callback_mode() ->
    [handle_event_function].

-record(state, {
    metadata :: telemetry:event_metadata(),
    broker :: kafine:broker(),
    connection :: kafine:connection() | undefined,
    connection_options :: kafine:connection_options(),
    producer_options :: kafine:producer_options(),
    pending :: kafine_connection:request_id_collection()
}).

init([
    Ref,
    ConnectionOptions,
    ProducerOptions = #{metadata := Metadata},
    Owner = Owner,
    Broker = #{node_id := NodeId}
]) ->
    process_flag(trap_exit, true),
    Metadata2 = maps:merge(#{ref => Ref, node_id => NodeId}, Metadata),
    logger:set_process_metadata(Metadata2),
    kafine_proc_lib:set_label({?MODULE, Ref, NodeId}),

    % Register ourselves with the producer proc
    kafine_producer:set_node_producer(Owner, Broker, self()),

    StateData = #state{
        metadata = Metadata2,
        broker = Broker,
        connection = undefined,
        connection_options = ConnectionOptions,
        producer_options = ProducerOptions,
        pending = kafine_connection:reqids_new()
    },
    {ok, disconnected, StateData, [{next_event, internal, connect}]}.

handle_event(
    internal,
    connect,
    _State = disconnected,
    StateData = #state{
        broker = Broker,
        connection_options = ConnectionOptions,
        metadata = Metadata
    }
) ->
    {ok, Connection} = kafine_connection:start_link(Broker, ConnectionOptions),
    telemetry:execute([kafine, node_producer, connected], #{}, Metadata),
    StateData2 = StateData#state{connection = Connection},
    {next_state, ready, StateData2, []};
handle_event(
    {call, From},
    {produce, Batch},
    _State = ready,
    StateData = #state{
        metadata = Metadata,
        connection_options = ConnectionOptions,
        producer_options = ProducerOptions,
        connection = Connection,
        pending = Pending
    }
) ->
    Request = kafine_produce:build_request(Batch, ConnectionOptions, ProducerOptions),
    Pending2 = kafine_connection:send_request(
        Connection,
        fun produce_request:encode_produce_request_8/1,
        Request,
        fun produce_response:decode_produce_response_8/1,
        {produce_batch, From},
        Pending,
        request_metadata(?PRODUCE, 8, Metadata)
    ),
    {keep_state, StateData#state{pending = Pending2}};
handle_event(
    {call, _From},
    {produce, _Batch},
    _State,
    _StateData
) ->
    {keep_state_and_data, postpone};
handle_event(info, Info, State, StateData = #state{pending = ReqIds}) ->
    % We can't tell the difference between send_request responses and normal info messages, so we have to check them
    % first.
    check_response(kafine_connection:check_response(Info, ReqIds), Info, State, StateData);
handle_event(
    {call, From},
    info,
    State,
    #state{
        broker = Broker = #{node_id := NodeId},
        connection_options = ConnectionOptions,
        producer_options = ProducerOptions,
        connection = Connection
    }
) ->
    Info = #{
        state => State,
        node_id => NodeId,
        broker => Broker,
        connection_options => ConnectionOptions,
        producer_options => ProducerOptions,
        connection => Connection
    },
    {keep_state_and_data, {reply, From, Info}}.

terminate(_Reason, _State, _StateData = #state{connection = Connection}) when
    Connection =/= undefined
->
    kafine_connection:stop(Connection),
    ok;
terminate(_Reason, _State, _StateData) ->
    ok.

check_response(_Result = {Response, Label, ReqIds2}, _Info, State, StateData) ->
    handle_response(Response, Label, State, StateData#state{pending = ReqIds2});
check_response(_Other, Info, State, StateData) ->
    handle_info(Info, State, StateData).

handle_info(
    {'EXIT', Connection, _Reason},
    _State,
    StateData = #state{connection = Connection, metadata = Metadata}
) ->
    ?LOG_WARNING("Connection closed; reconnecting"),
    StateData2 = StateData#state{connection = undefined},
    telemetry:execute([kafine, node_producer, disconnected], #{}, Metadata),
    {next_state, disconnected, StateData2, [{next_event, internal, connect}]};
handle_info(_Info, _State, _StateData) ->
    % Normal info message; ignore it.
    keep_state_and_data.

handle_response(
    ProduceResponse,
    {produce, From},
    _State,
    StateData
) ->
    {ok, #{responses := [TopicResponse]}} = ProduceResponse,
    #{partition_responses := [PartitionResponse]} = TopicResponse,
    {keep_state, StateData, [{reply, From, {ok, PartitionResponse}}]};
handle_response(
    ProduceResponse,
    {produce_batch, From},
    _State,
    StateData
) ->
    {ok, Response} = ProduceResponse,
    Result = kafine_produce:handle_response(Response),
    {keep_state, StateData, {reply, From, Result}}.

request_metadata(ApiKey, ApiVersion, Metadata) ->
    Metadata#{api_key => ApiKey, api_version => ApiVersion}.
