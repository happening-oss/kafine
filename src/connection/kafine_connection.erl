-module(kafine_connection).
-export([
    start_link/2,
    start_link/3,
    stop/1,

    call/4,
    call/5,

    send_request/6,
    send_request/7,
    check_response/2,

    reqids_new/0,
    reqids_size/1
]).
-behaviour(gen_statem).
-export([
    callback_mode/0,
    init/1,
    handle_event/4
]).

-export_type([
    encoder_fun/0,
    decoder_fun/0
]).

-include_lib("kernel/include/logger.hrl").

-type encoder_fun() :: fun((map()) -> iodata()).
-type decoder_fun() :: fun((binary()) -> decoder_result()).
-type decoder_result() :: {map(), binary()}.
-type request_id_collection() :: gen_statem:request_id_collection().

-export_type([request_id_collection/0]).

start_link(Broker = #{host := Host, port := Port}, Options) when
    (is_binary(Host) orelse is_list(Host)) andalso is_integer(Port) andalso is_map(Options)
->
    gen_statem:start_link(
        ?MODULE,
        [
            convert_broker(Broker),
            kafine_connection_options:validate_options(Options)
        ],
        start_options()
    ).

start_link(Host, Port, Options) ->
    start_link(#{host => Host, port => Port}, Options).

convert_broker(Broker = #{host := Host}) when is_list(Host) ->
    Broker#{host => list_to_binary(Host)};
convert_broker(Broker) ->
    Broker.

start_options() -> [{debug, kafine_trace:debug_options(#{mfa => {?MODULE, handle_event, 4}})}].

stop(Connection) when is_pid(Connection) ->
    gen_statem:stop(Connection).

-type correlation_id() :: integer().
-define(INITIAL_CORRELATION_ID, 1).

-type transport_messages() :: {OK :: atom(), Closed :: atom(), Error :: atom(), Passive :: atom()}.

-record(state, {
    % ClientId is sent in the request header for every request. Rather than have the caller specify it every time, we
    % keep it in our state.
    client_id :: binary(),

    % 'telemetry' metadata.
    metadata :: #{},

    % TCP/TLS transport module.
    transport :: module(),

    % The socket.
    socket :: inet:socket(),

    % TCP/TLS messages.
    messages :: transport_messages(),

    % The next correlation ID. Incrementing integer, starts at one.
    correlation_id :: correlation_id(),

    % Map of correlation ID and caller. Used to send responses back to the correct caller. We use a map because while we
    % generally don't have multiple requests in flight at the same time, there's nothing inherent in the implementation
    % that prevents it. For example, multiple processes could share the same connection instance, or you could use
    % gen_statem:send_request, etc.
    pending :: #{correlation_id() => gen_statem:from()}
}).

% Note that we can't really put a spec on this. There's no way to express that Encoder, Args and Decoder are related.
call(Pid, Encoder, Args, Decoder) ->
    call(Pid, Encoder, Args, Decoder, _Metadata = #{}).

call(Pid, Encoder, Args, Decoder, Metadata) when
    is_pid(Pid), is_function(Encoder, 1), is_map(Args), is_function(Decoder, 1), is_map(Metadata)
->
    % Try encoding the request in the caller's process; this results in better error reporting, at the expense of
    % encoding everything twice.
    _Try = Encoder(Args#{
        % Because the caller doesn't have the client id and next correlation ID, we fake them here. We'll use the real
        % ones later.
        client_id => <<>>,
        correlation_id => 0
    }),
    Metadata1 = kafine_telemetry:get_metadata(Pid),
    Metadata2 = maps:merge(Metadata, Metadata1),

    % Call the connection process: encode and send the request.
    DecodedResponse = kafine_connection_telemetry:span(
        [kafine, connection, call],
        Metadata2,
        fun() ->
            % We *don't* use the merged metadata here, because it'll get merged again later.
            {ok, Encoded} = gen_statem:call(Pid, {call, Encoder, Args, Metadata}),
            % We decode the response in the caller. This (hopefully) avoids sharing binary fragments between the two processes.
            {Response, <<>>} = Decoder(Encoded),
            {Response, Metadata2}
        end
    ),

    {ok, maps:without([correlation_id], DecodedResponse)};
call(Pid, Encoder, Args, Decoder, Metadata) ->
    erlang:error(badarg, [Pid, Encoder, Args, Decoder, Metadata]).

-spec send_request(
    Connection :: kafine:connection(),
    Encoder :: encoder_fun(),
    Request :: map(),
    Decoder :: decoder_fun(),
    Label :: term(),
    ReqIdCollection :: request_id_collection()
) -> request_id_collection().

send_request(Pid, Encoder, Args, Decoder, Label0, ReqIdCollection) ->
    send_request(Pid, Encoder, Args, Decoder, Label0, ReqIdCollection, _Metadata = #{}).

-spec send_request(
    Connection :: kafine:connection(),
    Encoder :: encoder_fun(),
    Request :: map(),
    Decoder :: decoder_fun(),
    Label :: term(),
    ReqIdCollection :: request_id_collection(),
    Metadata :: telemetry:event_metadata()
) -> request_id_collection().

send_request(Pid, Encoder, Args, Decoder, Label0, ReqIdCollection, Metadata) when
    is_pid(Pid), is_function(Encoder, 1), is_map(Args), is_function(Decoder, 1), is_map(Metadata)
->
    Metadata1 = kafine_telemetry:get_metadata(Pid),
    Metadata2 = maps:merge(Metadata, Metadata1),
    Span = kafine_connection_telemetry:start_span([kafine, connection, request], Metadata2),
    % We'll need the decoder later; we'll stash it in the label along with the original label and telemetry span.
    Label = {Label0, Span, Decoder},
    gen_statem:send_request(Pid, {call, Encoder, Args, Metadata}, Label, ReqIdCollection).

-spec check_response(Msg, ReqIdCollection) -> Result when
    Msg :: term(),
    ReqIdCollection :: request_id_collection(),
    Result ::
        {Response, Label, ReqIdCollection2}
        | no_request
        | no_reply,
    Response :: {ok, Decoded :: map()} | {error, {Reason :: term(), gen_statem:server_ref()}},
    Label :: term(),
    ReqIdCollection2 :: request_id_collection().

check_response(Msg, ReqIdCollection) ->
    check_response(gen_statem:check_response(Msg, ReqIdCollection, true)).

check_response({
    {reply, {ok, Encoded}}, _Label = {Label0, Span, Decoder}, ReqIdCollection2
}) ->
    % We stashed the decoder in the label.
    {Response, <<>>} = Decoder(Encoded),
    kafine_connection_telemetry:stop_span([kafine, connection, request], Span, Response),
    {{ok, maps:without([correlation_id], Response)}, Label0, ReqIdCollection2};
check_response({{error, Reason}, _Label = {Label0, Span, _Decoder}, ReqIdCollection2}) ->
    kafine_connection_telemetry:span_exception(
        [kafine, connection, request],
        Span,
        error,
        Reason
    ),
    {{error, Reason}, Label0, ReqIdCollection2};
check_response(Result) when Result == no_request; Result == no_reply ->
    Result.

-spec reqids_new() -> request_id_collection().

reqids_new() ->
    gen_statem:reqids_new().

reqids_size(ReqIdCollection) ->
    gen_statem:reqids_size(ReqIdCollection).

init([
    Broker = #{host := Host, port := Port},
    #{
        client_id := ClientId,
        transport := Transport,
        transport_options := TransportOptions,
        connect_timeout := ConnectTimeoutMs,
        metadata := Metadata0
    }
]) ->
    Metadata = maps:merge(maps:with([host, port, node_id], Broker), Metadata0),
    logger:set_process_metadata(Metadata),
    kafine_telemetry:put_metadata(Metadata),
    kafine_proc_lib:set_label({?MODULE, maps:get(node_id, Broker, -1)}),

    TransportOptions2 = [
        {mode, binary},
        % Kafka messages have a 4-byte size prefix, so we can use {packet,4}. This offloads the buffer management to the
        % C implementation in Erlang/OTP, where it's more efficient.
        {packet, 4},
        % ...and we get one process message for each Kafka message. We can use {active,true} because we only expect one
        % request to be in flight at a time, so there's no real danger of the process message queue growing unbounded.
        {active, true}
        | TransportOptions
    ],

    % Connect synchronously.
    Messages = transport_messages(Transport),
    {ok, Socket} = Transport:connect(
        binary_to_list(Host),
        Port,
        TransportOptions2,
        ConnectTimeoutMs
    ),

    State = #state{
        client_id = ClientId,
        metadata = Metadata,
        correlation_id = ?INITIAL_CORRELATION_ID,
        transport = Transport,
        messages = Messages,
        socket = Socket,
        pending = #{}
    },
    {ok, connected, State}.

transport_messages(gen_tcp) ->
    {tcp, tcp_closed, tcp_error, tcp_passive};
transport_messages(ssl) ->
    {ssl, ssl_closed, ssl_error, ssl_passive}.

callback_mode() ->
    handle_event_function.

handle_event(
    {call, From},
    _Req = {call, Encoder, Args, Metadata},
    _State = connected,
    StateData = #state{
        client_id = ClientId,
        metadata = Metadata0,
        correlation_id = CorrelationId,
        transport = Transport,
        socket = Socket,
        pending = Pending
    }
) ->
    Request = Encoder(Args#{client_id => ClientId, correlation_id => CorrelationId}),
    ok = Transport:send(Socket, Request),

    telemetry:execute(
        [kafine, connection, tcp, bytes_sent],
        #{size => iolist_size(Request)},
        maps:merge(Metadata, Metadata0)
    ),

    {next_state, connected, StateData#state{
        correlation_id = CorrelationId + 1,
        pending = Pending#{
            CorrelationId => {From, Metadata}
        }
    }};
handle_event(
    info,
    {Tcp, Socket, Buffer = <<CorrelationId:32/big-signed, _/binary>>},
    connected,
    StateData = #state{
        messages = {Tcp, _Closed, _Error, _Passive},
        socket = Socket,
        pending = Pending,
        metadata = Metadata0
    }
) ->
    % Response from the broker. Because we use {packet, 4}, we know it's an entire message.
    {{From, Metadata}, Pending2} = maps:take(CorrelationId, Pending),
    telemetry:execute(
        [kafine, connection, tcp, bytes_received],
        #{size => byte_size(Buffer)},
        maps:merge(Metadata, Metadata0)
    ),

    % Note that latency measurements are in the span.
    {next_state, connected, StateData#state{pending = Pending2}, [{reply, From, {ok, Buffer}}]};
handle_event(
    info,
    {Closed, Socket},
    _State,
    _StateData = #state{messages = {_Tcp, Closed, _Error, _Passive}, socket = Socket}
) ->
    ?LOG_NOTICE("Connection closed"),
    {stop, closed}.
