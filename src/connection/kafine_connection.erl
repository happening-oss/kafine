-module(kafine_connection).
-export([
    start_link/2,
    start_link/3,
    stop/1,

    call/4,
    call/5,
    call/6,

    send_request/6,
    send_request/7,
    send_request/8,
    check_response/2,
    wait_response/3,

    reqids_new/0,
    reqids_size/1,
    reqids_to_list/1
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

-type pending_request() :: {
    CorrelationId :: correlation_id(),
    From :: gen_statem:from(),
    Metadata :: telemetry:event_metadata(),
    Timeout :: timeout()
}.

-record(state, {
    % ClientId is sent in the request header for every request. Rather than have the caller specify it every time, we
    % keep it in our state.
    client_id :: binary(),

    % 'telemetry' metadata.
    metadata :: #{},

    % TCP/TLS transport module.
    transport :: module(),

    % Default request timeout in milliseconds.
    request_timeout_ms :: integer(),

    % The socket.
    socket :: inet:socket(),

    % TCP/TLS messages.
    messages :: transport_messages(),

    % When did we connect? Useful for diagnosing idle timeouts.
    started_at :: integer(),

    % The next correlation ID. Incrementing integer, starts at one.
    correlation_id :: correlation_id(),

    % List of data about requests which have been sent but have not yet received a response,
    % oldest first. Kafka will always respond to requests in order, so the correlation id in a
    % response should always match the head of this list.
    % Also contains the timeout for each request, so that timer can be started when the previous
    % response arrives. That prevents slower requests using up all of the timeout for requests
    % pipelined behind them.
    pending :: [pending_request()]
}).

% Note that we can't really put a spec on this. There's no way to express that Encoder, Args and Decoder are related.
call(Pid, Encoder, Args, Decoder) ->
    call(Pid, Encoder, Args, Decoder, _Metadata = #{}, default).

call(Pid, Encoder, Args, Decoder, Metadata) ->
    call(Pid, Encoder, Args, Decoder, Metadata, default).

call(Pid, Encoder, Args, Decoder, Metadata, Timeout) when
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
    StartMetadata = maps:merge(Metadata, Metadata1),

    % Call the connection process: encode and send the request.
    DecodedResponse = kafine_telemetry:span(
        [kafine, connection, call],
        % Note that 'telemetry' doesn't support putting extra measurements in the 'start' event (such as the request
        % object), so we don't support it either.
        %
        % But also note that you've got the request object anyway -- you passed it in as 'Args', above.
        StartMetadata,
        fun() ->
            % We *don't* use the merged metadata here, because it'll get merged again later.
            {ok, Encoded} = gen_statem:call(Pid, {call, Encoder, Args, Metadata, Timeout}),

            % We decode the response in the caller. This (hopefully) avoids sharing binary fragments between the two
            % processes.
            {Response, <<>>} = Decoder(Encoded),

            % kafine_telemetry:span() doesn't copy the start metadata into the stop metadata; we need to do it
            % ourselves.
            StopMetadata = StartMetadata,

            % We return the entire response in the measurements, so that you can pick out the individual topic/partition
            % errors.
            ExtraMeasurements = #{response => Response},
            {Response, ExtraMeasurements, StopMetadata}
        end
    ),

    {ok, maps:without([correlation_id], DecodedResponse)};
call(Pid, Encoder, Args, Decoder, Metadata, Timeout) ->
    erlang:error(badarg, [Pid, Encoder, Args, Decoder, Metadata, Timeout]).

-spec send_request(
    Connection :: kafine:connection(),
    Encoder :: encoder_fun(),
    Request :: map(),
    Decoder :: decoder_fun(),
    Label :: term(),
    ReqIdCollection :: request_id_collection()
) -> request_id_collection().

send_request(Pid, Encoder, Args, Decoder, Label0, ReqIdCollection) ->
    send_request(Pid, Encoder, Args, Decoder, Label0, ReqIdCollection, _Metadata = #{}, default).

-spec send_request(
    Connection :: kafine:connection(),
    Encoder :: encoder_fun(),
    Request :: map(),
    Decoder :: decoder_fun(),
    Label :: term(),
    ReqIdCollection :: request_id_collection(),
    Metadata :: telemetry:event_metadata()
) -> request_id_collection().

send_request(Pid, Encoder, Args, Decoder, Label0, ReqIdCollection, Metadata) ->
    send_request(Pid, Encoder, Args, Decoder, Label0, ReqIdCollection, Metadata, default).

-spec send_request(
    Connection :: kafine:connection(),
    Encoder :: encoder_fun(),
    Request :: map(),
    Decoder :: decoder_fun(),
    Label :: term(),
    ReqIdCollection :: request_id_collection(),
    Metadata :: telemetry:event_metadata(),
    Timeout :: timeout() | default
) -> request_id_collection().

send_request(Pid, Encoder, Args, Decoder, Label0, ReqIdCollection, Metadata, Timeout) when
    is_pid(Pid),
    is_function(Encoder, 1),
    is_map(Args),
    is_function(Decoder, 1),
    is_map(Metadata),
    is_integer(Timeout) orelse Timeout =:= default
->
    Metadata1 = kafine_telemetry:get_metadata(Pid),
    Metadata2 = maps:merge(Metadata, Metadata1),
    Span = kafine_telemetry:start_span([kafine, connection, request], Metadata2),
    % We'll need the decoder later; we'll stash it in the label along with the original label and telemetry span.
    Label = {Label0, Span, Decoder},
    gen_statem:send_request(Pid, {call, Encoder, Args, Metadata, Timeout}, Label, ReqIdCollection).

-spec check_response(Msg, ReqIdCollection) -> Result when
    Msg :: term(),
    ReqIdCollection :: request_id_collection(),
    Result ::
        {Response, Label, ReqIdCollection2}
        | no_request
        | no_reply,
    Response :: {ok, Decoded :: map()} | {error, {Reason :: term(), gen_statem:server_ref()}},
    Label :: dynamic(),
    ReqIdCollection2 :: request_id_collection().

check_response(Msg, ReqIdCollection) ->
    check_response(gen_statem:check_response(Msg, ReqIdCollection, true)).

check_response(
    {{reply, {ok, Encoded}}, _Label = {Label0, Span, Decoder}, ReqIdCollection2}
) ->
    % We stashed the decoder in the label.
    {Response, <<>>} = Decoder(Encoded),
    kafine_telemetry:stop_span([kafine, connection, request], Span, #{response => Response}, #{}),
    {{ok, maps:without([correlation_id], Response)}, Label0, ReqIdCollection2};
check_response({{error, Reason}, _Label = {Label0, Span, _Decoder}, ReqIdCollection2}) ->
    kafine_telemetry:span_exception(
        [kafine, connection, request],
        Span,
        error,
        Reason
    ),
    {{error, Reason}, Label0, ReqIdCollection2};
check_response(Result) when Result == no_request; Result == no_reply ->
    Result.

-type response_timeout() :: timeout() | {abs, integer()}.

-spec wait_response(ReqIdCollection, WaitTime, Delete) -> Result when
    ReqIdCollection :: request_id_collection(),
    WaitTime :: response_timeout(),
    Delete :: boolean(),
    Result ::
        {Response, Label, ReqIdCollection2}
        | no_request
        | no_reply,
    Response :: {ok, Decoded :: map()} | {error, {Reason :: term(), gen_statem:server_ref()}},
    Label :: dynamic(),
    ReqIdCollection2 :: request_id_collection().

wait_response(ReqIdCollection, WaitTime, Delete) ->
    check_response(gen_statem:wait_response(ReqIdCollection, WaitTime, Delete)).

-spec reqids_new() -> request_id_collection().

reqids_new() ->
    gen_statem:reqids_new().

reqids_size(ReqIdCollection) ->
    gen_statem:reqids_size(ReqIdCollection).

-spec reqids_to_list(request_id_collection()) -> [{gen_statem:request_id(), dynamic()}].

reqids_to_list(ReqIdCollection) ->
    lists:map(
        fun({Id, {Label, _, _}}) -> {Id, Label} end,
        gen_statem:reqids_to_list(ReqIdCollection)
    ).

init([
    Broker = #{host := Host, port := Port},
    #{
        client_id := ClientId,
        transport := Transport,
        transport_options := TransportOptions,
        connect_timeout := ConnectTimeoutMs,
        request_timeout_ms := RequestTimeoutMs,
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
    case
        Transport:connect(
            binary_to_list(Host),
            Port,
            TransportOptions2,
            ConnectTimeoutMs
        )
    of
        {ok, Socket} ->
            State = #state{
                client_id = ClientId,
                metadata = Metadata,
                correlation_id = ?INITIAL_CORRELATION_ID,
                transport = Transport,
                request_timeout_ms = RequestTimeoutMs,
                messages = Messages,
                socket = Socket,
                started_at = erlang:system_time(),
                pending = []
            },
            {ok, connected, State};
        {error, Reason} ->
            {error, Reason}
    end.

transport_messages(gen_tcp) ->
    {tcp, tcp_closed, tcp_error, tcp_passive};
transport_messages(ssl) ->
    {ssl, ssl_closed, ssl_error, ssl_passive}.

callback_mode() ->
    handle_event_function.

handle_event(
    {call, From},
    _Req = {call, Encoder, Args, Metadata, Timeout0},
    _State = connected,
    StateData = #state{
        client_id = ClientId,
        metadata = Metadata0,
        request_timeout_ms = RequestTimeoutMs,
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

    Timeout =
        case Timeout0 of
            default -> RequestTimeoutMs;
            T -> T
        end,

    Actions =
        case Pending of
            [] ->
                {{timeout, {request, CorrelationId}}, Timeout, undefined};
            _ ->
                % We're waiting for another request, don't start the timeout until we've had a
                % response for that
                []
        end,

    {next_state, connected,
        StateData#state{
            correlation_id = CorrelationId + 1,
            pending = Pending ++ [{CorrelationId, From, Metadata, Timeout}]
        },
        Actions};
handle_event(
    info,
    {Tcp, Socket, Buffer = <<CorrelationId:32/big-signed, _/binary>>},
    connected,
    StateData = #state{
        messages = {Tcp, _Closed, _Error, _Passive},
        socket = Socket,
        pending = [{CorrelationId, From, Metadata, _} | Rest],
        metadata = Metadata0
    }
) ->
    % Response from the broker. Because we use {packet, 4}, we know it's an entire message.
    telemetry:execute(
        [kafine, connection, tcp, bytes_received],
        #{size => byte_size(Buffer)},
        maps:merge(Metadata, Metadata0)
    ),

    TimeoutAction =
        case Rest of
            [] ->
                [];
            [{NextCorrelationId, _, _, Timeout} | _] ->
                % There's another request pipelined, start its timeout now.
                [{{timeout, {request, NextCorrelationId}}, Timeout, undefined}]
        end,

    % Note that latency measurements are in the span.
    {next_state, connected, StateData#state{pending = Rest}, [
        {reply, From, {ok, Buffer}} | TimeoutAction
    ]};
handle_event(
    info,
    {Tcp, Socket, <<BadCorrelationId:32/big-signed, _/binary>>},
    connected,
    #state{
        messages = {Tcp, _Closed, _Error, _Passive},
        socket = Socket,
        pending = [{ExpectedCorrelationId, _, _, _} | _],
        metadata = Metadata0,
        transport = Transport
    }
) ->
    ?LOG_WARNING("Received bad correlation id ~B, expected ~B; disconnecting", [
        BadCorrelationId, ExpectedCorrelationId
    ]),
    telemetry:execute(
        [kafine, connection, bad_correlation_id],
        #{},
        Metadata0
    ),

    % Kafka sends all responses in the order requests were received, so this is garbage data, not
    % an out of order response. We should drop the connection.
    Transport:close(Socket),
    {stop, closed};
handle_event(
    {timeout, {request, CorrelationId}},
    _,
    connected,
    #state{
        socket = Socket,
        pending = [{CorrelationId, _, Metadata, _} | _],
        metadata = Metadata0,
        transport = Transport
    }
) ->
    ?LOG_WARNING("Request with correlation id ~B timed out; disconnecting", [
        CorrelationId
    ]),
    telemetry:execute(
        [kafine, connection, request_timeout],
        #{},
        maps:merge(Metadata, Metadata0)
    ),

    % We should close the connection here because
    % a) There's no way of telling if it'll become usable again
    % b) that's a signal to the broker to stop working on this
    Transport:close(Socket),
    {stop, timeout};
handle_event(
    {timeout, {request, _}},
    _,
    _,
    _
) ->
    % Unexpected timeout, maybe a race with a request that completed very close to its timeout?
    % Ignore it
    keep_state_and_data;
handle_event(
    info,
    {Closed, Socket},
    _State,
    _StateData = #state{
        messages = {_Tcp, Closed, _Error, _Passive}, socket = Socket, started_at = StartedAt
    }
) ->
    Elapsed = erlang:system_time() - StartedAt,
    ?LOG_NOTICE("Connection closed (~B ms elapsed)", [
        erlang:convert_time_unit(Elapsed, native, millisecond)
    ]),
    {stop, closed}.
