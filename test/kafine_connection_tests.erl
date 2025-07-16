-module(kafine_connection_tests).
-include_lib("eunit/include/eunit.hrl").

-define(WAIT_TIMEOUT_MS, 2_000).
-define(LABEL, 'label').
-define(SOCKET, 'socket').

setup() ->
    {ok, _} = application:ensure_all_started(telemetry),
    meck:new(gen_tcp, [unstick]),
    meck:expect(gen_tcp, connect, fun(_Host, _Port, _Opts) -> {ok, ?SOCKET} end),
    meck:expect(gen_tcp, connect, fun(_Host, _Port, _Opts, _Timeout) -> {ok, ?SOCKET} end),
    meck:expect(gen_tcp, send, fun(_Socket, _Request) -> ok end),
    ok.

cleanup(_) ->
    meck:unload().

connect_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun connect_ok/0,
        fun send_request/0,
        fun send_request_metadata/0,
        fun call/0,
        fun call_metadata/0
    ]}.

connect_ok() ->
    {ok, _} = kafine_connection:start_link(<<"ignored">>, 0, #{}),
    meck:wait(gen_tcp, connect, '_', ?WAIT_TIMEOUT_MS),
    ok.

%% Do send_request/check_response work correctly?
send_request() ->
    {ok, Connection} = kafine_connection:start_link(
        #{host => <<"ignored">>, port => 0, node_id => 101}, #{}
    ),
    meck:wait(gen_tcp, connect, '_', ?WAIT_TIMEOUT_MS),

    % Use send_request to send the, well, request.
    ReqIds = kafine_connection:reqids_new(),
    Encoder = fun(_Request = #{correlation_id := CorrelationId}) ->
        [<<CorrelationId:32/big-signed>>, <<"encoded-request">>]
    end,
    Args = #{},
    Decoder = fun(<<_:32/big-signed, "encoded-response">>) ->
        {#{encoded => response}, <<>>}
    end,
    ReqIds2 = kafine_connection:send_request(Connection, Encoder, Args, Decoder, ?LABEL, ReqIds),

    % That should result in a call to gen_tcp:send.
    meck:wait(gen_tcp, send, '_', ?WAIT_TIMEOUT_MS),

    % To capture the correlation ID, we need to inspect the outbound packet.
    % The request is currently the iodata from 'Encoder', above, so we could just lift the correlation ID directly.
    % But there's nothing stopping kafine_connection from flattening the iodata before it gets to gen_tcp:send/2, so
    % so we'll do it properly.
    Request = iolist_to_binary(meck:capture(last, gen_tcp, send, '_', 2)),
    <<CorrelationId:32/big-signed, _/binary>> = Request,

    % Fake a 'tcp' message with a response.
    Connection !
        {tcp, ?SOCKET, iolist_to_binary([<<CorrelationId:32/big-signed>>, <<"encoded-response">>])},

    % Which should result in a message to us.
    receive
        Msg ->
            % Which should be a valid response.
            {{ok, Response}, Label, _} = kafine_connection:check_response(Msg, ReqIds2),
            ?assertEqual(#{encoded => response}, Response),
            ?assertEqual(?LABEL, Label)
    after 5_000 ->
        error(timeout)
    end,
    ok.

send_request_metadata() ->
    TelemetryRef = attach_telemetry(),

    {ok, Connection} = kafine_connection:start_link(
        #{host => <<"ignored">>, port => 0, node_id => 101},
        #{metadata => #{purpose => bootstrap}}
    ),
    meck:wait(gen_tcp, connect, '_', ?WAIT_TIMEOUT_MS),

    % Use send_request to send the, well, request.
    ReqIds = kafine_connection:reqids_new(),
    Encoder = fun(_Request = #{correlation_id := CorrelationId}) ->
        [<<CorrelationId:32/big-signed>>, <<"encoded-request">>]
    end,
    Args = #{},
    Decoder = fun(<<_:32/big-signed, "encoded-response">>) ->
        {#{encoded => response}, <<>>}
    end,
    Metadata = #{api_key => 42, api_version => 2},
    ReqIds2 = kafine_connection:send_request(
        Connection, Encoder, Args, Decoder, ?LABEL, ReqIds, Metadata
    ),

    % That should result in a call to gen_tcp:send.
    meck:wait(gen_tcp, send, '_', ?WAIT_TIMEOUT_MS),

    % To capture the correlation ID, we need to inspect the outbound packet.
    % The request is currently the iodata from 'Encoder', above, so we could just lift the correlation ID directly.
    % But there's nothing stopping kafine_connection from flattening the iodata before it gets to gen_tcp:send/2, so
    % so we'll do it properly.
    Request = iolist_to_binary(meck:capture(last, gen_tcp, send, '_', 2)),
    <<CorrelationId:32/big-signed, _/binary>> = Request,

    ?assertMatch(
        {[kafine, connection, request, start], _, _},
        receive_telemetry(TelemetryRef)
    ),

    ?assertMatch(
        {[kafine, connection, tcp, bytes_sent], #{size := _}, #{
            host := _,
            port := _,
            node_id := _,
            purpose := bootstrap,
            api_key := 42,
            api_version := 2
        }},
        receive_telemetry(TelemetryRef)
    ),

    % Fake a 'tcp' message with a response.
    Connection !
        {tcp, ?SOCKET, iolist_to_binary([<<CorrelationId:32/big-signed>>, <<"encoded-response">>])},

    ?assertMatch(
        {[kafine, connection, tcp, bytes_received], #{size := _}, #{
            host := _,
            port := _,
            node_id := _,
            purpose := bootstrap,
            api_key := 42,
            api_version := 2
        }},
        receive_telemetry(TelemetryRef)
    ),

    % Which should result in a message to us.
    receive
        Msg ->
            % Which should be a valid response.
            {{ok, Response}, Label, _} = kafine_connection:check_response(Msg, ReqIds2),
            ?assertEqual(#{encoded => response}, Response),
            ?assertEqual(?LABEL, Label)
    after 5_000 ->
        error(timeout)
    end,

    ?assertMatch(
        {[kafine, connection, request, stop], #{duration := _}, _},
        receive_telemetry(TelemetryRef)
    ),

    telemetry:detach(TelemetryRef),
    ok.

%% Does call work correctly?
call() ->
    {ok, Connection} = kafine_connection:start_link(<<"ignored">>, 0, #{}),
    meck:wait(gen_tcp, connect, '_', ?WAIT_TIMEOUT_MS),

    % Because 'call' is synchronous, we need to do this in a separate process.
    _Server = spawn_link(fun() ->
        % Wait for gen_tcp:send, then extract the correlation ID and fake a 'tcp' message.
        meck:wait(gen_tcp, send, '_', ?WAIT_TIMEOUT_MS),

        Request = iolist_to_binary(meck:capture(last, gen_tcp, send, '_', 2)),
        <<CorrelationId:32/big-signed, _/binary>> = Request,
        Connection !
            {tcp, ?SOCKET,
                iolist_to_binary([<<CorrelationId:32/big-signed>>, <<"encoded-response">>])}
    end),

    Encoder = fun(_Request = #{correlation_id := CorrelationId}) ->
        [<<CorrelationId:32/big-signed>>, <<"encoded-request">>]
    end,
    Args = #{},
    Decoder = fun(<<_:32/big-signed, "encoded-response">>) ->
        {#{encoded => response}, <<>>}
    end,

    % This is synchronous.
    Response = kafine_connection:call(Connection, Encoder, Args, Decoder),

    ?assertEqual({ok, #{encoded => response}}, Response),
    ok.

-define(EXPECTED_METADATA, #{
    host := <<"ignored">>,
    port := 0,
    node_id := 101,
    purpose := coordinator,
    api_key := 42,
    api_version := 2
}).

call_metadata() ->
    TelemetryRef = attach_telemetry(),

    {ok, Connection} = kafine_connection:start_link(<<"ignored">>, 0, #{
        metadata => #{
            purpose => coordinator,
            % Because we didn't pass the node id in the "broker" args, we need to pass it in the metadata.
            node_id => 101
        }
    }),
    meck:wait(gen_tcp, connect, '_', ?WAIT_TIMEOUT_MS),

    % Because 'call' is synchronous, we need to do this in a separate process.
    _Server = spawn_link(fun() ->
        % Wait for gen_tcp:send, then extract the correlation ID and fake a 'tcp' message.
        meck:wait(gen_tcp, send, '_', ?WAIT_TIMEOUT_MS),

        Request = iolist_to_binary(meck:capture(last, gen_tcp, send, '_', 2)),
        <<CorrelationId:32/big-signed, _/binary>> = Request,
        Connection !
            {tcp, ?SOCKET,
                iolist_to_binary([<<CorrelationId:32/big-signed>>, <<"encoded-response">>])}
    end),

    Encoder = fun(_Request = #{correlation_id := CorrelationId}) ->
        [<<CorrelationId:32/big-signed>>, <<"encoded-request">>]
    end,
    Args = #{},
    Decoder = fun(<<_:32/big-signed, "encoded-response">>) ->
        {#{encoded => response}, <<>>}
    end,

    % This is synchronous.
    Response = kafine_connection:call(Connection, Encoder, Args, Decoder, #{
        api_key => 42, api_version => 2
    }),
    ?assertEqual({ok, #{encoded => response}}, Response),

    % We should see some telemetry events.
    ?assertMatch(
        {[kafine, connection, call, start], _, ?EXPECTED_METADATA},
        receive_telemetry(TelemetryRef)
    ),
    ?assertMatch(
        {[kafine, connection, tcp, bytes_sent], #{size := _}, ?EXPECTED_METADATA},
        receive_telemetry(TelemetryRef)
    ),
    ?assertMatch(
        {[kafine, connection, tcp, bytes_received], #{size := _}, ?EXPECTED_METADATA},
        receive_telemetry(TelemetryRef)
    ),
    ?assertMatch(
        {[kafine, connection, call, stop], #{duration := _}, ?EXPECTED_METADATA},
        receive_telemetry(TelemetryRef)
    ),
    ok.

attach_telemetry() ->
    telemetry_test:attach_event_handlers(self(), [
        [kafine, connection, request, start],
        [kafine, connection, request, stop],
        [kafine, connection, request, exception],

        [kafine, connection, call, start],
        [kafine, connection, call, stop],
        [kafine, connection, call, exception],

        [kafine, connection, tcp, bytes_sent],
        [kafine, connection, tcp, bytes_received]
    ]).

receive_telemetry(TelemetryRef) ->
    receive
        {EventName, TelemetryRef, Measurements, Metadata} ->
            {EventName, Measurements, Metadata}
    end.
