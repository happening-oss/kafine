-module(kafine_connection_tests).
-include_lib("eunit/include/eunit.hrl").
-include("assert_meck.hrl").

-define(WAIT_TIMEOUT_MS, 2_000).
-define(LABEL, 'label').
-define(SOCKET, 'socket').

% These are ignored and don't mean anything. We include them in the fake requests for consistency with the real protocol.
-define(API_KEY, 101).
-define(API_VERSION, 0).

setup() ->
    {ok, _} = application:ensure_all_started(telemetry),
    meck:new(gen_tcp, [unstick]),
    meck:expect(gen_tcp, connect, fun(_Host, _Port, _Opts) -> {ok, ?SOCKET} end),
    meck:expect(gen_tcp, connect, fun(_Host, _Port, _Opts, _Timeout) -> {ok, ?SOCKET} end),
    meck:expect(gen_tcp, close, fun(?SOCKET) -> ok end),
    meck:expect(gen_tcp, send, fun(_Socket, _Request) -> ok end),
    ok.

cleanup(_) ->
    meck:unload().

connect_test_() ->
    {foreach, spawn, fun setup/0, fun cleanup/1, [
        fun connect_ok/0,
        fun econnrefused/0,
        fun send_request/0,
        fun send_request_metadata/0,
        fun call/0,
        fun call_metadata/0,
        fun disconnects_on_bad_correlation_id/0,
        fun send_request_timeout/0,
        fun override_send_request_timeout/0,
        fun call_timeout/0,
        fun override_call_timeout/0,
        fun timeouts_for_pipelines_requests_start_from_preceding_response/0
    ]}.

connect_ok() ->
    {ok, _} = kafine_connection:start_link(<<"ignored">>, 0, #{}),
    meck:wait(gen_tcp, connect, '_', ?WAIT_TIMEOUT_MS),
    ok.

econnrefused() ->
    % connect is synchronous, so should return the error immediately.
    meck:expect(gen_tcp, connect, fun(_Host, _Port, _Opts) -> {error, econnrefused} end),
    meck:expect(gen_tcp, connect, fun(_Host, _Port, _Opts, _Timeout) -> {error, econnrefused} end),
    {error, econnrefused} = kafine_connection:start_link(<<"ignored">>, 0, #{}),
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
        [
            <<?API_KEY:16/big-signed>>,
            <<?API_VERSION:16/big-signed>>,
            <<CorrelationId:32/big-signed>>,
            <<"encoded-request">>
        ]
    end,
    Args = #{},
    Decoder = fun(<<_CorrelationId:32/big-signed, "encoded-response">>) ->
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
    <<_ApiKey:16/big-signed, _ApiVersion:16/big-signed, CorrelationId:32/big-signed, _/binary>> =
        Request,

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
        [
            <<?API_KEY:16/big-signed>>,
            <<?API_VERSION:16/big-signed>>,
            <<CorrelationId:32/big-signed>>,
            <<"encoded-request">>
        ]
    end,
    Args = #{},
    Decoder = fun(<<_CorrelationId:32/big-signed, "encoded-response">>) ->
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
    <<_ApiKey:16/big-signed, _ApiVersion:16/big-signed, CorrelationId:32/big-signed, _/binary>> =
        Request,

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
        <<_ApiKey:16/big-signed, _ApiVersion:16/big-signed, CorrelationId:32/big-signed, _/binary>> =
            Request,
        Connection !
            {tcp, ?SOCKET,
                iolist_to_binary([<<CorrelationId:32/big-signed>>, <<"encoded-response">>])}
    end),

    Encoder = fun(_Request = #{correlation_id := CorrelationId}) ->
        [
            <<?API_KEY:16/big-signed>>,
            <<?API_VERSION:16/big-signed>>,
            <<CorrelationId:32/big-signed>>,
            <<"encoded-request">>
        ]
    end,
    Args = #{},
    Decoder = fun(<<_CorrelationId:32/big-signed, "encoded-response">>) ->
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
        <<_ApiKey:16/big-signed, _ApiVersion:16/big-signed, CorrelationId:32/big-signed, _/binary>> =
            Request,
        Connection !
            {tcp, ?SOCKET,
                iolist_to_binary([<<CorrelationId:32/big-signed>>, <<"encoded-response">>])}
    end),

    Encoder = fun(_Request = #{correlation_id := CorrelationId}) ->
        [
            <<?API_KEY:16/big-signed>>,
            <<?API_VERSION:16/big-signed>>,
            <<CorrelationId:32/big-signed>>,
            <<"encoded-request">>
        ]
    end,
    Args = #{},
    Decoder = fun(<<_CorrelationId:32/big-signed, "encoded-response">>) ->
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

disconnects_on_bad_correlation_id() ->
    process_flag(trap_exit, true),
    {ok, Connection} = kafine_connection:start_link(
        #{host => <<"ignored">>, port => 0, node_id => 101}, #{}
    ),
    meck:wait(gen_tcp, connect, '_', ?WAIT_TIMEOUT_MS),

    % Use send_request to send the, well, request.
    ReqIds = kafine_connection:reqids_new(),
    Encoder = fun(_Request = #{correlation_id := CorrelationId}) ->
        [
            <<?API_KEY:16/big-signed>>,
            <<?API_VERSION:16/big-signed>>,
            <<CorrelationId:32/big-signed>>,
            <<"encoded-request">>
        ]
    end,
    Args = #{},
    Decoder = fun(<<_CorrelationId:32/big-signed, "encoded-response">>) ->
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
    <<_ApiKey:16/big-signed, _ApiVersion:16/big-signed, CorrelationId:32/big-signed, _/binary>> =
        Request,

    % Pipeline another request behind the first. Correlation ids are sequential, so this should have CorrelationId+1.
    % That's useful since it's what we're going to send as our bad correlation id. Kafka promises to send responses
    % in the same order it receives requests, so even a known correlation id should be considered bad if it's not
    % the next one we're expecting.
    _ReqIds3 = kafine_connection:send_request(Connection, Encoder, Args, Decoder, ?LABEL, ReqIds2),

    % Fake a 'tcp' message with a response.
    Connection !
        {tcp, ?SOCKET,
            iolist_to_binary([<<(CorrelationId + 1):32/big-signed>>, <<"encoded-response">>])},

    % Which should result in an exit with reason closed
    receive
        Msg ->
            ?assertEqual({'EXIT', Connection, closed}, Msg)
    after 5_000 ->
        error(timeout)
    end,

    ?assertCalled(gen_tcp, close, [?SOCKET]),
    ok.

send_request_timeout() ->
    process_flag(trap_exit, true),
    {ok, Connection} = kafine_connection:start_link(
        #{host => <<"ignored">>, port => 0, node_id => 101}, #{request_timeout_ms => 50}
    ),
    meck:wait(gen_tcp, connect, '_', ?WAIT_TIMEOUT_MS),

    % Use send_request to send the, well, request.
    ReqIds = kafine_connection:reqids_new(),
    Encoder = fun(_Request = #{correlation_id := CorrelationId}) ->
        [
            <<?API_KEY:16/big-signed>>,
            <<?API_VERSION:16/big-signed>>,
            <<CorrelationId:32/big-signed>>,
            <<"encoded-request">>
        ]
    end,
    Args = #{},
    Decoder = fun(<<_CorrelationId:32/big-signed, "encoded-response">>) ->
        {#{encoded => response}, <<>>}
    end,
    _ = kafine_connection:send_request(Connection, Encoder, Args, Decoder, ?LABEL, ReqIds),

    % That should result in a call to gen_tcp:send.
    meck:wait(gen_tcp, send, '_', ?WAIT_TIMEOUT_MS),

    % Which should be followed by a disconnect
    receive
        Msg ->
            ?assertEqual({'EXIT', Connection, timeout}, Msg)
    after 5_000 ->
        error(timeout)
    end,

    ?assertCalled(gen_tcp, close, [?SOCKET]),
    ok.

override_send_request_timeout() ->
    process_flag(trap_exit, true),
    {ok, Connection} = kafine_connection:start_link(
        #{host => <<"ignored">>, port => 0, node_id => 101}, #{}
    ),
    meck:wait(gen_tcp, connect, '_', ?WAIT_TIMEOUT_MS),

    % Use send_request to send the, well, request.
    ReqIds = kafine_connection:reqids_new(),
    Encoder = fun(_Request = #{correlation_id := CorrelationId}) ->
        [
            <<?API_KEY:16/big-signed>>,
            <<?API_VERSION:16/big-signed>>,
            <<CorrelationId:32/big-signed>>,
            <<"encoded-request">>
        ]
    end,
    Args = #{},
    Decoder = fun(<<_CorrelationId:32/big-signed, "encoded-response">>) ->
        {#{encoded => response}, <<>>}
    end,
    _ = kafine_connection:send_request(Connection, Encoder, Args, Decoder, ?LABEL, ReqIds, #{}, 50),

    % That should result in a call to gen_tcp:send.
    meck:wait(gen_tcp, send, '_', ?WAIT_TIMEOUT_MS),

    % Which should be followed by a disconnect
    receive
        Msg ->
            ?assertEqual({'EXIT', Connection, timeout}, Msg)
    after 5_000 ->
        error(timeout)
    end,

    ?assertCalled(gen_tcp, close, [?SOCKET]),
    ok.

call_timeout() ->
    process_flag(trap_exit, true),
    {ok, Connection} = kafine_connection:start_link(<<"ignored">>, 0, #{request_timeout_ms => 50}),
    meck:wait(gen_tcp, connect, '_', ?WAIT_TIMEOUT_MS),

    Encoder = fun(_Request = #{correlation_id := CorrelationId}) ->
        [
            <<?API_KEY:16/big-signed>>,
            <<?API_VERSION:16/big-signed>>,
            <<CorrelationId:32/big-signed>>,
            <<"encoded-request">>
        ]
    end,
    Args = #{},
    Decoder = fun(<<_CorrelationId:32/big-signed, "encoded-response">>) ->
        {#{encoded => response}, <<>>}
    end,

    % Should exit with timeout, just like gen_statem:call(..., Timeout) would
    ?assertExit({timeout, _}, kafine_connection:call(Connection, Encoder, Args, Decoder)),

    % kafine_connection itself should exit
    receive
        Msg ->
            ?assertEqual({'EXIT', Connection, timeout}, Msg)
    after 5_000 ->
        error(timeout)
    end,

    % and the socket should've been closed
    ?assertCalled(gen_tcp, close, [?SOCKET]),
    ok.

override_call_timeout() ->
    process_flag(trap_exit, true),
    {ok, Connection} = kafine_connection:start_link(<<"ignored">>, 0, #{}),
    meck:wait(gen_tcp, connect, '_', ?WAIT_TIMEOUT_MS),

    Encoder = fun(_Request = #{correlation_id := CorrelationId}) ->
        [
            <<?API_KEY:16/big-signed>>,
            <<?API_VERSION:16/big-signed>>,
            <<CorrelationId:32/big-signed>>,
            <<"encoded-request">>
        ]
    end,
    Args = #{},
    Decoder = fun(<<_CorrelationId:32/big-signed, "encoded-response">>) ->
        {#{encoded => response}, <<>>}
    end,

    % Should exit with timeout, just like gen_statem:call(..., Timeout) would
    ?assertExit({timeout, _}, kafine_connection:call(Connection, Encoder, Args, Decoder, #{}, 50)),

    % kafine_connection itself should exit
    receive
        Msg ->
            ?assertEqual({'EXIT', Connection, timeout}, Msg)
    after 5_000 ->
        error(timeout)
    end,

    % and the socket should've been closed
    ?assertCalled(gen_tcp, close, [?SOCKET]),
    ok.

timeouts_for_pipelines_requests_start_from_preceding_response() ->
    process_flag(trap_exit, true),
    {ok, Connection} = kafine_connection:start_link(
        #{host => <<"ignored">>, port => 0, node_id => 101}, #{request_timeout_ms => 50}
    ),
    meck:wait(gen_tcp, connect, '_', ?WAIT_TIMEOUT_MS),

    % Use send_request to send the, well, request.
    ReqIds = kafine_connection:reqids_new(),
    Encoder = fun(_Request = #{correlation_id := CorrelationId}) ->
        [
            <<?API_KEY:16/big-signed>>,
            <<?API_VERSION:16/big-signed>>,
            <<CorrelationId:32/big-signed>>,
            <<"encoded-request">>
        ]
    end,
    Args = #{},
    Decoder = fun(<<_CorrelationId:32/big-signed, "encoded-response">>) ->
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
    <<_ApiKey:16/big-signed, _ApiVersion:16/big-signed, CorrelationId:32/big-signed, _/binary>> =
        Request,

    % Pipeline another request behind the first.
    _ReqIds3 = kafine_connection:send_request(Connection, Encoder, Args, Decoder, ?LABEL, ReqIds2),

    % Wait 30ms to respond
    timer:sleep(30),

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

    % Wait another 40ms, we shouldn't have timed out yet even though we're > 50ms since the second send request
    timer:sleep(40),

    receive
        Msg2 ->
            error({unexpected_message, Msg2})
    after 0 ->
        ok
    end,

    % Eventually we should get a timeout for the second message
    receive
        Msg3 ->
            ?assertEqual({'EXIT', Connection, timeout}, Msg3)
    after 5_000 ->
        error(timeout)
    end,

    ?assertCalled(gen_tcp, close, [?SOCKET]),
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
