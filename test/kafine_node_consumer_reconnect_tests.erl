-module(kafine_node_consumer_reconnect_tests).
-include_lib("eunit/include/eunit.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CALLBACK_STATE, {state, ?MODULE}).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {with, [
            fun initial_connect/1,
            fun should_reconnect_if_connection_drops/1
        ]}
    ]}.

setup() ->
    meck:new(test_consumer_callback, [non_strict]),
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {ok, ?CALLBACK_STATE} end),
    meck:expect(test_consumer_callback, begin_record_batch, fun(_T, _P, _O, _Info, St) ->
        {ok, St}
    end),
    meck:expect(test_consumer_callback, handle_record, fun(_T, _P, _M, St) -> {ok, St} end),
    meck:expect(test_consumer_callback, end_record_batch, fun(_T, _P, _N, _Info, St) -> {ok, St} end),

    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    Broker.

cleanup(Broker) ->
    kamock_broker:stop(Broker),
    meck:unload(),
    ok.

initial_connect(Broker) ->
    TelemetryRef = attach_event_handlers(),

    ConnectionOptions = #{},
    ConsumerOptions = #{},
    {ok, NodeConsumer} = kafine_node_consumer:start_link(
        ?CONSUMER_REF,
        Broker,
        ConnectionOptions,
        ConsumerOptions,
        self()
    ),

    receive
        {[kamock, protocol, connected], TelemetryRef, _, _} -> ok
    end,
    receive
        {[kafine, node_consumer, idle], TelemetryRef, _, _} -> ok
    end,

    ?assertMatch(#{active_connections := 1}, kamock_broker:info(Broker)),

    kafine_node_consumer:stop(NodeConsumer),
    telemetry:detach(TelemetryRef),
    ok.

should_reconnect_if_connection_drops(Broker) ->
    TelemetryRef = attach_event_handlers(),

    ConnectionOptions = #{},
    ConsumerOptions = #{},
    {ok, NodeConsumer} = kafine_node_consumer:start_link(
        ?CONSUMER_REF,
        Broker,
        ConnectionOptions,
        ConsumerOptions,
        self()
    ),

    receive
        {[kamock, protocol, connected], TelemetryRef, _, _} -> ok
    end,
    receive
        {[kafine, node_consumer, connected], TelemetryRef, _, _} -> ok
    end,
    receive
        {[kafine, node_consumer, idle], TelemetryRef, _, _} -> ok
    end,

    ?assertMatch(#{active_connections := 1}, kamock_broker:info(Broker)),

    % Tell the broker to drop the connection.
    kill_kamock_connections(),

    receive
        {[kafine, node_consumer, disconnected], TelemetryRef, _, _} -> ok
    end,

    receive
        {[kamock, protocol, connected], TelemetryRef, _, _} -> ok
    end,
    receive
        {[kafine, node_consumer, connected], TelemetryRef, _, _} -> ok
    end,
    receive
        {[kafine, node_consumer, idle], TelemetryRef, _, _} -> ok
    end,

    ?assertMatch(#{active_connections := 1}, kamock_broker:info(Broker)),

    kafine_node_consumer:stop(NodeConsumer),
    telemetry:detach(TelemetryRef),
    ok.

attach_event_handlers() ->
    telemetry_test:attach_event_handlers(self(), [
        [kafine, node_consumer, connected],
        [kafine, node_consumer, disconnected],
        [kafine, node_consumer, idle],
        [kafine, node_consumer, continue],
        [kamock, protocol, connected]
    ]).

kill_kamock_connections() ->
    % We don't bother passing the ranch ref here, because we'd also have to deal with the IPv6 listeners.
    % It's fine, because we filter by kamock_broker_protocol.
    Connections = [
        Pid
     || {_Ref, _N, Sup} <- ranch_server:get_connections_sups(),
        {kamock_broker_protocol, Pid, _, _} <- supervisor:which_children(Sup)
    ],
    lists:foreach(
        fun(Pid) ->
            exit(Pid, kill)
        end,
        Connections
    ).
