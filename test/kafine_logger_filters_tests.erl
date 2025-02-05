-module(kafine_logger_filters_tests).
-include_lib("eunit/include/eunit.hrl").

-define(WAIT_TIMEOUT_MS, 2_000).
-define(SOCKET, 'socket').

setup() ->
    {ok, _} = application:ensure_all_started(telemetry),
    meck:new(gen_tcp, [unstick]),
    meck:expect(gen_tcp, connect, fun(_Host, _Port, _Opts) -> {ok, ?SOCKET} end),
    meck:expect(gen_tcp, send, fun(_Socket, _Request) -> ok end),
    ok.

cleanup(_) ->
    meck:unload().

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun connection_error_filter_warn/0,
        fun connection_error_filter_stop/0
    ]}.

connection_error_filter_warn() ->
    install_logger_handler(),

    % Install the logger filter, configured to stop connection errors.
    ok = logger:add_primary_filter(
        stop_connection_errors, {fun kafine_logger_filters:connection_errors/2, warn}
    ),

    start_stop_connection(),

    % Assert that there were warnings instead of errors.
    Levels = get_logged_levels(),
    ?assertEqual([warning, warning], [L || L <- Levels, L == warning orelse L == error]),

    reset_logger(),
    ok.

connection_error_filter_stop() ->
    install_logger_handler(),

    % Install the logger filter, configured to stop connection errors.
    ok = logger:add_primary_filter(
        stop_connection_errors, {fun kafine_logger_filters:connection_errors/2, stop}
    ),

    start_stop_connection(),

    % Assert that there were no warnings or errors.
    Levels = get_logged_levels(),
    ?assertEqual([], [L || L <- Levels, L == warning orelse L == error]),

    reset_logger(),
    ok.

install_logger_handler() ->
    % Install a logger handler so we can see what log levels are emitted.
    meck:new(test_handler, [non_strict]),
    meck:expect(test_handler, log, fun(_LogEvent, _Config) -> ok end),
    ok = logger:add_handler(?MODULE, test_handler, #{}),
    ok.

start_stop_connection() ->
    % Start (and stop) a connection.
    {ok, Connection} = kafine_connection:start_link(<<"ignored">>, 0, #{}),
    MRef = monitor(process, Connection),
    unlink(Connection),
    meck:wait(gen_tcp, connect, '_', ?WAIT_TIMEOUT_MS),

    Connection ! {tcp_closed, ?SOCKET},

    receive
        {'DOWN', MRef, process, Connection, _Reason} ->
            ok
    end,
    ok.

get_logged_levels() ->
    % What levels were logged?
    lists:filtermap(
        fun
            ({_, {_, _, [#{level := Level}, _]}, _}) -> {true, Level};
            (_) -> false
        end,
        meck:history(test_handler)
    ).

reset_logger() ->
    % Remove the filter and the test handler.
    logger:remove_primary_filter(stop_connection_errors),
    logger:remove_handler(?MODULE),
    ok.
