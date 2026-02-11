-module(kafine_logger_filters_tests).
-include_lib("eunit/include/eunit.hrl").

-define(WAIT_TIMEOUT_MS, 2_000).
-define(SOCKET, 'socket').

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun connection_error_filter_warn/0,
        fun connection_error_filter_stop/0
    ]}.

setup() ->
    {ok, _} = application:ensure_all_started(telemetry),
    meck:new(gen_tcp, [unstick]),
    meck:expect(gen_tcp, connect, fun(_Host, _Port, _Opts) -> {ok, ?SOCKET} end),
    meck:expect(gen_tcp, connect, fun(_Host, _Port, _Opts, _Timeout) -> {ok, ?SOCKET} end),
    meck:expect(gen_tcp, send, fun(_Socket, _Request) -> ok end),

    % Remove default handler -- there'll be warnings and errors, which are scary.
    logger:remove_handler(default),

    % Install a logger handler so we can see what log levels are emitted.
    meck:new(test_handler, [non_strict]),
    meck:expect(test_handler, log, fun(_LogEvent, _Config) -> ok end),
    logger:add_handler(?MODULE, test_handler, #{}),

    % Set the log level so that the events are emitted.
    Config = logger:get_primary_config(),
    logger:set_primary_config(level, info),
    Config.

cleanup(Config) ->
    meck:unload(),

    % Remove the test handler.
    logger:remove_handler(?MODULE),

    % Put back the default handler.
    logger:add_handlers(kernel),

    % Put back the original config.
    logger:set_primary_config(Config),
    ok.

connection_error_filter_warn() ->
    % Install the logger filter, configured to stop connection errors.
    ok = logger:add_primary_filter(
        stop_connection_errors, {fun kafine_logger_filters:connection_errors/2, warn}
    ),

    start_stop_connection(),

    % Assert that there were warnings instead of errors.
    Levels = get_logged_levels(),
    ?assertEqual([warning, warning], [L || L <- Levels, L == warning orelse L == error]),

    logger:remove_primary_filter(stop_connection_errors),
    ok.

connection_error_filter_stop() ->
    % Install the logger filter, configured to stop connection errors.
    ok = logger:add_primary_filter(
        stop_connection_errors, {fun kafine_logger_filters:connection_errors/2, stop}
    ),

    start_stop_connection(),

    % Assert that there were no warnings or errors.
    Levels = get_logged_levels(),
    ?assertEqual([], [L || L <- Levels, L == warning orelse L == error]),

    logger:remove_primary_filter(stop_connection_errors),
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
            ({_Pid, {_M, _F, _A = [_LogEvent = #{level := Level}, _Config]}, _Return}) ->
                {true, Level};
            (_) ->
                false
        end,
        meck:history(test_handler)
    ).
