-module(test_helper).
-export([start/0]).

start() ->
    % 'ranch' is used by the mock broker. If we don't start 'telemetry', it logs warnings (which significantly slows
    % down the tests).
    application:ensure_all_started([kamock, gproc, telemetry]),
    % Make sure that the 'kafine' configuration is loaded; otherwise kafine_trace doesn't work.
    application:load(kafine),
    ok.
