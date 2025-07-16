-module(test_helper).
-export([start/0]).

start() ->
    % We start the mock broker, 'kamock', because we'll use it in lots of the tests. We use 'ranch' in the TLS tests.
    % 'gproc' is used by 'kafine_via', but we don't want to start the whole 'kafine' application. If we don't start
    % 'telemetry', it logs warnings (which significantly slows down the tests).
    application:ensure_all_started([kamock, ranch, gproc, telemetry]),
    % Make sure that the 'kafine' configuration is loaded; otherwise kafine_trace doesn't work.
    application:load(kafine),
    ok.
