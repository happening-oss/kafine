#!/usr/bin/env escript
%% ^^ escript requires _something_ here; it ignores the first line before looking for Erlang source code.
%% Might as well use a shebang.

main(_) ->
    % Because this runs before the 'apps' listed in rebar.config, we have to start the applications ourselves if we want
    % to do anything interesting.
    % {ok, _} = application:ensure_all_started(kafine),

    % Interesting things go here.
    ok.
