-module(kafine_behaviour_test_fail).
-export([init/1]).

%% This module doesn't implement the behaviour.

init(_Args) ->
    % doesn't matter; never called.
    {ok, no_state}.
