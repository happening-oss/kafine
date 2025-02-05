-module(kafine_behaviour_test_fail).
-export([init/1]).

%% Used in kafine_behaviour_test.
%% This module doesn't implement the behaviour.

init(_Args) ->
    % doesn't matter; never called.
    {ok, no_state}.
