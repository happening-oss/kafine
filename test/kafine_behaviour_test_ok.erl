-module(kafine_behaviour_test_ok).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, terminate/1]).

%% This module does implement the behaviour.

init(_Args) ->
    % doesn't matter; never called.
    {ok, no_state}.

handle_call(_Req, _From, S) ->
    {reply, ok, S}.

handle_cast(_Req, S) ->
    {noreply, S}.

terminate(_S) ->
    ok.
