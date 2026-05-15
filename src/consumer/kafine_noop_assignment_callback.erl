-module(kafine_noop_assignment_callback).
-behaviour(kafine_assignment_callback).

-export([
    init/1,
    before_assignment/3,
    after_assignment/3
]).

init(_) ->
    {ok, undefined}.

before_assignment(_, _, St) ->
    {ok, St}.

after_assignment(_, _, St) ->
    {ok, St}.
