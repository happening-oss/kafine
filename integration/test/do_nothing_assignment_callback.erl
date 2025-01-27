-module(do_nothing_assignment_callback).
-behaviour(kafine_assignment_callback).

-export([init/1]).
-export([before_assignment/3]).
-export([after_assignment/3]).

init(_) ->
  {ok, undefined}.

before_assignment(_, _, St) ->
  {ok, St}.

after_assignment(_, _, St) ->
  {ok, St}.
