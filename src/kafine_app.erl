-module(kafine_app).
-moduledoc false.
-behaviour(application).

-export([start/2]).
-export([stop/1]).

start(_Type, _Args) ->
    kafine_sup:start_link().

stop(_State) ->
    ok.
