-module(kafine_diag).
-export([info/0]).

info() ->
    #{
        consumers => kafine_consumer:info(),
        producers => kafine_producer:info()
    }.
