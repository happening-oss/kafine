-module(assert_received).
-export([flush/0]).

-include_lib("eunit/include/eunit.hrl").

flush() ->
    flush([]).

flush(Acc) ->
    receive
        M ->
            flush([M | Acc])
    after 100 ->
        lists:reverse(Acc)
    end.
