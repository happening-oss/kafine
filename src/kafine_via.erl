-module(kafine_via).
-export([
    register_name/2,
    unregister_name/1,
    whereis_name/1,
    send/2
]).

%% At the moment, this is a simple wrapper around gproc. Since we might want to do something different in future, that's
%% encapsulated here.

-define(GPROC_NAME(Name), {n, l, Name}).

register_name(Name, Pid) ->
    gproc:register_name(?GPROC_NAME(Name), Pid).

unregister_name(Name) ->
    gproc:unregister_name(?GPROC_NAME(Name)).

whereis_name(Name) ->
    gproc:whereis_name(?GPROC_NAME(Name)).

send(Name, Message) ->
    gproc:send(?GPROC_NAME(Name), Message).
