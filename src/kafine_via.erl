-module(kafine_via).
%%% Wrapper to allow `gproc` to be used with `via`.
-moduledoc false.
-export([
    register_name/2,
    unregister_name/1,
    whereis_name/1,
    send/2
]).

%% At the moment, this is a simple wrapper around gproc. Since we might want to do something different in future, that's
%% encapsulated here.

-define(GPROC_NAME(Name), {n, l, Name}).

-spec register_name(Name :: term(), Pid :: pid()) -> yes | no.
register_name(Name, Pid) ->
    gproc:register_name(?GPROC_NAME(Name), Pid).

-spec unregister_name(Name :: term()) -> true.
unregister_name(Name) ->
    gproc:unregister_name(?GPROC_NAME(Name)).

-spec whereis_name(Name :: term()) -> pid().
whereis_name(Name) ->
    gproc:whereis_name(?GPROC_NAME(Name)).

% Note that the return value is the message. For comparison, global:send/2 returns the pid.
-spec send(Name :: term(), Message) -> Message when Message :: term().
send(Name, Message) ->
    gproc:send(?GPROC_NAME(Name), Message).
