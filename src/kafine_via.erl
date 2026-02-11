-module(kafine_via).
%%% Wrapper to allow `gproc` to be used with `via`.
-moduledoc false.
-export([
    via/1
]).
-export([
    register_name/2,
    unregister_name/1,
    whereis_name/1,
    send/2
]).
-export_type([
    name/0,
    via/0
]).

-type name() :: term().
-type via() :: {via, kafine_via, name()}.

-spec via(Name :: name()) -> via().
via(Name) ->
    {via, kafine_via, Name}.

%% At the moment, this is a simple wrapper around gproc. Since we might want to do something different in future, that's
%% encapsulated here.

-define(GPROC_NAME(Name), {n, l, Name}).

-spec register_name(Name :: name(), Pid :: pid()) -> yes | no.
register_name(Name, Pid) ->
    gproc:register_name(?GPROC_NAME(Name), Pid).

-spec unregister_name(Name :: name()) -> true.
unregister_name(Name) ->
    gproc:unregister_name(?GPROC_NAME(Name)).

-spec whereis_name(Name :: name()) -> pid() | undefined.
whereis_name(Name) ->
    gproc:whereis_name(?GPROC_NAME(Name)).

% Note that the return value is the message. For comparison, global:send/2 returns the pid.
-spec send(Name, Message) -> Message when Name :: name(), Message :: term().
send(Name, Message) ->
    gproc:send(?GPROC_NAME(Name), Message).
