-module(kafine_trace).
%%% Convert `gen_server` and `gen_statem` tracing into calls to `logger`.
-moduledoc false.
-export([
    debug/0,
    debug/1,
    debug/2
]).

-record(trace, {
    level :: logger:level(),
    metadata :: logger:metadata()
}).

% TODO: Calling this 'debug', when the level is 'debug' is a bit confusing. Sort that out.
debug() ->
    debug(debug, #{}).

debug(Metadata) ->
    debug(debug, Metadata).

debug(Level, Metadata) ->
    % TODO: Maybe we'd prefer to be able to define a level in configuration. Question: is that a min/max level
    % (whichever way round) -- so we'd filter on 'Level', or the level at which to log -- so it'd replace 'Level'?
    maybe_debug(Level, Metadata, application:get_env(kafine, enable_trace, false)).

maybe_debug(Level, Metadata, _Enable = true) ->
    TraceState = #trace{level = Level, metadata = Metadata},
    % A word of caution: You can't pass a 2-tuple (i.e. a single-item record) as the function state in 'install',
    % because OTP mistakes {Func, {trace, Level}} for {FuncId, {Func, FuncState}}.
    %
    % To fix this, we're careful (even when we've got a 3-tuple) to pass the module name as FuncId. This has the bonus
    % that we won't get installed multiple times.
    [{install, {?MODULE, {fun tracer/3, TraceState}}}];
maybe_debug(_Level, _Metadata, _Enable = false) ->
    [].

% TODO: tracer only understands (some of) gen_statem right now. So it needs improving in two directions: more gen_statem
% support (see gen_statem:print_event/3); gen_server (and gen_event?) support. The second might require breaking it up
% and detecting the caller's behaviour.
tracer(
    TraceState = #trace{level = Level, metadata = Metadata},
    _Event = {in, Event, State},
    StateData
) ->
    % For gen_statem, StateData is the pid or the registered name.
    logger:log(
        Level,
        "~tp receive ~ts in state ~tp",
        [StateData, event_string(Event), State],
        Metadata
    ),
    TraceState;
tracer(
    TraceState = #trace{level = Level, metadata = Metadata},
    _Event = {out, Reply, {To, _Tag}},
    StateData
) ->
    % For gen_statem, StateData is the pid or the registered name.
    logger:log(
        Level,
        "~tp send ~tp to ~tw",
        [StateData, Reply, To],
        Metadata
    ),
    TraceState;
tracer(
    TraceState = #trace{level = Level, metadata = Metadata},
    _Event = {Tag, Event, State, State},
    StateData
) when Tag =:= postpone; Tag =:= consume ->
    % For gen_statem, StateData is the pid or the registered name.
    logger:log(
        Level,
        "~tp ~tw ~ts in state ~tp",
        [StateData, Tag, event_string(Event), State],
        Metadata
    ),
    TraceState;
tracer(
    TraceState = #trace{level = Level, metadata = Metadata},
    _Event = {Tag, Event, State, NextState},
    StateData
) when Tag =:= postpone; Tag =:= consume ->
    % For gen_statem, StateData is the pid or the registered name.
    logger:log(
        Level,
        "~tp ~tw ~ts in state ~tp => ~tp",
        [StateData, Tag, event_string(Event), State, NextState],
        Metadata
    ),
    TraceState;
tracer(
    TraceState = #trace{level = Level, metadata = Metadata},
    _Event = {terminate, Reason, State},
    StateData
) ->
    logger:log(
        Level,
        "~tp terminate ~tp in state ~tp",
        [StateData, Reason, State],
        Metadata
    ),
    TraceState;
tracer(TraceState = #trace{level = Level, metadata = Metadata}, Event, StateData) ->
    logger:log(Level, "~p: ~p", [Event, StateData], Metadata),
    TraceState.

event_string({{call, {Pid, _Tag}}, Request}) ->
    io_lib:format("call ~tp from ~tw", [Request, Pid]);
event_string({EventType, EventContent}) ->
    io_lib:format("~tw ~tp", [EventType, EventContent]).
