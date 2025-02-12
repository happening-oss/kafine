-module(kafine_trace).
%%% Convert `gen_server` and `gen_statem` tracing into calls to `logger`.
-moduledoc false.
-export([
    debug_options/0,
    debug_options/1,
    debug_options/2
]).

-record(trace, {
    level :: logger:level(),
    metadata :: logger:metadata()
}).

-spec debug_options() -> [sys:debug_option()].
debug_options() ->
    debug_options(debug, #{}).

-spec debug_options(Metadata :: logger:metadata()) -> [sys:debug_option()].
debug_options(Metadata) ->
    debug_options(debug, Metadata).

-spec debug_options(Level :: logger:level(), Metadata :: logger:metadata()) -> [sys:debug_option()].
debug_options(Level, Metadata) ->
    maybe_debug(Level, Metadata, is_trace_enabled()).

-spec is_trace_enabled() -> true | false.
is_trace_enabled() ->
    case application:get_env(kafine, enable_trace, false) of
        true -> true;
        _ -> false
    end.

-spec maybe_debug(Level :: logger:level(), Metadata :: logger:metadata(), Enable :: true | false) ->
    [sys:debug_option()].
maybe_debug(Level, Metadata, _Enable = true) ->
    % A word of caution: You can't pass a 2-tuple (i.e. a single-item record) as the function state in 'install',
    % because OTP mistakes {Func, {trace, Level}} for {FuncId, {Func, FuncState}}, which is how it internally stores
    % {FuncId, Func, FuncState}; see https://github.com/erlang/otp/blob/master/lib/stdlib/src/sys.erl#L1069
    %
    % To fix this, we're careful to use the 3-tuple form, passing the module name as FuncId. This has the bonus that we
    % won't get installed multiple times.
    FuncId = ?MODULE,
    Func = fun tracer/3,
    FuncState = #trace{level = Level, metadata = Metadata},
    [{install, {FuncId, Func, FuncState}}];
maybe_debug(_Level, _Metadata, _Enable = false) ->
    % If 'enable_trace' is false, use an empty list. gen_server and gen_statem will spot this and optimise for it.
    [].

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
