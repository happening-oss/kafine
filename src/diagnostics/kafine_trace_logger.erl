-module(kafine_trace_logger).
-export([init/2]).

-record(trace, {
    level :: logger:level(),
    metadata :: logger:metadata()
}).

init(Level, Metadata) ->
    FuncId = ?MODULE,
    Func = fun tracer/3,
    FuncState = #trace{level = Level, metadata = Metadata},
    {FuncId, Func, FuncState}.

% TODO: gen_server has {in, '$gen_call', '$gen_cast'}, out, noreply
tracer(
    TraceState = #trace{level = Level, metadata = Metadata},
    _Event = {in, Event, State},
    StateData
) ->
    % For gen_statem and gen_server, StateData is the pid or the registered name, as returned by gen:get_proc_name/1.
    % If using {via, Reg, Name}, 'Name' is used.
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
    _Event = {enter, Module, State},
    StateData
) ->
    logger:log(
        Level,
        "~tp enter ~tp in state ~tp",
        [StateData, Module, State],
        Metadata
    ),
    TraceState;
% TODO: module, state_timer, insert_timeout
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
tracer(TraceState = #trace{level = Level, metadata = Metadata}, Event, StateData) ->
    logger:log(Level, "~p: ~p", [Event, StateData], Metadata),
    TraceState.

% TODO: handle $gen_call, $gen_cast here...?
event_string({{call, {From, _Tag}}, Request}) ->
    io_lib:format("call ~tp from ~tw", [Request, From]);
event_string({EventType, EventContent}) ->
    io_lib:format("~tw ~tp", [EventType, EventContent]).
