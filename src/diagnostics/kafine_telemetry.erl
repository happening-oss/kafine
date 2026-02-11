-module(kafine_telemetry).
-moduledoc false.

-export([
    put_metadata/1,
    get_metadata/1,

    span/3,

    start_span/2,
    start_span/3,

    stop_span/2,
    stop_span/4,

    span_exception/4,
    span_exception/5
]).

-export_type([
    span/0,
    span_function/1
]).

-include("../kafine_eqwalizer.hrl").

-define(TELEMETRY_KEY, ?MODULE).

-type span() :: {telemetry:event_measurements(), telemetry:event_metadata()}.

% It's occasionally useful to wrap a gen_server:call or gen_statem:call in a telemetry:span, but to do that we need the
% callee's telemetry metadata. We put it in the process dictionary. Currently used only for kafine_connection.
put_metadata(Metadata) when is_map(Metadata) ->
    put(?TELEMETRY_KEY, Metadata),
    ok.

get_metadata(Pid) when is_pid(Pid) ->
    kafine_proc_lib:get_dictionary(Pid, ?TELEMETRY_KEY, #{}).

-type span_function(SpanResult) :: fun(() -> span_function_ret(SpanResult)).
-type span_function_ret(SpanResult) ::
    {SpanResult, StopMetadata :: telemetry:event_metadata()}
    | {SpanResult, ExtraMeasurements :: telemetry:event_measurements(),
        StopMetadata :: telemetry:event_metadata()}.

-spec span(
    EventPrefix :: telemetry:event_prefix(),
    StartMetadata :: telemetry:event_metadata(),
    SpanFunction :: span_function(SpanResult)
) -> SpanResult.

% Note that, unlike telemetry:span/3, we _do_ merge the start metadata and the stop metadata. This more closely mirrors
% the behaviour of our start_span() and stop_span() functions.
span(EventPrefix, StartMetadata, SpanFunction) ->
    % The type specs on telemetry:span are too tight -- `span_result() :: term()` should be a type variable.
    % It annoys eqwalizer. Use ?DYNAMIC_CAST to hide it.
    % ELP doesn't recognise telemetry:span/3 because of the docstring macros, so we suppress that warning as well.
    % elp:ignore W0017 (undefined_function)
    ?DYNAMIC_CAST(
        telemetry:span(
            EventPrefix,
            StartMetadata,
            wrap_span_function(StartMetadata, SpanFunction)
        )
    ).

wrap_span_function(StartMetadata, SpanFunction) ->
    fun() ->
        case SpanFunction() of
            {Result, StopMetadata} ->
                {Result, maps:merge(StartMetadata, StopMetadata)};
            {Result, ExtraMeasurements, StopMetadata} ->
                {Result, ExtraMeasurements, maps:merge(StartMetadata, StopMetadata)}
        end
    end.

-spec start_span(
    EventPrefix :: telemetry:event_prefix(),
    Metadata :: telemetry:event_metadata()
) ->
    span().

start_span(EventPrefix, Metadata) ->
    start_span(EventPrefix, #{}, Metadata).

start_span(EventPrefix, Measurements, Metadata) ->
    StartTime = erlang:monotonic_time(),
    DefaultCtx = erlang:make_ref(),
    Measurements2 = maps:merge(Measurements, #{
        monotonic_time => StartTime, system_time => erlang:system_time()
    }),
    Metadata2 = merge_ctx(Metadata, DefaultCtx),
    telemetry:execute(
        EventPrefix ++ [start],
        Measurements2,
        Metadata2
    ),
    {Measurements2, Metadata2}.

-spec stop_span(EventPrefix :: telemetry:event_prefix(), Span :: span()) -> ok.

stop_span(
    EventPrefix, Span = {_Measurements0, _Metadata0}
) ->
    stop_span(EventPrefix, Span, #{}, #{}).

-spec stop_span(
    EventPrefix :: telemetry:event_prefix(),
    Span :: span(),
    Measurements :: telemetry:event_measurements(),
    Metadata :: telemetry:event_metadata()
) -> ok.

stop_span(
    EventPrefix, _Span = {Measurements0, Metadata0}, Measurements, Metadata
) ->
    #{monotonic_time := StartTime} = Measurements0,
    StopTime = erlang:monotonic_time(),
    StopMetadata = maps:merge(Metadata0, Metadata),
    StopMeasurements = maps:merge(Measurements0, Measurements#{
        duration => StopTime - StartTime, monotonic_time => StopTime
    }),
    telemetry:execute(
        EventPrefix ++ [stop],
        StopMeasurements,
        StopMetadata#{}
    ).

span_exception(
    EventPrefix,
    Span,
    Class,
    Reason
) ->
    span_exception(EventPrefix, Span, Class, Reason, undefined).

span_exception(
    EventPrefix,
    _Span = {Measurements, Metadata},
    Class,
    Reason,
    Stacktrace
) ->
    #{monotonic_time := StartTime} = Measurements,
    StopTime = erlang:monotonic_time(),
    StopMetadata = Metadata,
    telemetry:execute(
        EventPrefix ++ [exception],
        #{duration => StopTime - StartTime, monotonic_time => StopTime},
        StopMetadata#{kind => Class, reason => Reason, stacktrace => Stacktrace}
    ).

merge_ctx(Metadata = #{telemetry_span_context := _}, _Ctx) -> Metadata;
merge_ctx(Metadata, Ctx) -> Metadata#{telemetry_span_context => Ctx}.
