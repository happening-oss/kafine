-module(kafine_connection_telemetry).
-export([
    span/3,

    start_span/2,
    stop_span/3,
    span_exception/4
]).

-define(SPAN_ENRICHMENT_MODULE_ENV_VAR, span_enrichment_module).

-spec span(
    SpanEvent :: telemetry:event_prefix(),
    Metadata :: telemetry:event_metadata(),
    SpanFunction :: kafine_telemetry:span_function(SpanResult)
) -> SpanResult.
% @doc Very similar to telemetry:span but uses kafine_telemetry to handle span start and stop/exception.
span(SpanEvent, Metadata0, SpanFunction) when
    is_list(SpanEvent), is_map(Metadata0), is_function(SpanFunction)
->
    {Measurements, Metadata1} = start_span(SpanEvent, Metadata0),

    try
        {Result, Metadata2} = SpanFunction(),
        stop_span(SpanEvent, {Measurements, Metadata2}, Result),
        Result
    catch
        Class:Reason:Stacktrace ->
            span_exception(
                SpanEvent,
                {Measurements, Metadata1},
                error,
                Reason
            ),
            erlang:raise(Class, Reason, Stacktrace)
    end.

start_span(SpanEvent, Metadata) ->
    kafine_telemetry:start_span(SpanEvent, Metadata).

stop_span(SpanEvent, Span, Response) ->
    SpanCallback = application:get_env(kafine, ?SPAN_ENRICHMENT_MODULE_ENV_VAR, undefined),
    do_stop_span(SpanEvent, Span, Response, SpanCallback).

do_stop_span(SpanEvent, Span0, _Response, undefined) ->
    kafine_telemetry:stop_span(SpanEvent, Span0);
do_stop_span(SpanEvent, Span0, Response, SpanCallback) ->
    Span1 = SpanCallback:enrich_span(Response, Span0),
    kafine_telemetry:stop_span(SpanEvent, Span1).

span_exception(SpanEvent, Span, Class, Reason) ->
    kafine_telemetry:span_exception(
        SpanEvent,
        Span,
        Class,
        Reason
    ).
