-module(kafine_telemetry).
-moduledoc false.

-export([
    put_metadata/1,
    get_metadata/1,

    span/3,

    start_span/2,
    stop_span/2,
    span_exception/4,
    span_exception/5,

    subscribed_to/1
]).

-export_type([span/0]).

-define(TELEMETRY_KEY, ?MODULE).

-type span() :: {telemetry:event_measurements(), telemetry:event_metadata()}.

% It's occasionally useful to wrap a gen_server:call or gen_statem:call in a telemetry:span, but to do that we need the
% callee's telemetry metadata. We put it in the process dictionary.
put_metadata(Metadata) when is_map(Metadata) ->
    put(?TELEMETRY_KEY, Metadata),
    ok.

get_metadata(Pid) when is_pid(Pid) ->
    try
        case process_info(Pid, {dictionary, ?TELEMETRY_KEY}) of
            {_, Metadata} -> Metadata;
            _ -> #{}
        end
    catch
        error:badarg ->
            {dictionary, D} = process_info(Pid, dictionary),
            proplists:get_value(?TELEMETRY_KEY, D, #{})
    end.

-spec span(
    EventPrefix :: telemetry:event_prefix(),
    StartMetadata :: telemetry:event_metadata(),
    SpanFunction :: fun(() -> {SpanResult, telemetry:event_metadata()})
) -> SpanResult.

span(EventPrefix, StartMetadata, SpanFunction) ->
    % The type specs on telemetry:span are too tight. It annoys eqwalizer. Use a utility function to hide it.
    delete_type_(telemetry:span(EventPrefix, StartMetadata, SpanFunction)).

delete_type_(Value) ->
    % The underscore suffix in the name means "ugly".
    Value.

-spec start_span(
    EventPrefix :: telemetry:event_prefix(),
    Metadata :: telemetry:event_metadata()
) ->
    span().

start_span(EventPrefix, StartMetadata) ->
    StartTime = erlang:monotonic_time(),
    DefaultCtx = erlang:make_ref(),
    Measurements = #{monotonic_time => StartTime, system_time => erlang:system_time()},
    Metadata = merge_ctx(StartMetadata, DefaultCtx),
    telemetry:execute(
        EventPrefix ++ [start],
        Measurements,
        Metadata
    ),
    {Measurements, Metadata}.

-spec stop_span(EventPrefix :: telemetry:event_prefix(), Span :: span()) -> ok.

stop_span(
    EventPrefix, _Span = {Measurements, Metadata}
) ->
    #{monotonic_time := StartTime} = Measurements,
    StopTime = erlang:monotonic_time(),
    StopMetadata = Metadata,
    telemetry:execute(
        EventPrefix ++ [stop],
        #{duration => StopTime - StartTime, monotonic_time => StopTime},
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

subscribed_to(TopicPartitionStates) ->
    % This creates a map of #{topic => partitions} from the TopicPartitionStates
    % that is returned from the consumer.
    maps:map(
        fun(_T, Ps) ->
            maps:keys(Ps)
        end,
        TopicPartitionStates
    ).
