-module(kafine_connection_telemetry_tests).

-include_lib("eunit/include/eunit.hrl").

span_returns_span_fun_result_test() ->
    Metadata = #{},
    SpanFunResponse = kafine_connection_telemetry:span(
        [test, event],
        Metadata,
        fun() ->
            Result = #{response => true},
            {Result, Metadata}
        end
    ),
    ?assertMatch(#{response := true}, SpanFunResponse),
    ok.

start_and_stop_span_is_called_when_no_errors_test() ->
    meck:new(kafine_telemetry, [passthrough]),
    Metadata = #{},
    SpanFunResponse = kafine_connection_telemetry:span(
        [test, event],
        Metadata,
        fun() ->
            Result = #{response => true},
            {Result, Metadata}
        end
    ),
    ?assertMatch(#{response := true}, SpanFunResponse),
    ?assert(meck:called(kafine_telemetry, start_span, '_')),
    ?assert(meck:called(kafine_telemetry, stop_span, '_')),
    meck:unload(kafine_telemetry),
    ok.

span_exception_is_called_when_error_is_raised_test() ->
    meck:new(kafine_telemetry, [passthrough]),
    Metadata = #{},
    ?assertError(
        test_error,
        kafine_connection_telemetry:span(
            [test, event],
            Metadata,
            fun() ->
                erlang:raise(error, test_error, [])
            end
        )
    ),
    ?assert(meck:called(kafine_telemetry, span_exception, '_')),
    meck:unload(kafine_telemetry),
    ok.

metadata_is_passed_through_correctly_test() ->
    meck:new(kafine_telemetry, [passthrough]),
    Event = [test, event],
    Metadata = #{meta => []},
    Metadata1 = #{meta => [1]},
    SpanFunResponse = kafine_connection_telemetry:span(
        Event,
        Metadata,
        fun() ->
            Result = #{response => true},
            {Result, Metadata1}
        end
    ),
    ?assertMatch(#{response := true}, SpanFunResponse),
    ?assert(
        meck:called(kafine_telemetry, start_span, [
            meck:is(fun(E) -> E =:= Event end),
            meck:is(fun(Meta) -> Meta =:= Metadata end)
        ])
    ),
    ?assert(
        meck:called(kafine_telemetry, stop_span, [
            meck:is(fun(E) -> E =:= Event end),
            meck:is(fun({_, Meta}) ->
                Meta =:= Metadata1
            end)
        ])
    ),
    meck:unload(kafine_telemetry),
    ok.
