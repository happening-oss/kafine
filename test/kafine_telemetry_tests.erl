-module(kafine_telemetry_tests).
-include_lib("eunit/include/eunit.hrl").

span_test() ->
    {ok, _} = application:ensure_all_started(telemetry),

    Ref = telemetry_test:attach_event_handlers(self(), [
        [worker, processing, start], [worker, processing, stop], [worker, processing, exception]
    ]),

    % If you can't wrap something in telemetry:span (e.g. you're doing something asynchronously), you will need to use
    % separate start_span/stop_span functions.
    Span = kafine_telemetry:start_span([worker, processing], #{node_id => 101}),
    kafine_telemetry:stop_span([worker, processing], Span),

    Messages = flush(),
    ?assertMatch(
        [
            {[worker, processing, start], Ref, #{monotonic_time := _, system_time := _}, #{
                node_id := 101, telemetry_span_context := _
            }},
            {[worker, processing, stop], Ref, #{monotonic_time := _, duration := _}, #{
                node_id := 101, telemetry_span_context := _
            }}
        ],
        Messages
    ),
    telemetry:detach(Ref),
    ok.

flush() ->
    flush([]).

flush(Acc) ->
    receive
        M ->
            flush([M | Acc])
    after 100 ->
        lists:reverse(Acc)
    end.
