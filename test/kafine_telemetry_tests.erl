-module(kafine_telemetry_tests).
-include_lib("eunit/include/eunit.hrl").
-include("../src/kafine_eqwalizer.hrl").

-define(TOPIC_NAME, atom_to_binary(?FUNCTION_NAME)).
-define(PARTITION, 61).

all_test_() ->
    % Note that if any of the tests fail, later tests might get duplicate telemetry events, because detach didn't get
    % called.
    {setup, fun() -> {ok, _} = application:ensure_all_started(telemetry) end,
        % Use 'spawn' so that each test runs in a separate process; otherwise a failing test fails to detach the
        % telemetry event handlers, and we get duplicate messages in later tests, which causes them to fail in a
        % confusing way.
        %
        % By using a separate process for each test, the handler is automatically detached (or the messages go into the
        % void, which works just as well for our purposes).
        {foreach, spawn, fun() -> ok end, [
            fun start_stop/0,
            fun start_stop_extra/0,

            fun telemetry_span/0,
            fun telemetry_span_extra/0,
            fun telemetry_span_exception/0,

            fun span/0,
            fun span_extra/0,
            fun span_exception/0
        ]}}.

start_stop() ->
    Ref = telemetry_test:attach_event_handlers(self(), [
        [worker, processing, start], [worker, processing, stop], [worker, processing, exception]
    ]),

    % If you can't wrap something in telemetry:span (e.g. you're doing something asynchronously), you will need to use
    % separate start_span/stop_span functions.
    Span = kafine_telemetry:start_span([worker, processing], #{
        node_id => 101, topic => ?TOPIC_NAME, partition => ?PARTITION
    }),

    % Work happens here.

    kafine_telemetry:stop_span([worker, processing], Span),

    Messages = flush(),
    ?assertMatch(
        [
            {[worker, processing, start], Ref, #{monotonic_time := _, system_time := _}, #{
                node_id := 101, topic := _, partition := _, telemetry_span_context := _
            }},
            % Unlike telemetry:span(), kafine_telemetry:stop_span() _does_ copy the metadata from the start (it's in the Span object).
            {[worker, processing, stop], Ref, #{monotonic_time := _, duration := _}, #{
                node_id := 101, topic := _, partition := _, telemetry_span_context := _
            }}
        ],
        Messages
    ),
    telemetry:detach(Ref),
    ok.

%% kafine_telemetry allows you to add extra measurements and metadata to the span.
start_stop_extra() ->
    Ref = telemetry_test:attach_event_handlers(self(), [
        [worker, processing, start], [worker, processing, stop], [worker, processing, exception]
    ]),

    % If you can't wrap something in telemetry:span (e.g. you're doing something asynchronously), you will need to use
    % separate start_span/stop_span functions.
    Span = kafine_telemetry:start_span(
        [worker, processing],
        % Extra measurement: the fetch offset.
        #{fetch_offset => 4473},
        #{node_id => 101, topic => ?TOPIC_NAME, partition => ?PARTITION}
    ),

    % Work happens here.

    kafine_telemetry:stop_span(
        [worker, processing],
        Span,
        % Extra measurement: the next offset.
        #{next_offset => 4498},
        #{}
    ),

    Messages = flush(),
    ?assertMatch(
        [
            {
                [worker, processing, start],
                Ref,
                #{monotonic_time := _, system_time := _, fetch_offset := _},
                #{
                    node_id := 101, topic := _, partition := _, telemetry_span_context := _
                }
            },
            {
                [worker, processing, stop],
                Ref,
                #{monotonic_time := _, duration := _, fetch_offset := _, next_offset := _},
                #{
                    node_id := 101, topic := _, partition := _, telemetry_span_context := _
                }
            }
        ],
        Messages
    ),
    telemetry:detach(Ref),
    ok.

%% See what telemetry:span/3 does.
telemetry_span() ->
    Ref = telemetry_test:attach_event_handlers(self(), [
        [worker, processing, start], [worker, processing, stop], [worker, processing, exception]
    ]),

    StartMetadata = #{node_id => 101, topic => ?TOPIC_NAME, partition => ?PARTITION},
    ?assertEqual(
        ok,
        telemetry:span(
            [worker, processing], StartMetadata, fun() ->
                % `telemetry:span/3` doesn't copy the start metadata into the stop event, so you have to do it yourself.
                % Here, we don't.
                Result = ok,
                % Any metadata returned here is merged with the default 'telemetry_span_context' metadata.
                StopMetadata = #{worker_vsn => 12},
                {Result, StopMetadata}
            end
        )
    ),

    Messages = flush(),
    ?assertMatch(
        [
            {[worker, processing, start], Ref, #{monotonic_time := _, system_time := _}, #{
                node_id := 101, topic := _, partition := _, telemetry_span_context := _
            }},
            % node_id is missing from the stop metadata.
            {[worker, processing, stop], Ref, #{monotonic_time := _, duration := _}, #{
                telemetry_span_context := _, worker_vsn := _
            }}
        ],
        Messages
    ),
    telemetry:detach(Ref),
    ok.

%% See what telemetry:span/3 does, when we return extra info.
telemetry_span_extra() ->
    Ref = telemetry_test:attach_event_handlers(self(), [
        [worker, processing, start], [worker, processing, stop], [worker, processing, exception]
    ]),

    StartMetadata = #{node_id => 101, topic => ?TOPIC_NAME, partition => ?PARTITION},
    ?assertEqual(
        ok,
        telemetry:span(
            [worker, processing],
            StartMetadata,
            ?DYNAMIC_CAST(fun() ->
                % `telemetry:span/3` doesn't copy the start metadata into the stop event, so you have to do it yourself.
                % Here, we don't.
                Result = ok,
                % Any metadata returned here is merged with the default 'telemetry_span_context' metadata.
                StopMetadata = #{worker_vsn => 12},
                ExtraMeasurements = #{records_processed => 42},
                {Result, ExtraMeasurements, StopMetadata}
            end)
        )
    ),

    Messages = flush(),
    ?assertMatch(
        [
            {[worker, processing, start], Ref, #{monotonic_time := _, system_time := _}, #{
                node_id := 101, topic := _, partition := _, telemetry_span_context := _
            }},
            % node_id is missing from the stop metadata; records_processed is added to the measurements.
            {
                [worker, processing, stop],
                Ref,
                #{monotonic_time := _, duration := _, records_processed := _},
                #{telemetry_span_context := _, worker_vsn := _}
            }
        ],
        Messages
    ),
    telemetry:detach(Ref),
    ok.

span() ->
    Ref = telemetry_test:attach_event_handlers(self(), [
        [worker, processing, start], [worker, processing, stop], [worker, processing, exception]
    ]),

    ?assertEqual(
        ok,
        kafine_telemetry:span(
            [worker, processing],
            #{node_id => 101, topic => ?TOPIC_NAME, partition => ?PARTITION},
            fun() ->
                % `telemetry:span/3` doesn't copy the start metadata into the stop event, but we _do_, to make it more
                % like kafine_telemetry:start_span/2 and kafine_telemetry:stop_span/2.
                Result = ok,
                % Any metadata we return here overrides the start metadata
                StopMetadata = #{worker_vsn => 12, node_id => 102},
                {Result, StopMetadata}
            end
        )
    ),

    Messages = flush(),
    ?assertMatch(
        [
            {[worker, processing, start], Ref, #{monotonic_time := _, system_time := _}, #{
                node_id := 101, telemetry_span_context := _
            }},
                % node_id is updated; topic and partition are copied; telemetry_span_context is preserved; worker_vsn is added.
            {[worker, processing, stop], Ref, #{monotonic_time := _, duration := _}, #{
                node_id := 102,
                topic := _,
                partition := _,
                telemetry_span_context := _,
                worker_vsn := _
            }}
        ],
        Messages
    ),
    telemetry:detach(Ref),
    ok.

span_extra() ->
    Ref = telemetry_test:attach_event_handlers(self(), [
        [worker, processing, start], [worker, processing, stop], [worker, processing, exception]
    ]),

    ?assertEqual(
        ok,
        kafine_telemetry:span(
            [worker, processing],
            #{node_id => 101, topic => ?TOPIC_NAME, partition => ?PARTITION},
            fun() ->
                % `telemetry:span/3` doesn't copy the start metadata into the stop event, but we _do_, to make it more
                % like kafine_telemetry:start_span/2 and kafine_telemetry:stop_span/2.
                Result = ok,
                % Any metadata we return here is merged with the start metadata.
                StopMetadata = #{worker_vsn => 12, node_id => 102},
                ExtraMeasurements = #{records_processed => 42},
                {Result, ExtraMeasurements, StopMetadata}
            end
        )
    ),

    Messages = flush(),
    ?assertMatch(
        [
            {[worker, processing, start], Ref, #{monotonic_time := _, system_time := _}, #{
                node_id := 101, telemetry_span_context := _
            }},
            {
                [worker, processing, stop],
                Ref,
                #{monotonic_time := _, duration := _, records_processed := _},
                % node_id is updated; topic and partition are copied; telemetry_span_context is preserved; worker_vsn is added.
                #{
                    node_id := 102,
                    topic := _,
                    partition := _,
                    telemetry_span_context := _,
                    worker_vsn := _
                }
            }
        ],
        Messages
    ),
    telemetry:detach(Ref),
    ok.

telemetry_span_exception() ->
    Ref = telemetry_test:attach_event_handlers(self(), [
        [worker, processing, start], [worker, processing, stop], [worker, processing, exception]
    ]),

    StartMetadata = #{node_id => 101, topic => ?TOPIC_NAME, partition => ?PARTITION},
    ?assertError(
        computer_says_no,
        telemetry:span(
            [worker, processing], StartMetadata, fun() ->
                % `telemetry:span/3` doesn't copy the start metadata into the stop event.
                error(computer_says_no),
                {[], #{}}
            end
        )
    ),

    assert_span_exception_messages(),
    telemetry:detach(Ref),
    ok.

span_exception() ->
    Ref = telemetry_test:attach_event_handlers(self(), [
        [worker, processing, start], [worker, processing, stop], [worker, processing, exception]
    ]),

    StartMetadata = #{node_id => 101, topic => ?TOPIC_NAME, partition => ?PARTITION},
    ?assertError(
        computer_says_no,
        kafine_telemetry:span(
            [worker, processing], StartMetadata, fun() ->
                % `telemetry:span/3` doesn't copy the start metadata into the stop event, so neither do we.
                error(computer_says_no),
                {[], #{}}
            end
        )
    ),

    assert_span_exception_messages(),
    telemetry:detach(Ref),
    ok.

assert_span_exception_messages() ->
    Messages = flush(),
    ?assertMatch(
        [
            {[worker, processing, start], Ref, #{monotonic_time := _, system_time := _}, #{
                node_id := 101, topic := _, partition := _, telemetry_span_context := _
            }},
            % exception metadata _does_ have the start metadata in it. Also kind (class), reason, stacktrace
            {[worker, processing, exception], Ref, #{monotonic_time := _, duration := _}, #{
                node_id := 101,
                topic := _,
                partition := _,
                telemetry_span_context := _,
                kind := _,
                reason := _,
                stacktrace := _
            }}
        ],
        Messages
    ).

flush() ->
    flush([]).

flush(Acc) ->
    receive
        M ->
            flush([M | Acc])
    after 100 ->
        lists:reverse(Acc)
    end.
