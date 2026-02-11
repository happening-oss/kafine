-module(kafine_connection_telemetry_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/api_key.hrl").
-include_lib("kafcod/include/error_code.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(LABEL, 'label').
-define(EMPTY_REQUEST_IDS, #{}).

all_test_() ->
    [
        fun call_api_versions/0,
        fun request_api_versions/0,
        fun call_metadata_without_metadata/0
    ].

call_api_versions() ->
    TelemetryRef = attach_event_handlers(),

    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    ClusterId = <<"kamock_cluster">>,
    Metadata = #{cluster_id => ClusterId},
    {ok, C} = kafine_connection:start_link(Broker, #{}),

    ?assertMatch(
        {ok, #{error_code := ?NONE, api_keys := [_ | _]}},
        kafine_connection:call(
            C,
            fun api_versions_request:encode_api_versions_request_1/1,
            #{},
            fun api_versions_response:decode_api_versions_response_1/1,
            Metadata#{api_key => ?API_VERSIONS, api_version => 1}
        )
    ),

    % Did we get enough information from the telemetry event?
    ?assertMatch(
        [
            {
                [kafine, connection, call, start],
                TelemetryRef,
                #{monotonic_time := _, system_time := _},
                #{
                    % These are standard telemetry measurements.
                    telemetry_span_context := _,
                    % These are from Metadata, above.
                    api_key := ?API_VERSIONS,
                    api_version := 1,
                    cluster_id := _,
                    % kafine_connection adds these.
                    host := _,
                    port := _,
                    node_id := 101
                }
            },
            {
                [kafine, connection, call, stop],
                TelemetryRef,
                #{
                    % These are standard telemetry measurements for stop events.
                    monotonic_time := _,
                    duration := _,
                    % kafine_connection adds this.
                    response := _
                },
                #{
                    % These are standard telemetry measurements.
                    telemetry_span_context := _,
                    % These are from Metadata, above.
                    api_key := ?API_VERSIONS,
                    api_version := 1,
                    cluster_id := _,
                    % kafine_connection adds these.
                    host := _,
                    port := _,
                    node_id := 101
                }
            }
        ],
        flush()
    ),

    kafine_connection:stop(C),
    kamock_broker:stop(Broker),

    telemetry:detach(TelemetryRef),
    ok.

request_api_versions() ->
    TelemetryRef = attach_event_handlers(),

    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    ClusterId = <<"kamock_cluster">>,
    Metadata = #{cluster_id => ClusterId},
    {ok, C} = kafine_connection:start_link(Broker, #{}),

    ReqIds = kafine_connection:reqids_new(),
    ReqIds2 = kafine_connection:send_request(
        C,
        fun api_versions_request:encode_api_versions_request_1/1,
        #{},
        fun api_versions_response:decode_api_versions_response_1/1,
        ?LABEL,
        ReqIds,
        Metadata#{api_key => ?API_VERSIONS, api_version => 1}
    ),

    ?assertMatch(
        {{ok, #{error_code := ?NONE, api_keys := [_ | _]}}, ?LABEL, ?EMPTY_REQUEST_IDS},
        kafine_connection:wait_response(ReqIds2, 1_000, true)
    ),

    % Did we get enough information from the telemetry event?
    ?assertMatch(
        [
            {
                [kafine, connection, request, start],
                TelemetryRef,
                #{monotonic_time := _, system_time := _},
                #{
                    % These are standard telemetry measurements.
                    telemetry_span_context := _,
                    % These are from Metadata, above.
                    api_key := ?API_VERSIONS,
                    api_version := 1,
                    cluster_id := _,
                    % kafine_connection adds these.
                    host := _,
                    port := _,
                    node_id := 101
                }
            },
            {
                [kafine, connection, request, stop],
                TelemetryRef,
                #{
                    % These are standard telemetry measurements for stop events.
                    monotonic_time := _,
                    duration := _,
                    % kafine_connection adds this.
                    response := _
                },
                #{
                    % These are standard telemetry measurements.
                    telemetry_span_context := _,
                    % These are from Metadata, above.
                    api_key := ?API_VERSIONS,
                    api_version := 1,
                    cluster_id := _,
                    % kafine_connection adds these.
                    host := _,
                    port := _,
                    node_id := 101
                }
            }
        ],
        flush()
    ),

    kafine_connection:stop(C),
    kamock_broker:stop(Broker),

    telemetry:detach(TelemetryRef),
    ok.

call_metadata_without_metadata() ->
    TelemetryRef = attach_event_handlers(),

    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, C} = kafine_connection:start_link(Broker, #{}),

    ?assertMatch(
        {ok, #{cluster_id := _, brokers := _, controller_id := _, topics := _}},
        kafine_connection:call(
            C,
            fun metadata_request:encode_metadata_request_5/1,
            #{
                allow_auto_topic_creation => false,
                topics => []
            },
            fun metadata_response:decode_metadata_response_5/1
        )
    ),

    kafine_connection:stop(C),
    kamock_broker:stop(Broker),

    telemetry:detach(TelemetryRef),
    ok.

attach_event_handlers() ->
    telemetry_test:attach_event_handlers(self(), [
        [kafine, connection, request, start],
        [kafine, connection, request, stop],
        [kafine, connection, request, exception],

        [kafine, connection, call, start],
        [kafine, connection, call, stop],
        [kafine, connection, call, exception]
    ]).

flush() ->
    flush([]).

flush(Acc) ->
    receive
        M ->
            flush([M | Acc])
    after 100 ->
        lists:reverse(Acc)
    end.
