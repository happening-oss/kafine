-module(kafine_node_consumer_telemetry_tests).
-include_lib("eunit/include/eunit.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(PARTITION_1, 61).
-define(WAIT_TIMEOUT_MS, 2_000).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun partition_data_telemetry/0
        % TODO: with some records.
        % TODO: with some lag.
    ]}.

setup() ->
    kafine_node_consumer_tests:setup(?MODULE).

cleanup(_) ->
    meck:unload().

partition_data_telemetry() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    TelemetryRef = telemetry_test:attach_event_handlers(self(), [
        [kafine, fetch, partition_data, start],
        [kafine, fetch, partition_data, stop]
    ]),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            ?PARTITION_1 => #{}
        }
    }),

    {ok, Pid} = start_node_consumer(?CONSUMER_REF, Broker, TopicPartitionStates),

    % Wait for the second fetch (so that we know we've completed the first one).
    meck:wait(2, kamock_fetch, handle_fetch_request, '_', ?WAIT_TIMEOUT_MS),

    % We should see a span.
    ?assertMatch(
        {
            [kafine, fetch, partition_data, start],
            #{monotonic_time := _, system_time := _, fetch_offset := 0, high_watermark := 0},
            #{
                ref := ?CONSUMER_REF,
                node_id := 101,
                topic := TopicName,
                partition_index := ?PARTITION_1,
                telemetry_span_context := _
            }
        },
        receive_telemetry(TelemetryRef)
    ),
    ?assertMatch(
        {
            [kafine, fetch, partition_data, stop],
            #{
                monotonic_time := _,
                system_time := _,
                duration := _,
                fetch_offset := 0,
                next_offset := 0,
                high_watermark := 0
            },
            #{
                ref := ?CONSUMER_REF,
                node_id := 101,
                topic := TopicName,
                partition_index := ?PARTITION_1,
                telemetry_span_context := _
            }
        },
        receive_telemetry(TelemetryRef)
    ),

    stop_node_consumer(Pid, TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

init_topic_partition_states(InitStates) ->
    kafine_fetch_response_tests:init_topic_partition_states(InitStates).

start_node_consumer(Ref, Broker, TopicPartitionStates) ->
    kafine_node_consumer_tests:start_node_consumer(Ref, Broker, TopicPartitionStates).

stop_node_consumer(Pid, TopicPartitionStates) ->
    kafine_node_consumer_tests:stop_node_consumer(Pid, TopicPartitionStates).

receive_telemetry(TelemetryRef) ->
    receive
        {EventName, TelemetryRef, Measurements, Metadata} ->
            {EventName, Measurements, Metadata}
    end.
