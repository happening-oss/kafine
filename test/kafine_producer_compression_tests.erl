-module(kafine_producer_compression_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/error_code.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(PRODUCER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(CONNECTION_OPTIONS, #{}).
-define(PARTITION_1, 1).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun compression_gzip/0,
        fun compression_snappy/0
    ]}.

setup() ->
    meck:new(kamock_partition_produce_response, [passthrough]),
    ok.

cleanup(_) ->
    meck:unload(),
    ok.

compression_gzip() ->
    compression(gzip).

compression_snappy() ->
    compression(snappy).

compression(Compression) ->
    TelemetryRef = attach_telemetry(),

    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    {ok, Pid} = kafine_producer:start_link(?PRODUCER_REF, Broker, ?CONNECTION_OPTIONS),

    BatchAttributes = #{compression => Compression},
    {ok, #{error_code := ?NONE}} = kafine_producer:produce(
        Pid, ?TOPIC_NAME, ?PARTITION_1, #{}, BatchAttributes, [
            #{
                key => <<"key">>,
                value => <<"value">>,
                headers => []
            }
        ]
    ),

    % We should see some telemetry from kafcod that tells us about the compression.
    ?assertMatch(
        {
            [kafcod, record_batch, compress_records],
            #{
                compression := Compression,
                uncompressed_byte_size := _,
                compressed_byte_size := _
            },
            #{}
        },
        receive_telemetry(TelemetryRef)
    ),

    % The mock broker, once we're in the handler, sees the uncompressed records, though it still sees that it _was_
    % compressed.
    [{_, {_, _, [_Topic, PartitionProduceData, _Env]}, _}] = meck:history(
        kamock_partition_produce_response
    ),
    #{index := ?PARTITION_1, records := [RecordBatch]} = PartitionProduceData,
    ?assertMatch(#{attributes := #{compression := Compression}}, RecordBatch),

    kafine_producer:stop(Pid),
    kamock_broker:stop(Broker),
    telemetry:detach(TelemetryRef),
    ok.

attach_telemetry() ->
    telemetry_test:attach_event_handlers(self(), [
        [kafcod, record_batch, compress_records]
    ]).

receive_telemetry(TelemetryRef) ->
    receive
        {EventName, TelemetryRef, Measurements, Metadata} ->
            {EventName, Measurements, Metadata}
    end.
