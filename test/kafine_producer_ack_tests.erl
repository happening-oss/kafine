-module(kafine_producer_ack_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/error_code.hrl").
-include_lib("kafcod/include/ack.hrl").

-define(CLUSTER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(PRODUCER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(CONNECTION_OPTIONS, #{}).
-define(PRODUCER_OPTIONS, #{}).
-define(PARTITION_1, 1).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun ack_none/0,
        fun ack_leader/0,
        fun ack_full_isr/0
    ]}.

setup() ->
    kafine_producer_sup_sup:start_link(),
    meck:new(kamock_produce, [passthrough]),
    ok.

cleanup(_) ->
    kafine_producer_sup_sup:stop(),
    meck:unload(),
    ok.

ack_none() ->
    ack(none).

ack_leader() ->
    ack(leader).

ack_full_isr() ->
    ack(all).

ack(Acks) ->
    TelemetryRef = attach_telemetry(),

    {ok, Cluster, [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF),

    Ref = {?PRODUCER_REF, Acks},
    ProducerOptions = maps:put(acks, Acks, ?PRODUCER_OPTIONS),
    {ok, _} = kafine:start_producer(Ref, Bootstrap, ?CONNECTION_OPTIONS, ProducerOptions),

    ok = kafine_producer:produce_sync(Ref, ?TOPIC_NAME, ?PARTITION_1, #{
        key => <<"key">>,
        value => <<"value">>,
        headers => []
    }),

    % Assert that the setting made it to the mock broker.
    [{_, {_, _, [ProduceRequest, _Env]}, _}] = meck:history(kamock_produce),
    #{acks := AcksRequested} = ProduceRequest,
    ?assertEqual(Acks, convert_acks(AcksRequested)),

    kafine:stop_producer(Ref),
    kamock_cluster:stop(Cluster),
    telemetry:detach(TelemetryRef),
    ok.

attach_telemetry() ->
    telemetry_test:attach_event_handlers(self(), [
        [kafcod, record_batch, compress_records]
    ]).

convert_acks(?ACK_NONE) -> none;
convert_acks(?ACK_LEADER) -> leader;
convert_acks(?ACK_FULL_ISR) -> all.
