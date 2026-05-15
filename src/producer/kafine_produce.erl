-module(kafine_produce).

-export([
    build_request/3,
    handle_response/1
]).

-include_lib("kafcod/include/error_code.hrl").

-spec build_request(
    Batch :: kafine_topic_partition_data:t([]),
    ConnectionOptions :: kafine:connection_options(),
    ProducerOptions :: kafine:producer_options()
) -> produce_request:produce_request_8().

build_request(
    Batch,
    _ConnectionOptions = #{request_timeout_ms := TimeoutMs},
    ProducerOptions = #{acks := Acks}
) ->
    #{
        transactional_id => null,
        acks => acks(Acks),
        timeout_ms => TimeoutMs,
        topic_data => topic_data(Batch, ProducerOptions)
    }.

topic_data(Batch, ProducerOptions) ->
    [
        #{
            name => Topic,
            partition_data => partition_data(PartitionData, ProducerOptions)
        }
     || Topic := PartitionData <- Batch
    ].

partition_data(PartitionData, ProducerOptions) ->
    [
        #{
            index => Partition,
            records => records(Records, ProducerOptions)
        }
     || Partition := Records <- PartitionData
    ].

records(Records, ProducerOptions) ->
    kafcod_message_set:prepare_message_set(batch_opts(ProducerOptions), Records).

batch_opts(ProducerOptions) ->
    maps:with([compression], ProducerOptions).

acks(none) -> 0;
acks(leader) -> 1;
acks(all) -> -1.

handle_response(#{responses := Responses}) ->
    #{
        Topic => partition_responses(PartitionResponses)
     || #{name := Topic, partition_responses := PartitionResponses} <- Responses
    }.

partition_responses(PartitionResponses) ->
    #{
        Partition => partition_response(PartitionResponse)
     || PartitionResponse = #{index := Partition} <- PartitionResponses
    }.

partition_response(#{error_code := ?NONE}) ->
    ok;
partition_response(#{error_code := ErrorCode}) ->
    {error, {kafka_error, ErrorCode}}.
