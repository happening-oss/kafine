-module(kafine_fetch_request).
-moduledoc false.
-export([build_fetch_request/2]).

-include_lib("kafcod/include/isolation_level.hrl").
-include("kafine_topic_partition_state.hrl").

-spec build_fetch_request(
    TopicPartitionStates :: kafine_consumer:topic_partition_states(),
    Options :: kafine:consumer_options()
) ->
    fetch_request:fetch_request_11().

build_fetch_request(
    TopicPartitionStates,
    Options = #{
        max_wait_ms := MaxWaitMs,
        min_bytes := MinBytes,
        max_bytes := MaxBytes,
        isolation_level := IsolationLevel0
    }
) ->
    IsolationLevel = isolation_level(IsolationLevel0),

    #{
        % We're a client, not a broker.
        replica_id => -1,

        max_wait_ms => MaxWaitMs,
        min_bytes => MinBytes,
        max_bytes => MaxBytes,

        isolation_level => IsolationLevel,

        % Sessions and forgotten topics are for inter-broker replication.
        session_id => 0,
        session_epoch => -1,
        forgotten_topics_data => [],

        topics => build_fetch_topics(TopicPartitionStates, Options),

        % TODO: preferred read replica.
        rack_id => <<>>
    }.

isolation_level(read_committed) ->
    ?READ_COMMITTED;
isolation_level(read_uncommitted) ->
    ?READ_UNCOMMITTED.

-spec build_fetch_topics(
    TopicPartitionStates :: kafine_consumer:topic_partition_states(),
    Options :: kafine:consumer_options()
) -> [fetch_request:fetch_topic_11()].

build_fetch_topics(TopicPartitionStates, Options) ->
    maps:fold(
        fun(Topic, PartitionStates, Acc) ->
            [build_fetch_topic(Topic, PartitionStates, Options) | Acc]
        end,
        [],
        TopicPartitionStates
    ).

build_fetch_topic(Topic, PartitionStates, Options) when
    is_binary(Topic)
->
    #{
        topic => Topic,
        partitions => build_fetch_partitions(PartitionStates, Options)
    }.

build_fetch_partitions(PartitionStates, Options) ->
    maps:fold(
        fun
            (PartitionIndex, #topic_partition_state{state = active, offset = Offset}, Acc) ->
                [build_fetch_partition(PartitionIndex, Offset, Options) | Acc];
            (_PartitionIndex, #topic_partition_state{state = paused}, Acc) ->
                Acc
        end,
        [],
        PartitionStates
    ).

-spec build_fetch_partition(
    PartitionIndex :: kafine:partition(), Offset :: integer(), Options :: kafine:consumer_options()
) -> fetch_request:fetch_partition_11().

build_fetch_partition(
    PartitionIndex, Offset, _Options = #{partition_max_bytes := PartitionMaxBytes}
) when
    is_integer(PartitionIndex),
    is_integer(Offset),
    Offset >= 0,
    is_integer(PartitionMaxBytes)
->
    #{
        partition => PartitionIndex,

        fetch_offset => Offset,

        partition_max_bytes => PartitionMaxBytes,

        % Only used by brokers following the leader; not us.
        log_start_offset => -1,

        % TODO: We don't care about the leader epoch. At some point we might.
        current_leader_epoch => -1
    }.
