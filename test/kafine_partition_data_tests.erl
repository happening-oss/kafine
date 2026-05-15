-module(kafine_partition_data_tests).
-include_lib("eunit/include/eunit.hrl").

-define(PARTITION, 2).
-define(OFFSET, 3563).

all_test_() ->
    [
        fun empty_next_offset/0,

        fun message_count_zero/0,
        fun message_count_one/0,
        fun message_count_many/0,
        fun message_count_many_skip/0
    ].

empty_next_offset() ->
    % should return FetchOffset
    PartitionData0 = kamock_partition_data_builder:make_empty(?PARTITION, ?OFFSET, ?OFFSET),
    PartitionData = kafine_partition_data:new(PartitionData0, ?OFFSET),
    ?assertEqual(?OFFSET, kafine_partition_data:next_offset(PartitionData)),
    ok.

message_count_zero() ->
    PartitionData = kafine_partition_data:new(kamock_partition_data:make_empty(?PARTITION), 0),
    ?assertEqual(0, kafine_partition_data:message_count(PartitionData)),
    ?assert(kafine_partition_data:is_empty(PartitionData)).

message_count_one() ->
    PartitionData = kafine_partition_data:new(
        kamock_partition_data:make_single_message(?PARTITION, 0, 0, 1, #{}), 0
    ),
    ?assertEqual(1, kafine_partition_data:message_count(PartitionData)),
    ?assertNot(kafine_partition_data:is_empty(PartitionData)).

message_count_many() ->
    RecordBatches = [
        make_record_batch(0, 3),
        make_record_batch(4, 3)
    ],
    PartitionData0 = kamock_partition_data_builder:make_partition_data(
        ?PARTITION, 0, 12, RecordBatches
    ),
    PartitionData = kafine_partition_data:new(PartitionData0, 0),
    ?assertEqual(8, kafine_partition_data:message_count(PartitionData)),
    ?assertNot(kafine_partition_data:is_empty(PartitionData)).

message_count_many_skip() ->
    RecordBatches = [
        make_record_batch(0, 3),
        make_record_batch(4, 3)
    ],
    PartitionData0 = kamock_partition_data_builder:make_partition_data(
        ?PARTITION, 0, 12, RecordBatches
    ),
    PartitionData = kafine_partition_data:new(PartitionData0, 2),
    ?assertEqual(6, kafine_partition_data:message_count(PartitionData)),
    ?assertNot(kafine_partition_data:is_empty(PartitionData)).

make_record_batch(BaseOffset, LastOffsetDelta) ->
    BaseTimestamp = erlang:system_time(millisecond),
    Records = [
        make_record(BaseOffset, OffsetDelta)
     || OffsetDelta <- lists:seq(0, LastOffsetDelta)
    ],
    kamock_partition_data_builder:make_record_batch(
        BaseOffset, LastOffsetDelta, BaseTimestamp, Records
    ).

make_record(BaseOffset, OffsetDelta) ->
    Offset = BaseOffset + OffsetDelta,
    Message = #{
        key => iolist_to_binary(io_lib:format("key~B", [Offset])),
        value => iolist_to_binary(io_lib:format("value~B", [Offset])),
        headers => []
    },
    kamock_partition_data_builder:make_record(OffsetDelta, Message).
