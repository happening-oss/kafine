-module(kafine_partition_data_iterator_tests).
-include_lib("eunit/include/eunit.hrl").

all_test_() ->
    [
        fun empty_response_iterator/0,
        fun single_batch_iterator/0,
        fun single_batch_skip_iterator/0,
        fun two_batches_iterator/0,
        fun batch_with_some_records_missing_iterator/0
    ].

empty_response_iterator() ->
    PartitionData =
        #{
            partition_index => 2,
            error_code => 0,
            % An empty fetch response returns empty 'records' here.
            % See 'fetch_response_tests:v11_empty_response_test/0' in kafcod.
            records => [],
            high_watermark => 0,
            last_stable_offset => 0,
            log_start_offset => 0,
            aborted_transactions => [],
            preferred_read_replica => -1
        },
    It = kafine_partition_data:iterator({PartitionData, 0}),
    {none, Final} = kafine_partition_data:next(It),
    ?assertEqual(0, kafine_partition_data:next_offset(Final)),
    ok.

single_batch_iterator() ->
    FetchResponse = kafine_fetch_response_partition_data_tests:canned_fetch_response_single_batch(),
    {Topic, PartitionData} = kafine_fetch_response_partition_data_tests:split_fetch_response(
        FetchResponse
    ),

    ?assertEqual(<<"cars">>, Topic),

    It = kafine_partition_data:iterator({PartitionData, 0}),

    {R1, It2} = kafine_partition_data:next(It),
    #{
        key := <<"key3">>,
        value := <<"value3">>,
        headers := [],
        offset := 3,
        timestamp := 1725550237492
    } = R1,

    ?assertEqual(4, kafine_partition_data:next_offset(It2)),

    {R2, It3} = kafine_partition_data:next(It2),
    #{
        key := <<"key4">>,
        value := <<"value4">>,
        headers := [],
        offset := 4,
        timestamp := 1725550237493
    } = R2,

    {R3, It4} = kafine_partition_data:next(It3),
    #{
        key := <<"key5">>,
        value := <<"value5">>,
        headers := [],
        offset := 5,
        timestamp := 1725550237494
    } = R3,

    {none, Final} = kafine_partition_data:next(It4),
    ?assertEqual(6, kafine_partition_data:next_offset(Final)),
    ok.

single_batch_skip_iterator() ->
    FetchResponse = kafine_fetch_response_partition_data_tests:canned_fetch_response_single_batch(),
    {Topic, PartitionData} = kafine_fetch_response_partition_data_tests:split_fetch_response(
        FetchResponse
    ),

    ?assertEqual(<<"cars">>, Topic),

    It = kafine_partition_data:iterator({PartitionData, 4}),

    {R1, It2} = kafine_partition_data:next(It),
    #{
        key := <<"key4">>,
        value := <<"value4">>,
        headers := [],
        offset := 4,
        timestamp := 1725550237493
    } = R1,

    ?assertEqual(5, kafine_partition_data:next_offset(It2)),

    {R2, It3} = kafine_partition_data:next(It2),
    #{
        key := <<"key5">>,
        value := <<"value5">>,
        headers := [],
        offset := 5,
        timestamp := 1725550237494
    } = R2,

    {none, Final} = kafine_partition_data:next(It3),
    ?assertEqual(6, kafine_partition_data:next_offset(Final)),
    ok.

two_batches_iterator() ->
    FetchResponse = kafine_fetch_response_partition_data_tests:canned_fetch_response_two_batches(),
    {_Topic, PartitionData} = kafine_fetch_response_partition_data_tests:split_fetch_response(
        FetchResponse
    ),

    It = kafine_partition_data:iterator({PartitionData, 3}),

    {R1, It2} = kafine_partition_data:next(It),
    #{
        key := <<"key3">>,
        value := <<"value3">>,
        headers := [],
        offset := 3,
        timestamp := 1725550237492
    } = R1,

    {R2, It3} = kafine_partition_data:next(It2),
    #{
        key := <<"key4">>,
        value := <<"value4">>,
        headers := [],
        offset := 4,
        timestamp := 1725550237493
    } = R2,

    {R3, It4} = kafine_partition_data:next(It3),
    #{
        key := <<"key5">>,
        value := <<"value5">>,
        headers := [],
        offset := 5,
        timestamp := 1725550237494
    } = R3,

    % Between batches
    ?assertEqual(6, kafine_partition_data:next_offset(It4)),

    {R4, It5} = kafine_partition_data:next(It4),
    #{
        key := <<"key6">>,
        value := <<"value6">>,
        headers := [],
        offset := 6,
        timestamp := 1725550238414
    } = R4,

    {R5, It6} = kafine_partition_data:next(It5),
    #{
        key := <<"key7">>,
        value := <<"value7">>,
        headers := [],
        offset := 7,
        timestamp := 1725550238415
    } = R5,

    {R6, It7} = kafine_partition_data:next(It6),
    #{
        key := <<"key8">>,
        value := <<"value8">>,
        headers := [],
        offset := 8,
        timestamp := 1725550238416
    } = R6,

    {none, Final} = kafine_partition_data:next(It7),
    ?assertEqual(9, kafine_partition_data:next_offset(Final)),
    ok.

batch_with_some_records_missing_iterator() ->
    FetchResponse = kafine_fetch_response_partition_data_tests:canned_fetch_response_batch_with_some_records_missing(),
    {Topic, PartitionData} = kafine_fetch_response_partition_data_tests:split_fetch_response(
        FetchResponse
    ),

    ?assertEqual(<<"cars">>, Topic),

    It = kafine_partition_data:iterator({PartitionData, 0}),

    {R1, It2} = kafine_partition_data:next(It),
    #{
        key := <<"key4">>,
        value := <<"value4">>,
        headers := [],
        offset := 4,
        timestamp := 1725550237493
    } = R1,

    ?assertEqual(6, kafine_partition_data:next_offset(It2)),

    {R2, It3} = kafine_partition_data:next(It2),
    #{
        key := <<"key6">>,
        value := <<"value6">>,
        headers := [],
        offset := 6,
        timestamp := 1725550237494
    } = R2,

    {none, Final} = kafine_partition_data:next(It3),
    ?assertEqual(9, kafine_partition_data:next_offset(Final)),
    ok.
