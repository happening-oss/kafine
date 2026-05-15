-module(kafine_partition_data_flatten_tests).
-include_lib("eunit/include/eunit.hrl").

all_test_() ->
    [
        fun single_batch_skip_flatten/0,
        fun two_batches_flatten/0,
        fun batch_with_some_records_missing_flatten/0
    ].

single_batch_skip_flatten() ->
    FetchResponse = kafine_fetch_response_partition_data_tests:canned_fetch_response_single_batch(),
    {_Topic, PartitionData} = kafine_fetch_response_partition_data_tests:split_fetch_response(
        FetchResponse
    ),

    ?assertMatch(
        {
            [
                #{value := <<"value4">>, key := <<"key4">>, headers := []},
                #{value := <<"value5">>, key := <<"key5">>, headers := []}
            ],
            6
        },
        kafine_partition_data:flatten({PartitionData, 4})
    ),
    ok.

two_batches_flatten() ->
    FetchResponse = kafine_fetch_response_partition_data_tests:canned_fetch_response_two_batches(),
    {_Topic, PartitionData} = kafine_fetch_response_partition_data_tests:split_fetch_response(
        FetchResponse
    ),

    ?assertMatch(
        {
            [
                #{value := <<"value3">>, key := <<"key3">>, headers := []},
                #{value := <<"value4">>, key := <<"key4">>, headers := []},
                #{value := <<"value5">>, key := <<"key5">>, headers := []},
                #{value := <<"value6">>, key := <<"key6">>, headers := []},
                #{value := <<"value7">>, key := <<"key7">>, headers := []},
                #{value := <<"value8">>, key := <<"key8">>, headers := []}
            ],
            9
        },
        kafine_partition_data:flatten({PartitionData, 0})
    ),
    ok.

batch_with_some_records_missing_flatten() ->
    FetchResponse = kafine_fetch_response_partition_data_tests:canned_fetch_response_batch_with_some_records_missing(),
    {_Topic, PartitionData} = kafine_fetch_response_partition_data_tests:split_fetch_response(
        FetchResponse
    ),

    ?assertMatch(
        {
            [
                #{value := <<"value4">>, key := <<"key4">>, headers := []},
                #{value := <<"value6">>, key := <<"key6">>, headers := []}
            ],
            9
        },
        kafine_partition_data:flatten({PartitionData, 4})
    ),
    ok.
