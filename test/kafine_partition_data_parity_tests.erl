-module(kafine_partition_data_parity_tests).
-include_lib("eunit/include/eunit.hrl").

all_test_() ->
    [
        fun single_batch_at_parity/0,
        fun two_batches_at_parity/0
    ].

single_batch_at_parity() ->
    FetchResponse = kafine_fetch_response_partition_data_tests:canned_fetch_response_single_batch(),
    {_Topic, PartitionData} = kafine_fetch_response_partition_data_tests:split_fetch_response(
        FetchResponse
    ),

    % The single batch canned data is not at parity.
    ?assertNot(kafine_partition_data:at_parity({PartitionData, 0})),
    ok.

two_batches_at_parity() ->
    FetchResponse = kafine_fetch_response_partition_data_tests:canned_fetch_response_two_batches(),
    {_Topic, PartitionData} = kafine_fetch_response_partition_data_tests:split_fetch_response(
        FetchResponse
    ),

    ?assert(kafine_partition_data:at_parity({PartitionData, 0})),
    ok.
