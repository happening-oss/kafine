-module(kafine_partition_data_fold_tests).
-include_lib("eunit/include/eunit.hrl").

all_test_() ->
    [
        fun single_batch_fold/0,
        fun two_batches_fold/0,
        fun empty_batch_fold/0
    ].

single_batch_fold() ->
    FetchResponse = kafine_fetch_response_partition_data_tests:canned_fetch_response_single_batch(),
    {_Topic, PartitionData} = kafine_fetch_response_partition_data_tests:split_fetch_response(
        FetchResponse
    ),

    ?assertEqual(
        {[<<"key5">>, <<"key4">>, <<"key3">>], 6},
        kafine_partition_data:fold(
            fun(_Record = #{key := Key}, Acc) ->
                [Key | Acc]
            end,
            [],
            {PartitionData, 0}
        )
    ),
    ok.

two_batches_fold() ->
    FetchResponse = kafine_fetch_response_partition_data_tests:canned_fetch_response_two_batches(),
    {_Topic, PartitionData} = kafine_fetch_response_partition_data_tests:split_fetch_response(
        FetchResponse
    ),

    ?assertEqual(
        {1725550238416, 9},
        kafine_partition_data:fold(
            fun(#{timestamp := Timestamp}, _) -> Timestamp end,
            undefined,
            {PartitionData, 3}
        )
    ),
    ok.

empty_batch_fold() ->
    PartitionData = kafine_partition_data:new(kamock_partition_data_builder:make_empty(0), 0),
    ?assertMatch(
        {_, 0},
        kafine_partition_data:fold(
            fun(_, Acc) -> Acc end,
            undefined,
            PartitionData
        )
    ).
