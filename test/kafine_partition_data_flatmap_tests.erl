-module(kafine_partition_data_flatmap_tests).
-include_lib("eunit/include/eunit.hrl").

all_test_() ->
    [
        fun single_batch_flatmap/0,
        fun two_batches_flatmap/0,
        fun empty_batch_flatmap/0
    ].

single_batch_flatmap() ->
    FetchResponse = kafine_fetch_response_partition_data_tests:canned_fetch_response_single_batch(),
    {_Topic, PartitionData} = kafine_fetch_response_partition_data_tests:split_fetch_response(
        FetchResponse
    ),

    ?assertEqual(
        {[<<"key3">>, <<"key4">>, <<"key5">>], 6},
        kafine_partition_data:flatmap(
            fun(_Record = #{key := Key}) ->
                Key
            end,
            {PartitionData, 0}
        )
    ),
    ok.

two_batches_flatmap() ->
    FetchResponse = kafine_fetch_response_partition_data_tests:canned_fetch_response_two_batches(),
    {_Topic, PartitionData} = kafine_fetch_response_partition_data_tests:split_fetch_response(
        FetchResponse
    ),

    ?assertEqual(
        {
            [
                1725550237492,
                1725550237493,
                1725550237494,
                1725550238414,
                1725550238415,
                1725550238416
            ],
            9
        },
        kafine_partition_data:flatmap(
            fun(#{timestamp := Timestamp}) -> Timestamp end,
            {PartitionData, 3}
        )
    ),
    ok.

empty_batch_flatmap() ->
    PartitionData = kafine_partition_data:new(kamock_partition_data_builder:make_empty(0), 0),
    ?assertMatch(
        {[], 0},
        kafine_partition_data:flatmap(
            fun(_) -> error(unexpected) end,
            PartitionData
        )
    ).
