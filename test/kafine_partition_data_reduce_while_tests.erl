-module(kafine_partition_data_reduce_while_tests).
-include_lib("eunit/include/eunit.hrl").

all_test_() ->
    [
        fun single_batch_reduce_while_cont/0,
        fun single_batch_reduce_while_halt/0,
        fun two_batches_reduce_while_halt/0,
        fun empty_batch_reduce_while_cont/0
    ].

single_batch_reduce_while_cont() ->
    FetchResponse = kafine_fetch_response_partition_data_tests:canned_fetch_response_single_batch(),
    {_Topic, PartitionData} = kafine_fetch_response_partition_data_tests:split_fetch_response(
        FetchResponse
    ),

    ?assertEqual(
        {[<<"key5">>, <<"key4">>, <<"key3">>], 6},
        kafine_partition_data:reduce_while(
            fun(_Record = #{key := Key}, Acc) ->
                {cont, [Key | Acc]}
            end,
            [],
            {PartitionData, 0}
        )
    ),
    ok.

single_batch_reduce_while_halt() ->
    FetchResponse = kafine_fetch_response_partition_data_tests:canned_fetch_response_single_batch(),
    {_Topic, PartitionData} = kafine_fetch_response_partition_data_tests:split_fetch_response(
        FetchResponse
    ),

    ?assertEqual(
        {undefined, 5},
        kafine_partition_data:reduce_while(
            fun
                (#{offset := 4}, Acc) -> {halt, Acc};
                (_, Acc) -> {cont, Acc}
            end,
            undefined,
            {PartitionData, 0}
        )
    ),
    ok.

two_batches_reduce_while_halt() ->
    FetchResponse = kafine_fetch_response_partition_data_tests:canned_fetch_response_two_batches(),
    {_Topic, PartitionData} = kafine_fetch_response_partition_data_tests:split_fetch_response(
        FetchResponse
    ),

    ?assertEqual(
        {1725550238415, 8},
        kafine_partition_data:reduce_while(
            fun
                (#{offset := Offset, timestamp := Timestamp}, _) when Offset >= 7 ->
                    {halt, Timestamp};
                (_, _) ->
                    {cont, undefined}
            end,
            undefined,
            {PartitionData, 3}
        )
    ),
    ok.

empty_batch_reduce_while_cont() ->
    PartitionData = kafine_partition_data:new(kamock_partition_data_builder:make_empty(0), 0),
    ?assertMatch(
        {_, 0},
        kafine_partition_data:reduce_while(fun(_, Acc) -> {cont, Acc} end, undefined, PartitionData)
    ).
