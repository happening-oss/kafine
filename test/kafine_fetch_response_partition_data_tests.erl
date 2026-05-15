-module(kafine_fetch_response_partition_data_tests).
-include_lib("eunit/include/eunit.hrl").

-include("history_matchers.hrl").

-export([
    canned_fetch_response_single_batch/0,
    canned_fetch_response_two_batches/0,
    canned_fetch_response_batch_with_some_records_missing/0,
    split_fetch_response/1
]).

-define(CALLBACK_STATE, {state, ?MODULE}).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun single_batch/0,
        fun single_batch_skip/0,
        fun two_batches/0,
        fun batch_with_some_records_missing/0
    ]}.

setup() ->
    meck:new(test_consumer_callback, [non_strict]),
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {ok, ?CALLBACK_STATE} end),
    meck:expect(test_consumer_callback, handle_partition_data, fun(_T, _P, _PD, St) ->
        {ok, St}
    end),
    ok.

cleanup(_) ->
    meck:unload().

% The fetch response contains a single batch.
single_batch() ->
    FetchOffset = 3,
    FetchResponse = canned_fetch_response_single_batch(),
    {Topic, PartitionData} = split_fetch_response(FetchResponse),
    FoldResult = kafine_fetch_response_partition_data:fold(
        Topic,
        PartitionData,
        FetchOffset,
        test_consumer_callback,
        ?CALLBACK_STATE
    ),
    ?assertMatch({6, active, ?CALLBACK_STATE}, FoldResult),
    ok.

single_batch_skip() ->
    FetchResponse = canned_fetch_response_single_batch(),
    {Topic, PartitionData} = split_fetch_response(FetchResponse),

    FetchOffset = 4,
    FoldResult = kafine_fetch_response_partition_data:fold(
        Topic,
        PartitionData,
        FetchOffset,
        test_consumer_callback,
        ?CALLBACK_STATE
    ),
    ?assertMatch({6, active, ?CALLBACK_STATE}, FoldResult),
    ok.

% The fetch response contains two batches.
two_batches() ->
    FetchOffset = 3,
    FetchResponse = canned_fetch_response_two_batches(),
    {Topic, PartitionData} = split_fetch_response(FetchResponse),
    FoldResult = kafine_fetch_response_partition_data:fold(
        Topic,
        PartitionData,
        FetchOffset,
        test_consumer_callback,
        ?CALLBACK_STATE
    ),
    ?assertMatch({9, active, ?CALLBACK_STATE}, FoldResult),
    ok.

% There's one batch in the fetch response, but the last record in that batch has a different delta
% to last_offset_delta. This happens due to compaction. We need the NextOffset to be based on the
% last_offset_delta, not the last record.
batch_with_some_records_missing() ->
    FetchOffset = 3,
    FetchResponse = canned_fetch_response_batch_with_some_records_missing(),
    {Topic, PartitionData} = split_fetch_response(FetchResponse),
    FoldResult = kafine_fetch_response_partition_data:fold(
        Topic,
        PartitionData,
        FetchOffset,
        test_consumer_callback,
        ?CALLBACK_STATE
    ),
    ?assertMatch({9, active, ?CALLBACK_STATE}, FoldResult),
    ok.

% Given a canned fetch response, break it into the two pieces required by kafine_fetch_response_partition_data:fold().
split_fetch_response(FetchResponse) ->
    #{responses := [FetchableTopicResponse]} = FetchResponse,
    #{topic := Topic} = FetchableTopicResponse,
    #{partitions := [PartitionData]} = FetchableTopicResponse,
    {Topic, PartitionData}.

canned_fetch_response_single_batch() ->
    % This is a fetch response containing [[3, 4, 5]].
    %
    % We created it by editing the captured two batch response.
    {ok, [Term]} = file:consult("test/data/fetch_response_single_batch.terms"),
    Term.

canned_fetch_response_two_batches() ->
    {ok, [Term]} = file:consult("test/data/fetch_response_two_batches.terms"),
    Term.

canned_fetch_response_batch_with_some_records_missing() ->
    {ok, [Term]} = file:consult("test/data/fetch_response_batch_with_some_records_missing.terms"),
    Term.
