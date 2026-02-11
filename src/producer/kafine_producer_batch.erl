-module(kafine_producer_batch).
-moduledoc false.
-export([
    produce_batch/4
]).

produce_batch(RefOrPid, ProduceOptions, Batch, BatchAttributes) ->
    ReqIds = kafine_producer:reqids_new(),
    Fun = send_topic_requests(RefOrPid, ProduceOptions, BatchAttributes),
    ReqIds1 = maps:fold(Fun, ReqIds, Batch),
    merge_responses(ReqIds1).

send_topic_requests(RefOrPid, ProduceOptions, BatchAttributes) ->
    fun(Topic, PartitionMsgs, ReqIdCollection) ->
        Fun = fun(PartitionIndex, Messages, ReqIdCollection1) ->
            kafine_producer:produce_async(
                RefOrPid,
                Topic,
                PartitionIndex,
                ProduceOptions,
                BatchAttributes,
                Messages,
                {Topic, PartitionIndex},
                ReqIdCollection1
            )
        end,
        maps:fold(Fun, ReqIdCollection, PartitionMsgs)
    end.

merge_responses(ReqIdCollection) ->
    merge_responses(gen_statem:wait_response(ReqIdCollection, infinity, true), #{}).

merge_responses({{reply, {ok, Response}}, {Topic, PartitionIndex}, ReqIdCollection}, Acc) ->
    Acc1 = kafine_maps:put([Topic, PartitionIndex], Response, Acc),
    merge_responses(gen_statem:wait_response(ReqIdCollection, infinity, true), Acc1);
merge_responses(no_request, Acc) ->
    {ok, Acc}.
