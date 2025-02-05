-module(kafine_fetch_response_partition_data).
-moduledoc false.
-export([fold/5]).

%% See the module comments in kafine_fetch_response.erl

-include_lib("kernel/include/logger.hrl").

fold(
    Topic,
    _PartitionData = #{
        partition_index := PartitionIndex,
        log_start_offset := LogStartOffset,
        last_stable_offset := LastStableOffset,
        high_watermark := HighWatermark,
        records := RecordBatches
    },
    FetchOffset,
    Callback,
    StateData1
) ->
    Info = #{
        % If old log segments are deleted, the log won't start at zero.
        log_start_offset => LogStartOffset,
        % The last stable offset marks the end of committed transactions.
        last_stable_offset => LastStableOffset,
        % The high watermark is the next offset to be written or read.
        high_watermark => HighWatermark
    },

    % The partition data can contain multiple record batches. We flatten it into a single record batch here.
    {ok, StateData2} = Callback:begin_record_batch(
        Topic, PartitionIndex, FetchOffset, Info, StateData1
    ),
    {NextOffset, State3, StateData3} = fold_record_batches(
        Topic, PartitionIndex, RecordBatches, FetchOffset, Callback, StateData2
    ),
    case
        Callback:end_record_batch(
            Topic, PartitionIndex, NextOffset, Info, StateData3
        )
    of
        {ok, StateData4} -> {NextOffset, State3, StateData4};
        {pause, StateData4} -> {NextOffset, paused, StateData4}
    end.

fold_record_batches(
    Topic, PartitionIndex, RecordBatches, FetchOffset, Callback, StateData
) ->
    {_, {NextOffset, State2, StateData2}} = reduce_while(
        fun(RecordBatch, {_, _, StateData1}) ->
            fold_record_batch(
                Topic,
                PartitionIndex,
                RecordBatch,
                FetchOffset,
                Callback,
                StateData1
            )
        end,
        {FetchOffset, active, StateData},
        RecordBatches
    ),
    {NextOffset, State2, StateData2}.

-spec fold_record_batch(
    kafine:topic(),
    kafine:partition(),
    kafcod_record_batch:record_batch(),
    FetchOffset :: non_neg_integer(),
    Callback :: module(),
    StateData :: term()
) ->
    {cont | halt, {NextOffset :: non_neg_integer(), State :: active | paused, StateData2 :: term()}}.

fold_record_batch(
    Topic,
    PartitionIndex,
    RecordBatch = #{
        base_offset := BaseOffset,
        base_timestamp := _BaseTimestamp,
        records := Records,
        last_offset_delta := LastOffsetDelta,
        partition_leader_epoch := _,
        magic := _,
        crc := _,
        attributes := _,
        max_timestamp := _,
        producer_id := _,
        producer_epoch := _,
        base_sequence := _
    },
    FetchOffset,
    Callback,
    StateData
) ->
    ?LOG_DEBUG("current_offset = ~B, base_offset = ~B, last_offset_delta = ~B, last_offset = ~B", [
        FetchOffset, BaseOffset, LastOffsetDelta, BaseOffset + LastOffsetDelta
    ]),

    fold_records(
        Topic,
        PartitionIndex,
        RecordBatch,
        Records,
        FetchOffset,
        Callback,
        StateData
    ).

fold_records(
    Topic,
    PartitionIndex,
    RecordBatch = #{base_offset := BaseOffset},
    Records,
    FetchOffset,
    Callback,
    StateData
) ->
    % We assume that the callback is active, otherwise you wouldn't have received any records, right?
    % XXX: However, there _will_ be a bug introduced here if we ever allow pausing while a Fetch request is in flight.
    % TODO: ^^
    State = active,
    reduce_while(
        fun(Record = #{offset_delta := OffsetDelta}, {_, _, StateData1}) ->
            {Cont, {State2, StateData2}} = fold_record(
                Topic,
                PartitionIndex,
                RecordBatch,
                Record,
                FetchOffset,
                Callback,
                StateData1
            ),
            NextOffset = BaseOffset + OffsetDelta + 1,
            {Cont, {NextOffset, State2, StateData2}}
        end,
        {FetchOffset, State, StateData},
        Records
    ).

-spec fold_record(
    kafine:topic(),
    kafine:partition(),
    kafcod_record_batch:record_batch(),
    kafcod_record:record(),
    FetchOffset :: non_neg_integer(),
    Callback :: module(),
    StateData :: term()
) -> {cont | halt, {State :: active | paused, StateData2 :: term()}}.

fold_record(
    Topic,
    PartitionIndex,
    _RecordBatch = #{base_offset := BaseOffset, base_timestamp := BaseTimestamp},
    _Record = #{
        key := Key,
        value := Value,
        headers := Headers,
        offset_delta := OffsetDelta,
        timestamp_delta := TimestampDelta,
        attributes := _
    },
    FetchOffset,
    Callback,
    StateData
) ->
    % The offset and timestamp are stored relative to the batch; adjust them before invoking the callback.
    Offset = BaseOffset + OffsetDelta,
    Timestamp = BaseTimestamp + TimestampDelta,
    fold_record(
        Topic,
        PartitionIndex,
        Offset,
        Timestamp,
        Key,
        Value,
        Headers,
        FetchOffset,
        Callback,
        StateData
    ).

fold_record(
    Topic,
    PartitionIndex,
    Offset,
    Timestamp,
    Key,
    Value,
    Headers,
    FetchOffset,
    Callback,
    StateData
) when Offset >= FetchOffset ->
    ?LOG_DEBUG("Topic ~s, partition ~B, offset ~B: ~p = ~p", [
        Topic, PartitionIndex, Offset, Key, Value
    ]),
    Message = #{
        offset => Offset, timestamp => Timestamp, key => Key, value => Value, headers => Headers
    },
    case Callback:handle_record(Topic, PartitionIndex, Message, StateData) of
        {ok, StateData2} -> {cont, {active, StateData2}};
        {pause, StateData2} -> {halt, {paused, StateData2}}
    end;
fold_record(
    Topic,
    PartitionIndex,
    Offset,
    _Timestamp,
    Key,
    Value,
    _Headers,
    _FetchOffset,
    _Callback,
    StateData
) ->
    ?LOG_DEBUG("Topic ~s, partition ~B, offset ~B: ~p = ~p (skipped)", [
        Topic, PartitionIndex, Offset, Key, Value
    ]),
    {cont, {active, StateData}}.

% Similar to Elixir's Enum.reduce_while/3, except that we return the 'cont' | 'halt', so that our caller knows whether
% to continue or halt.
%
% It's *not* in a separate module (e.g. kafine_lists or kafine_enum) *because* of that slight difference from Elixir.

-spec reduce_while(Fun, Acc, [Elem]) -> Result when
    Fun :: fun((Elem, Acc) -> Result),
    Result :: {cont | halt, Acc}.

reduce_while(Fun, Acc, List) when is_function(Fun, 2), is_list(List) ->
    reduce_while_(Fun, {cont, Acc}, List).

reduce_while_(_Fun, Result, []) ->
    Result;
reduce_while_(Fun, {cont, Acc}, [Elem | Rest]) ->
    reduce_while_(Fun, Fun(Elem, Acc), Rest);
reduce_while_(_Fun, Result = {halt, _Acc}, _List) ->
    Result.
