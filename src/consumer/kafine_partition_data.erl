-module(kafine_partition_data).

% Public API over kafine_fetch_response_partition_data.

-export([
    info/1,
    at_parity/1,
    is_empty/1,

    % Iterators
    iterator/1,
    next/1,
    next_offset/1,

    % Flatten, etc.
    flatten/1,
    flatmap/2,
    fold/3,
    reduce_while/3,

    % Useful in tests; avoid in real code.
    message_count/1
]).

% Internal
-export([
    new/2
]).

-export_type([
    iterator/0,
    partition_data/0,
    fetch_offset/0,
    partition_data_info/0,
    record/0
]).

-type record() :: #{
    offset := kafine:offset(),
    timestamp := kafine:timestamp(),
    key := binary(),
    value := binary(),
    headers := headers()
}.

% Per KIP-82, "duplicate headers with the same key must be supported.", so it's a list of KV.
-type headers() :: [{header_key(), header_value()}].
-type header_key() :: binary().
-type header_value() :: binary() | null.

-type fetch_offset() :: non_neg_integer().

-record(iter, {
    records :: [kafcod_record:record()],
    record_batch :: kafcod_record_batch:record_batch(),
    record_batches :: [kafcod_record_batch:record_batch()],
    % We need the fetch offset so that if it's in the middle of a batch, we can skip past the unwanted records.
    fetch_offset :: fetch_offset()
}).

-record(iter_empty, {
    fetch_offset :: fetch_offset()
}).

% iterator() and next() can be used to traverse the PartitionData object in a linear manner, rather than using a
% recursive fold.
%
% They're implemented such that the first record batch is popped from the partition data, and the first record is popped
% from that record batch. If we reach the end of the records, we pop another record batch. If we reach the end of the
% record batches, we're done.

-opaque iterator() :: #iter{} | #iter_empty{}.
-opaque partition_data() :: {fetch_response:partition_data_11(), fetch_offset()}.

-type partition_data_info() :: #{
    log_start_offset := integer(),
    last_stable_offset := integer(),
    high_watermark := integer(),
    fetch_offset := integer()
}.

% Because partition_data() is opaque, dialyzer wants us to provide a constructor.
-spec new(fetch_response:partition_data_11(), fetch_offset()) -> partition_data().

new(PartitionData, FetchOffset) ->
    {PartitionData, FetchOffset}.

-spec info(PartitionData :: partition_data()) -> partition_data_info().

info({
    _PartitionData = #{
        log_start_offset := LogStartOffset,
        last_stable_offset := LastStableOffset,
        high_watermark := HighWatermark
    },
    FetchOffset
}) ->
    #{
        log_start_offset => LogStartOffset,
        last_stable_offset => LastStableOffset,
        high_watermark => HighWatermark,
        fetch_offset => FetchOffset
    }.

-spec at_parity(PartitionData :: partition_data()) -> boolean().

at_parity(PartitionData) ->
    #{high_watermark := HighWatermark} = info(PartitionData),
    NextOffset = next_offset(PartitionData),
    NextOffset =:= HighWatermark.

-spec iterator(
    PartitionData :: partition_data()
) -> iterator().

iterator(_PartitionData = {#{records := [RecordBatch | RecordBatches]}, FetchOffset}) ->
    #{records := Records} = RecordBatch,
    #iter{
        records = Records,
        record_batch = RecordBatch,
        record_batches = RecordBatches,
        fetch_offset = FetchOffset
    };
iterator(_PartitionData = {#{records := []}, FetchOffset}) ->
    % If there are no records, return a special iterator that stores only the original fetch offset.
    #iter_empty{
        fetch_offset = FetchOffset
    }.

-spec next(Iterator) -> {Record, NextIterator} | {none, FinalIterator} when
    Iterator :: iterator(),
    Record :: record(),
    NextIterator :: iterator(),
    FinalIterator :: iterator().

next(
    Iterator = #iter{
        records = [Record0 = #{offset_delta := OffsetDelta} | Records],
        record_batch = #{base_offset := BaseOffset, base_timestamp := BaseTimestamp},
        fetch_offset = FetchOffset
    }
) when BaseOffset + OffsetDelta >= FetchOffset ->
    Record = transform_record(Record0, BaseOffset, BaseTimestamp),
    {Record, Iterator#iter{records = Records}};
next(
    Iterator = #iter{
        records = [#{offset_delta := OffsetDelta} | Records],
        record_batch = #{base_offset := BaseOffset},
        fetch_offset = FetchOffset
    }
) when BaseOffset + OffsetDelta < FetchOffset ->
    next(Iterator#iter{records = Records});
next(Iterator = #iter{records = [], record_batches = [RecordBatch | RecordBatches]}) ->
    #{records := Records} = RecordBatch,
    next(Iterator#iter{
        records = Records, record_batch = RecordBatch, record_batches = RecordBatches
    });
next(Iterator = #iter{records = [], record_batches = []}) ->
    {none, Iterator};
next(Iterator = #iter_empty{}) ->
    {none, Iterator}.

% Replace offset_delta with offset and timestamp_delta with timestamp.
transform_record(
    _Record = #{
        key := Key,
        value := Value,
        headers := Headers,
        offset_delta := OffsetDelta,
        timestamp_delta := TimestampDelta
    },
    BaseOffset,
    BaseTimestamp
) ->
    Offset = BaseOffset + OffsetDelta,
    Timestamp = BaseTimestamp + TimestampDelta,
    #{key => Key, value => Value, headers => Headers, offset => Offset, timestamp => Timestamp}.

-spec flatten(PartitionData) -> {[record()], NextOffset} when
    PartitionData :: partition_data(), NextOffset :: non_neg_integer().

flatten(PartitionData) ->
    {Acc, NextOffset} = fold(fun(Record, Acc) -> [Record | Acc] end, [], PartitionData),
    {lists:reverse(Acc), NextOffset}.

-spec flatmap(Fun, PartitionData) -> {[Dest], NextOffset} when
    Fun :: fun((record()) -> Dest),
    PartitionData :: partition_data(),
    NextOffset :: non_neg_integer().

flatmap(Fun, PartitionData) when is_function(Fun, 1) ->
    {Acc, NextOffset} = fold(fun(Record, Acc) -> [Fun(Record) | Acc] end, [], PartitionData),
    {lists:reverse(Acc), NextOffset}.

-spec fold(Fun, Acc0, PartitionData) -> {Acc1, NextOffset} when
    Fun :: fun((Record, AccIn) -> AccOut),
    Acc0 :: Acc,
    PartitionData :: partition_data(),
    Record :: record(),
    AccIn :: Acc,
    AccOut :: Acc,
    Acc1 :: Acc,
    NextOffset :: non_neg_integer().

fold(Fun, Acc0, PartitionData) when
    is_function(Fun, 2)
->
    It = iterator(PartitionData),
    fold_it(Fun, Acc0, It).

fold_it(Fun, Acc, It) ->
    case next(It) of
        {none, Final} ->
            {Acc, next_offset(Final)};
        {Record, Next} ->
            fold_it(Fun, Fun(Record, Acc), Next)
    end.

-spec reduce_while(Fun, Acc0, PartitionData) -> {Acc1, NextOffset} when
    Fun :: fun((Record, AccIn) -> {cont | halt, AccOut}),
    Acc0 :: Acc,
    PartitionData :: partition_data(),
    Record :: record(),
    AccIn :: Acc,
    AccOut :: Acc,
    Acc1 :: Acc,
    NextOffset :: non_neg_integer().

reduce_while(Fun, Acc0, PartitionData) when
    is_function(Fun, 2)
->
    It = iterator(PartitionData),
    reduce_it_while(Fun, Acc0, next(It)).

reduce_it_while(_Fun, Acc0, {none, Final}) ->
    {Acc0, next_offset(Final)};
reduce_it_while(Fun, Acc0, {Record, It}) ->
    case Fun(Record, Acc0) of
        {cont, Acc1} ->
            reduce_it_while(Fun, Acc1, next(It));
        {halt, Acc1} ->
            {Acc1, next_offset(It)}
    end.

-spec next_offset(IteratorOrPartitionData :: iterator() | partition_data()) -> integer().

next_offset(
    _Iter = #iter{
        records = [#{offset_delta := OffsetDelta} | _], record_batch = #{base_offset := BaseOffset}
    }
) ->
    BaseOffset + OffsetDelta;
next_offset(
    _Iter = #iter{
        records = [],
        record_batch = #{base_offset := BaseOffset, last_offset_delta := LastOffsetDelta}
    }
) ->
    BaseOffset + LastOffsetDelta + 1;
next_offset(_Iter = #iter_empty{fetch_offset = FetchOffset}) ->
    FetchOffset;
next_offset(_PartitionData = {#{records := []}, FetchOffset}) ->
    % When the partition data is empty, the next offset is the fetch offset.
    FetchOffset;
next_offset(_PartitionData = {#{records := RecordBatches}, _}) ->
    % Otherwise, it's the next offset of the last record batch.
    RecordBatch = lists:last(RecordBatches),
    #{base_offset := BaseOffset, last_offset_delta := LastOffsetDelta} = RecordBatch,
    BaseOffset + LastOffsetDelta + 1.

is_empty(_PartitionData = {#{records := []}, _}) ->
    % Most fetches return nothing, so this is, by far, the most common case; match it explicitly.
    %
    % Note: kafine_parallel_handler has already optimised empty fetches away (apart from the first) before we see the
    % partition data, but the optimisation is probably still worth keeping, in case that gets changed later.
    true;
is_empty(PartitionData) ->
    message_count(PartitionData) == 0.

% This is provided to avoid duplication in test code, but you should probably avoid using it otherwise.
message_count(PartitionData) ->
    {Records, _} = flatten(PartitionData),
    length(Records).
