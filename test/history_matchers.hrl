% macros for assertMatch with meck:history.
-define(init_callback(Topic, Partition, Args),
    {_,
        {_, init, [
            Topic,
            Partition,
            Args
        ]},
        _}
).

-define(begin_record_batch(
    Topic, Partition, CurrentOffset, LogStartOffset, LastStableOffset, HighWatermark
),
    {_,
        {_, begin_record_batch, [
            Topic,
            Partition,
            CurrentOffset,
            #{
                high_watermark := HighWatermark,
                last_stable_offset := LastStableOffset,
                log_start_offset := LogStartOffset
            },
            _
        ]},
        _}
).

-define(handle_record(Topic, Partition, Key, Value),
    {_,
        {_, handle_record, [
            Topic, Partition, #{key := Key, value := Value}, _
        ]},
        _}
).

-define(handle_record(Topic, Partition, Offset, Key, Value),
    {_,
        {_, handle_record, [
            Topic, Partition, #{offset := Offset, key := Key, value := Value}, _
        ]},
        _}
).

-define(end_record_batch(
    Topic, Partition, NextOffset, LogStartOffset, LastStableOffset, HighWatermark
),
    {_,
        {_, end_record_batch, [
            Topic,
            Partition,
            NextOffset,
            #{
                high_watermark := HighWatermark,
                last_stable_offset := LastStableOffset,
                log_start_offset := LogStartOffset
            },
            _
        ]},
        _}
).
