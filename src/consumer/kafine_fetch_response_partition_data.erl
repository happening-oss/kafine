-module(kafine_fetch_response_partition_data).
-moduledoc false.
-export([fold/5]).

fold(
    Topic,
    PartitionData0 = #{
        partition_index := PartitionIndex,
        log_start_offset := LogStartOffset,
        last_stable_offset := LastStableOffset,
        high_watermark := HighWatermark
    },
    FetchOffset,
    Callback,
    StateData1
) when
    is_binary(Topic),
    is_integer(PartitionIndex),
    is_integer(LogStartOffset),
    is_integer(LastStableOffset),
    is_integer(HighWatermark),
    is_integer(FetchOffset)
->
    PartitionData = kafine_partition_data:new(PartitionData0, FetchOffset),
    case
        Callback:handle_partition_data(
            Topic, PartitionIndex, PartitionData, StateData1
        )
    of
        {ok, StateData2} ->
            % Accepting `{ok, NextOffset, StateData2}` would allow rewinding (among other things), which would be weird,
            % so we require `{ok, StateData2}`.
            NextOffset = kafine_partition_data:next_offset(PartitionData),
            {NextOffset, active, StateData2};
        {pause, StateData2} ->
            NextOffset = kafine_partition_data:next_offset(PartitionData),
            {NextOffset, paused, StateData2};
        {pause, NextOffset, StateData2} ->
            % Pausing, on the other hand, has to allow arbitrarily pausing within the partition data, so we allow it.
            {NextOffset, paused, StateData2}
    end.
