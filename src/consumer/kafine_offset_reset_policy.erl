-module(kafine_offset_reset_policy).
-include_lib("kafcod/include/timestamp.hrl").

% Returns the value to be passed to `ListOffsets`.
-callback timestamp() -> Timestamp :: ?LATEST_TIMESTAMP | ?EARLIEST_TIMESTAMP.

% Once the offset is returned from `ListOffsets`, this can adjust the result.
%
% - `LastOffsetFetched` is the offset we used in the failing Fetch.
%    - For the first fetch in a simple topic consumer, this will be -1.
%    - For the first fetch in a group consumer, it will be the value returned from `OffsetFetch`.
%    - For subsequent fetches, it's possible that Kafka will delete the segment; it will be an offset that no longer
%      exists.
%
% - `NextOffsetToFetch` is the offset returned from `ListOffsets`. The callback can adjust it. But note that the
%   adjusted offset might not exist either.
-callback adjust_offset(
    LastOffsetFetched :: integer(),
    NextOffsetToFetch :: integer()
) -> AdjustedOffset :: non_neg_integer().
