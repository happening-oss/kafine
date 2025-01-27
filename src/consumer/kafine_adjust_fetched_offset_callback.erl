-module(kafine_adjust_fetched_offset_callback).

% The primary purpose of this callback is to support a kashim use case
% where the offset must be incremented by one because kashim consumers
% commit last offset instead of latest.
-callback adjust_committed_offset(CommittedOffset :: kafine:offset() | -1) -> kafine:offset() | -1.
