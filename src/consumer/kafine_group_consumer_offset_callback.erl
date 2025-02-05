-module(kafine_group_consumer_offset_callback).
-behaviour(kafine_adjust_fetched_offset_callback).

-export([adjust_committed_offset/1]).

adjust_committed_offset(CommittedOffset) ->
    CommittedOffset.
