-module(kafine_last_offset_reset_policy).
-behaviour(kafine_offset_reset_policy).

-export([
    adjust_offset/2,
    timestamp/0
]).
-include_lib("kafcod/include/timestamp.hrl").

% The `kafine_last_offset_reset_policy` module implements Kafire's `last` offset reset policy. That is: where `earliest`
% fetches from the start of the topic, and `latest` watches for _new_ messages on the topic, `last` fetches the
% most-recent _existing_ message.

% To do this, we (essentially) ask for the `latest` offset, and then subtract one.

timestamp() ->
    % We want the `latest` offset; we'll skip backwards from there.
    ?LATEST_TIMESTAMP.

adjust_offset(LastFetchedOffset, NextOffsetToFetch) when
    LastFetchedOffset == NextOffsetToFetch - 1
->
    % The last thing we attempted to fetch would have been the last message, but it's not there.
    % Resume from latest instead.
    NextOffsetToFetch;
adjust_offset(_LastFetchedOffset, NextOffsetToFetch) when NextOffsetToFetch > 0 ->
    NextOffsetToFetch - 1;
adjust_offset(LastFetchedOffset, NextOffsetToFetch) when LastFetchedOffset > NextOffsetToFetch ->
    % This is icky; it deals with the case where the topic has been recreated and the offset rewound.
    NextOffsetToFetch.
