-module(kafine_last_offset_reset_policy_tests).

-include_lib("eunit/include/eunit.hrl").

initial_fetch_with_messages_test() ->
    % It's an initial fetch, so we don't know the offset; use -1.
    LastFetchedOffset = -1,
    % ListOffsets tells us that the next offset is 2.
    NextOffset = 2,
    % So we want to fetch the message with offset 1.
    ?assertEqual(1, kafine_last_offset_reset_policy:adjust_offset(LastFetchedOffset, NextOffset)).

initial_fetch_with_new_topic_test() ->
    % It's an initial fetch, so we don't know the offset; use -1.
    LastFetchedOffset = -1,
    % It's a new topic; ListOffsets tells us that the next offset is zero.
    NextOffset = 0,
    % Because there are no messages (i.e. there is no "last" message), we just fetch from offset zero.
    ?assertEqual(0, kafine_last_offset_reset_policy:adjust_offset(LastFetchedOffset, NextOffset)).

single_message_deleted_test() ->
    % We've attempted to fetch from the partition at offset 2, but the message doesn't exist.
    LastFetchedOffset = 2,
    % The segment was deleted (for example); ListOffsets tells us that the next offset is 3.
    NextOffset = 3,
    % Since there are no messages, we should start from the end, i.e. offset 3.
    ?assertEqual(3, kafine_last_offset_reset_policy:adjust_offset(LastFetchedOffset, NextOffset)).

multiple_messages_deleted_test() ->
    % We've attempted to fetch from the partition at offset 12, but the message doesn't exist.
    LastFetchedOffset = 12,
    % The segment was deleted (for example); ListOffsets tells us that the next offset is 30.
    NextOffset = 30,
    % Since there may be messages, we should start from the one before the end, i.e. offset 29.
    ?assertEqual(29, kafine_last_offset_reset_policy:adjust_offset(LastFetchedOffset, NextOffset)).

all_messages_deleted_test() ->
    % Following from the test above, all of the messages were deleted, so that wasn't a valid offset, either.
    % We've attempted to fetch from the partition at offset 29, but the message doesn't exist.
    LastFetchedOffset = 29,
    % The segment was deleted (for example); ListOffsets tells us that the next offset is 30.
    NextOffset = 30,
    % There are no messages, we should start from the end, offset 30.
    ?assertEqual(30, kafine_last_offset_reset_policy:adjust_offset(LastFetchedOffset, NextOffset)).

topic_recreated_no_messages_test() ->
    % IMPORTANT: recreating topics breaks _everything_, so it might not be worth testing this.
    % We've attempted to fetch from the partition at offset 83, but the topic was recreated.
    LastFetchedOffset = 83,
    % The topic was recreated, and is empty.
    NextOffset = 0,
    % There are no messages, we should start from the end, offset 0.
    ?assertEqual(0, kafine_last_offset_reset_policy:adjust_offset(LastFetchedOffset, NextOffset)).

topic_recreated_has_messages_test() ->
    % IMPORTANT: recreating topics breaks _everything_, so it might not be worth testing this.
    % We've attempted to fetch from the partition at offset 37, but the topic was recreated.
    LastFetchedOffset = 37,
    % The topic was recreated, but has messages. We should (?) resume from the last message.
    NextOffset = 10,
    ?assertEqual(9, kafine_last_offset_reset_policy:adjust_offset(LastFetchedOffset, NextOffset)).

topic_recreated_has_more_messages_test() ->
    % IMPORTANT: recreating topics breaks _everything_, so it might not be worth testing this.
    % We've attempted to fetch from the partition at offset 37, but the topic was recreated, and has _more_ messages.
    LastFetchedOffset = 37,
    % The topic was recreated, but has more messages. We should (?) resume from the last message.
    NextOffset = 42,
    ?assertEqual(41, kafine_last_offset_reset_policy:adjust_offset(LastFetchedOffset, NextOffset)).
