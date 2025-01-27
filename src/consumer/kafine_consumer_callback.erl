-module(kafine_consumer_callback).

% kafine_consumer_callback defines the behaviour for the callback module that you provide to kafine_consumer.

% init/3 is called to initialise your callback. It's called for each (topic, partition).
-callback init(Topic :: kafine:topic(), Partition :: kafine:partition(), Args :: term()) ->
    {ok, State :: term()} | {pause, State2 :: term()}.

% TODO: There's nothing in the callback definition that tells us that state is per (topic, partition) -- can we come up
% with a better shape for that?
-callback begin_record_batch(
    Topic :: kafine:topic(),
    Partition :: kafine:partition(),
    CurrentOffset :: kafine:offset(),
    Info :: log_info(),
    State :: term()
) -> {ok, State2 :: term()}.

% handle_record/4 is called for each message in the fetch response.
-callback handle_record(
    Topic :: kafine:topic(),
    Partition :: kafine:partition(),
    Message :: message(),
    State :: term()
) -> {ok, State2 :: term()} | {pause, State2 :: term()}.

-callback end_record_batch(
    Topic :: kafine:topic(),
    Partition :: kafine:partition(),
    NextOffset :: kafine:offset(),
    Info :: log_info(),
    State :: term()
) -> {ok, State2 :: term()} | {pause, State2 :: term()}.

-type log_info() :: #{
    % If old log segments are deleted, the log won't start at zero.
    log_start_offset := non_neg_integer(),
    % The last stable offset marks the end of committed transactions.
    last_stable_offset := non_neg_integer(),
    % The high watermark is the next offset to be written or read.
    high_watermark => non_neg_integer()
}.

-export_type([message/0]).

-type message() :: #{
    offset := kafine:offset(),
    timestamp := kafine:timestamp(),
    key := binary(),
    value := binary(),
    headers := headers()
}.

% Per KIP-82, "duplicate headers with the same key must be supported.", so it's a list of KV.
-type headers() :: [{binary(), binary() | null}].

% A note on message batching.
%
% Kafka is returning us messages in batches, wouldn't it be more performant to invoke the callback with the entire
% batch?
%
% Not particularly, no. We would still need to walk over the messages contained within a batch to apply the correct
% timestamp and offset to each message, so rather than try to cater for both the call-per-batch and call-per-message
% models, kafine (as with much else) punts that to the caller.
%
% If you want a batch because (for example) you want to produce that batch to another topic, or you want to do a single
% POST with multiple records, you can collect the messages yourself.
%
% Ultimately, this gives you more flexibility: One message at a time? You've got that. All the messages together? You've
% can do that. You want to collect the messages 10 at a time? You can do that too.
