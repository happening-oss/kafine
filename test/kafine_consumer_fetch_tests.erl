-module(kafine_consumer_fetch_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/error_code.hrl").
-include_lib("kafcod/include/api_key.hrl").

-elvis([{elvis_style, dont_repeat_yourself, disable}]).
-elvis([{elvis_style, no_import, disable}]).
-import(hamcrest_matchers, [all_of/1]).

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(PARTITION, 1).
-define(CONNECTION_OPTIONS, kafine_connection_options:validate_options(#{})).
-define(CONSUMER_OPTIONS, kafine_consumer_options:validate_options(#{})).
-define(TOPIC_OPTIONS, kafine_topic_options:validate_options([?TOPIC_NAME], #{})).
-define(FETCHER_METADATA, #{}).
-define(CALLBACK_ARGS, undefined).
-define(WAIT_TIMEOUT_MS, 2_000).

setup() ->
    meck:new(test_consumer_callback, [non_strict]),
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {ok, dummy} end),
    meck:expect(test_consumer_callback, handle_partition_data, fun(_T, _P, _PD, St) ->
        {ok, St}
    end),

    ok.

cleanup(_) ->
    meck:unload().

% There's a fair bit of duplication in this test: we essentially explore a test matrix.
% I haven't figured out a good way to reduce the duplication without making a mess.
% 'foreachx' initially looks like it ought to work, but the test generator is ugly.
kafine_node_consumer_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun single_message_fetch/0,
        % multiple messages, produced (and returned) separately and together.
        fun separate_produces_fetch_zero_offset/0,
        fun separate_produces_fetch_positive_offset/0,
        fun combined_produce_fetch_zero_offset/0,
        fun combined_produce_fetch_positive_offset/0,
        fun recovers_after_disconnect_during_fetch/0
    ]}.

single_message_fetch() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    mock_single_produce(Broker, 1),

    TopicName = ?TOPIC_NAME,
    {ok, Sup} = kafine_consumer_sup:start_link(
        ?CONSUMER_REF,
        Broker,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        [TopicName],
        ?TOPIC_OPTIONS,
        ?FETCHER_METADATA,
        [parallel_callback(?CONSUMER_REF, ?TOPIC_OPTIONS, ?FETCHER_METADATA)]
    ),
    kafine_parallel_subscription_callback:subscribe_partitions(
        not_used, #{TopicName => [?PARTITION]}, ?CONSUMER_REF
    ),

    % We should see two record batches, one with a single message, one empty:
    meck:wait(
        test_consumer_callback,
        handle_partition_data,
        [
            '_',
            '_',
            meck:is(
                all_of([
                    has_message_count(1),
                    contains_message_matching(#{
                        offset => 0, key => <<"key0">>, value => <<"value0">>
                    })
                ])
            ),
            '_'
        ],
        ?WAIT_TIMEOUT_MS
    ),
    meck:wait(
        test_consumer_callback,
        handle_partition_data,
        ['_', '_', meck:is(has_message_count(0)), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    kafine_consumer_sup:stop(Sup),
    kamock_broker:stop(Broker),
    ok.

separate_produces_fetch_zero_offset() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    MessageCount = 3,
    mock_separate_produces(Broker, MessageCount),

    TopicName = ?TOPIC_NAME,
    {ok, Sup} = kafine_consumer_sup:start_link(
        ?CONSUMER_REF,
        Broker,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        [TopicName],
        ?TOPIC_OPTIONS,
        ?FETCHER_METADATA,
        [parallel_callback(?CONSUMER_REF, ?TOPIC_OPTIONS, ?FETCHER_METADATA)]
    ),
    kafine_parallel_subscription_callback:subscribe_partitions(
        not_used, #{TopicName => [?PARTITION]}, ?CONSUMER_REF
    ),

    % We should see two record batches, one with our 3 messages, one empty. Note that the response actually has 3 record
    % batches in it and that we've flattened them into one. That's more of a naming thing, and maybe we want to revisit
    % that.
    meck:wait(
        test_consumer_callback,
        handle_partition_data,
        ['_', '_', meck:is(has_message_count(3)), '_'],
        ?WAIT_TIMEOUT_MS
    ),
    meck:wait(
        test_consumer_callback,
        handle_partition_data,
        ['_', '_', meck:is(has_message_count(0)), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    kafine_consumer_sup:stop(Sup),
    kamock_broker:stop(Broker),
    ok.

separate_produces_fetch_positive_offset() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    MessageCount = 3,
    mock_separate_produces(Broker, MessageCount),

    % Initial offset is two; we expect to see a single message.
    InitialOffset = 2,
    TopicName = ?TOPIC_NAME,
    TopicOptions = kafine_topic_options:validate_options(
        [TopicName], #{TopicName => #{initial_offset => InitialOffset}}
    ),
    {ok, Sup} = kafine_consumer_sup:start_link(
        ?CONSUMER_REF,
        Broker,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        [TopicName],
        TopicOptions,
        ?FETCHER_METADATA,
        [parallel_callback(?CONSUMER_REF, TopicOptions, ?FETCHER_METADATA)]
    ),
    kafine_parallel_subscription_callback:subscribe_partitions(
        not_used, #{TopicName => [?PARTITION]}, ?CONSUMER_REF
    ),

    % We should see two record batches, one with our expected message, one empty.
    meck:wait(
        test_consumer_callback,
        handle_partition_data,
        ['_', '_', meck:is(has_message_count(1)), '_'],
        ?WAIT_TIMEOUT_MS
    ),
    meck:wait(
        test_consumer_callback,
        handle_partition_data,
        ['_', '_', meck:is(has_message_count(0)), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    kafine_consumer_sup:stop(Sup),
    kamock_broker:stop(Broker),
    ok.

combined_produce_fetch_zero_offset() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    MessageCount = 3,
    mock_single_produce(Broker, MessageCount),

    TopicName = ?TOPIC_NAME,
    {ok, Sup} = kafine_consumer_sup:start_link(
        ?CONSUMER_REF,
        Broker,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        [TopicName],
        ?TOPIC_OPTIONS,
        ?FETCHER_METADATA,
        [parallel_callback(?CONSUMER_REF, ?TOPIC_OPTIONS, ?FETCHER_METADATA)]
    ),
    kafine_parallel_subscription_callback:subscribe_partitions(
        not_used, #{TopicName => [?PARTITION]}, ?CONSUMER_REF
    ),

    % We should see two record batches, one with our 3 messages, one empty. Note that in this case, the messages really
    % are in a single batch (unlike above).
    meck:wait(
        test_consumer_callback,
        handle_partition_data,
        ['_', '_', meck:is(has_message_count(3)), '_'],
        ?WAIT_TIMEOUT_MS
    ),
    meck:wait(
        test_consumer_callback,
        handle_partition_data,
        ['_', '_', meck:is(has_message_count(0)), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    kafine_consumer_sup:stop(Sup),
    kamock_broker:stop(Broker),
    ok.

combined_produce_fetch_positive_offset() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    MessageCount = 5,
    mock_single_produce(Broker, MessageCount),

    % 5 messages, initial offset 3; we should see 2 messages (0, 1, 2, [3, 4]).
    InitialOffset = 3,
    TopicName = ?TOPIC_NAME,
    TopicOptions = kafine_topic_options:validate_options(
        [TopicName], #{TopicName => #{initial_offset => InitialOffset}}
    ),
    {ok, Sup} = kafine_consumer_sup:start_link(
        ?CONSUMER_REF,
        Broker,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        [TopicName],
        TopicOptions,
        ?FETCHER_METADATA,
        [parallel_callback(?CONSUMER_REF, TopicOptions, ?FETCHER_METADATA)]
    ),
    kafine_parallel_subscription_callback:subscribe_partitions(
        not_used, #{TopicName => [?PARTITION]}, ?CONSUMER_REF
    ),

    % We should see two record batches, one with our 2 messages, one empty.
    meck:wait(
        test_consumer_callback,
        handle_partition_data,
        ['_', '_', meck:is(has_message_count(2)), '_'],
        ?WAIT_TIMEOUT_MS
    ),
    meck:wait(
        test_consumer_callback,
        handle_partition_data,
        ['_', '_', meck:is(has_message_count(0)), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    kafine_consumer_sup:stop(Sup),
    kamock_broker:stop(Broker),
    ok.

recovers_after_disconnect_during_fetch() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    mock_single_produce(Broker, 1),

    % Make the first fetch request fail with a connection close
    meck:new(kamock_broker_handler, [passthrough]),
    meck:expect(
        kamock_broker_handler,
        handle_request,
        [
            {
                [?FETCH, '_', '_', '_'],
                meck:seq([
                    meck:exec(fun(_, _, _, _) -> stop end),
                    meck:passthrough()
                ])
            },
            {['_', '_', '_', '_'], meck:passthrough()}
        ]
    ),

    TopicName = ?TOPIC_NAME,
    {ok, Sup} = kafine_consumer_sup:start_link(
        ?CONSUMER_REF,
        Broker,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        [TopicName],
        ?TOPIC_OPTIONS,
        ?FETCHER_METADATA,
        [parallel_callback(?CONSUMER_REF, ?TOPIC_OPTIONS, ?FETCHER_METADATA)]
    ),
    kafine_parallel_subscription_callback:subscribe_partitions(
        not_used, #{TopicName => [?PARTITION]}, ?CONSUMER_REF
    ),

    % We should see two record batches, one with a single message, one empty:
    meck:wait(
        test_consumer_callback,
        handle_partition_data,
        [
            '_',
            '_',
            meck:is(
                all_of([
                    has_message_count(1),
                    contains_message_matching(#{
                        offset => 0, key => <<"key0">>, value => <<"value0">>
                    })
                ])
            ),
            '_'
        ],
        ?WAIT_TIMEOUT_MS
    ),
    meck:wait(
        test_consumer_callback,
        handle_partition_data,
        ['_', '_', meck:is(has_message_count(0)), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    kafine_consumer_sup:stop(Sup),
    kamock_broker:stop(Broker),
    ok.

% TODO: Single produce, multiple messages, tail offset.

% TODO: Produce, Fetch, Produce, Fetch, repeatedly.

% TODO: Multiple messages, returned over multiple Fetch (i.e. NextOffset and HighWatermark aren't the same until the
% end). Don't forget to have the number of available messages and the number of returned messages result in some slop.

mock_separate_produces(_Broker, MessageCount) ->
    % If, with a real broker, I do 3 separate Produce requests, then a single Fetch request, then I get back a response
    % containing 3 record batches, each with a single record. This test replicates that.
    %
    % To replicate this with a real broker, run the following:
    %
    %   echo -n "key=value" | kcat -b localhost -P -D '|' -K '=' -t three-messages -p 0
    %   echo -n "key=value" | kcat -b localhost -P -D '|' -K '=' -t three-messages -p 0
    %   echo -n "key=value" | kcat -b localhost -P -D '|' -K '=' -t three-messages -p 0
    %
    % Then run:
    %
    %   kcat -C -b localhost -t three-messages -p 0
    %
    % With different offsets, I get 2, then 1, then zero record batches, each with a single record:
    %
    %   kcat -C -b localhost -t three-messages -p 0 -o 1
    %   kcat -C -b localhost -t three-messages -p 0 -o 2
    %   kcat -C -b localhost -t three-messages -p 0 -o 3
    %
    FirstOffset = 0,
    LastOffset = MessageCount,

    % Note: If you specify an offset (kcat -o 1, e.g.), then there's no ListOffsets request.
    meck:expect(
        kamock_list_offsets_partition_response,
        make_list_offsets_partition_response,
        kamock_list_offsets_partition_response:range(FirstOffset, LastOffset)
    ),

    meck:expect(
        kamock_partition_data,
        make_partition_data,
        fun
            (
                _Topic,
                _FetchPartition = #{partition := PartitionIndex, fetch_offset := FetchOffset},
                _Env
            ) when FetchOffset < LastOffset ->
                make_separate_produces(PartitionIndex, FirstOffset, LastOffset, FetchOffset);
            (
                _Topic,
                _FetchPartition = #{partition := PartitionIndex, fetch_offset := FetchOffset},
                _Env
            ) when FetchOffset == LastOffset ->
                kamock_partition_data:make_empty(PartitionIndex, FirstOffset, LastOffset);
            (
                _Topic,
                _FetchPartition = #{partition := PartitionIndex},
                _Env
            ) ->
                kamock_partition_data:make_error(PartitionIndex, ?OFFSET_OUT_OF_RANGE)
        end
    ),
    ok.

make_separate_produces(PartitionIndex, FirstOffset, LastOffset, FetchOffset) ->
    % Unix epoch, milliseconds; 2024-08-14T17:41:14.686Z
    Timestamp = 1723657274686,
    _FormattedTimestamp = calendar:system_time_to_rfc3339(
        Timestamp, [{unit, millisecond}, {offset, "Z"}]
    ),

    % Three record batches. Note the use of lists:seq(), below, to allow for differing start offsets.
    RecordBatches = [
        make_record_batch(BaseOffset, Timestamp, [
            % Each with a single record.
            make_record(BaseOffset, 0)
        ])
     || BaseOffset <- lists:seq(FetchOffset, LastOffset - 1)
    ],
    make_partition_data(PartitionIndex, FirstOffset, LastOffset, RecordBatches).

make_partition_data(PartitionIndex, FirstOffset, LastOffset, RecordBatches) ->
    kamock_partition_data_builder:make_partition_data(
        PartitionIndex, FirstOffset, LastOffset, RecordBatches
    ).

make_record_batch(BaseOffset, BaseTimestamp, Records) ->
    LastOffsetDelta = length(Records) - 1,
    kamock_partition_data_builder:make_record_batch(
        BaseOffset, LastOffsetDelta, BaseTimestamp, Records
    ).

mock_single_produce(_Broker, MessageCount) ->
    % If, with a real broker, I do a Produce request with 3 messages, then a single Fetch request, then I get back a
    % response containing a single record batch, containing 3 messages.
    %
    % To replicate this with a real broker, run the following:
    %
    %   echo -n "key=value|key=value|key=value" | kcat -b localhost -P -D '|' -K '=' -t three-messages -p 0
    %
    % Then run:
    %
    %   kcat -C -b localhost -t three-messages -p 0
    %
    % Important note: if I ask for messages from offset 2 (i.e. I only want the last message), with this:
    %
    %   kcat -C -b localhost -t three-messages -p 0 -o 2
    %
    % ...then the returned batch actually contains all of the messages. It's the client's responsibility to filter those
    % out.

    % TODO: At some point, we'll need to replicate a partition that _starts_ at a non-zero offset.
    FirstOffset = 0,
    % LastOffset is exclusive.
    LastOffset = FirstOffset + MessageCount,

    meck:expect(
        kamock_list_offsets_partition_response,
        make_list_offsets_partition_response,
        kamock_list_offsets_partition_response:range(FirstOffset, LastOffset)
    ),

    meck:expect(
        kamock_partition_data,
        make_partition_data,
        fun
            (
                _Topic,
                _FetchPartition = #{partition := PartitionIndex, fetch_offset := FetchOffset},
                _Env
            ) when FetchOffset < LastOffset ->
                make_single_produce(PartitionIndex, FirstOffset, LastOffset, FetchOffset);
            (
                _Topic,
                _FetchPartition = #{partition := PartitionIndex, fetch_offset := FetchOffset},
                _Env
            ) when FetchOffset == LastOffset ->
                kamock_partition_data:make_empty(PartitionIndex, FirstOffset, LastOffset);
            (
                _Topic,
                _FetchPartition = #{partition := PartitionIndex},
                _Env
            ) ->
                kamock_partition_data:make_error(PartitionIndex, ?OFFSET_OUT_OF_RANGE)
        end
    ),
    ok.

make_single_produce(PartitionIndex, FirstOffset, LastOffset, _FetchOffset) ->
    % Unix epoch, milliseconds; 2024-08-14T17:41:14.686Z
    Timestamp = 1723657274686,
    _FormattedTimestamp = calendar:system_time_to_rfc3339(
        Timestamp, [{unit, millisecond}, {offset, "Z"}]
    ),

    BaseOffset = FirstOffset,
    LastOffsetDelta = LastOffset - BaseOffset - 1,
    Records = [
        % All records in one batch.
        make_record(BaseOffset, OffsetDelta)
     || OffsetDelta <- lists:seq(0, LastOffsetDelta)
    ],
    RecordBatches = [make_record_batch(BaseOffset, Timestamp, Records)],
    make_partition_data(PartitionIndex, FirstOffset, LastOffset, RecordBatches).

make_record(BaseOffset, OffsetDelta) ->
    Offset = BaseOffset + OffsetDelta,
    Message = #{
        key => iolist_to_binary(io_lib:format("key~B", [Offset])),
        value => iolist_to_binary(io_lib:format("value~B", [Offset])),
        headers => []
    },
    kamock_partition_data_builder:make_record(OffsetDelta, Message).

parallel_callback(Ref, TopicOptions, Metadata) ->
    Options = kafine_parallel_subscription_callback:validate_options(
        #{
            topic_options => TopicOptions,
            callback_mod => test_consumer_callback,
            callback_arg => ?CALLBACK_ARGS,
            skip_empty_fetches => false
        }
    ),

    #{
        id => kafine_parallel_subscription,
        start => {kafine_parallel_subscription_impl, start_link, [Ref, Options, Metadata]},
        restart => permanent,
        shutdown => 5000,
        type => supervisor,
        modules => [kafine_parallel_subscription_impl]
    }.

has_message_count(ExpectedCount) ->
    fun(PartitionData) ->
        kafine_partition_data:message_count(PartitionData) == ExpectedCount
    end.

contains_message_matching(ExpectedMessage) ->
    fun(PartitionData) ->
        {Records, _} = kafine_partition_data:flatten(PartitionData),
        % It's not an exact match, 'cos we don't care about (e.g.) the timestamp.
        Pred = fun(R) ->
            maps:intersect(R, ExpectedMessage) == ExpectedMessage
        end,
        {value, _} = lists:search(Pred, Records)
    end.
