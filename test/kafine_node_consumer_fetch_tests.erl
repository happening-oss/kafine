-module(kafine_node_consumer_fetch_tests).
-include_lib("eunit/include/eunit.hrl").

-include_lib("kafcod/include/error_code.hrl").

-include("history_matchers.hrl").

-elvis([{elvis_style, dont_repeat_yourself, disable}]).

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(PARTITION, 61).
-define(CALLBACK_ARGS, undefined).
-define(CALLBACK_STATE, ?MODULE).
-define(WAIT_TIMEOUT_MS, 2_000).

setup() ->
    meck:new(test_consumer_callback, [non_strict]),
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {ok, ?CALLBACK_STATE} end),
    meck:expect(test_consumer_callback, begin_record_batch, fun(_T, _P, _O, _Info, St) ->
        {ok, St}
    end),
    meck:expect(test_consumer_callback, handle_record, fun(_T, _P, _M, St) -> {ok, St} end),
    meck:expect(test_consumer_callback, end_record_batch, fun(_T, _P, _N, _Info, St) -> {ok, St} end),

    meck:expect(kafine_consumer, init_ack, fun(_Ref, _Topic, _Partition, _State) -> ok end),

    meck:new(kamock_list_offsets, [passthrough]),
    meck:new(kamock_fetch, [passthrough]),
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
        fun combined_produce_fetch_positive_offset/0
    ]}.

single_message_fetch() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    mock_single_produce(Broker, 1),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            ?PARTITION => #{}
        }
    }),
    {ok, Pid} = start_node_consumer(Broker, TopicPartitionStates),

    % We should see two record batches, one with a single message, one empty:
    meck:wait(2, test_consumer_callback, end_record_batch, '_', ?WAIT_TIMEOUT_MS),
    ?assertMatch(
        [
            ?init_callback(TopicName, ?PARTITION, ?CALLBACK_ARGS),
            ?begin_record_batch(TopicName, ?PARTITION, 0, 0, 1, 1),
            ?handle_record(TopicName, ?PARTITION, <<"key0">>, <<"value0">>),
            ?end_record_batch(TopicName, ?PARTITION, 1, 0, 1, 1),

            ?begin_record_batch(TopicName, ?PARTITION, 1, 0, 1, 1),
            ?end_record_batch(TopicName, ?PARTITION, 1, 0, 1, 1)
        ],
        meck:history(test_consumer_callback)
    ),

    kafine_node_consumer:stop(Pid),
    cleanup_topic_partition_states(TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

separate_produces_fetch_zero_offset() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    MessageCount = 3,
    mock_separate_produces(Broker, MessageCount),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            ?PARTITION => #{}
        }
    }),
    {ok, Pid} = start_node_consumer(Broker, TopicPartitionStates),

    % We should see two record batches, one with our 3 messages, one empty. Note that the response actually has 3 record
    % batches in it and that we've flattened them into one. That's more of a naming thing, and maybe we want to revisit
    % that.
    meck:wait(2, test_consumer_callback, end_record_batch, '_', ?WAIT_TIMEOUT_MS),
    ?assertMatch(
        [
            ?init_callback(TopicName, ?PARTITION, ?CALLBACK_ARGS),
            ?begin_record_batch(TopicName, ?PARTITION, 0, 0, 3, 3),
            ?handle_record(TopicName, ?PARTITION, <<"key0">>, _),
            ?handle_record(TopicName, ?PARTITION, <<"key1">>, _),
            ?handle_record(TopicName, ?PARTITION, <<"key2">>, _),
            ?end_record_batch(TopicName, ?PARTITION, 3, 0, 3, 3),

            ?begin_record_batch(TopicName, ?PARTITION, 3, 0, 3, 3),
            ?end_record_batch(TopicName, ?PARTITION, 3, 0, 3, 3)
        ],
        meck:history(test_consumer_callback)
    ),

    kafine_node_consumer:stop(Pid),
    cleanup_topic_partition_states(TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

separate_produces_fetch_positive_offset() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    MessageCount = 3,
    mock_separate_produces(Broker, MessageCount),

    % Initial offset is two; we expect to see a single message.
    InitialOffset = 2,
    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            ?PARTITION => #{offset => InitialOffset}
        }
    }),
    {ok, Pid} = start_node_consumer(Broker, TopicPartitionStates),

    % We should see two record batches, one with our expected messages, one empty. Note that the response actually has 3 record
    % batches in it and that we've flattened them into one. That's more of a naming thing, and maybe we want to revisit
    % that.
    meck:wait(2, test_consumer_callback, end_record_batch, '_', ?WAIT_TIMEOUT_MS),
    ?assertMatch(
        [
            ?init_callback(TopicName, ?PARTITION, ?CALLBACK_ARGS),
            ?begin_record_batch(TopicName, ?PARTITION, 2, 0, 3, 3),
            ?handle_record(TopicName, ?PARTITION, <<"key2">>, _),
            ?end_record_batch(TopicName, ?PARTITION, 3, 0, 3, 3),

            ?begin_record_batch(TopicName, ?PARTITION, 3, 0, 3, 3),
            ?end_record_batch(TopicName, ?PARTITION, 3, 0, 3, 3)
        ],
        meck:history(test_consumer_callback)
    ),

    kafine_node_consumer:stop(Pid),
    cleanup_topic_partition_states(TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

combined_produce_fetch_zero_offset() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    MessageCount = 3,
    mock_single_produce(Broker, MessageCount),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            ?PARTITION => #{}
        }
    }),
    {ok, Pid} = start_node_consumer(Broker, TopicPartitionStates),

    % We should see two record batches, one with our 3 messages, one empty. Note that in this case, the messages really
    % are in a single batch (unlike above).
    meck:wait(2, test_consumer_callback, end_record_batch, '_', ?WAIT_TIMEOUT_MS),
    ?assertMatch(
        [
            ?init_callback(TopicName, ?PARTITION, ?CALLBACK_ARGS),
            ?begin_record_batch(TopicName, ?PARTITION, 0, 0, 3, 3),
            ?handle_record(TopicName, ?PARTITION, <<"key0">>, _),
            ?handle_record(TopicName, ?PARTITION, <<"key1">>, _),
            ?handle_record(TopicName, ?PARTITION, <<"key2">>, _),
            ?end_record_batch(TopicName, ?PARTITION, 3, 0, 3, 3),

            ?begin_record_batch(TopicName, ?PARTITION, 3, 0, 3, 3),
            ?end_record_batch(TopicName, ?PARTITION, 3, 0, 3, 3)
        ],
        meck:history(test_consumer_callback)
    ),

    kafine_node_consumer:stop(Pid),
    cleanup_topic_partition_states(TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

combined_produce_fetch_positive_offset() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    MessageCount = 5,
    mock_single_produce(Broker, MessageCount),

    % 5 messages, initial offset 3; we should see 2 messages (0, 1, 2, [3, 4]).
    InitialOffset = 3,
    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            ?PARTITION => #{
                offset => InitialOffset
            }
        }
    }),
    {ok, Pid} = start_node_consumer(Broker, TopicPartitionStates),

    % We should see two record batches, one with our 2 messages, one empty.
    meck:wait(2, test_consumer_callback, end_record_batch, '_', ?WAIT_TIMEOUT_MS),
    ?assertMatch(
        [
            ?init_callback(TopicName, ?PARTITION, ?CALLBACK_ARGS),
            ?begin_record_batch(TopicName, ?PARTITION, 3, 0, 5, 5),
            ?handle_record(TopicName, ?PARTITION, <<"key3">>, _),
            ?handle_record(TopicName, ?PARTITION, <<"key4">>, _),
            ?end_record_batch(TopicName, ?PARTITION, 5, 0, 5, 5),

            ?begin_record_batch(TopicName, ?PARTITION, 5, 0, 5, 5),
            ?end_record_batch(TopicName, ?PARTITION, 5, 0, 5, 5)
        ],
        meck:history(test_consumer_callback)
    ),

    kafine_node_consumer:stop(Pid),
    kamock_broker:stop(Broker),
    ok.

init_topic_partition_states(InitStates) ->
    kafine_fetch_response_tests:init_topic_partition_states(InitStates).

cleanup_topic_partition_states(TopicPartitionStates) ->
    kafine_fetch_response_tests:cleanup_topic_partition_states(TopicPartitionStates).

start_node_consumer(Broker, TopicPartitionStates) ->
    kafine_node_consumer_tests:start_node_consumer(Broker, TopicPartitionStates).

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
                make_partition_data(PartitionIndex, FirstOffset, LastOffset, RecordBatches);
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

make_partition_data(PartitionIndex, FirstOffset, LastOffset, RecordBatches) ->
    #{
        partition_index => PartitionIndex,
        error_code => ?NONE,
        log_start_offset => FirstOffset,
        high_watermark => LastOffset,
        last_stable_offset => LastOffset,
        aborted_transactions => [],
        preferred_read_replica => -1,
        % Here, 'records' is actually 'record batches'.
        records => RecordBatches
    }.

make_record_batch(BaseOffset, BaseTimestamp, Records) ->
    #{
        base_offset => BaseOffset,
        partition_leader_epoch => 0,
        magic => 2,
        crc => -1,
        attributes => #{compression => none},
        last_offset_delta => length(Records) - 1,
        base_timestamp => BaseTimestamp,
        max_timestamp => BaseTimestamp,
        producer_id => -1,
        producer_epoch => -1,
        base_sequence => -1,
        records => Records
    }.

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
                make_partition_data(PartitionIndex, FirstOffset, LastOffset, RecordBatches);
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

make_record(BaseOffset, OffsetDelta) ->
    Offset = BaseOffset + OffsetDelta,
    Key = iolist_to_binary(io_lib:format("key~B", [Offset])),
    Value = iolist_to_binary(io_lib:format("value~B", [Offset])),
    Headers = [],
    #{
        attributes => 0,
        key => Key,
        value => Value,
        headers => Headers,
        offset_delta => OffsetDelta,
        timestamp_delta => 0
    }.
