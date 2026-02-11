-module(kafine_consumer_negative_offset_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/timestamp.hrl").

-include("assert_meck.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(CONNECTION_OPTIONS, kafine_connection_options:validate_options(#{})).
-define(CONSUMER_OPTIONS, kafine_consumer_options:validate_options(#{})).
-define(FETCHER_METADATA, #{}).
-define(CALLBACK_ARGS, undefined).
-define(WAIT_TIMEOUT_MS, 2_000).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun last_with_new_topic/0,
        fun last_with_single_message/0,
        fun last_with_multiple_messages/0
    ]}.

setup() ->
    meck:new(test_fetcher_callback, [non_strict]),
    meck:expect(test_fetcher_callback, handle_partition_data, fun(_A, _T, _P, _O, _U) -> ok end),

    meck:new(kamock_list_offsets, [passthrough]),
    meck:new(kamock_fetch, [passthrough]),
    ok.

cleanup(_) ->
    meck:unload(),
    ok.

last_with_new_topic() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    TopicName = ?TOPIC_NAME,
    Partition = 0,
    TopicOptions = kafine_topic_options:validate_options([TopicName], #{
        ?TOPIC_NAME => #{
            initial_offset => -1, offset_reset_policy => latest
        }
    }),

    {ok, B} = kafine_bootstrap:start_link(?CONSUMER_REF, Broker, ?CONNECTION_OPTIONS),
    {ok, M} = kafine_metadata_cache:start_link(?CONSUMER_REF),
    {ok, Sup} = kafine_fetcher_sup:start_link(
        ?CONSUMER_REF,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        TopicOptions,
        ?FETCHER_METADATA
    ),

    % By default, there are no messages on the topic.
    ok = kafine_fetcher:set_topic_partitions(?CONSUMER_REF, #{TopicName => [Partition]}),
    fetch(?CONSUMER_REF, TopicName, Partition, -1),

    % We should see a ListOffsets, then a Fetch at zero.
    meck:wait(
        kamock_list_offsets,
        handle_list_offsets_request,
        [has_timestamp(?LATEST_TIMESTAMP), '_'],
        ?WAIT_TIMEOUT_MS
    ),
    meck:wait(
        kamock_fetch,
        handle_fetch_request,
        [has_fetch_offset(0), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    ?assertWait(
        test_fetcher_callback,
        handle_partition_data,
        ['_', ?TOPIC_NAME, has_next_offset(0), 0, '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % If we produce a message, we be able to fetch it.
    kafine_kamock:produce(0, 1),
    fetch(?CONSUMER_REF, TopicName, Partition, 0),

    ?assertWait(
        test_fetcher_callback,
        handle_partition_data,
        ['_', ?TOPIC_NAME, has_next_offset(1), 0, '_'],
        ?WAIT_TIMEOUT_MS
    ),
    ?assertMatch(
        [
            {TopicName, Partition, #{key := <<"key0">>}}
        ],
        received_records()
    ),

    kafine_fetcher_sup:stop(Sup),
    kafine_metadata_cache:stop(M),
    kafine_bootstrap:stop(B),
    kamock_broker:stop(Broker),
    ok.

last_with_single_message() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    TopicName = ?TOPIC_NAME,
    Partition = 0,
    TopicOptions = kafine_topic_options:validate_options([TopicName], #{
        ?TOPIC_NAME => #{
            initial_offset => -1, offset_reset_policy => latest
        }
    }),

    {ok, B} = kafine_bootstrap:start_link(?CONSUMER_REF, Broker, ?CONNECTION_OPTIONS),
    {ok, M} = kafine_metadata_cache:start_link(?CONSUMER_REF),
    {ok, Sup} = kafine_fetcher_sup:start_link(
        ?CONSUMER_REF,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        TopicOptions,
        ?FETCHER_METADATA
    ),

    % There's one message on the partition.
    kafine_kamock:produce(0, 1),

    ok = kafine_fetcher:set_topic_partitions(?CONSUMER_REF, #{TopicName => [Partition]}),
    fetch(?CONSUMER_REF, TopicName, Partition, -1),

    % We should see a ListOffsets, then a Fetch at zero.
    meck:wait(
        kamock_list_offsets,
        handle_list_offsets_request,
        [has_timestamp(?LATEST_TIMESTAMP), '_'],
        ?WAIT_TIMEOUT_MS
    ),
    meck:wait(
        kamock_fetch,
        handle_fetch_request,
        [has_fetch_offset(0), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % We should see the single message.
    ?assertWait(
        test_fetcher_callback,
        handle_partition_data,
        ['_', ?TOPIC_NAME, has_next_offset(1), 0, '_'],
        ?WAIT_TIMEOUT_MS
    ),
    ?assertMatch(
        [
            {TopicName, Partition, #{key := <<"key0">>}}
        ],
        received_records()
    ),
    meck:reset(test_fetcher_callback),

    % If we produce another message, we be able to fetch it.
    kafine_kamock:produce(0, 2),

    fetch(?CONSUMER_REF, TopicName, Partition, 1),

    ?assertWait(
        test_fetcher_callback,
        handle_partition_data,
        ['_', ?TOPIC_NAME, has_next_offset(2), '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),
    ?assertMatch(
        [
            {TopicName, Partition, #{key := <<"key1">>}}
        ],
        received_records()
    ),

    kafine_fetcher_sup:stop(Sup),
    kafine_metadata_cache:stop(M),
    kafine_bootstrap:stop(B),
    kamock_broker:stop(Broker),
    ok.

last_with_multiple_messages() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    TopicName = ?TOPIC_NAME,
    Partition = 0,
    TopicOptions = kafine_topic_options:validate_options([TopicName], #{
        ?TOPIC_NAME => #{
            initial_offset => -1, offset_reset_policy => latest
        }
    }),

    {ok, B} = kafine_bootstrap:start_link(?CONSUMER_REF, Broker, ?CONNECTION_OPTIONS),
    {ok, M} = kafine_metadata_cache:start_link(?CONSUMER_REF),
    {ok, Sup} = kafine_fetcher_sup:start_link(
        ?CONSUMER_REF,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        TopicOptions,
        ?FETCHER_METADATA
    ),

    % There's a bunch of messages on the partition.
    kafine_kamock:produce(10, 15),

    ok = kafine_fetcher:set_topic_partitions(?CONSUMER_REF, #{TopicName => [Partition]}),
    fetch(?CONSUMER_REF, TopicName, Partition, -1),

    % We should see a ListOffsets, then a Fetch.
    meck:wait(
        kamock_list_offsets,
        handle_list_offsets_request,
        [has_timestamp(?LATEST_TIMESTAMP), '_'],
        ?WAIT_TIMEOUT_MS
    ),
    meck:wait(
        kamock_fetch,
        handle_fetch_request,
        [has_fetch_offset(14), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % We should see a single message.
    ?assertWait(
        test_fetcher_callback,
        handle_partition_data,
        ['_', ?TOPIC_NAME, has_next_offset(15), '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),
    ?assertMatch(
        [
            {TopicName, Partition, #{key := <<"key14">>}}
        ],
        received_records()
    ),
    meck:reset(test_fetcher_callback),

    % If we produce another message, we be able to fetch it.
    kafine_kamock:produce(10, 16),

    fetch(?CONSUMER_REF, TopicName, Partition, 15),

    ?assertWait(
        test_fetcher_callback,
        handle_partition_data,
        ['_', ?TOPIC_NAME, has_next_offset(16), '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),
    ?assertMatch(
        [
            % We don't see key0, key1.
            {TopicName, Partition, #{key := <<"key15">>}}
        ],
        received_records()
    ),

    kafine_fetcher_sup:stop(Sup),
    kafine_metadata_cache:stop(M),
    kafine_bootstrap:stop(B),
    kamock_broker:stop(Broker),
    ok.

has_timestamp(ExpectedTimestamp) ->
    meck:is(
        fun(ListOffsetsRequest) ->
            #{
                topics := [
                    #{partitions := [#{partition_index := 0, timestamp := Timestamp}]}
                ]
            } = ListOffsetsRequest,
            Timestamp == ExpectedTimestamp
        end
    ).

has_fetch_offset(ExpectedOffset) ->
    meck:is(
        fun(FetchRequest) ->
            #{
                topics := [
                    #{partitions := [#{partition := 0, fetch_offset := FetchOffset}]}
                ]
            } = FetchRequest,
            FetchOffset == ExpectedOffset
        end
    ).

has_next_offset(ExpectedNextOffset) ->
    meck:is(
        fun
            (#{records := [#{base_offset := Base, last_offset_delta := LastDelta}]}) ->
                NextOffset = Base + LastDelta + 1,
                NextOffset =:= ExpectedNextOffset;
            (#{records := [], high_watermark := HighWatermark}) ->
                HighWatermark =:= ExpectedNextOffset
        end
    ).

received_records() ->
    lists:flatten(
        lists:map(
            fun
                (
                    {_,
                        {_, handle_partition_data, [
                            _,
                            Topic,
                            #{partition_index := Partition, records := [#{records := Records}]},
                            _,
                            _
                        ]},
                        _}
                ) ->
                    [{Topic, Partition, Record} || Record <- Records];
                (_) ->
                    []
            end,
            meck:history(test_fetcher_callback)
        )
    ).

fetch(Ref, Topic, Partition, Offset) ->
    kafine_fetcher:fetch(
        Ref,
        Topic,
        Partition,
        Offset,
        test_fetcher_callback,
        ?CALLBACK_ARGS
    ).
