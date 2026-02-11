-module(kafine_consumer_offset_reset_policy_tests).
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

setup() ->
    meck:new(test_fetcher_callback, [non_strict]),
    meck:expect(test_fetcher_callback, handle_partition_data, fun(_A, _T, _P, _O, _U) -> ok end),

    meck:new(kamock_list_offsets, [passthrough]),
    meck:new(kamock_fetch, [passthrough]),

    ok.

cleanup(_) ->
    meck:unload().

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun earliest/0,
        fun latest/0
    ]}.

earliest() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    TopicName = ?TOPIC_NAME,
    Partition = 0,
    TopicOptions = #{
        TopicName => kafine_topic_options:validate_options(#{offset_reset_policy => earliest})
    },

    {ok, B} = kafine_bootstrap:start_link(?CONSUMER_REF, Broker, ?CONNECTION_OPTIONS),
    {ok, M} = kafine_metadata_cache:start_link(?CONSUMER_REF),
    {ok, Sup} = kafine_fetcher_sup:start_link(
        ?CONSUMER_REF,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        TopicOptions,
        ?FETCHER_METADATA
    ),

    % Initially, pretend to have 3 messages.
    kafine_kamock:produce(0, 1),

    % If we specify 'earliest', we should get the first message.
    ok = kafine_fetcher:set_topic_partitions(?CONSUMER_REF, #{TopicName => [Partition]}),
    fetch(?CONSUMER_REF, TopicName, Partition, earliest),

    % kamock only sends the first message to our first fetch
    ?assertWait(test_fetcher_callback, handle_partition_data, '_', ?WAIT_TIMEOUT_MS),
    ?assertMatch(
        [
            {TopicName, Partition, #{key := <<"key0">>}}
        ],
        received_records()
    ),
    meck:reset(test_fetcher_callback),
    meck:reset(kamock_list_offsets),
    meck:reset(kamock_fetch),

    % Produce more messages, and discard everything up to and including offset 3
    kafine_kamock:produce(4, 5),

    % fetching from 3 should reset, and we should get the new messages.
    fetch(?CONSUMER_REF, TopicName, Partition, 3),

    % should query the earliest offset
    ?assertWait(
        kamock_list_offsets,
        handle_list_offsets_request,
        [has_timestamp(?EARLIEST_TIMESTAMP), '_'],
        ?WAIT_TIMEOUT_MS
    ),
    % then fetch from it
    ?assertWait(
        kamock_fetch,
        handle_fetch_request,
        [has_fetch_offset(4), '_'],
        ?WAIT_TIMEOUT_MS
    ),
    ?assertWait(
        test_fetcher_callback,
        handle_partition_data,
        ['_', ?TOPIC_NAME, has_next_offset(5), 4, '_'],
        ?WAIT_TIMEOUT_MS
    ),
    ?assertMatch(
        [
            {TopicName, Partition, #{key := <<"key4">>}}
        ],
        received_records()
    ),

    kafine_fetcher_sup:stop(Sup),
    kafine_metadata_cache:stop(M),
    kafine_bootstrap:stop(B),
    kamock_broker:stop(Broker),
    ok.

latest() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    TopicName = ?TOPIC_NAME,
    Partition = 0,
    TopicOptions = #{
        TopicName => kafine_topic_options:validate_options(#{offset_reset_policy => latest})
    },

    {ok, B} = kafine_bootstrap:start_link(?CONSUMER_REF, Broker, ?CONNECTION_OPTIONS),
    {ok, M} = kafine_metadata_cache:start_link(?CONSUMER_REF),
    {ok, Sup} = kafine_fetcher_sup:start_link(
        ?CONSUMER_REF,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        TopicOptions,
        ?FETCHER_METADATA
    ),

    % Initially, pretend to have 3 messages.
    kafine_kamock:produce(0, 3),

    % If we specify 'latest', we should initially get no messages.
    ok = kafine_fetcher:set_topic_partitions(?CONSUMER_REF, #{TopicName => [Partition]}),
    fetch(?CONSUMER_REF, TopicName, Partition, latest),

    ?assertWait(
        test_fetcher_callback,
        handle_partition_data,
        ['_', ?TOPIC_NAME, has_next_offset(3), 3, '_'],
        ?WAIT_TIMEOUT_MS
    ),
    ?assertMatch([], received_records()),
    meck:reset(test_fetcher_callback),
    meck:reset(kamock_list_offsets),
    meck:reset(kamock_fetch),

    % Produce more messages, and discard everything up to and including offset 3
    kafine_kamock:produce(4, 5),

    % fetching from 3 should reset, and we should get the new messages.
    fetch(?CONSUMER_REF, TopicName, Partition, 3),

    % should query the latest offset
    ?assertWait(
        kamock_list_offsets,
        handle_list_offsets_request,
        [has_timestamp(?LATEST_TIMESTAMP), '_'],
        ?WAIT_TIMEOUT_MS
    ),
    % then fetch from it
    ?assertWait(
        kamock_fetch,
        handle_fetch_request,
        [has_fetch_offset(5), '_'],
        ?WAIT_TIMEOUT_MS
    ),
    % fetch should be empty again
    ?assertWait(
        test_fetcher_callback,
        handle_partition_data,
        ['_', ?TOPIC_NAME, has_next_offset(5), 5, '_'],
        ?WAIT_TIMEOUT_MS
    ),
    ?assertMatch([], received_records()),

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
