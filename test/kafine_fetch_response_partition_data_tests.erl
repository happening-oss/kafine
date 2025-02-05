-module(kafine_fetch_response_partition_data_tests).
-include_lib("eunit/include/eunit.hrl").

-export([
    canned_fetch_response_single_batch/0,
    canned_fetch_response_two_batches/0,
    split_fetch_response/1
]).

-define(CALLBACK_STATE, ?MODULE).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun single_batch/0,
        fun two_batches/0
    ]}.

setup() ->
    meck:new(test_consumer_callback, [non_strict]),
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {ok, ?CALLBACK_STATE} end),
    meck:expect(test_consumer_callback, begin_record_batch, fun(_T, _P, _O, _Info, St) ->
        {ok, St}
    end),
    meck:expect(test_consumer_callback, handle_record, fun(_T, _P, _M, St) -> {ok, St} end),
    meck:expect(test_consumer_callback, end_record_batch, fun(_T, _P, _N, _Info, St) -> {ok, St} end),
    ok.

cleanup(_) ->
    meck:unload().

% The fetch response contains a single batch.
single_batch() ->
    FetchOffset = 3,
    FetchResponse = canned_fetch_response_single_batch(),
    {Topic, PartitionData} = split_fetch_response(FetchResponse),
    FoldResult = kafine_fetch_response_partition_data:fold(
        Topic,
        PartitionData,
        FetchOffset,
        test_consumer_callback,
        ?CALLBACK_STATE
    ),
    ?assertMatch({6, active, ?CALLBACK_STATE}, FoldResult),
    ok.

% The fetch response contains two batches.
two_batches() ->
    FetchOffset = 3,
    FetchResponse = canned_fetch_response_two_batches(),
    {Topic, PartitionData} = split_fetch_response(FetchResponse),
    FoldResult = kafine_fetch_response_partition_data:fold(
        Topic,
        PartitionData,
        FetchOffset,
        test_consumer_callback,
        ?CALLBACK_STATE
    ),
    ?assertMatch({9, active, ?CALLBACK_STATE}, FoldResult),
    ok.

% Given a canned fetch response, break it into the two pieces required by kafine_fetch_response_partition_data:fold().
split_fetch_response(FetchResponse) ->
    #{responses := [FetchableTopicResponse]} = FetchResponse,
    #{topic := Topic} = FetchableTopicResponse,
    #{partitions := [PartitionData]} = FetchableTopicResponse,
    {Topic, PartitionData}.

canned_fetch_response_single_batch() ->
    % This is a fetch response containing [[3, 4, 5]].
    %
    % We create it by mangling the captured response.
    FetchResponse = canned_fetch_response_two_batches(),
    #{responses := [FetchableTopicResponse]} = FetchResponse,
    #{partitions := [PartitionData]} = FetchableTopicResponse,
    #{records := [RecordBatch | _]} = PartitionData,
    % Truncate the record batches, so that there's only a single batch.
    PartitionData2 = PartitionData#{records := [RecordBatch]},
    FetchableTopicResponse2 = FetchableTopicResponse#{partitions := [PartitionData2]},
    FetchResponse2 = FetchResponse#{responses := [FetchableTopicResponse2]},
    FetchResponse2.

canned_fetch_response_two_batches() ->
    % This is a fetch response containing [[3, 4, 5], [6, 7, 8]].
    %
    % I created it with a real broker and wireshark like this:
    %
    % Produce 3 batches, each with 3 messages:
    %
    %   echo -n "key=value|key=value|key=value" | kcat -b localhost -P -D '|' -K '=' -t cars -p 2
    %   echo -n "key=value|key=value|key=value" | kcat -b localhost -P -D '|' -K '=' -t cars -p 2
    %   echo -n "key=value|key=value|key=value" | kcat -b localhost -P -D '|' -K '=' -t cars -p 2
    %
    % Fetch the last two batches:
    %
    %   kcat -b localhost -C -t cars -p 2 -o 3
    %
    % See the Wireshark doc in kafcod for how to convert it into an Erlang value.
    #{
        responses =>
            [
                #{
                    topic => <<"cars">>,
                    partitions =>
                        [
                            #{
                                high_watermark => 9,
                                records =>
                                    [
                                        #{
                                            attributes => #{compression => none},
                                            records =>
                                                [
                                                    #{
                                                        attributes => 0,
                                                        value => <<"value">>,
                                                        key => <<"key">>,
                                                        headers => [],
                                                        offset_delta => 0,
                                                        timestamp_delta => 0
                                                    },
                                                    #{
                                                        attributes => 0,
                                                        value => <<"value">>,
                                                        key => <<"key">>,
                                                        headers => [],
                                                        offset_delta => 1,
                                                        timestamp_delta => 0
                                                    },
                                                    #{
                                                        attributes => 0,
                                                        value => <<"value">>,
                                                        key => <<"key">>,
                                                        headers => [],
                                                        offset_delta => 2,
                                                        timestamp_delta => 0
                                                    }
                                                ],
                                            producer_id => -1,
                                            producer_epoch => -1,
                                            partition_leader_epoch => 0,
                                            max_timestamp => 1725550237492,
                                            magic => 2,
                                            last_offset_delta => 2,
                                            base_timestamp => 1725550237492,
                                            base_sequence => -1,
                                            base_offset => 3,
                                            crc => 381418536
                                        },
                                        #{
                                            attributes => #{compression => none},
                                            records =>
                                                [
                                                    #{
                                                        attributes => 0,
                                                        value => <<"value">>,
                                                        key => <<"key">>,
                                                        headers => [],
                                                        offset_delta => 0,
                                                        timestamp_delta => 0
                                                    },
                                                    #{
                                                        attributes => 0,
                                                        value => <<"value">>,
                                                        key => <<"key">>,
                                                        headers => [],
                                                        offset_delta => 1,
                                                        timestamp_delta => 0
                                                    },
                                                    #{
                                                        attributes => 0,
                                                        value => <<"value">>,
                                                        key => <<"key">>,
                                                        headers => [],
                                                        offset_delta => 2,
                                                        timestamp_delta => 0
                                                    }
                                                ],
                                            producer_id => -1,
                                            producer_epoch => -1,
                                            partition_leader_epoch => 0,
                                            max_timestamp => 1725550238414,
                                            magic => 2,
                                            last_offset_delta => 2,
                                            base_timestamp => 1725550238414,
                                            base_sequence => -1,
                                            base_offset => 6,
                                            crc => 2128003712
                                        }
                                    ],
                                partition_index => 2,
                                error_code => 0,
                                last_stable_offset => 9,
                                aborted_transactions => [],
                                log_start_offset => 0,
                                preferred_read_replica => -1
                            }
                        ]
                }
            ],
        correlation_id => 2,
        error_code => 0,
        throttle_time_ms => 0,
        session_id => 0
    }.
