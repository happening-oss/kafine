-module(kafine_fetch_response_error_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/error_code.hrl").

-define(TOPIC_1, <<"topic1">>).
-define(TOPIC_2, <<"topic2">>).
-define(PARTITION_1, 61).
-define(PARTITION_2, 62).

%% When we see multiple OFFSET_OUT_OF_RANGE errors, we want to issue only one ListOffsets request. The magic happens in
%% the error handling, so let's make sure that keeps working.
offset_out_of_range_errors_are_combined_test() ->
    meck:new(kafine_fetcher, [stub_all]),

    % TopicPartitionStates is untouched if there are no records returned, so we don't need to initialise it.
    FetchInfo = #{
        ?TOPIC_1 => #{
            ?PARTITION_1 => {1, dummy, undefined},
            ?PARTITION_2 => {2, dummy, undefined}
        },
        ?TOPIC_2 => #{
            ?PARTITION_1 => {1, dummy, undefined},
            ?PARTITION_2 => {2, dummy, undefined}
        }
    },

    % Construct some fake partition data with two topics, each with two partitions, all with OFFSET_OUT_OF_RANGE errors.
    PartitionData = #{
        records => [],
        high_watermark => 0,
        last_stable_offset => 0,
        log_start_offset => 0,
        aborted_transactions => [],
        preferred_read_replica => -1
    },
    PartitionData1 = PartitionData#{
        partition_index => ?PARTITION_1,
        error_code => ?OFFSET_OUT_OF_RANGE
    },
    PartitionData2 = PartitionData#{
        partition_index => ?PARTITION_2,
        error_code => ?OFFSET_OUT_OF_RANGE
    },

    FetchResponse = #{
        error_code => ?NONE,
        responses => [
            #{
                topic => ?TOPIC_1,
                partitions => [PartitionData1, PartitionData2]
            },
            #{
                topic => ?TOPIC_2,
                partitions => [PartitionData1, PartitionData2]
            }
        ],
        throttle_time_ms => 0,
        session_id => 0
    },

    {ok, Result} = kafine_fetch:handle_response(FetchResponse, FetchInfo),

    ?assertEqual(
        #{
            ?TOPIC_1 => #{
                ?PARTITION_1 => {error, {kafka_error, ?OFFSET_OUT_OF_RANGE}},
                ?PARTITION_2 => {error, {kafka_error, ?OFFSET_OUT_OF_RANGE}}
            },
            ?TOPIC_2 => #{
                ?PARTITION_1 => {error, {kafka_error, ?OFFSET_OUT_OF_RANGE}},
                ?PARTITION_2 => {error, {kafka_error, ?OFFSET_OUT_OF_RANGE}}
            }
        },
        Result
    ).
