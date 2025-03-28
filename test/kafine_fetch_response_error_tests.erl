-module(kafine_fetch_response_error_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/error_code.hrl").

-define(TOPIC_1, <<"topic1">>).
-define(TOPIC_2, <<"topic2">>).
-define(PARTITION_1, 61).
-define(PARTITION_2, 62).
-define(TELEMETRY_METADATA, #{}).

%% When we see multiple OFFSET_OUT_OF_RANGE errors, we want to issue only one ListOffsets request. The magic happens in
%% the error handling, so let's make sure that keeps working.
offset_out_of_range_errors_are_combined_test() ->
    % TopicPartitionStates is untouched if there are no records returned, so we don't need to initialise it.
    TopicPartitionStates = undefined,

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

    {_TopicPartitionStates2, Errors} = kafine_fetch_response:fold(
        FetchResponse, TopicPartitionStates, ?TELEMETRY_METADATA
    ),

    % The errors should be combined. Note that they're still a tuple-list at this point. They get converted again in
    % kafine_node_consumer:collect_topic_partitions/1. Note also that the order isn't important and might change in
    % future. Currently it's reversed from the FetchResponse, above.
    ?assertEqual(
        #{
            ?OFFSET_OUT_OF_RANGE => [
                {?TOPIC_2, ?PARTITION_2},
                {?TOPIC_2, ?PARTITION_1},
                {?TOPIC_1, ?PARTITION_2},
                {?TOPIC_1, ?PARTITION_1}
            ]
        },
        Errors
    ),
    ok.
