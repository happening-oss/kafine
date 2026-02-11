-module(kafine_fetch_response_error_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/error_code.hrl").

-include("assert_meck.hrl").

-define(TOPIC_1, <<"topic1">>).
-define(TOPIC_2, <<"topic2">>).
-define(PARTITION_1, 61).
-define(PARTITION_2, 62).
-define(JOB_ID, 6).
-define(NODE_ID, 104).

%% When we see multiple OFFSET_OUT_OF_RANGE errors, we want to issue only one ListOffsets request. The magic happens in
%% the error handling, so let's make sure that keeps working.
offset_out_of_range_errors_are_combined_test() ->
    meck:new(kafine_fetcher, [stub_all]),

    % TopicPartitionStates is untouched if there are no records returned, so we don't need to initialise it.
    FetchInfo = #{
        <<"topic">> => #{
            61 => {1, dummy, undefined},
            62 => {2, dummy, undefined}
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

    TopicOptions = #{
        ?TOPIC_1 => #{offset_reset_policy => earliest},
        ?TOPIC_2 => #{offset_reset_policy => latest}
    },

    ok = kafine_fetch:handle_response(
        FetchResponse, FetchInfo, ?JOB_ID, ?NODE_ID, TopicOptions, self()
    ),

    ?assertCalled(kafine_fetcher, complete_job, [
        self(),
        ?JOB_ID,
        ?NODE_ID,
        #{
            ?TOPIC_1 => #{
                ?PARTITION_1 => {update_offset, earliest},
                ?PARTITION_2 => {update_offset, earliest}
            },
            ?TOPIC_2 => #{
                ?PARTITION_1 => {update_offset, latest},
                ?PARTITION_2 => {update_offset, latest}
            }
        }
    ]).
