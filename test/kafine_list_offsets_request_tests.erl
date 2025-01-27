-module(kafine_list_offsets_request_tests).
-include_lib("eunit/include/eunit.hrl").

-include_lib("kafcod/include/timestamp.hrl").
-include_lib("kafcod/include/isolation_level.hrl").

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun build_list_offsets_request/0
    ]}.

setup() ->
    meck:new(test_offset_reset_policy, [non_strict]),
    ok.

cleanup(_) ->
    meck:unload(),
    ok.

build_list_offsets_request() ->
    meck:expect(test_offset_reset_policy, timestamp, fun() -> ?LATEST_TIMESTAMP end),

    TopicPartitions = #{<<"cats">> => [61, 62], <<"dogs">> => [61, 62], <<"fish">> => [61, 62]},
    TopicOptions = #{
        <<"cats">> => #{offset_reset_policy => earliest},
        <<"dogs">> => #{offset_reset_policy => latest},
        <<"fish">> => #{offset_reset_policy => test_offset_reset_policy}
    },
    IsolationLevel = read_committed,
    ?assertEqual(
        #{
            isolation_level => ?READ_COMMITTED,
            replica_id => -1,
            topics =>
                [
                    #{
                        name => <<"fish">>,
                        partitions => [
                            #{
                                timestamp => ?LATEST_TIMESTAMP,
                                current_leader_epoch => -1,
                                partition_index => 61
                            },
                            #{
                                timestamp => ?LATEST_TIMESTAMP,
                                current_leader_epoch => -1,
                                partition_index => 62
                            }
                        ]
                    },
                    #{
                        name => <<"dogs">>,
                        partitions => [
                            #{
                                timestamp => ?LATEST_TIMESTAMP,
                                current_leader_epoch => -1,
                                partition_index => 61
                            },
                            #{
                                timestamp => ?LATEST_TIMESTAMP,
                                current_leader_epoch => -1,
                                partition_index => 62
                            }
                        ]
                    },
                    #{
                        name => <<"cats">>,
                        partitions => [
                            #{
                                timestamp => ?EARLIEST_TIMESTAMP,
                                current_leader_epoch => -1,
                                partition_index => 61
                            },
                            #{
                                timestamp => ?EARLIEST_TIMESTAMP,
                                current_leader_epoch => -1,
                                partition_index => 62
                            }
                        ]
                    }
                ]
        },
        kafine_list_offsets_request:build_list_offsets_request(
            TopicPartitions, TopicOptions, IsolationLevel
        )
    ),
    ok.
