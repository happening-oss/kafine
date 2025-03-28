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
    TopicPartitionTimestamps = #{
        <<"cats">> => #{61 => earliest, 62 => earliest},
        <<"dogs">> => #{51 => latest, 52 => latest},
        <<"fish">> => #{41 => -5, 42 => -5}
    },
    IsolationLevel = read_committed,

    % The topics are in reverse order. This is an implementation detail and might change.
    Expected = #{
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
                            partition_index => 41
                        },
                        #{
                            timestamp => ?LATEST_TIMESTAMP,
                            current_leader_epoch => -1,
                            partition_index => 42
                        }
                    ]
                },
                #{
                    name => <<"dogs">>,
                    partitions => [
                        #{
                            timestamp => ?LATEST_TIMESTAMP,
                            current_leader_epoch => -1,
                            partition_index => 51
                        },
                        #{
                            timestamp => ?LATEST_TIMESTAMP,
                            current_leader_epoch => -1,
                            partition_index => 52
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
    ?assertEqual(
        Expected,
        kafine_list_offsets_request:build_list_offsets_request(
            TopicPartitionTimestamps, IsolationLevel
        )
    ),
    ok.
