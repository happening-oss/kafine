-module(kafine_fetch_request_tests).

-include_lib("eunit/include/eunit.hrl").
-include("src/consumer/kafine_topic_partition_state.hrl").

-define(DEFAULT_CLIENT_ID, <<"kafine">>).
% kcat defaults
-define(MAX_WAIT_TIME_MS, 500).
-define(MIN_BYTES, 1).
% kcat default: 50MiB.
-define(MAX_BYTES, 52_428_800).
% kcat default: 1MiB
-define(PARTITION_MAX_BYTES, 1_048_576).
-define(DEFAULT_OFFSET_RESET_POLICY, earliest).
-define(DEFAULT_ISOLATION_LEVEL, read_uncommitted).

default_options() ->
    #{
        client_id => ?DEFAULT_CLIENT_ID,

        max_wait_ms => ?MAX_WAIT_TIME_MS,
        min_bytes => ?MIN_BYTES,
        max_bytes => ?MAX_BYTES,

        partition_max_bytes => ?PARTITION_MAX_BYTES,

        offset_reset_policy => ?DEFAULT_OFFSET_RESET_POLICY,
        isolation_level => ?DEFAULT_ISOLATION_LEVEL
    }.

active_test() ->
    TopicPartitionStates = #{
        <<"topic">> => #{
            61 => #topic_partition_state{state = active, offset = 0},
            62 => #topic_partition_state{state = active, offset = 0}
        }
    },
    Options = default_options(),
    ?assertMatch(
        #{
            min_bytes := ?MIN_BYTES,
            max_wait_ms := ?MAX_WAIT_TIME_MS,
            max_bytes := ?MAX_BYTES,
            isolation_level := 0,
            replica_id := -1,
            topics :=
                [
                    #{
                        % Note that they're in reverse order; implementation detail.
                        partitions :=
                            [
                                #{
                                    partition := 62,
                                    partition_max_bytes := ?PARTITION_MAX_BYTES,
                                    fetch_offset := 0
                                },
                                #{
                                    partition := 61,
                                    partition_max_bytes := ?PARTITION_MAX_BYTES,
                                    fetch_offset := 0
                                }
                            ],
                        topic := <<"topic">>
                    }
                ]
        },
        kafine_fetch_request:build_fetch_request(TopicPartitionStates, Options)
    ).

paused_test() ->
    TopicPartitionStates = #{
        <<"topic">> => #{
            61 => #topic_partition_state{state = active, offset = 0},
            62 => #topic_partition_state{state = paused, offset = 0}
        }
    },
    Options = default_options(),
    ?assertMatch(
        #{
            min_bytes := ?MIN_BYTES,
            max_wait_ms := ?MAX_WAIT_TIME_MS,
            max_bytes := ?MAX_BYTES,
            isolation_level := 0,
            replica_id := -1,
            topics :=
                [
                    #{
                        partitions :=
                            [
                                #{
                                    partition := 61,
                                    partition_max_bytes := ?PARTITION_MAX_BYTES,
                                    fetch_offset := 0
                                }
                            ],
                        topic := <<"topic">>
                    }
                ]
        },
        kafine_fetch_request:build_fetch_request(TopicPartitionStates, Options)
    ).
