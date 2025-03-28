-module(kafine_list_offsets_response_tests).
-include_lib("eunit/include/eunit.hrl").

-include_lib("kafcod/include/error_code.hrl").

-include("src/consumer/kafine_topic_partition_state.hrl").

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun fold/0,
        fun unsubscribed/0,
        fun fold_negative/0
    ]}.

setup() ->
    meck:new(test_offset_reset_policy, [non_strict]),
    ok.

cleanup(_) ->
    meck:unload(),
    ok.

fold() ->
    TopicPartitionStates = #{
        <<"cats">> => #{
            61 => #topic_partition_state{offset = 34},
            62 => #topic_partition_state{offset = 75}
        },
        <<"dogs">> => #{
            61 => #topic_partition_state{offset = 48},
            62 => #topic_partition_state{offset = 54}
        },
        <<"fish">> => #{
            61 => #topic_partition_state{offset = 68},
            62 => #topic_partition_state{offset = 78}
        }
    },

    % Note: the offsets here correspond to the offset reset policy. That is: we asked for the earliest, we get the
    % earliest offset, and so on.
    ListOffsetsResponse = #{
        topics => [
            #{
                name => <<"cats">>,
                partitions => [
                    #{partition_index => 61, error_code => ?NONE, offset => 44},
                    #{partition_index => 62, error_code => ?NONE, offset => 75}
                ]
            },
            #{
                name => <<"dogs">>,
                partitions => [
                    #{partition_index => 61, error_code => ?NONE, offset => 52},
                    #{partition_index => 62, error_code => ?NONE, offset => 54}
                ]
            },
            #{
                name => <<"fish">>,
                partitions => [
                    #{partition_index => 61, error_code => ?NONE, offset => 68},
                    #{partition_index => 62, error_code => ?NONE, offset => 90}
                ]
            }
        ]
    },

    ?assertMatch(
        #{
            <<"cats">> := #{
                61 := #topic_partition_state{offset = 44},
                62 := #topic_partition_state{offset = 75}
            },
            <<"dogs">> := #{
                61 := #topic_partition_state{offset = 52},
                62 := #topic_partition_state{offset = 54}
            },
            <<"fish">> := #{
                61 := #topic_partition_state{offset = 68},
                62 := #topic_partition_state{offset = 90}
            }
        },
        kafine_list_offsets_response:fold(ListOffsetsResponse, TopicPartitionStates)
    ),
    ok.

unsubscribed() ->
    TopicPartitionStates = #{
        <<"cats">> => #{
            61 => #topic_partition_state{offset = 34}
        }
    },

    ListOffsetsResponse = #{
        topics => [
            #{
                name => <<"cats">>,
                partitions => [
                    #{partition_index => 61, error_code => ?NONE, offset => 44},
                    #{partition_index => 62, error_code => ?NONE, offset => 75}
                ]
            }
        ]
    },
    ?assertMatch(
        #{
            <<"cats">> := #{
                61 := #topic_partition_state{offset = 44}
            }
        },
        kafine_list_offsets_response:fold(ListOffsetsResponse, TopicPartitionStates)
    ),
    ok.

fold_negative() ->
    TopicPartitionStates = #{
        <<"cats">> => #{
            61 => #topic_partition_state{offset = -5},
            62 => #topic_partition_state{offset = -5}
        },
        <<"dogs">> => #{
            61 => #topic_partition_state{offset = -10},
            62 => #topic_partition_state{offset = -10}
        }
    },

    % Note: the offsets here correspond to the offset reset policy. That is: we asked for the earliest, we get the
    % earliest offset, and so on.
    ListOffsetsResponse = #{
        topics => [
            #{
                name => <<"cats">>,
                partitions => [
                    #{partition_index => 61, error_code => ?NONE, offset => 10},
                    #{partition_index => 62, error_code => ?NONE, offset => 5}
                ]
            },
            #{
                name => <<"dogs">>,
                partitions => [
                    #{partition_index => 61, error_code => ?NONE, offset => 2},
                    #{partition_index => 62, error_code => ?NONE, offset => 4}
                ]
            }
        ]
    },

    ?assertMatch(
        #{
            <<"cats">> := #{
                61 := #topic_partition_state{offset = 5},
                62 := #topic_partition_state{offset = 0}
            },
            <<"dogs">> := #{
                61 := #topic_partition_state{offset = 0},
                62 := #topic_partition_state{offset = 0}
            }
        },
        kafine_list_offsets_response:fold(ListOffsetsResponse, TopicPartitionStates)
    ),
    ok.
