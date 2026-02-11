-module(kafine_range_assignor_tests).
-include_lib("eunit/include/eunit.hrl").

one_consumer_no_topic_test() ->
    M1 = create_member_id(1),
    Members = [
        #{
            member_id => M1,
            group_instance_id => null,
            metadata => kafine_range_assignor:metadata([])
        }
    ],
    TopicPartitions = #{},
    Assignment = kafine_range_assignor:assign(Members, TopicPartitions, <<>>),
    ?assertEqual(#{M1 => #{assigned_partitions => #{}, user_data => <<>>}}, Assignment),
    ok.

one_consumer_one_topic_test() ->
    M1 = create_member_id(1),
    Members = [
        #{
            member_id => M1,
            group_instance_id => null,
            metadata => kafine_range_assignor:metadata([<<"topic-a">>])
        }
    ],
    TopicPartitions = #{<<"topic-a">> => [0, 1, 2]},
    Assignment = kafine_range_assignor:assign(Members, TopicPartitions, <<>>),
    ?assertEqual(
        #{M1 => #{assigned_partitions => #{<<"topic-a">> => [0, 1, 2]}, user_data => <<>>}},
        Assignment
    ),
    ok.

one_consumer_multiple_topics_test() ->
    M1 = create_member_id(1),
    Members = [
        #{
            metadata => kafine_range_assignor:metadata([<<"topic-a">>, <<"topic-b">>]),
            member_id => M1,
            group_instance_id => null
        }
    ],
    TopicPartitions = #{
        <<"topic-a">> => [0],
        <<"topic-b">> => [0, 1]
    },
    Assignment = kafine_range_assignor:assign(Members, TopicPartitions, <<>>),
    ?assertEqual(
        #{
            M1 => #{
                assigned_partitions => #{
                    <<"topic-a">> => [0],
                    <<"topic-b">> => [0, 1]
                },
                user_data => <<>>
            }
        },
        Assignment
    ),
    ok.

two_consumers_one_topic_one_partition_test() ->
    M1 = create_member_id(1),
    M2 = create_member_id(2),
    Members = [
        #{
            metadata => kafine_range_assignor:metadata([<<"topic-a">>]),
            member_id => M,
            group_instance_id => null
        }
     || M <- [M1, M2]
    ],
    TopicPartitions = #{<<"topic-a">> => [0]},
    Assignment = kafine_range_assignor:assign(Members, TopicPartitions, <<>>),
    ?assertEqual(
        #{
            M1 => #{assigned_partitions => #{<<"topic-a">> => [0]}, user_data => <<>>},
            M2 => #{assigned_partitions => #{<<"topic-a">> => []}, user_data => <<>>}
        },
        Assignment
    ),
    ok.

two_consumers_one_topic_two_partitions_test() ->
    M1 = create_member_id(1),
    M2 = create_member_id(2),
    Members = [
        #{
            metadata => kafine_range_assignor:metadata([<<"topic-a">>]),
            member_id => M,
            group_instance_id => null
        }
     || M <- [M1, M2]
    ],
    TopicPartitions = #{<<"topic-a">> => [0, 1]},
    Assignment = kafine_range_assignor:assign(Members, TopicPartitions, <<>>),
    ?assertEqual(
        #{
            M1 => #{assigned_partitions => #{<<"topic-a">> => [0]}, user_data => <<>>},
            M2 => #{assigned_partitions => #{<<"topic-a">> => [1]}, user_data => <<>>}
        },
        Assignment
    ),
    ok.

two_consumers_two_topics_six_partitions_test() ->
    M1 = create_member_id(1),
    M2 = create_member_id(2),
    Members = [
        #{
            metadata => kafine_range_assignor:metadata([<<"topic-a">>, <<"topic-b">>]),
            member_id => M,
            group_instance_id => null
        }
     || M <- [M1, M2]
    ],
    TopicPartitions = #{
        <<"topic-a">> => [0, 1, 2, 3, 4, 5],
        <<"topic-b">> => [0, 1, 2, 3, 4, 5]
    },
    Assignment = kafine_range_assignor:assign(Members, TopicPartitions, <<>>),
    ?assertEqual(
        #{
            M1 => #{
                assigned_partitions => #{<<"topic-a">> => [0, 1, 2], <<"topic-b">> => [0, 1, 2]},
                user_data => <<>>
            },
            M2 => #{
                assigned_partitions => #{<<"topic-a">> => [3, 4, 5], <<"topic-b">> => [3, 4, 5]},
                user_data => <<>>
            }
        },
        Assignment
    ),
    ok.

multiple_consumers_mixed_topics_test() ->
    Members = [
        create_member(1, [<<"topic-a">>]),
        create_member(2, [<<"topic-a">>, <<"topic-b">>]),
        create_member(3, [<<"topic-a">>])
    ],
    [M1, M2, M3] = [M || #{member_id := M} <- Members],
    TopicPartitions = #{
        <<"topic-a">> => [0, 1, 2],
        <<"topic-b">> => [0, 1]
    },
    Assignment = kafine_range_assignor:assign(Members, TopicPartitions, <<>>),
    ?assertEqual(
        #{
            M1 =>
                #{assigned_partitions => #{<<"topic-a">> => [0]}, user_data => <<>>},
            M2 =>
                #{
                    assigned_partitions =>
                        #{
                            <<"topic-a">> => [1],
                            <<"topic-b">> => [0, 1]
                        },
                    user_data => <<>>
                },
            M3 =>
                #{assigned_partitions => #{<<"topic-a">> => [2]}, user_data => <<>>}
        },
        Assignment
    ),
    ok.

multiple_consumers_mixed_topics_2_test() ->
    Members = [
        create_member(1, [<<"topic-a">>, <<"topic-b">>]),
        create_member(2, [<<"topic-a">>, <<"topic-c">>])
    ],
    [M1, M2] = [M || #{member_id := M} <- Members],
    TopicPartitions = #{
        <<"topic-a">> => [0, 1, 2],
        <<"topic-b">> => [0, 1, 2],
        <<"topic-c">> => [0, 1, 2]
    },
    Assignment = kafine_range_assignor:assign(Members, TopicPartitions, <<>>),

    % M1 gets half of topic-a, all of topic-b; M2 gets the other half of topic-a, all of topic-c.
    ?assertEqual(
        #{
            M1 =>
                #{
                    assigned_partitions => #{
                        <<"topic-a">> => [0, 1],
                        <<"topic-b">> => [0, 1, 2]
                    },
                    user_data => <<>>
                },
            M2 =>
                #{
                    assigned_partitions =>
                        #{
                            <<"topic-a">> => [2],
                            <<"topic-c">> => [0, 1, 2]
                        },
                    user_data => <<>>
                }
        },
        Assignment
    ),
    ok.

multiple_consumers_unwanted_topics_test() ->
    % Question: what if none of the consumers express an interest in one of the topics?
    % Answer: it'll fail when attempting to share the partitions between zero interested consumers.
    %
    % Don't do that.
    % Make sure that the TopicPartitions passed to 'assign' only contains topics that the consumers ask for.
    %
    % This test is a placeholder, so I can hang this comment somewhere.
    ok.

create_member_id(Index) ->
    create_member_id(<<"member">>, Index).

create_member_id(Prefix, Index) when is_binary(Prefix), is_integer(Index) ->
    % Member IDs are usually <prefix>-<uuid>, but we need them to be deterministic for tests.
    Fixed = <<"11223344-5566-7788-9900">>,
    iolist_to_binary(io_lib:format("~s-~s-~12..0B", [Prefix, Fixed, Index])).

create_member(Index, Topics) ->
    #{
        member_id => create_member_id(Index),
        metadata => kafine_range_assignor:metadata(Topics),
        group_instance_id => null
    }.
