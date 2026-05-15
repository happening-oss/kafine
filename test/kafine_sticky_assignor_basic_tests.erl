-module(kafine_sticky_assignor_basic_tests).
-include_lib("eunit/include/eunit.hrl").

% When there are no previous assignments (owned_partitions), the sticky assignor behaves the same as the round-robin
% assignor.

-define(REF, ?MODULE).
-define(CLUSTER_METADATA, kafine_cluster_metadata:from(?REF)).

-define(EMPTY_EXISTING_ASSIGNMENT_USER_DATA, <<>>).

name_test() ->
    ?assertEqual(<<"sticky">>, kafine_sticky_assignor:name()).

one_consumer_no_topic_test() ->
    M1 = create_member_id(1),
    Members = [
        #{
            member_id => M1,
            group_instance_id => null,
            metadata => create_subscription_metadata([])
        }
    ],

    TopicPartitions = #{},
    meck:expect(kafine_cluster_metadata, partitions, create_topic_partition_info(TopicPartitions)),

    Assignment = kafine_sticky_assignor:assign(
        Members, ?CLUSTER_METADATA, ?EMPTY_EXISTING_ASSIGNMENT_USER_DATA
    ),
    ?assertEqual(
        #{M1 => #{assigned_partitions => #{}, user_data => <<>>}},
        ensure_assignments_ordering(Assignment)
    ),
    ?assert(is_balanced(Assignment)),
    ok.

one_consumer_nonexistent_topic_test() ->
    M1 = create_member_id(1),
    Members = [
        #{
            member_id => M1,
            group_instance_id => null,
            metadata => create_subscription_metadata([<<"topic-a">>])
        }
    ],

    TopicPartitions = #{},
    meck:expect(kafine_cluster_metadata, partitions, create_topic_partition_info(TopicPartitions)),

    Assignment = kafine_sticky_assignor:assign(
        Members, ?CLUSTER_METADATA, ?EMPTY_EXISTING_ASSIGNMENT_USER_DATA
    ),
    ?assertEqual(
        #{M1 => #{assigned_partitions => #{}, user_data => <<>>}},
        ensure_assignments_ordering(Assignment)
    ),
    ?assert(is_balanced(Assignment)),
    ok.

one_consumer_one_topic_test() ->
    M1 = create_member_id(1),
    Members = [
        #{
            member_id => M1,
            group_instance_id => null,
            metadata => create_subscription_metadata([<<"topic-a">>])
        }
    ],

    TopicPartitions = #{<<"topic-a">> => [0, 1, 2]},
    meck:expect(kafine_cluster_metadata, partitions, create_topic_partition_info(TopicPartitions)),

    Assignment = kafine_sticky_assignor:assign(
        Members, ?CLUSTER_METADATA, ?EMPTY_EXISTING_ASSIGNMENT_USER_DATA
    ),
    ?assertEqual(
        #{M1 => #{assigned_partitions => #{<<"topic-a">> => [0, 1, 2]}, user_data => <<>>}},
        ensure_assignments_ordering(Assignment)
    ),
    ?assert(is_balanced(Assignment)),
    ok.

only_assigns_partitions_from_subscribed_topics_test() ->
    M1 = create_member_id(1),
    Members = [
        #{
            member_id => M1,
            group_instance_id => null,
            metadata => create_subscription_metadata([<<"topic-a">>])
        }
    ],

    TopicPartitions = #{
        <<"topic-a">> => [0, 1, 2],
        <<"other">> => [0, 1, 2]
    },
    meck:expect(kafine_cluster_metadata, partitions, create_topic_partition_info(TopicPartitions)),

    Assignment = kafine_sticky_assignor:assign(
        Members, ?CLUSTER_METADATA, ?EMPTY_EXISTING_ASSIGNMENT_USER_DATA
    ),
    ?assertEqual(
        #{M1 => #{assigned_partitions => #{<<"topic-a">> => [0, 1, 2]}, user_data => <<>>}},
        ensure_assignments_ordering(Assignment)
    ),
    ?assert(is_balanced(Assignment)),
    ok.

one_consumer_multiple_topics_test() ->
    M1 = create_member_id(1),
    Members = [
        #{
            metadata => create_subscription_metadata([<<"topic-a">>, <<"topic-b">>]),
            member_id => M1,
            group_instance_id => null
        }
    ],

    TopicPartitions = #{
        <<"topic-a">> => [0],
        <<"topic-b">> => [0, 1]
    },
    meck:expect(kafine_cluster_metadata, partitions, create_topic_partition_info(TopicPartitions)),

    Assignment = kafine_sticky_assignor:assign(
        Members, ?CLUSTER_METADATA, ?EMPTY_EXISTING_ASSIGNMENT_USER_DATA
    ),
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
        ensure_assignments_ordering(Assignment)
    ),
    ?assert(is_balanced(Assignment)),
    ok.

two_consumers_one_topic_one_partition_test() ->
    M1 = create_member_id(1),
    M2 = create_member_id(2),
    Members = [
        #{
            metadata => create_subscription_metadata([<<"topic-a">>]),
            member_id => M,
            group_instance_id => null
        }
     || M <- [M1, M2]
    ],

    TopicPartitions = #{<<"topic-a">> => [0]},
    meck:expect(kafine_cluster_metadata, partitions, create_topic_partition_info(TopicPartitions)),

    Assignment = kafine_sticky_assignor:assign(
        Members, ?CLUSTER_METADATA, ?EMPTY_EXISTING_ASSIGNMENT_USER_DATA
    ),
    ?assertEqual(
        #{
            M1 => #{assigned_partitions => #{<<"topic-a">> => [0]}, user_data => <<>>},
            M2 => #{assigned_partitions => #{}, user_data => <<>>}
        },
        ensure_assignments_ordering(Assignment)
    ),
    ?assert(is_balanced(Assignment)),
    ok.

two_consumers_one_topic_two_partitions_test() ->
    M1 = create_member_id(1),
    M2 = create_member_id(2),
    Members = [
        #{
            metadata => create_subscription_metadata([<<"topic-a">>]),
            member_id => M,
            group_instance_id => null
        }
     || M <- [M1, M2]
    ],

    TopicPartitions = #{<<"topic-a">> => [0, 1]},
    meck:expect(kafine_cluster_metadata, partitions, create_topic_partition_info(TopicPartitions)),

    Assignment = kafine_sticky_assignor:assign(
        Members, ?CLUSTER_METADATA, ?EMPTY_EXISTING_ASSIGNMENT_USER_DATA
    ),
    ?assertEqual(
        #{
            M1 => #{assigned_partitions => #{<<"topic-a">> => [0]}, user_data => <<>>},
            M2 => #{assigned_partitions => #{<<"topic-a">> => [1]}, user_data => <<>>}
        },
        Assignment
    ),
    ?assert(is_balanced(Assignment)),
    ok.

two_consumers_two_topics_six_partitions_test() ->
    % Alternative way to build the member list.
    Members = [
        create_member(1, [<<"topic-a">>, <<"topic-b">>]),
        create_member(2, [<<"topic-a">>, <<"topic-b">>])
    ],
    % ...but we have to get the IDs back out like this:
    [M1, M2] = [M || #{member_id := M} <- Members],

    TopicPartitions = #{
        <<"topic-a">> => [0, 1, 2, 3, 4, 5],
        <<"topic-b">> => [0, 1, 2, 3, 4, 5]
    },
    meck:expect(kafine_cluster_metadata, partitions, create_topic_partition_info(TopicPartitions)),

    Assignment = kafine_sticky_assignor:assign(
        Members, ?CLUSTER_METADATA, ?EMPTY_EXISTING_ASSIGNMENT_USER_DATA
    ),
    ?assertEqual(
        #{
            M1 => #{
                assigned_partitions => #{
                    <<"topic-a">> => [0, 2, 4],
                    <<"topic-b">> => [0, 2, 4]
                },
                user_data => <<>>
            },
            M2 => #{
                assigned_partitions => #{
                    <<"topic-a">> => [1, 3, 5],
                    <<"topic-b">> => [1, 3, 5]
                },
                user_data => <<>>
            }
        },
        ensure_assignments_ordering(Assignment)
    ),
    ?assert(is_balanced(Assignment)),
    ok.

multiple_consumers_mixed_topics_test() ->
    % Alternative way to build the member list.
    Members = [
        create_member(1, [<<"topic-a">>]),
        create_member(2, [<<"topic-a">>, <<"topic-b">>]),
        create_member(3, [<<"topic-a">>])
    ],
    % ...but we have to get the IDs back out like this:
    [M1, M2, M3] = [M || #{member_id := M} <- Members],

    TopicPartitions = #{
        <<"topic-a">> => [0, 1, 2],
        <<"topic-b">> => [0, 1]
    },
    meck:expect(kafine_cluster_metadata, partitions, create_topic_partition_info(TopicPartitions)),

    Assignment = kafine_sticky_assignor:assign(
        Members, ?CLUSTER_METADATA, ?EMPTY_EXISTING_ASSIGNMENT_USER_DATA
    ),
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
        ensure_assignments_ordering(Assignment)
    ),
    % We don't assert that it's balanced, 'cos it's not.
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
    meck:expect(kafine_cluster_metadata, partitions, create_topic_partition_info(TopicPartitions)),

    Assignment = kafine_sticky_assignor:assign(
        Members, ?CLUSTER_METADATA, ?EMPTY_EXISTING_ASSIGNMENT_USER_DATA
    ),

    % M1 gets half of topic-a, all of topic-b; M2 gets the other half of topic-a, all of topic-c.
    ?assertEqual(
        #{
            M1 =>
                #{
                    assigned_partitions => #{
                        <<"topic-a">> => [0, 2],
                        <<"topic-b">> => [0, 1, 2]
                    },
                    user_data => <<>>
                },
            M2 =>
                #{
                    assigned_partitions =>
                        #{
                            <<"topic-a">> => [1],
                            <<"topic-c">> => [0, 1, 2]
                        },
                    user_data => <<>>
                }
        },
        ensure_assignments_ordering(Assignment)
    ),
    ?assert(is_balanced(Assignment)),
    ok.

multiple_consumers_unwanted_topics_test() ->
    Members = [
        create_member(1, [<<"topic-a">>]),
        create_member(2, [<<"topic-a">>])
    ],
    [M1, M2] = [M || #{member_id := M} <- Members],

    TopicPartitions = #{
        <<"topic-a">> => [0, 1, 2],
        <<"other">> => [0, 1]
    },
    meck:expect(kafine_cluster_metadata, partitions, create_topic_partition_info(TopicPartitions)),

    Assignment = kafine_sticky_assignor:assign(
        Members, ?CLUSTER_METADATA, ?EMPTY_EXISTING_ASSIGNMENT_USER_DATA
    ),
    ?assertEqual(
        #{
            M1 =>
                #{assigned_partitions => #{<<"topic-a">> => [0, 2]}, user_data => <<>>},
            M2 =>
                #{assigned_partitions => #{<<"topic-a">> => [1]}, user_data => <<>>}
        },
        ensure_assignments_ordering(Assignment)
    ),
    ?assert(is_balanced(Assignment)),
    ok.

create_member_id(Index) ->
    kafine_assignor_tests:create_member_id(Index).

create_member(Index, Topics) when is_integer(Index) ->
    MemberId = create_member_id(Index),
    create_member(MemberId, Topics);
create_member(MemberId, Topics) when is_binary(MemberId) ->
    #{
        member_id => MemberId,
        metadata => create_subscription_metadata(Topics),
        group_instance_id => null
    }.

create_subscription_metadata(Topics) ->
    #{topics => Topics, user_data => <<>>, owned_partitions => []}.

create_topic_partition_info(TopicPartitions) ->
    kafine_assignor_tests:create_topic_partition_info(TopicPartitions).

is_balanced(Assignment) ->
    kafine_assignor_tests:is_balanced(Assignment).

ensure_assignments_ordering(Assignments) ->
    kafine_assignor_tests:ensure_assignments_ordering(Assignments).
