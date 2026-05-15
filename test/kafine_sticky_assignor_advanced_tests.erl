-module(kafine_sticky_assignor_advanced_tests).
-include_lib("eunit/include/eunit.hrl").

-define(REF, ?MODULE).
-define(CLUSTER_METADATA, kafine_cluster_metadata:from(?REF)).

-define(EMPTY_EXISTING_ASSIGNMENT_USER_DATA, <<>>).

-elvis([{elvis_style, macro_names, disable}]).

% -define(debug_balance(A), debug_balance(A, true)).
-define(debug_balance(A), debug_balance(A, false)).

add_remove_consumer_one_topic_test() ->
    Partitions = lists:seq(0, 6),
    TopicPartitions = #{<<"t0">> => Partitions},
    meck:expect(kafine_cluster_metadata, partitions, create_topic_partition_info(TopicPartitions)),

    Topics = maps:keys(TopicPartitions),

    % Single member.
    Member1 = create_member(1, Topics),
    Members0 = [Member1],
    Assignment0 = kafine_sticky_assignor:assign(
        Members0, ?CLUSTER_METADATA, ?EMPTY_EXISTING_ASSIGNMENT_USER_DATA
    ),

    ?assertEqual(
        #{
            <<"member-11223344-5566-7788-9900-000000000001">> =>
                #{
                    user_data => <<>>,
                    assigned_partitions => #{<<"t0">> => Partitions}
                }
        },
        ensure_assignments_ordering(Assignment0)
    ),

    % Add another member. This motivates stealing.
    Member2 = create_member(2, Topics),
    Members1 = [Member2 | update_members_from_assignment(Members0, Assignment0)],
    Assignment1 = kafine_sticky_assignor:assign(
        Members1, ?CLUSTER_METADATA, ?EMPTY_EXISTING_ASSIGNMENT_USER_DATA
    ),

    ?debug_balance(Assignment1),
    ?assert(is_balanced(Assignment1)),

    ?assertEqual(
        #{
            <<"member-11223344-5566-7788-9900-000000000001">> =>
                #{
                    user_data => <<>>,
                    assigned_partitions => #{<<"t0">> => [0, 1, 2, 3]}
                },
            <<"member-11223344-5566-7788-9900-000000000002">> =>
                #{
                    user_data => <<>>,
                    assigned_partitions => #{<<"t0">> => [4, 5, 6]}
                }
        },
        ensure_assignments_ordering(Assignment1)
    ),

    % Remove the first member.
    Members2 = remove_member(
        create_member_id(1), update_members_from_assignment(Members1, Assignment1)
    ),
    Assignment2 = kafine_sticky_assignor:assign(
        Members2, ?CLUSTER_METADATA, ?EMPTY_EXISTING_ASSIGNMENT_USER_DATA
    ),
    ?assertEqual(
        #{
            <<"member-11223344-5566-7788-9900-000000000002">> =>
                #{
                    user_data => <<>>,
                    assigned_partitions => #{<<"t0">> => [0, 1, 2, 3, 4, 5, 6]}
                }
        },
        ensure_assignments_ordering(Assignment2)
    ),
    ok.

remove_many_consumers_test() ->
    % Start with 10 partitions, 3 consumers.
    TopicPartitions = #{<<"t0">> => lists:seq(0, 9)},
    meck:expect(kafine_cluster_metadata, partitions, create_topic_partition_info(TopicPartitions)),

    Topics = maps:keys(TopicPartitions),

    Member1 = create_member(1, Topics),
    Member2 = create_member(2, Topics),
    Member3 = create_member(3, Topics),
    Members0 = [Member1, Member2, Member3],
    Assignment0 = kafine_sticky_assignor:assign(
        Members0, ?CLUSTER_METADATA, ?EMPTY_EXISTING_ASSIGNMENT_USER_DATA
    ),

    % We should be reasonably balanced. Ideally: 4, 3, 3.
    ?debug_balance(Assignment0),
    ?assert(is_balanced(Assignment0)),

    ?assertEqual(
        #{
            <<"member-11223344-5566-7788-9900-000000000001">> => 4,
            <<"member-11223344-5566-7788-9900-000000000002">> => 3,
            <<"member-11223344-5566-7788-9900-000000000003">> => 3
        },
        get_assignment_partition_counts(Assignment0)
    ),

    % Remove 1 of the consumers, such that we're slightly unbalanced. Ideally: 4, 3.
    Members1 = remove_member(
        create_member_id(3), update_members_from_assignment(Members0, Assignment0)
    ),
    Assignment1 = kafine_sticky_assignor:assign(
        Members1, ?CLUSTER_METADATA, ?EMPTY_EXISTING_ASSIGNMENT_USER_DATA
    ),

    % If the assignor isn't keeping track of counts while rebalacing, we'll get unbalanced. Probably: 7, 3.
    ?debug_balance(Assignment1),
    ?assert(is_balanced(Assignment1)),

    ?assertEqual(
        #{
            <<"member-11223344-5566-7788-9900-000000000001">> => 5,
            <<"member-11223344-5566-7788-9900-000000000002">> => 5
        },
        get_assignment_partition_counts(Assignment1)
    ),
    ok.

get_assignment_partition_counts(Assignment) ->
    maps:map(
        fun(_MemberId, #{assigned_partitions := AssignedPartitions}) ->
            maps:fold(
                fun(_Topic, Partitions, C) -> C + length(Partitions) end, 0, AssignedPartitions
            )
        end,
        Assignment
    ).

triangular_topic_partitions_test() ->
    % For some reason, as far as I can tell, the Java tests create t0 with 1 partition, t1 with 2, and so on. We'll do
    % the same.

    % I want to see whether we can assign those sensibly before we do anything else.
    TopicPartitions = #{create_topic_name(N) => lists:seq(0, N) || N <- lists:seq(0, 19)},
    meck:expect(kafine_cluster_metadata, partitions, create_topic_partition_info(TopicPartitions)),

    % There are 210 partitions, so there should be roughly 10 partitions per consumer.
    ?assertEqual(210, count_partitions(TopicPartitions)),

    Topics = maps:keys(TopicPartitions),
    Members0 = [
        create_member(N, Topics)
     || N <- lists:seq(0, 19)
    ],

    % We then run the assignor once.
    Assignment0 = kafine_sticky_assignor:assign(
        Members0, ?CLUSTER_METADATA, ?EMPTY_EXISTING_ASSIGNMENT_USER_DATA
    ),

    ?debug_balance(Assignment0),
    ?assert(is_balanced(Assignment0)),

    % Are there 10 or 11 partitions per consumer?
    maps:foreach(
        fun(_MemberId, #{assigned_partitions := AssignedPartitions}) ->
            Count = count_partitions(AssignedPartitions),
            ?assert(Count =:= 10 orelse Count =:= 11)
        end,
        Assignment0
    ),
    ok.

reassignment_after_one_consumer_leaves_test() ->
    % For some reason, as far as I can tell, the Java tests create t0 with 1 partition, t1 with 2, and so on. We'll do
    % the same.
    Size = 20,
    TopicPartitions = #{create_topic_name(N) => lists:seq(0, N) || N <- lists:seq(0, Size - 1)},
    meck:expect(kafine_cluster_metadata, partitions, create_topic_partition_info(TopicPartitions)),

    Topics = maps:keys(TopicPartitions),
    Members0 = [
        create_member(N, Topics)
     || N <- lists:seq(0, Size - 1)
    ],
    ?assertEqual(Size, length(Members0)),

    % We then run the assignor once.
    Assignment0 = kafine_sticky_assignor:assign(
        Members0, ?CLUSTER_METADATA, ?EMPTY_EXISTING_ASSIGNMENT_USER_DATA
    ),
    ?debug_balance(Assignment0),
    ?assert(is_balanced(Assignment0)),

    % Now, if we remove one of the consumers, _none_ of the other consumers should lose a partition, and they should all
    % get a fair share of the left-overs.
    Members1 = remove_member(
        create_member_id(Size div 2), update_members_from_assignment(Members0, Assignment0)
    ),
    ?assertEqual(Size - 1, length(Members1)),
    Assignment1 = kafine_sticky_assignor:assign(
        Members1, ?CLUSTER_METADATA, ?EMPTY_EXISTING_ASSIGNMENT_USER_DATA
    ),

    ?debug_balance(Assignment1),
    ?assert(is_balanced(Assignment1)),

    % Because a member left, every surviving member should still have the assignments they already had.
    maps:foreach(
        fun(MemberId, #{assigned_partitions := Partitions1}) ->
            % For each member, its assigned partitions should be a superset of its previous assigned partitions.
            #{assigned_partitions := Partitions0} = maps:get(MemberId, Assignment0),
            TopicPartitions0 = sets:from_list(flatten_partitions(Partitions0)),
            TopicPartitions1 = sets:from_list(flatten_partitions(Partitions1)),

            % A ⊆ B iff (A ∩ B) ≡ A
            ?assertEqual(
                TopicPartitions0,
                sets:intersection(TopicPartitions0, TopicPartitions1)
            )
        end,
        Assignment1
    ),
    ok.

% Apply 'Assignment' to 'Members' so that we can run the assignor again with members that have their new assignments.
update_members_from_assignment(Members, Assignment) when is_list(Members), is_map(Assignment) ->
    maps:fold(
        fun(
            MemberId,
            #{assigned_partitions := AssignedPartitions, user_data := _AssignmentUserData},
            Acc
        ) ->
            lists:map(
                fun
                    (Member = #{member_id := M, metadata := SubscriptionMetadata0}) when
                        M =:= MemberId
                    ->
                        OwnedPartitions = lists:sort(
                            [
                                #{topic => Topic, partitions => Partitions}
                             || Topic := Partitions <- AssignedPartitions
                            ]
                        ),
                        SubscriptionMetadata = SubscriptionMetadata0#{
                            owned_partitions => OwnedPartitions
                        },
                        Member#{metadata := SubscriptionMetadata};
                    (Member) ->
                        Member
                end,
                Acc
            )
        end,
        Members,
        Assignment
    ).

remove_member(MemberId, Members) ->
    [M || M = #{member_id := Id} <- Members, Id /= MemberId].

create_topic_name(T) when is_integer(T) ->
    iolist_to_binary(io_lib:format("t~2..0B", [T])).

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

flatten_partitions(TopicPartitions) ->
    kafine_assignor_tests:flatten_partitions(TopicPartitions).

count_partitions(TopicPartitions) ->
    kafine_assignor_tests:count_partitions(TopicPartitions).

ensure_assignments_ordering(Assignments) ->
    kafine_assignor_tests:ensure_assignments_ordering(Assignments).

debug_balance(Assignment, true) ->
    maps:foreach(
        fun(MemberId, #{assigned_partitions := AssignedPartitions}) ->
            ?debugFmt("~p has ~B partitions", [
                MemberId, kafine_assignor_tests:count_partitions(AssignedPartitions)
            ])
        end,
        Assignment
    );
debug_balance(_Assignment, false) ->
    ok.
