-module(kafine_assignor).
-export([get_topic_partitions/2]).

-export_type([
    metadata/0,
    member/0,
    assignments/0,
    member_assignment/0,
    user_data/0
]).

-callback name() -> binary().

% Assignor:assign is called by the leader to assign the group members to the specified topics and partitions.
-callback assign(
    % Members is a list of member-subscription items.
    Members :: [member()],
    % ClusterMetadata allows the assignor to inspect the cluster.
    ClusterMetadata :: kafine_cluster_metadata:t(),
    % AssignmentUserData is the user_data from the previous rebalance, if any.
    AssignmentUserData :: user_data()
) -> assignments().

-type member_id() :: binary().
-type metadata() ::
    consumer_protocol_subscription:consumer_protocol_subscription_0()
    | consumer_protocol_subscription:consumer_protocol_subscription_1()
    | consumer_protocol_subscription:consumer_protocol_subscription_2()
    | consumer_protocol_subscription:consumer_protocol_subscription_3().

-type member() :: #{
    member_id := member_id(), group_instance_id := binary() | null, metadata := metadata()
}.

-type assignments() :: #{
    member_id() => member_assignment()
}.
-type member_assignment() :: #{
    % The list of topics and partitions assigned to this member.
    assigned_partitions := assigned_partitions(),

    % The user_data to be sent to this member. For example, it could be used for hot-spares: to tell them which
    % partitions to follow (but not act on), or it could be used to replicate the information needed for stickiness to
    % all members.
    user_data := user_data()
}.

-type assigned_partitions() :: kafine_topic_partitions:t().
-type user_data() :: opaque_binary() | null.
-type opaque_binary() :: binary().

get_topic_partitions(Members, ClusterMetadata) ->
    Topics = lists:foldl(
        fun(_M = #{metadata := #{topics := Topics}}, Acc) ->
            sets:union(Acc, sets:from_list(Topics))
        end,
        sets:new(),
        Members
    ),
    TopicPartitionInfo = kafine_cluster_metadata:partitions(ClusterMetadata, sets:to_list(Topics)),
    maps:map(
        fun(_Topic, Partitions) -> maps:keys(Partitions) end, TopicPartitionInfo
    ).
