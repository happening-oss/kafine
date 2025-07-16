-module(kafine_assignor).
-export_type([
    metadata/0,
    member/0,
    topic_partitions/0,
    assignments/0,
    member_assignment/0,
    user_data/0
]).

-callback name() -> binary().
-callback metadata(Topics :: [kafine:topic()]) -> metadata().

-type metadata() :: #{
    % The list of topics to which you want to subscribe.
    topics := [kafine:topic()],

    % The user_data in metadata is opaque to the broker. It can be used (for example) as a hint to the assignor about
    % assignment decisions. The example given in the Kafka docs suggests that you could include the member's rack ID and
    % have a rack-aware assignor (though this is covered by ConsumerProtocolSubscription v3, should we choose to support
    % it).
    user_data := user_data()
}.

% Assignor:assign is called by the leader to assign the group members to the specified topics and partitions.
-callback assign(
    Members :: [member()],
    TopicPartitions :: topic_partitions(),
    AssignmentUserData :: user_data()
) -> assignments().

-type member_id() :: binary().
-type member() :: #{
    member_id := member_id(), group_instance_id := binary() | null, metadata := metadata()
}.

-type topic_partitions() :: [#{name := kafine:topic(), partitions := [non_neg_integer()]}].

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

-type assigned_partitions() :: #{
    Topic :: binary() => [Partition :: non_neg_integer()]
}.
-type user_data() :: opaque_binary().
-type opaque_binary() :: binary().
