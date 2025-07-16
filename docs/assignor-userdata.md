# Use of user data in group rebalancing

Read [client-side-assignment](client-side-assignment.md) first.

## User Data

The user data in both the subscription and assignment objects is opaque to the broker; it only means anything to the
consumers.

In the `JoinGroup` / `SyncGroup` protocol used by Kafka when performing client-side assignment, there are two things
called `user_data`. They're _not_ the same; this is confusing.

## MemberMetadata (Consumer Protocol Subscription)

In the `JoinGroup` request, each member includes its own `MemberMetadata` object (also known as
`ConsumerProtocolSubscription`). It contains the list of topics that the member wants to consume from, and an opaque
`user_data` field.

When the leader gets the `JoinGroup` response, it includes the metadata/subscription object for all of the members, and
it can use the desired topics and the user data to guide its behaviour.

The user data in the metadata/subscription is provided by each member and can be used as a hint to the leader when it's
assigning topics and partitions.

For example, the Kafka documentation for
[ConsumerPartitionAssignor](https://kafka.apache.org/28/javadoc/org/apache/kafka/clients/consumer/ConsumerPartitionAssignor.html)
suggests that you might use this to implement a rack-aware assignor.

Note that, since version 3 (we're still using version 0), `ConsumerProtocolSubscription` contains the rack hint, so the
above is less relevant.

You might, for example, include some metrics (CPU usage, memory usage, etc.), which could hint to the leader that it
should assign more/fewer partitions to this member.

## MemberState (Consumer Protocol Assignment)

Once the leader has assigned topics and partitions to the group members, it sends a `SyncGroup` request to the
coordinator. This request contains a list of `(MemberId, ConsumerProtocolAssignment)` tuples. The
`ConsumerProtocolAssignment` object is a list of topics and partitions, and it also contains an opaque `user_data`
field.

This is _not_ the same user data as in the metadata. It's sent from the leader to each member (and it can be
different for each member).

It could be used, for example, to implement "warm-spare" scaling: you could give the same topics and partitions to more
than one group member, but use the user data to tell the "spares" to _only_ catch up with events, and _not_ to react to
them.

## Sticky Assignments

Another use for the assignment metadata might be for stickiness.

When a group rebalance occurs, if all members lose all of their assigned partitions, there will be quite a lot of churn
if they're stateful. To avoid this churn, we'd like to make sure that members keep most of their previously-assigned
partitions.

For example, if we have two members and twelve partitions, assigned such that:

- `M1: P0, P1, P2, P3, P4, P5`
- `M2: P6, P7, P8, P9, P10, P11`

Another member -- `M3` -- joins the group, and a particularly stupid assignor implementation could result in:

- `M1: P8, P9, P10, P11`
- `M2: P0, P1, P2, P3`
- `M3: P4, P5, P6, P7`.

This is obviously a degenerate case, where no member retained any of their previously-assigned partitions.

So we'd like some degree of "stickiness", where a member retains at least some of their partitions. This means that we
need to retain state between rebalance events.

If the coordinator consistently elects the same leader (and it currently tends to, but this is not guaranteed), the
leader could just retain the state, but this doesn't help if the coordinator elects a new leader, or if the leader dies
and is forced to leave the group.

In this situation, it would be useful for all of the members to have a copy of the complete assignment, with both their
assignments and their peers'.

We can use the assignment `user_data` for this. If the leader includes a copy of the existing assignments in the
returned `user_data`, then every member has a copy. If a member then becomes the new leader, it can use this user data
to implement the stickiness that we're interested in.
