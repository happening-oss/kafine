# Use of user data in group rebalancing

Read [client-side-assignment](client-side-assignment.md) first.

## User Data

The user data in both the subscription and assignment objects is opaque to the broker; it only means anything to the
consumers.

In the `JoinGroup` / `SyncGroup` protocol used by Kafka when performing client-side assignment, there are two things
called `user_data`. They're _not_ the same:

1. In the `JoinGroup` request, the member includes data to be passed to the leader. Let's call this *subscription user
   data*.

2. The leader receives this data for each member in the `JoinGroup` response. This subscription user data can be used as
   a hint to the leader when it's assigning topics and partitions. You might, for example, include some metrics (CPU
   usage, memory usage, etc.), which could hint to the leader that it should assign more/fewer partitions to this
   member.

3. In the `SyncGroup` request, the leader includes data to be passed to each member. Let's call this the *assignment
   user data*. This doesn't have to be related to the subscription user data. It could be used, for example, to
   implement "warm-spare" scaling: you could give the same topics and partitions to more than one group member, but use
   the user data to tell the "spares" to _only_ catch up with events, and _not_ to react to them.

4. In the `SyncGroup` response, each member receives the assignment user data from the leader.
