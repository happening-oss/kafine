# Sticky Assignments

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

So we'd like some degree of "stickiness", where a member retains at least some of their partitions.

There are a number of different ways to do this. Some examples:

- Java's `StickyAssignor` reports its currently-assigned topics and partitions in the subscription user data, which is
  passed to the leader in `JoinGroup`. The leader can then use this information to ensure that to preserve existing
  assignments as much as possible.
- v1 and later of `ConsumerProtocolSubscription`, sent in `JoinGroup`, can also contain the existing assignments.
