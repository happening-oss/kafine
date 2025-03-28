# Fetch starvation

It's _possible_ that, depending on broker implementation and load, fetch requests might see starvation of some
partitions in favour of others.

Specifically, for a particular consumer, `kafine_node_consumer` sends a single `Fetch` request for all of the topics and
partitions where that node is the leader (or read replica). The order in which the broker fulfils the topics and
partitions in the request is implementation-defined. If, for example, one of the partitions is particularly busy, it's
possible that the broker only ever responds with messages for that partition, and we never see messages for any of the
other partitions.

It _might_ be possible to shuffle the topics and partitions in the `Fetch` request. However, partitions are nested
inside topics, so I suspect that wouldn't be particularly fine-grained.

_If_ this kind of starvation becomes a problem, one possible solution (which we have not implemented) is to have more
than one `kafine_node_consumer` for each node, so that each `Fetch` request is fairer, for some definition of "fairer".
