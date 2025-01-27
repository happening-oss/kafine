# Fetch Architecture

I considered a bunch of different architectures for Kafine. Here are some notes on the pros/cons.

---

Kafire implements "fetchers", where a single Erlang process sits in a loop, repeatedly fetching a single topic/partition
(TODO: Check this in the face of multiple topics). Connections are pooled, and there's one pool per broker. Every
request takes a connection from the relevant pool, uses it and then releases it.

The model's simpler (but the code, not so much), and has a few nice features:

1. One partition can't be blocked by another, unless there's a problem with the connection pool, or the network, or the broker.
2. TODO. There have to be others :)

But, downsides:

1. Lots of active connections to the broker, which increases resource use.
2. Multiple small requests to the broker; this also increases resource use.

---

The model currently in use in Kafine uses a consumer-per-node, which batches together all of the topics and partitions
on a particular node (either because it's the leader, or because it's a preferred replica -- which we haven't
implemented yet).

This means:

1. Fewer connections to the broker -- specifically one per broker, plus one for the controller and (if we're doing group
   consumers) one to the group coordinator.
2. Fewer, larger, requests.

The idea is that this reduces broker load.

Downsides:

1. Might result in starvation of particular topics and partitions, depending on how the broker deals with the batching.
2. Less parallelism, at least using a callback module (as we currently do).

---

One other option I've considered is a hybrid of the two above. If we had a bunch of "fetchers", which would generate
fetch requests, the _requests_ could be gathered by the per-node workers before sending a single combined fetch request.

I didn't explore this too much, since I'd already implemented most of the current model, and it seemed like there
wouldn't be enough advantages in changing. We might revisit that decision as we get more use out of Kafine.
