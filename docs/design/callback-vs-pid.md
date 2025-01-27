# On whether to use a callback module or a process

## Currently: callback module

Currently, kafine requires that users provide a callback module (defined by the kafine_consumer_callback behaviour). On
each fetch, we pass over the returned responses and invoke callback functions as appropriate.

This has several advantages:

1. It's relatively easy to understand.
2. It provides flow control/back-pressure -- we don't issue another fetch until we've invoked the callback for the
   entire response.

It has a few disadvantages:

1. It's inflexible -- you MUST implement the callback, and you can't (easily) do this with a separate process (if you
   spawn and monitor extra processes, how would you be notified, for example?).
2. It requires careful thought about isolating errors in the callback (we currently don't).
3. If the callback takes a long time to process a message from a particular partition, all other partitions on the same
   broker will block until that's complete. This is because we batch up fetch requests to each broker.
4. We're responsible for holding onto the callback state, and we have to be careful to hand it off if a partition moves
   from one node to another.

## Possibly: a process

I've been considering using a callback process instead. There's prior art: this is what `brod` does. It has the
following advantages:

1. Process isolation is good.
2. It's more flexible. If you've got a separate process, you can do Erlang/OTP things in it. For example, you could
   drive a `gen_statem` from fetched messages. Of course, you could do _some_ of this with the callback anyway, but see the note above about monitoring processes.
3. We can potentially get some parallelism going, which is probably good.
4. We have less state to hold onto (it's just a pid), which is potentially cleaner from an introspection point of view.
   It also makes it easier to ellide sensitive information from the state (but there are other ways to do this).

Among possible disadvantages are:

1. A lack of flow control. Fetch responses will just pile up in the process message queue. Unless we implement flow
   control (of which more later).
2. The callback currently brackets each record batch, and tells you about each message. This allows you to opt to deal
   with individual messages _or_ batches. If we used a process, we'd have to choose one (probably batching), or make you
   declare which you preferred.
3. We'd need to be careful about sharing sub-binaries across the process boundary.

Options for the process are:
- Something ranch-like -- we spawn a process for each topic/partition. Or, more precisely, we ask _you_ to spawn one --
  this allows you to reuse one if you want to share the state.
- You simply provide a pid.

### Flow control

`brod` deals with this by requiring that the process acknowledge each batch. We could do something similar.

In fact, this leads to some "interesting" options.

- What if, for example, we allow more than one pending acknowledgement? We get pre-fetching "for free".
- We could, when we require an acknowledgement, temporarily drop that topic/partition from the next fetch request,
  allowing us to improve parallelism, without building a backlog of records to be processed.
