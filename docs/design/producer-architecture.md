# Producer Architecture

This document serves as a detailed description of how producing works in kafine

## Basic architecture

### Supervision tree

```
                                   ┌─────────────────────┐
                                   │                     │
         ┌─────────────────────────┤ kafine_producer_sup ├──────────────────────┐
         │                         │                     │                      │
         │                         └─┬─────────────────┬─┘                      │
         │                           │                 │                        │
         │                           │                 │                        │
         │                           │                 │                        │
┌────────┴─────────┐ ┌───────────────┴───────┐ ┌───────┴─────────┐ ┌────────────┴─────────────┐
│                  │ │                       │ │                 │ │                          │
│ kafine_bootstrap │ │ kafine_metadata_cache │ │ kafine_producer │ │ kafine_node_producer_sup │
│                  │ │                       │ │                 │ │                          │
└──────────────────┘ └───────────────────────┘ └─────────────────┘ └───────────┬┬┬────────────┘
                                                                               │││
                                                                               │││
                                                                    ┌──────────┴┴┴─────────┐
                                                                    │                      │
                                                                    │ kafine_node_producer │
                                                                    │                      │
                                                                    └──────────────────────┘
```

### kafine_producer and kafine_node_producer

`kafine_producer` is where the user interacts with the producer by calling `kafine_producer:produce` or `kafine_producer:produce_sync`. `produce` produces a single record to a specific topic partition.

The first time `kafine_producer` receives a message for a topic, it will query `kafine_metadata_cache` to find the leader for all partitions in that topic (it will also refresh leaders for all other topics it has seen at the same time).

When `kafine_producer` decides it's time to send messages, it will find all partitions for the leader it wishes to produce to, and group messages from those partitions into a single produce request. This request is sent to `kafine_node_producer`, which sends the message to the broker.

## Batching

### linger_ms

The `linger_ms` producer option is one of the primary mechanisms that determine the creation of batches. When `kafine_producer` is passed a message, if the topic-partition for that message has no queued messages, a timer with timeout `linger_ms` is started for that partition. When the linger timer expires, a batch will be gathered.

### max_batch_size_bytes

The `max_batch_size_bytes` producer is the other primary mechanism which results in batch creation. If adding a record to a batch would cause the uncompressed (TODO: compression estimates should factor in to this) encoded size of the batch to exceed this threshold, the a batch will be gathered.

Note that if a single message is produced which on its own would exceed this limit, then the message will be placed in a batch on its own. This means that a single large message can cause two batches to be produced - one with the batch previously being accumulated, followed by one with the large message.

### Batch gathering

When kafine receives a message through `produce`/`produce_sync`, it stores this message in a per-partition queue called the batch accumulator. The process of converting this queue into a batch is known as gathering a batch. Gathered batches are placed in a per-partition queue ready for encoding and sending.

After gathering a batch, the partition is checked to see if it is in the `ready` state, which means:
- It has no active produce requests in flight
- It is not backing off due to a failed retryable request

If this is the case, `kafine_producer` produce batches for the partition's leader.

## Producing

### Producing batches

When `kafine_producer` decides it is time to send a produce request to the broker, it will collect batches from all partitions for which that broker is leader. This is a two-step process:

1. Iterate over all topic-partitions for the leader, collecting the newest completed batch into the request
2. Iterate over topic-partitions which had no completed batches, but did have a batch in the process of being accumulated, gathering and collecting a batch from them

`kafine_producer` keeps track of the total uncompressed size of batches added to the request so far, and will stop collecting early if any batch would take this total over `max_request_size_bytes`.

To mitigate the risk of partitions starving other partitions, iteration of topics and partitions is started at a random point within the list. While this isn't perfectly fair in all scenarios, it does prevent partitions from being completely starved of producer access.

The collected request is then sent to `kafine_node_producer`. When this happens, all topic partitions in that request are marked as `busy`. This prevents any additional requests for those topic partition from being sent until a response arrives. TODO: The java client permits having multiple requests in flight for the same topic-partition, we should look to do the same.

`kafine_node_producer` encodes and sends the request to the broker, and returns the broker's response to `kafine_producer`. The broker responds per-topic-partition, so `kafine_node_producer` does the same. `kafine_node_producer` will return a partition response of `ok` if the producer returned no error, or `{error, {kafka_error, ERROR_CODE}}` if the producer returned an error.

### Retries

Batches will be automatically retried if the producer responds with an error code for that partition of:

- CORRUPT_MESSAGE
- LEADER_NOT_AVAILABLE
- NOT_LEADER_OR_FOLLOWER
- REQUEST_TIMED_OUT
- NOT_ENOUGH_REPLICAS
- NOT_ENOUGH_REPLICAS_AFTER_APPEND
- KAFKA_STORAGE_ERROR
- THROTTLING_QUOTA_EXCEEDED

If a batch for a partition is to be retried, the process is as follows:

1. Mark the failed topic-partitions as being in the `backoff` state. This prevents activity on other partitions from triggering batch gathers or produce requests from this partition's queued batches.
2. Add the failed batch(es) to the front of the batch queue for their respective topic-partitions. TODO: If/when we support multiple in-flight requests per partition, re-order things here. Idempotence requires sequence numbers, so arguably we should use those anyway.
3. If any error was NOT_LEADER_OR_FOLLOWER, refresh leaders for all known topics

Retries timing is governed by the `retry_backoff` option, and default to an exponential backoff starting at 100ms increasing to an maximum of 1s, with unlimited attempts.

Note that the first NOT_LEADER_OR_FOLLOWER error for a partition will not result in a backoff. Instead the request will be retried immediately after metadata is refreshed. If the partition immediately receives another NOT_LEADER_OR_FOLLOWER error then it will back off as normal.

### Produce after response

It is possible that one or more batches will be present in the queue for topic-partitions which have just received a response. As such, after handling a produce response, `kafine_producer` will immediately attempt to produce batches fot the leader again.

## Handling results

`kafine_producer` will return `ok` if an individual message produce succeeded, or `{error, Reason}` if it failed. If the failure was due to an error from the broker, `Reason` will be of the form `{kafka_error, ErrorCode}`..

If you used `kafine_producer:produce_sync`, the process will block until this result is ready, and it will be returned directly.

If you used `kafine_producer:produce`, you will have a req id collection. `kafine_producer` provides simple wrappers around `check_response` and `wait_response` in order to retrieve these responses.

It also provides `wait_all_responses`, which will block until the entire contents of a `req_id_collection` has received responses, or until some timeout has elapsed. Responses will be returned as a per-topic-partition list of tuples of `{Label, Result}` (where Label is the one provided to `produce`).

### Back-pressure

It is recommended that you do something that causes you to wait for responses to a message/batch of messages before sending another. Failure to do this will mean that the kafka broker will be unable to apply back-pressure through your application, and could result in messages accumulating in `kafine_producer` faster than they can be produced. There is no cap on the number of messages `kafine_producer` will attempt to queue.
