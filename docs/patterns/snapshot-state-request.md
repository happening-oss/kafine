# The Snapshot/State/Request pattern

This is a pattern we often see with stateful services that use Kafka. The service holds some state, consumes a 'request'
topic, and replies based on the request and the stored state.

When the service starts up, it needs to rebuild its state, and would usually do this by consuming the 'state' topic from
the start, incrementally building its state.

This is _really_ slow, so we introduce a 'snapshot' topic.

Periodically, the service writes out its entire internal state (and the current 'state' topic offset) to the snapshot
topic.

Then, when starting up, the service can read the last message from the snapshot topic, restoring its internal state with
one message. Once it's caught up with the 'snapshot' topic, it resumes consuming the 'state' topic from the stored
offset. Once it's caught up with the 'state' topic, it has rebuilt its internal state, and can resume consuming the
'request' topic.
