-record(topic_partition_state, {
    offset :: kafine:offset(),
    state :: active | paused,
    state_data :: term()
}).
