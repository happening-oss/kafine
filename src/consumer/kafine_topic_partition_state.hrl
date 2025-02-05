-record(topic_partition_state, {
    offset :: kafine:offset(),
    state :: active | paused,
    client_pid :: pid()
}).
