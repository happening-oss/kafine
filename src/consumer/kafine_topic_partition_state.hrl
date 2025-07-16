-record(topic_partition_state, {
    offset :: kafine:offset(),
    state :: active | paused | busy,
    client_pid :: pid()
}).
