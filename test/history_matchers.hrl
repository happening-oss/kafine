% macros for assertMatch with meck:history.
-define(init_callback(Topic, Partition, Args),
    {_,
        {_, init, [
            Topic,
            Partition,
            Args
        ]},
        _}
).

-define(handle_partition_data(
    Topic, Partition, PartitionData, State
),
    {_,
        {_, handle_partition_data, [
            Topic,
            Partition,
            PartitionData,
            State
        ]},
        _}
).
