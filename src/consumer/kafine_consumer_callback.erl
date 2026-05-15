-module(kafine_consumer_callback).

% kafine_consumer_callback defines the behaviour for the callback module that you provide to kafine_consumer.

% init/3 is called to initialise your callback. It's called for each (topic, partition).
-callback init(Topic :: kafine:topic(), Partition :: kafine:partition(), Args :: term()) ->
    {ok, State :: term()} | {pause, State2 :: term()}.

% handle_partition_data is called to notify you when some records are fetched.
%
% Use the functions in the 'kafine_partition_data' module, such as kafine_partition_data:iterator/1 and
% kafine_partition_data:next/1, to process the partition data.
-callback handle_partition_data(
    Topic :: kafine:topic(),
    Partition :: kafine:partition(),
    PartitionData :: kafine_partition_data:partition_data(),
    State :: term()
) ->
    {ok, NewState :: term()}.
