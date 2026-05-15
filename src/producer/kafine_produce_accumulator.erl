-module(kafine_produce_accumulator).
-moduledoc false.

-export([
    init/2,
    append/5,
    gather_batch/3,
    collect_request/3,
    requeue_batches/2,
    info/1
]).

-ifdef(TEST).
-export([
    fair_reduce_while/3
]).
-endif.

-export_type([
    state/0,
    stats/0,
    info/0
]).

-include_lib("kernel/include/logger.hrl").
-include("../kafine_eqwalizer.hrl").

-record(batch, {
    messages :: [{kafine_producer:message(), gen_statem:from()}],
    size :: non_neg_integer(),
    length :: non_neg_integer(),
    % erlang:monotonic_time(microsecond) when the first message in the batch was passed to append
    init_timestamp :: non_neg_integer(),
    % erlang:monotonic_time(microsecond) when the batch was added to the partition's queued_batches
    enqueue_timestamp :: non_neg_integer()
}).

-type batch() :: #batch{}.

-record(partition_data, {
    batch_acc = [] :: [{kafine_producer:message(), gen_statem:from()}],
    batch_size_acc = kafcod_message_set:uncompressed_size_init() :: kafcod_message_set:uncompressed_size_acc(),
    batch_len = 0 :: non_neg_integer(),
    batch_init_timestamp = undefined :: non_neg_integer() | undefined,
    queued_batches = queue:new() :: queue:queue(Batch :: batch())
}).

-type partition_data() :: #partition_data{}.

-record(state, {
    options :: kafine:producer_options(),
    metadata :: telemetry:event_metadata(),
    partitions = kafine_topic_partition_data:new() :: kafine_topic_partition_data:t(
        partition_data()
    )
}).

-opaque state() :: #state{}.

-spec init(Options :: kafine:producer_options(), TelemetryMetadata :: telemetry:event_metadata()) ->
    state().

init(Options, TelemetryMetadata) ->
    #state{
        options = Options,
        metadata = TelemetryMetadata
    }.

-spec append(
    Topic :: kafine:topic(),
    Partition :: kafine:partition(),
    Message :: kafine_producer:message(),
    From :: gen_statem:from(),
    State :: state()
) ->
    {ok, NewBatch :: boolean(), BatchCompleted :: boolean(), NewState :: state()}
    | {error, Reason :: term()}.

append(
    Topic,
    Partition,
    Message,
    From,
    State = #state{
        options = #{
            max_batch_size_bytes := MaxBatchSize,
            max_request_size_bytes := MaxRequestSize
        },
        metadata = Metadata,
        partitions = Partitions
    }
) ->
    case kafcod_message_set:uncompressed_size([?DYNAMIC_CAST(Message)]) > MaxRequestSize of
        true ->
            {error, message_too_large};
        false ->
            PartitionData =
                case kafine_topic_partition_data:get(Topic, Partition, Partitions, undefined) of
                    undefined ->
                        #partition_data{};
                    Data ->
                        Data
                end,
            {NewBatch, BatchComplete, NewPartitionData} = append(
                Topic, Partition, Message, From, MaxBatchSize, PartitionData, Metadata
            ),
            NewPartitions = kafine_topic_partition_data:put(
                Topic, Partition, NewPartitionData, Partitions
            ),
            NewState = State#state{partitions = NewPartitions},
            {ok, NewBatch, BatchComplete, NewState}
    end.

append(
    Topic,
    Partition,
    Message,
    From,
    MaxBatchSize,
    PartitionData = #partition_data{
        batch_acc = BatchAcc,
        batch_size_acc = BatchSizeAcc,
        batch_len = BatchLen
    },
    Metadata
) ->
    % If the batch accumulator was empty, we need to start a linger timer
    NewBatch = BatchAcc =:= [],
    NewBatchAcc = [{Message, From} | BatchAcc],
    {NewSize, _, _} =
        NewBatchSizeAcc = kafcod_message_set:uncompressed_size_add(Message, BatchSizeAcc),
    case NewSize > MaxBatchSize andalso not NewBatch of
        true ->
            ?LOG_DEBUG(
                "Message doesn't fit in current batch, gathering ~s/~B before appending",
                [Topic, Partition]
            ),
            % Gather existing accumulator into batch
            PartitionData1 = do_gather_batch(Topic, Partition, PartitionData, Metadata),
            % Then redo the append
            {NewBatch1, _, PartitionData2} = append(
                Topic, Partition, Message, From, MaxBatchSize, PartitionData1, Metadata
            ),
            {NewBatch1, true, PartitionData2};
        false ->
            PartitionData1 =
                case NewBatch of
                    true ->
                        % We need to start a span for this batch
                        ?LOG_DEBUG("Starting new batch for ~s/~B", [Topic, Partition]),
                        telemetry:execute(
                            [kafine, producer, init_batch],
                            #{},
                            Metadata#{topic => Topic, partition => Partition}
                        ),
                        Timestamp = erlang:monotonic_time(microsecond),
                        PartitionData#partition_data{batch_init_timestamp = Timestamp};
                    false ->
                        ?LOG_DEBUG("Appending to existing batch for ~s/~B", [Topic, Partition]),
                        PartitionData
                end,
            % Add message to the current batch
            PartitionData2 = PartitionData1#partition_data{
                batch_acc = NewBatchAcc,
                batch_size_acc = NewBatchSizeAcc,
                batch_len = BatchLen + 1
            },
            case NewSize > MaxBatchSize of
                true ->
                    % This is a single message that caused us to exceed the max batch size. Gather
                    % it immediately
                    {false, true, do_gather_batch(Topic, Partition, PartitionData2, Metadata)};
                false ->
                    {NewBatch, false, PartitionData2}
            end
    end.

-spec gather_batch(Topic :: kafine:topic(), Partition :: kafine:partition(), State :: state()) ->
    state().

gather_batch(Topic, Partition, State = #state{partitions = Partitions, metadata = Metadata}) ->
    case kafine_topic_partition_data:get(Topic, Partition, Partitions, undefined) of
        undefined ->
            error(badkey);
        PartitionData ->
            NewPartitionData = do_gather_batch(Topic, Partition, PartitionData, Metadata),
            NewPartitions = kafine_topic_partition_data:put(
                Topic, Partition, NewPartitionData, Partitions
            ),
            State#state{partitions = NewPartitions}
    end.

do_gather_batch(
    Topic,
    Partition,
    PartitionData = #partition_data{
        batch_acc = BatchAcc,
        batch_size_acc = {BatchSize, _, _},
        batch_len = BatchLen,
        batch_init_timestamp = InitTimestamp,
        queued_batches = Queue
    },
    Metadata
) ->
    ?LOG_DEBUG("Gathering batch for ~s/~B", [Topic, Partition]),
    GatherTimestamp = erlang:monotonic_time(microsecond),
    Batch = #batch{
        messages = lists:reverse(BatchAcc),
        size = BatchSize,
        length = BatchLen,
        init_timestamp = InitTimestamp,
        enqueue_timestamp = GatherTimestamp
    },
    NewQueue = queue:in(Batch, Queue),
    telemetry:execute(
        [kafine, producer, gather_batch],
        #{
            linger_us => GatherTimestamp - InitTimestamp,
            uncompressed_size_bytes => BatchSize,
            length => BatchLen,
            queue_length => queue:len(NewQueue)
        },
        Metadata#{topic => Topic, partition => Partition}
    ),
    PartitionData#partition_data{
        batch_acc = [],
        batch_size_acc = kafcod_message_set:uncompressed_size_init(),
        batch_len = 0,
        batch_init_timestamp = undefined,
        queued_batches = NewQueue
    }.

-type stats() :: #{
    uncompressed_size_bytes := non_neg_integer(),
    message_count := non_neg_integer(),
    partition_count := non_neg_integer()
}.

-spec collect_request(
    TopicPartitions :: kafine_topic_partitions:t(),
    AllowGather :: boolean(),
    State :: state()
) -> {Collected, Gathered, Stats, NewState} | no_messages when
    Collected :: kafine_topic_partition_data:t({
        Messages :: [{kafine_producer:message(), gen_statem:from()}],
        InitTimestamp :: non_neg_integer()
    }),
    Gathered :: [{kafine:topic(), kafine:partition()}],
    Stats :: stats(),
    NewState :: state().

collect_request(
    TopicPartitions,
    AllowGather,
    StateData = #state{
        options = #{
            max_request_size_bytes := MaxRequestSize
        },
        partitions = Partitions,
        metadata = Metadata
    }
) ->
    Stats0 = #{uncompressed_size_bytes => 0, message_count => 0, partition_count => 0},
    % Extract completed batches for each topic-partition. Topics is a list of
    % {Topic, {Collected, Updated, Gatherable}}, see collect_topic for details
    {_, {Topics, Stats1, RemainingBytes}} = fair_reduce_while(
        fun({Topic, PartitionsToCollect}, {HandledTopicsAcc, StatsAcc, RemainingBytesAcc}) ->
            % collect_topic returns a 4-tuple:
            % - Collected: a list of {PartitionIndex, Batch} of collected batches
            % - Updated: a list of {PartitionIndex, PartitionData} of updated partition data
            % - Gatherable: a list of PartitionIndex for partitions which were untouched, but could
            %   be gathered if we have space remaining in the request
            % - RemainingBytes: the remaining bytes after collection, or limit_reached
            {Continue, {Collected, Updated, Gatherable, NewStatsAcc, NewRemainingBytes}} = collect_topic(
                Topic,
                PartitionsToCollect,
                AllowGather,
                Partitions,
                StatsAcc,
                RemainingBytesAcc,
                Metadata
            ),
            {
                Continue,
                {
                    [{Topic, {Collected, Updated, Gatherable}} | HandledTopicsAcc],
                    NewStatsAcc,
                    NewRemainingBytes
                }
            }
        end,
        {[], Stats0, MaxRequestSize},
        maps:to_list(TopicPartitions)
    ),
    % Fold over Topics doing two things:
    % 1. If there's in the request, gather and collect gatherable partitions
    % 2. Convert Collected and Updated topics into maps
    % This is a fold instead of a map because we need to track RemainingBytes
    % It's a fold instead of a reduce_while because we always need to do step 2
    {Collected, UpdatedPartitions, Gathered, Stats2, _} =
        lists:foldl(
            fun(
                {Topic, {InitialCollected, InitialUpdated, Gatherable}},
                {CollectedAcc, UpdatedPartitionsAcc, GatheredAcc, StatsAcc, RemainingBytesAcc}
            ) ->
                {Collected, Updated, Gathered, NewStatsAcc, NewRemainingBytes} =
                    case RemainingBytesAcc of
                        limit_reached ->
                            % No space in the request to collect more gathered batches
                            {InitialCollected, InitialUpdated, GatheredAcc, StatsAcc,
                                limit_reached};
                        _ ->
                            % InitialCollected and InitialUpdates are likely to have completed
                            % batches in them already from when we called collect_topic. To avoid
                            % using ++, we pass them in so that gathered batches can be prepended
                            % 1 by 1
                            gather_collect_topic(
                                Topic,
                                lists:reverse(Gatherable),
                                Partitions,
                                InitialCollected,
                                InitialUpdated,
                                GatheredAcc,
                                StatsAcc,
                                RemainingBytesAcc,
                                Metadata
                            )
                    end,
                case Collected of
                    [] ->
                        % Nothing to do for this topic, don't include it in the request
                        {CollectedAcc, UpdatedPartitionsAcc, GatheredAcc, NewStatsAcc,
                            NewRemainingBytes};
                    _ ->
                        % Convert partition lists to maps
                        {
                            [{Topic, maps:from_list(Collected)} | CollectedAcc],
                            [{Topic, maps:from_list(Updated)} | UpdatedPartitionsAcc],
                            Gathered,
                            NewStatsAcc,
                            NewRemainingBytes
                        }
                end
            end,
            {[], [], [], Stats1, RemainingBytes},
            lists:reverse(Topics)
        ),
    case Collected of
        [] ->
            no_messages;
        _ ->
            NewPartitions = kafine_topic_partition_data:merge(
                Partitions,
                maps:from_list(UpdatedPartitions)
            ),
            {maps:from_list(Collected), Gathered, Stats2, StateData#state{
                partitions = NewPartitions
            }}
    end.

-spec collect_topic(
    Topic :: kafine:topic(),
    PartitionsToCollect :: [kafine:partition()],
    AllowGather :: boolean(),
    Partitions :: kafine_topic_partition_data:t(partition_data()),
    StatsAcc :: stats(),
    RemainingBytes :: non_neg_integer(),
    Metadata :: telemetry:event_metadata()
) ->
    {
        cont | halt,
        {
            Collected :: [
                {
                    kafine:partition(),
                    {
                        Messages :: [{kafine_producer:message(), gen_statem:from()}],
                        InitTimestamp :: non_neg_integer()
                    }
                }
            ],
            Updated :: [{kafine:partition(), partition_data()}],
            Gatherable :: [kafine:partition()],
            NewStatsAcc :: stats(),
            RemainingBytes :: non_neg_integer() | limit_reached
        }
    }.

% Collect the first completed batch for each partition in Topic
% We actually return a 4-tuple:
% - Collected: a list of {PartitionIndex, Batch} of collected batches
% - Updated: a list of {PartitionIndex, PartitionData} of updated partition data
% - Gatherable: a list of PartitionIndex for partitions which were untouched, but could
%   be gathered if we have space remaining in the request
% - RemainingBytes: the remaining bytes after collection, or limit_reached
collect_topic(
    Topic, PartitionsToCollect, AllowGather, Partitions, StatsAcc0, RemainingBytes, Metadata
) ->
    fair_reduce_while(
        fun(
            Partition, Acc = {CollectedAcc, UpdatedAcc, GatherableAcc, StatsAcc, RemainingBytesAcc}
        ) ->
            case
                collect_topic_partition(
                    Topic, Partition, Partitions, AllowGather, StatsAcc, RemainingBytesAcc, Metadata
                )
            of
                no_messages ->
                    {cont, Acc};
                incomplete_batch ->
                    % No changes, but add this partition to the gatherable list
                    {
                        cont,
                        {
                            CollectedAcc,
                            UpdatedAcc,
                            [Partition | GatherableAcc],
                            StatsAcc,
                            RemainingBytesAcc
                        }
                    };
                limit_reached ->
                    % No changes other than setting remaining bytes to limit_reached
                    {halt, {CollectedAcc, UpdatedAcc, GatherableAcc, StatsAcc, limit_reached}};
                {Batch, NewPartitionData, NewStatsAcc, NewRemainingBytes} ->
                    {
                        cont,
                        {
                            [{Partition, Batch} | CollectedAcc],
                            [{Partition, NewPartitionData} | UpdatedAcc],
                            GatherableAcc,
                            NewStatsAcc,
                            NewRemainingBytes
                        }
                    }
            end
        end,
        {[], [], [], StatsAcc0, RemainingBytes},
        PartitionsToCollect
    ).

-spec collect_topic_partition(
    Topic :: kafine:topic(),
    Partition :: kafine:partition(),
    Partitions :: kafine_topic_partition_data:t(partition_data()),
    AllowGather :: boolean(),
    StatsAcc :: stats(),
    RemainingBytes :: non_neg_integer(),
    Metadata :: telemetry:event_metadata()
) ->
    {
        Batch :: {
            Messages :: [{kafine_producer:message(), gen_statem:from()}],
            InitTimestamp :: non_neg_integer()
        },
        NewPartitionData :: partition_data(),
        NewStatsAcc :: stats(),
        RemainingBytes :: non_neg_integer()
    }
    | no_messages
    | incomplete_batch
    | limit_reached.

collect_topic_partition(
    Topic, Partition, Partitions, AllowGather, StatsAcc, RemainingBytes, Metadata
) ->
    case kafine_topic_partition_data:get(Topic, Partition, Partitions, undefined) of
        undefined ->
            no_messages;
        PartitionData ->
            collect(
                Topic, Partition, PartitionData, AllowGather, StatsAcc, RemainingBytes, Metadata
            )
    end.

-spec collect(
    Topic :: kafine:topic(),
    Partition :: kafine:partition(),
    Data :: partition_data(),
    AllowGather :: boolean(),
    StatsAcc :: stats(),
    RemainingBytes :: non_neg_integer(),
    Metadata :: telemetry:event_metadata()
) ->
    {
        Batch :: {
            Messages :: [{kafine_producer:message(), gen_statem:from()}],
            InitTimestamp :: non_neg_integer()
        },
        NewPartitionData :: partition_data(),
        NewStatsAcc :: stats(),
        RemainingBytes :: non_neg_integer()
    }
    | no_messages
    | incomplete_batch
    | limit_reached.

collect(
    Topic,
    Partition,
    Data = #partition_data{queued_batches = Queue, batch_acc = BatchAcc},
    AllowGather,
    StatsAcc = #{
        uncompressed_size_bytes := SizeAcc,
        message_count := MessagesAcc,
        partition_count := PartitionsAcc
    },
    RemainingBytes,
    Metadata
) ->
    case queue:out(Queue) of
        {
            {value, #batch{
                messages = Messages,
                size = Size,
                length = Length,
                init_timestamp = InitTimestamp,
                enqueue_timestamp = EnqueueTimestamp
            }},
            NewQueue
        } when
            Size =< RemainingBytes
        ->
            % Collect batch
            Now = erlang:monotonic_time(microsecond),
            telemetry:execute(
                [kafine, producer, collect_batch],
                #{
                    queue_length => queue:len(NewQueue),
                    queue_time_us => Now - EnqueueTimestamp,
                    batch_age_us => Now - InitTimestamp
                },
                Metadata#{topic => Topic, partition => Partition}
            ),
            NewStats = StatsAcc#{
                uncompressed_size_bytes := SizeAcc + Size,
                message_count := MessagesAcc + Length,
                partition_count := PartitionsAcc + 1
            },
            {
                {Messages, InitTimestamp},
                Data#partition_data{queued_batches = NewQueue},
                NewStats,
                RemainingBytes - Size
            };
        {{value, _}, _} ->
            % There's a batch, but it doesn't fit because we reached the limit
            limit_reached;
        {empty, _} ->
            case {BatchAcc, AllowGather} of
                {[], _} ->
                    % No messages (gathered or otherwise) for this partition
                    no_messages;
                {_, false} ->
                    % There are ungathered messages, but we're not gathering so ignore them
                    no_messages;
                _ ->
                    % We could gather this partition if there's space
                    incomplete_batch
            end
    end.

-spec gather_collect_topic(
    Topic :: kafine:topic(),
    Gatherable :: [kafine:partition()],
    Partitions :: kafine_topic_partition_data:t(partition_data()),
    InitialCollected :: [{kafine:partition(), [{kafine_producer:message(), gen_statem:from()}]}],
    InitialUpdated :: [{kafine:partition(), partition_data()}],
    InitialGathered :: [{kafine:topic(), kafine:partition()}],
    StatsAcc :: stats(),
    RemainingBytes :: non_neg_integer(),
    Metadata :: telemetry:event_metadata()
) ->
    {
        Collected :: [
            {
                kafine:partition(),
                {
                    Messages :: [{kafine_producer:message(), gen_statem:from()}],
                    InitTimestamp :: non_neg_integer()
                }
            }
        ],
        Updated :: [{kafine:partition(), partition_data()}],
        Gathered :: [{kafine:topic(), kafine:partition()}],
        NewStatsAcc :: stats(),
        RemainingBytes :: non_neg_integer() | limit_reached
    }
    | no_messages.

gather_collect_topic(
    Topic,
    Gatherable,
    Partitions,
    InitialCollected,
    InitialUpdated,
    InitialGathered,
    StatsAcc0,
    RemainingBytes,
    Metadata
) ->
    {_, Result} =
        reduce_while(
            fun(Partition, {CollectedAcc, UpdatedAcc, GatheredAcc, StatsAcc, RemainingBytesAcc}) ->
                case
                    gather_collect_topic_partition(
                        Topic, Partition, Partitions, StatsAcc, RemainingBytesAcc, Metadata
                    )
                of
                    no_messages ->
                        % Completely empty topic-partition, move on to the next
                        {cont,
                            {CollectedAcc, UpdatedAcc, GatheredAcc, StatsAcc, RemainingBytesAcc}};
                    limit_reached ->
                        % No space in request, stop iterating
                        {halt, {CollectedAcc, UpdatedAcc, GatheredAcc, StatsAcc, limit_reached}};
                    {Batch, NewPartitionData, NewStatsAcc, NewRemainingBytes} ->
                        % Gathered and collected a batch
                        {
                            cont,
                            {
                                [{Partition, Batch} | CollectedAcc],
                                [{Partition, NewPartitionData} | UpdatedAcc],
                                [{Topic, Partition} | GatheredAcc],
                                NewStatsAcc,
                                NewRemainingBytes
                            }
                        }
                end
            end,
            {InitialCollected, InitialUpdated, InitialGathered, StatsAcc0, RemainingBytes},
            Gatherable
        ),
    Result.

-spec gather_collect_topic_partition(
    Topic :: kafine:topic(),
    Partition :: kafine:partition(),
    Partitions :: kafine_topic_partition_data:t(partition_data()),
    StatsAcc :: stats(),
    RemainingBytes :: non_neg_integer(),
    Metadata :: telemetry:event_metadata()
) ->
    {
        Batch :: {
            Messages :: [{kafine_producer:message(), gen_statem:from()}],
            InitTimestamp :: non_neg_integer()
        },
        NewPartitionData :: partition_data(),
        NewStatsAcc :: stats(),
        RemainingBytes :: non_neg_integer()
    }
    | no_messages
    | limit_reached.

gather_collect_topic_partition(Topic, Partition, Partitions, StatsAcc, RemainingBytes, Metadata) ->
    case kafine_topic_partition_data:get(Topic, Partition, Partitions, undefined) of
        undefined ->
            no_messages;
        PartitionData ->
            gather_collect(Topic, Partition, PartitionData, StatsAcc, RemainingBytes, Metadata)
    end.

-spec gather_collect(
    Topic :: kafine:topic(),
    Partition :: kafine:partition(),
    PartitionData :: partition_data(),
    StatsAcc :: stats(),
    RemainingBytes :: non_neg_integer(),
    Metadata :: telemetry:event_metadata()
) ->
    {
        Batch :: {
            Messages :: [{kafine_producer:message(), gen_statem:from()}],
            InitTimestamp :: non_neg_integer()
        },
        NewPartitionData :: partition_data(),
        NewStatsAcc :: stats(),
        RemainingBytes :: non_neg_integer()
    }
    | no_messages
    | limit_reached.

gather_collect(Topic, Partition, PartitionData, StatsAcc, RemainingBytes, Metadata) ->
    case
        collect(
            Topic,
            Partition,
            do_gather_batch(Topic, Partition, PartitionData, Metadata),
            false,
            StatsAcc,
            RemainingBytes,
            Metadata
        )
    of
        incomplete_batch ->
            % Shouldn't happen because AllowGather = false, mostly here to shut eqwalizer up
            no_messages;
        no_messages ->
            no_messages;
        limit_reached ->
            limit_reached;
        Result ->
            ?LOG_DEBUG("Gathered batch for ~s/~B for produce request", [Topic, Partition]),
            Result
    end.

-spec reduce_while(
    Fun :: fun(
        (Item :: ItemType, Acc :: AccType) -> {cont, NewAcc :: AccType} | {halt, NewAcc :: AccType}
    ),
    Acc :: AccType,
    List :: [ItemType]
) -> {Cont, AccType} when
    Cont :: cont | halt, ItemType :: dynamic(), AccType :: dynamic().

reduce_while(Fun, Acc, [Item | Rest]) ->
    case Fun(Item, Acc) of
        {cont, NewAcc} ->
            reduce_while(Fun, NewAcc, Rest);
        {halt, NewAcc} ->
            {halt, NewAcc}
    end;
reduce_while(_, Acc, []) ->
    {cont, Acc}.

-spec fair_reduce_while(
    Fun :: fun(
        (Item :: ItemType, Acc :: AccType) -> {cont, NewAcc :: AccType} | {halt, NewAcc :: AccType}
    ),
    Acc :: AccType,
    List :: [ItemType]
) -> {Cont, AccType} when
    Cont :: cont | halt, ItemType :: dynamic(), AccType :: dynamic().

% It's likely that the topic-partitions passed to collect_request will be in a consistent order.
% That means busy partitions early in the structure could starve partitions later in the structure.
% To prevent this, start at a random point in the set of topics/list of partitions. It's not
% perfect - if eg. partition 9 is busy and we can't fit many batches in a request then partition 10
% will still get less attention than other partitions - but it prevents partitions getting no
% attention at all, and it's O(N) unlike shuffling the list.
fair_reduce_while(_, Acc, []) ->
    {cont, Acc};
fair_reduce_while(Fun, Acc0, Items) ->
    Length = length(Items),
    StartIndex = rand:uniform(Length) - 1,
    % reduce over everything from StartIndex onwards
    {Continue1, EndAcc1} = reduce_while(Fun, Acc0, lists:nthtail(StartIndex, Items)),
    case Continue1 of
        cont ->
            % We got to the end, start from the start and reduce until we get to StartIndex
            {_, EndAcc2} =
                reduce_while(
                    fun
                        (_, {Index, Acc}) when Index =:= StartIndex ->
                            % Reached StartIndex
                            {halt, {Index, Acc}};
                        (Item, {N, Acc}) ->
                            case Fun(Item, Acc) of
                                {cont, NewAcc} ->
                                    {cont, {N + 1, NewAcc}};
                                {halt, NewAcc} ->
                                    {halt, {N, NewAcc}}
                            end
                    end,
                    {0, EndAcc1},
                    Items
                ),
            case EndAcc2 of
                {StartIndex, FinalAcc} ->
                    {cont, FinalAcc};
                {_, FinalAcc} ->
                    {halt, FinalAcc}
            end;
        halt ->
            {halt, EndAcc1}
    end.

-spec requeue_batches(
    Batches :: kafine_topic_partition_data:t({
        [{kafine_producer:message(), gen_statem:from()}], InitTimestamp :: non_neg_integer()
    }),
    State :: state()
) -> state().

requeue_batches(Batches, State = #state{metadata = Metadata, partitions = Partitions}) ->
    UpdatedPartitions = kafine_topic_partition_data:map(
        fun(Topic, Partition, {Messages, InitTimestamp}) ->
            BatchSize = kafcod_message_set:uncompressed_size([Message || {Message, _} <- Messages]),
            BatchLength = length(Messages),
            Batch = #batch{
                messages = Messages,
                size = BatchSize,
                length = BatchLength,
                init_timestamp = InitTimestamp,
                enqueue_timestamp = erlang:monotonic_time(microsecond)
            },
            NewPartitionData =
                case kafine_topic_partition_data:get(Topic, Partition, Partitions, undefined) of
                    undefined ->
                        Queue0 = queue:new(),
                        Queue = queue:in_r(Batch, Queue0),
                        #partition_data{queued_batches = Queue};
                    Data = #partition_data{queued_batches = Queue0} ->
                        Queue = queue:in_r(Batch, Queue0),
                        Data#partition_data{queued_batches = Queue}
                end,
            QueueLength = queue:len(NewPartitionData#partition_data.queued_batches),
            telemetry:execute(
                [kafine, producer, requeue_batch],
                #{
                    uncompressed_size_bytes => BatchSize,
                    length => BatchLength,
                    queue_length => QueueLength,
                    batch_age_us => erlang:monotonic_time(microsecond) - InitTimestamp
                },
                Metadata#{topic => Topic, partition => Partition}
            ),
            NewPartitionData
        end,
        Batches
    ),
    State#state{partitions = kafine_topic_partition_data:merge(Partitions, UpdatedPartitions)}.

-type info() :: kafine_topic_partition_data:t(#{
    current_batch := #{
        message_count := non_neg_integer(),
        batch_size_bytes := non_neg_integer()
    },
    queued_batches := [
        #{
            message_count := non_neg_integer(),
            batch_size_bytes := non_neg_integer()
        }
    ]
}).

-spec info(State :: state()) -> info().

info(#state{partitions = Partitions}) ->
    kafine_topic_partition_data:map(
        fun(
            _,
            _,
            #partition_data{
                batch_len = BatchLen,
                batch_size_acc = {BatchSizeBytes, _, _},
                queued_batches = QueuedBatches
            }
        ) ->
            #{
                current_batch => #{
                    message_count => BatchLen,
                    batch_size_bytes => BatchSizeBytes
                },
                queued_batches => [
                    #{
                        message_count => Length,
                        batch_size_bytes => Size
                    }
                 || #batch{length = Length, size = Size} <- queue:to_list(QueuedBatches)
                ]
            }
        end,
        Partitions
    ).
