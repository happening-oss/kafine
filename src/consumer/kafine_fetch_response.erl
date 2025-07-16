-module(kafine_fetch_response).
-moduledoc false.
-export([fold/3]).

%% A Kafka FetchResponse is a nested data structure:
%%
%% FetchResponse
%% - []FetchableTopicResponse
%%   - []PartitionData
%%     - []RecordBatch (aka RECORDS, aka message set)
%%       - []Record
%%
%% The code in this module recursively walks through that data structure, invoking a callback at appropriate points.
%% In OOP design pattern terms, it's a Visitor pattern. In FP terms, it's a recursive fold.
%%
%% As we recurse through the levels, we pass the parent levels as leading arguments, and we pass the telemetry metadata
%% (Metadata) and accumulator (Fold) as the trailing arguments.
%%
%% For example:
%%
%% fold_fetchable_topic_response(FetchableTopicResponse, Meta, Fold)
%% calls: fold_partition_data(FetchableTopicResponse, PartitionData, Meta, Fold)
%% calls: fold_record_batch(FetchableTopicResponse, PartitionData, RecordBatch, Meta, Fold)
%% calls: fold_record(FetchableTopicResponse, PartitionData, RecordBatch, Record, Meta, Fold)
%%
%% ...etc.
%%
%% Note that everything from partition_data down is in a separate module, because that's where we start dealing with
%% per-partition state, so it's a reasonable place to break the module up. It's also easier to test like that.
%%
%% The telemetry metadata is NOT stored in the #fold{} record, because we update it at each level (adding the topic
%% name, then adding the partition index), and we don't want those changes persisted as we return back up the stack.

-include_lib("kernel/include/logger.hrl").
-include_lib("kafcod/include/error_code.hrl").
-include("kafine_topic_partition_state.hrl").

-export_type([
    fold_errors/0
]).

%% To maintain the state as we recurse through the FetchResponse, we use this record, named 'fold'.
-record(fold, {
    % 'states' holds the fetch offset and client process associated with each (topic, partition) in the response.
    states :: kafine_consumer:topic_partition_states(),
    % 'errors' holds the errors encountered for each (topic, partition).
    errors :: fold_errors()
}).

-type fold() :: #fold{}.
-type fold_errors() :: #{kafine:error_code() => [{kafine:topic(), kafine:partition()}]}.

-spec fold(
    FetchResponse :: fetch_response:fetch_response_11(),
    States :: kafine_consumer:topic_partition_states(),
    Metadata :: telemetry:event_metadata()
) -> {States2 :: kafine_consumer:topic_partition_states(), Errors :: fold_errors()}.

fold(FetchResponse, States, Metadata) ->
    Fold0 = #fold{
        states = States,
        errors = #{}
    },
    FoldResult = fold_fetch_response(
        FetchResponse, Metadata, Fold0
    ),
    #fold{states = States2, errors = Errors} = FoldResult,
    {States2, Errors}.

-spec fold_fetch_response(
    FetchResponse :: fetch_response:fetch_response_11(),
    Metadata :: telemetry:event_metadata(),
    Fold0 :: fold()
) -> Fold :: fold().

fold_fetch_response(
    #{
        error_code := ?NONE,
        responses := Responses,
        throttle_time_ms := _,
        session_id := _
    },
    Metadata,
    Fold0
) ->
    lists:foldl(
        fun(FetchableTopicResponse, Fold) ->
            fold_fetchable_topic_response(FetchableTopicResponse, Metadata, Fold)
        end,
        Fold0,
        Responses
    ).

-spec fold_fetchable_topic_response(
    fetch_response:fetchable_topic_response_11(), Metadata :: telemetry:event_metadata(), fold()
) -> fold().

fold_fetchable_topic_response(
    FetchableTopicResponse = #{
        topic := Topic,
        partitions := Partitions
    },
    Metadata0,
    Fold0
) ->
    Metadata = Metadata0#{topic => Topic},

    % As we recurse through the record structure, we stick the parent levels on the front of the calls. This mirrors the
    % scoping rules of using nested functions, but ... well, our functions aren't nested.
    lists:foldl(
        fun(PartitionData, Acc) ->
            fold_partition_data(FetchableTopicResponse, PartitionData, Metadata, Acc)
        end,
        Fold0,
        Partitions
    ).

-spec fold_partition_data(
    fetch_response:fetchable_topic_response_11(),
    fetch_response:partition_data_11(),
    Metadata :: telemetry:event_metadata(),
    Fold0 :: fold()
) -> Fold :: fold().

fold_partition_data(
    FetchableTopicResponse = #{topic := Topic},
    PartitionData = #{
        partition_index := PartitionIndex,
        error_code := ?NONE
    },
    Metadata,
    Fold = #fold{states = States0}
) ->
    % We allow for the fetch response to refer to a topic/partition that we no longer care about.
    TopicPartitionState = kafine_maps:get([Topic, PartitionIndex], States0, undefined),
    fold_partition_data(FetchableTopicResponse, PartitionData, TopicPartitionState, Metadata, Fold);
fold_partition_data(
    _FetchableTopicResponse = #{topic := Topic},
    _PartitionData = #{
        partition_index := PartitionIndex,
        error_code := ErrorCode
    },
    _Metadata,
    Fold0 = #fold{errors = Errors0}
) ->
    ?LOG_NOTICE("Topic ~s, partition ~B, error ~B", [Topic, PartitionIndex, ErrorCode]),
    Errors = maps:update_with(
        ErrorCode,
        fun(Value) ->
            [{Topic, PartitionIndex} | Value]
        end,
        [{Topic, PartitionIndex}],
        Errors0
    ),
    Fold0#fold{errors = Errors}.

fold_partition_data(
    _FetchableTopicResponse,
    _PartitionData,
    _TopicPartitionState = undefined,
    _Metadata,
    Fold
) ->
    % We're not interested in this topic/partition. For example, we unsubscribed while the Fetch request was in flight.
    Fold;
fold_partition_data(
    _FetchableTopicResponse = #{topic := Topic},
    PartitionData = #{
        partition_index := PartitionIndex,
        error_code := ?NONE,
        records := _,
        log_start_offset := _,
        last_stable_offset := _,
        high_watermark := HighWatermark,
        aborted_transactions := _,
        preferred_read_replica := _
    },
    TopicPartitionState,
    Metadata0,
    Fold0 = #fold{states = States0}
) ->
    Metadata = Metadata0#{partition_index => PartitionIndex},
    FetchOffset = TopicPartitionState#topic_partition_state.offset,
    ClientPid = TopicPartitionState#topic_partition_state.client_pid,

    Span = kafine_telemetry:start_span(
        [kafine, fetch, partition_data],
        #{fetch_offset => FetchOffset, high_watermark => HighWatermark},
        Metadata
    ),

    ClientPid ! {partition_data, {Topic, PartitionData, FetchOffset, Span}},

    TopicPartitionState2 = TopicPartitionState#topic_partition_state{
        state = busy
    },
    States = kafine_maps:put([Topic, PartitionIndex], TopicPartitionState2, States0),
    Fold0#fold{states = States}.
