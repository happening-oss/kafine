-module(kafine_producer).
-export([
    start_link/2,
    stop/1,

    all/0,
    info/0,
    info/1,

    produce/6,
    check_response/2,
    wait_response/2,
    wait_all_responses/2,

    produce_sync/4,

    reqids_new/0
]).
-export([set_node_producer/3]).

% Exported for mocking purposes
-export([timestamp/0]).

-behaviour(gen_statem).
-export([
    init/1,
    callback_mode/0,
    handle_event/4
]).
-export_type([
    ref/0,
    message/0,
    start_ret/0
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("kafcod/include/error_code.hrl").

-type ref() :: any().

-type request_id_collection() :: gen_statem:request_id_collection().

-type message() :: #{
    timestamp => non_neg_integer(),
    key => binary(),
    value => binary(),
    headers => [{binary(), binary() | null}]
}.

-spec start_link(Ref :: ref(), ProducerOptions :: kafine:producer_options()) -> start_ret().
-type start_ret() :: gen_statem:start_ret().

start_link(Ref, ProducerOptions) ->
    gen_statem:start_link(
        via(Ref),
        ?MODULE,
        [Ref, ProducerOptions],
        start_options()
    ).

start_options() -> [{debug, kafine_trace:debug_options(#{mfa => {?MODULE, handle_event, 4}})}].

-spec via(Ref :: ref()) -> kafine_via:via().

id(Ref) -> {?MODULE, Ref}.

via(Ref) ->
    kafine_via:via(id(Ref)).

stop(Producer) when is_pid(Producer) ->
    gen_statem:stop(Producer).

-spec all() -> [ref()].

all() ->
    [Ref || {Ref, _, _, _} <- supervisor:which_children(kafine_producer_sup_sup)].

-spec info() -> #{ref() => dynamic()}.

info() ->
    #{Ref => info(Ref) || Ref <- all()}.

-spec info(RefOrPid :: ref() | pid()) ->
    #{
        producer_options := kafine:producer_options(),
        partitions := kafine_topic_partition_data:t(#{
            leader_id := kafine:node_id(),
            state := partition_state()
        }),
        accumulator := kafine_produce_accumulator:info(),
        node_producers := #{kafine:node_id() => kafine_node_producer:info()}
    }.

info(RefOrPid) ->
    call(RefOrPid, info).

-spec produce_sync(
    RefOrPid :: ref() | pid(),
    Topic :: kafine:topic(),
    Partition :: kafine:partition(),
    Message :: message()
) -> ok | {error, term()}.

produce_sync(RefOrPid, Topic, Partition, Message) ->
    call(RefOrPid, {produce, Topic, Partition, validate_message(Message)}).

-spec produce(
    RefOrPid :: ref() | pid(),
    Topic :: kafine:topic(),
    Partition :: kafine:partition(),
    Message :: message(),
    Label :: any(),
    ReqIdCollection :: request_id_collection()
) -> request_id_collection().

produce(RefOrPid, Topic, Partition, Message, Label, ReqIdCollection) ->
    RequestLabel = {{Topic, Partition}, Label},
    send_request(
        RefOrPid,
        {produce, Topic, Partition, validate_message(Message)},
        RequestLabel,
        ReqIdCollection
    ).

validate_message(Message) ->
    Message1 = maps:merge(message_defaults(), Message),
    maps:foreach(fun validate_message/2, Message1),
    Message1.

message_defaults() ->
    #{
        % Use a non-local call to this module so that it can be mocked
        timestamp => ?MODULE:timestamp(),
        key => null,
        value => null,
        headers => []
    }.

validate_message(timestamp, Timestamp) when is_integer(Timestamp), Timestamp >= 0 ->
    ok;
validate_message(key, Key) when is_binary(Key); Key =:= null ->
    ok;
validate_message(value, Value) when is_binary(Value); Value =:= null ->
    ok;
validate_message(headers, Headers) when is_list(Headers) ->
    lists:foreach(fun validate_header/1, Headers);
validate_message(Key, Value) ->
    error(badarg, [Key, Value]).

validate_header({Key, Value}) when is_binary(Key), is_binary(Value) orelse Value =:= null ->
    ok;
validate_header(BadHeader) ->
    error(badarg, [headers, BadHeader]).

timestamp() ->
    os:system_time(millisecond).

-spec check_response(Msg :: any(), ReqIdCollection :: request_id_collection()) -> Result when
    Result ::
        {Response, Label :: any(), NewReqIdCollection :: request_id_collection()}
        | no_request
        | no_reply,
    Response :: {reply, Reply} | {error, {Reason :: any(), ServerRef :: gen_statem:server_ref()}},
    Reply :: ok | {error, term()}.

check_response(Msg, ReqIdCollection) ->
    transform_response(gen_statem:check_response(Msg, ReqIdCollection, true)).

-spec wait_response(ReqIdCollection :: request_id_collection(), WaitTimeMs :: non_neg_integer()) ->
    Result
when
    Result ::
        {Response, Label :: any(), NewReqIdConnection :: request_id_collection()}
        | no_request
        | timeout,
    Response :: {reply, Reply} | {error, {Reason :: any(), ServerRef :: gen_statem:server_ref()}},
    Reply :: ok | {error, any()}.

wait_response(ReqIdCollection, WaitTimeMs) ->
    transform_response(gen_statem:wait_response(ReqIdCollection, WaitTimeMs, true)).

transform_response({Result, {_TopicPartition, Label}, ReqIdCollection}) ->
    {Result, Label, ReqIdCollection};
transform_response(Other) ->
    Other.

-spec wait_all_responses(
    ReqIdCollection :: request_id_collection(), WaitTimeMs :: non_neg_integer()
) ->
    Result
when
    Result :: {ResultType, TopicPartitionResults, RemainingReqIdConnection} | no_request,
    ResultType :: ok | timeout,
    TopicPartitionResults :: kafine_topic_partition_data:t(
        list(
            {Label :: any(), PartitionResult :: ok | {error, Reason :: any()}}
        )
    ),
    RemainingReqIdConnection :: request_id_collection().

wait_all_responses(ReqIdCollection, WaitTimeMs) ->
    StartTime = erlang:monotonic_time(millisecond),
    do_wait_all_responses(ReqIdCollection, StartTime + WaitTimeMs, []).

do_wait_all_responses(ReqIdCollection, Deadline, CollectedResponses) ->
    case gen_statem:wait_response(ReqIdCollection, {abs, Deadline}, true) of
        {{reply, Reply}, RequestLabel, NewReqIdCollection} ->
            do_wait_all_responses_cont(NewReqIdCollection, Deadline, [
                {RequestLabel, Reply} | CollectedResponses
            ]);
        {{error, {Reason, _}}, RequestLabel, NewReqIdCollection} ->
            do_wait_all_responses_cont(NewReqIdCollection, Deadline, [
                {RequestLabel, {error, Reason}} | CollectedResponses
            ]);
        no_request ->
            no_request;
        timeout ->
            do_wait_all_responses_finish(
                timeout,
                ReqIdCollection,
                lists:reverse(CollectedResponses)
            )
    end.

do_wait_all_responses_cont(ReqIdCollection, Deadline, CollectedResponses) ->
    case gen_statem:reqids_size(ReqIdCollection) of
        0 ->
            do_wait_all_responses_finish(ok, ReqIdCollection, lists:reverse(CollectedResponses));
        _ ->
            do_wait_all_responses(ReqIdCollection, Deadline, CollectedResponses)
    end.

do_wait_all_responses_finish(Result, RemainingReqIds, CollectedResponses) ->
    % Create a map from {Topic, Partition} to [{Label, Response}]. Note: It's possible that the
    % caller used the same label multiple times, hence the need for a list of {Label, Response}.
    ResponsesByTopicPartition = maps:to_list(
        maps:groups_from_list(
            fun({{{Topic, Partition}, _}, _}) -> {Topic, Partition} end,
            fun({{_, Label}, Response}) -> {Label, Response} end,
            CollectedResponses
        )
    ),

    ResponsesByTopic = maps:groups_from_list(
        fun({{Topic, _}, _}) -> Topic end,
        fun({{_, Partition}, Responses}) -> {Partition, Responses} end,
        ResponsesByTopicPartition
    ),

    Responses = maps:map(
        fun(_, PartitionResponses) ->
            maps:from_list(PartitionResponses)
        end,
        ResponsesByTopic
    ),

    {Result, Responses, RemainingReqIds}.

-spec set_node_producer(Pid :: pid(), Broker :: kafine:broker(), Pid :: pid()) -> ok.

set_node_producer(ServerPid, Broker, NodeProducerPid) ->
    gen_statem:cast(ServerPid, {set_node_producer, Broker, NodeProducerPid}).

call(Producer, Request) when is_pid(Producer) ->
    gen_statem:call(Producer, Request);
call(Producer, Request) ->
    gen_statem:call(via(Producer), Request).

send_request(Pid, Request, Label, ReqIdCollection) when is_pid(Pid) ->
    gen_statem:send_request(Pid, Request, Label, ReqIdCollection);
send_request(Ref, Request, Label, ReqIdCollection) ->
    gen_statem:send_request(via(Ref), Request, Label, ReqIdCollection).

reqids_new() ->
    gen_statem:reqids_new().

callback_mode() ->
    [handle_event_function].

% Topic-partition state.
% - ready: Partition had received a non-retryable response for every batch produced from it. It is
%   ready to send a batch whenever one is prepared.
% - busy: Partition has an in-flight batch for which we're awaiting a response
% - backoff: Partition is not sending batches because one returned a retryable error, and we're
%   giving the broker time to recover
-type partition_state() :: ready | busy | backoff.
-record(partition_status, {
    leader_id :: kafine:node_id(),
    state = ready :: partition_state(),
    backoff_state = undefined :: kafine_backoff:state() | undefined
}).

-type partition_status() :: #partition_status{}.

-record(state, {
    ref :: ref(),
    producer_options :: kafine:producer_options(),
    metadata :: telemetry:event_metadata(),
    brokers = [] :: [kafine:broker()],
    partitions = kafine_topic_partition_data:new() :: kafine_topic_partition_data:t(
        partition_status()
    ),
    accumulator_state,
    node_producers = #{} :: #{kafine:node_id() => pid()},
    pending :: kafine_node_producer:request_id_collection()
}).

init([Ref, ProducerOptions = #{metadata := Metadata0}]) ->
    process_flag(trap_exit, true),
    Metadata = Metadata0#{ref => Ref},
    logger:set_process_metadata(#{ref => Ref}),
    kafine_proc_lib:set_label({?MODULE, Ref}),
    StateData = #state{
        ref = Ref,
        metadata = Metadata,
        producer_options = ProducerOptions,
        accumulator_state = kafine_produce_accumulator:init(ProducerOptions, Metadata),
        pending = kafine_node_producer:reqids_new()
    },
    {ok, ready, StateData}.

handle_event(
    {call, From},
    {produce, Topic, Partition, Message},
    ready,
    StateData = #state{
        metadata = Metadata,
        producer_options = #{linger_ms := LingerMs},
        accumulator_state = AccState
    }
) ->
    ?LOG_DEBUG("Produce for ~s/~B: ~p", [Topic, Partition, Message]),
    StateData1 = #state{partitions = Partitions} = ensure_topic(Topic, StateData),
    case kafine_topic_partition_data:get(Topic, Partition, Partitions, undefined) of
        undefined ->
            ?LOG_DEBUG("Unknown topic-partition ~s/~B", [Topic, Partition]),
            telemetry:execute(
                [kafine, producer, produce],
                #{},
                Metadata#{topic => Topic, partition => Partition, error => unknown_topic_partition}
            ),
            {keep_state_and_data, {reply, From, {error, unknown_topic_partition}}};
        #partition_status{leader_id = LeaderId, state = PartitionState} ->
            case kafine_produce_accumulator:append(Topic, Partition, Message, From, AccState) of
                {ok, NewBatch, BatchComplete, NewAccState} ->
                    telemetry:execute(
                        [kafine, producer, produce],
                        #{},
                        Metadata#{topic => Topic, partition => Partition}
                    ),

                    StateData2 = StateData1#state{accumulator_state = NewAccState},

                    {TimeoutCancelActions, StateData3} =
                        case {BatchComplete, PartitionState} of
                            {true, ready} ->
                                % Need to produce
                                ?LOG_DEBUG("Completed batch for ~s/~B, producing to node ~B", [
                                    Topic, Partition, LeaderId
                                ]),
                                {Gathered, StateData21} = produce_to_leader(
                                    LeaderId, true, StateData2
                                ),
                                CancelActions = [
                                    {{timeout, {linger, T, P}}, cancel}
                                 || {T, P} <- Gathered
                                ],
                                {CancelActions, StateData21};
                            _ ->
                                {[], StateData2}
                        end,

                    Actions =
                        case {NewBatch, BatchComplete} of
                            {true, _} ->
                                % This is the first message in the batch, start a linger timeout
                                ?LOG_DEBUG("Starting linger timeout of ~B ms for ~s/~B", [
                                    LingerMs, Topic, Partition
                                ]),
                                [
                                    {{timeout, {linger, Topic, Partition}}, LingerMs, complete}
                                    | TimeoutCancelActions
                                ];
                            {false, true} ->
                                % Completed a batch, and didn't start a new one - cancel any linger
                                % timeout for this partition
                                [
                                    {{timeout, {linger, Topic, Partition}}, cancel}
                                    | TimeoutCancelActions
                                ];
                            _ ->
                                TimeoutCancelActions
                        end,

                    {keep_state, StateData3, Actions};
                {error, Reason} ->
                    ?LOG_DEBUG("Failed to append message for ~s/~B: ~p", [Topic, Partition, Reason]),
                    telemetry:execute(
                        [kafine, producer, produce],
                        #{},
                        Metadata#{topic => Topic, partition => Partition, error => Reason}
                    ),
                    {keep_state_and_data, {reply, From, {error, Reason}}}
            end
    end;
handle_event(
    {timeout, {linger, Topic, Partition}},
    complete,
    ready,
    StateData = #state{
        partitions = Partitions,
        accumulator_state = AccState
    }
) ->
    case kafine_topic_partition_data:get(Topic, Partition, Partitions, undefined) of
        undefined ->
            ?LOG_DEBUG("Unexpected linger timeout event for unknown topic-partition ~s/~B", [
                Topic, Partition
            ]),
            keep_state_and_data;
        #partition_status{leader_id = LeaderId, state = State} ->
            ?LOG_DEBUG("Linger timeout for ~s/~B", [Topic, Partition]),
            NewAccumulatorState = kafine_produce_accumulator:gather_batch(
                Topic, Partition, AccState
            ),
            StateData1 = StateData#state{accumulator_state = NewAccumulatorState},

            {LingerTimeoutCancelActions, NewStateData} =
                case State of
                    ready ->
                        {Gathered, StateData2} = produce_to_leader(LeaderId, true, StateData1),
                        CancelActions = [{{timeout, {linger, T, P}}, cancel} || {T, P} <- Gathered],
                        {CancelActions, StateData2};
                    _ ->
                        {[], StateData1}
                end,
            {keep_state, NewStateData, LingerTimeoutCancelActions}
    end;
handle_event(
    {timeout, {backoff, Topic, Partition}},
    complete,
    ready,
    StateData = #state{partitions = Partitions}
) ->
    % TODO: Should we linger for other partitions which may have started a backoff at the same time?
    case kafine_topic_partition_data:get(Topic, Partition, Partitions, undefined) of
        undefined ->
            ?LOG_DEBUG("Unexpected backoff timeout event for unknown topic-partition ~s/~B", [
                Topic, Partition
            ]),
            keep_state_and_data;
        PartitionStatus = #partition_status{leader_id = LeaderId} ->
            ?LOG_DEBUG("Backoff timeout complete for ~s/~B", [Topic, Partition]),
            NewPartitionData = PartitionStatus#partition_status{state = ready},
            StateData1 = StateData#state{
                partitions = kafine_topic_partition_data:put(
                    Topic, Partition, NewPartitionData, Partitions
                )
            },
            {Gathered, StateData2} = produce_to_leader(LeaderId, true, StateData1),
            CancelActions = [{{timeout, {linger, T, P}}, cancel} || {T, P} <- Gathered],
            {keep_state, StateData2, CancelActions}
    end;
handle_event(
    cast,
    {set_node_producer, #{node_id := LeaderId}, Pid},
    _State,
    StateData = #state{node_producers = NodeProducers}
) ->
    NewStateData = StateData#state{
        node_producers = NodeProducers#{LeaderId => Pid}
    },
    {keep_state, NewStateData};
handle_event(
    {call, From},
    info,
    _State,
    #state{
        ref = Ref,
        producer_options = ProducerOptions,
        partitions = Partitions,
        accumulator_state = AccumulatorState
    }
) ->
    PartitionInfos = kafine_topic_partition_data:map(
        fun(_, _, #partition_status{leader_id = LeaderId, state = PartitionState}) ->
            #{leader_id => LeaderId, state => PartitionState}
        end,
        Partitions
    ),
    NodeProducerInfos =
        #{
            NodeId => NodeInfo
         || Pid <- kafine_node_producer_sup:list_children(Ref),
            NodeInfo = #{node_id := NodeId} <- [kafine_node_producer:info(Pid)]
        },
    Info = #{
        producer_options => ProducerOptions,
        partitions => PartitionInfos,
        accumulator => kafine_produce_accumulator:info(AccumulatorState),
        node_producers => NodeProducerInfos
    },
    {keep_state_and_data, {reply, From, Info}};
handle_event(info, Info, State, StateData = #state{pending = ReqIds}) ->
    % We can't tell the difference between send_request responses and normal info messages, so we have to check them
    % first.
    check_response(kafine_node_producer:check_response(Info, ReqIds), Info, State, StateData).

check_response(_Result = {Response, Label, ReqIds2}, _Info, State, StateData) ->
    handle_response(Response, Label, State, StateData#state{pending = ReqIds2});
check_response(_Other, Info, State, StateData) ->
    handle_info(Info, State, StateData).

handle_info(
    {timeout, _, {produce, From, OriginalRequest}},
    _State,
    _StateData
) ->
    {keep_state_and_data, [{next_event, {call, From}, OriginalRequest}]};
handle_info(
    _Info,
    _State,
    _StateData
) ->
    % Normal info message; ignore it.
    keep_state_and_data.

handle_response(
    Responses,
    {produce, LeaderId, Batch},
    _State,
    StateData = #state{
        producer_options = #{retry_backoff := RetryBackoff},
        metadata = Metadata,
        partitions = Partitions,
        accumulator_state = AccState
    }
) ->
    % Send responses and update data for partitions where we should send a response
    HandledResponses = kafine_topic_partition_data:map(
        fun(Topic, Partition, Response) ->
            {ok, PartitionData} = kafine_topic_partition_data:find(Topic, Partition, Partitions),
            PartitionBatch = kafine_topic_partition_data:get(Topic, Partition, Batch, []),
            handle_partition_response(
                Topic, Partition, Response, PartitionBatch, PartitionData, RetryBackoff, Metadata
            )
        end,
        Responses
    ),

    UpdatedPartitions = kafine_topic_partition_data:map(
        fun(_, _, {_, PartitionData}) -> PartitionData end,
        HandledResponses
    ),

    NewPartitions = kafine_topic_partition_data:merge(Partitions, UpdatedPartitions),

    RequeueBatches = kafine_topic_partition_data:filtermap(
        fun
            (_Topic, _Partition, {done, _}) ->
                false;
            (_Topic, _Partition, {{_, B}, _}) ->
                {true, B}
        end,
        HandledResponses
    ),

    NewAccState = kafine_produce_accumulator:requeue_batches(RequeueBatches, AccState),

    StateData1 = StateData#state{
        partitions = NewPartitions,
        accumulator_state = NewAccState
    },

    % Look for NOT_LEADER_OR_FOLLOWER errors. If we have any, we need to do a metadata refresh
    HasGiveAway = kafine_topic_partition_data:any(
        fun(_T, _P, Response) -> Response =:= {error, {kafka_error, ?NOT_LEADER_OR_FOLLOWER}} end,
        Responses
    ),

    {Gathered, StateData2} =
        case HasGiveAway of
            true ->
                MetadataRefreshRequired = kafine_topic_partition_data:any(
                    fun
                        (Topic, Partition, {error, {kafka_error, ?NOT_LEADER_OR_FOLLOWER}}) ->
                            % Only refresh metadata if the topic-partitions leader matches the one
                            % we sent this request to. If it doesn't, that's probably because
                            % refreshed metadata already
                            {ok, #partition_status{leader_id = PartitionLeader}} = kafine_topic_partition_data:find(
                                Topic, Partition, Partitions
                            ),
                            PartitionLeader =:= LeaderId;
                        (_, _, _) ->
                            false
                    end,
                    Responses
                ),

                StateData3 =
                    case MetadataRefreshRequired of
                        true ->
                            ?LOG_DEBUG("Refreshing metadata due to give away for node ~B", [
                                LeaderId
                            ]),
                            refresh_metadata(StateData1);
                        _ ->
                            StateData1
                    end,

                % We always produce to all leaders with anything to produce if there's a
                % NOT_LEADER_OR_FOLLOWER error, because retryable partitions could be anywhere
                produce_to_all(StateData3);
            false ->
                % Immediately send any queued up batches for this leader
                produce_to_leader(LeaderId, false, StateData1)
        end,

    CancelActions = [{{timeout, {linger, T, P}}, cancel} || {T, P} <- Gathered],

    Actions = kafine_topic_partition_data:fold(
        fun(Topic, Partition, {Result, _}, Acc) ->
            case Result of
                {{backoff, DelayMs}, _} ->
                    Action = {{timeout, {backoff, Topic, Partition}}, DelayMs, complete},
                    [Action | Acc];
                _ ->
                    Acc
            end
        end,
        CancelActions,
        HandledResponses
    ),

    {keep_state, StateData2, Actions}.

-spec handle_partition_response(
    Topic, Partition, Response, Batch, PartitionDataIn, RetryBackoff, Metadata
) ->
    {Result, PartitionDataOut}
when
    Topic :: kafine:topic(),
    Partition :: kafine:partition(),
    Response :: ok | {error, term()},
    Batch :: [{message(), gen_statem:from()}],
    PartitionDataIn :: partition_status(),
    RetryBackoff :: kafine_backoff:config(),
    Metadata :: telemetry:event_metadata(),
    Result :: {retry, Batch} | {{backoff, non_neg_integer()}, Batch} | done,
    PartitionDataOut :: partition_status().

handle_partition_response(Topic, Partition, Response, Batch, PartitionData, RetryBackoff, Metadata) ->
    case response_behaviour(Response) of
        return ->
            reply_all(Topic, Partition, Response, Batch, PartitionData, Metadata);
        retry ->
            retry_partition(
                Topic, Partition, Response, Batch, PartitionData, RetryBackoff, Metadata
            )
    end.

retry_partition(
    Topic,
    Partition,
    {error, {kafka_error, ?NOT_LEADER_OR_FOLLOWER}},
    Batch,
    PartitionStatus = #partition_status{backoff_state = undefined},
    RetryBackoff,
    _Metadata
) ->
    % The first NOT_LEADER_OR_FOLLOWER bypasses backoff. We're pretty sure we know how to solve this
    % one (send it to the correct broker), so lets allow that to be retried immediately.
    % Do init the backoff state though, since if we get a bunch of these in succession something is
    % probably going wrong in the kafka cluster
    ?LOG_DEBUG("Give away for ~s/~B. Produce will be retried on the refreshed leader", [
        Topic, Partition
    ]),
    NewPartitionStatus = PartitionStatus#partition_status{
        state = ready,
        backoff_state = kafine_backoff:init(RetryBackoff)
    },
    {{retry, Batch}, NewPartitionStatus};
retry_partition(
    Topic,
    Partition,
    Response,
    Batch,
    PartitionStatus = #partition_status{backoff_state = undefined},
    RetryBackoff,
    Metadata
) ->
    % First failure, initialise backoff state
    NewBackoffState = kafine_backoff:init(RetryBackoff),
    retry_partition(
        Topic,
        Partition,
        Response,
        Batch,
        PartitionStatus#partition_status{backoff_state = NewBackoffState},
        RetryBackoff,
        Metadata
    );
retry_partition(
    _Topic,
    _Partition,
    _Response,
    Batch,
    PartitionStatus = #partition_status{state = backoff},
    _RetryBackoff,
    _Metadata
) ->
    % We're already backing off, so no need to update the status
    {{retry, Batch}, PartitionStatus};
retry_partition(
    Topic,
    Partition,
    Response,
    Batch,
    PartitionStatus = #partition_status{backoff_state = BackoffState},
    _RetryBackoff,
    Metadata
) ->
    case kafine_backoff:backoff(BackoffState) of
        limit_exceeded ->
            % We're at the limit for retrying on this partition. Respond with the last request.
            % This'll clear the backoff timeout
            reply_all(Topic, Partition, Response, Batch, PartitionStatus, Metadata);
        {DelayMs, NewBackoffState} ->
            ?LOG_DEBUG("Error ~p for ~s/~B. Produce will be retried in ~B ms", [
                Response, Topic, Partition, DelayMs
            ]),
            NewPartitionStatus = PartitionStatus#partition_status{
                state = backoff,
                backoff_state = NewBackoffState
            },
            {{{backoff, DelayMs}, Batch}, NewPartitionStatus}
    end.

reply_all(Topic, Partition, Response, {Messages, BatchInitTimestamp}, PartitionStatus, Metadata) ->
    Now = erlang:monotonic_time(microsecond),
    lists:foreach(
        fun({_, From}) ->
            telemetry:execute(
                [kafine, producer, response],
                #{batch_age_us => Now - BatchInitTimestamp},
                Metadata#{topic => Topic, partition => Partition, response => Response}
            ),
            gen_statem:reply(From, Response)
        end,
        Messages
    ),
    {done, PartitionStatus#partition_status{state = ready, backoff_state = undefined}}.

produce_to_leader(
    LeaderId,
    AllowGather,
    StateData = #state{
        metadata = Metadata,
        pending = Pending,
        partitions = Partitions,
        accumulator_state = AccState
    }
) ->
    TopicPartitions = ready_partitions_for_leader(LeaderId, Partitions),
    case kafine_produce_accumulator:collect_request(TopicPartitions, AllowGather, AccState) of
        no_messages ->
            ?LOG_DEBUG("No batches to produce for leader ~B", [LeaderId]),
            {[], StateData};
        {ToSend, Gathered, Stats, NewAccState} ->
            Batch = kafine_topic_partition_data:map(
                fun(_Topic, _Partition, {Messages, _BatchInitTimestamp}) ->
                    lists:map(fun({Message, _From}) -> Message end, Messages)
                end,
                ToSend
            ),

            % Find the actual pid to send the batch to
            {Pid, StateData1} = get_node_producer(LeaderId, StateData),
            ?LOG_DEBUG("Producing to node ~B: ~p", [LeaderId, Batch]),

            % Mark every partition in this batch as busy
            NewPartitions = kafine_topic_partition_data:map(
                fun(Topic, Partition, Status) ->
                    case kafine_topic_partition_data:is_key(Topic, Partition, ToSend) of
                        true ->
                            Status#partition_status{state = busy};
                        false ->
                            Status
                    end
                end,
                Partitions
            ),

            % and Finally, send the batch
            NewPending = kafine_node_producer:produce(
                Pid, Batch, {produce, LeaderId, ToSend}, Pending
            ),

            telemetry:execute(
                [kafine, producer, request],
                Stats,
                Metadata#{leader_id => LeaderId}
            ),

            StateData2 = StateData1#state{
                partitions = NewPartitions,
                accumulator_state = NewAccState,
                pending = NewPending
            },
            {Gathered, StateData2}
    end.

ready_partitions_for_leader(LeaderId, Partitions) ->
    kafine_topic_partition_data:filter_keys(
        fun(_Topic, _Partition, #partition_status{leader_id = L, state = S}) ->
            L =:= LeaderId andalso S =:= ready
        end,
        Partitions
    ).

produce_to_all(StateData = #state{brokers = Brokers}) ->
    {Gathered, FinalStateData} =
        lists:foldl(
            fun(#{node_id := NodeId}, {AccGathered, AccStateData}) ->
                {Gathered, NewStateData} = produce_to_leader(NodeId, false, AccStateData),
                {[Gathered | AccGathered], NewStateData}
            end,
            {[], StateData},
            Brokers
        ),
    {lists:flatten(Gathered), FinalStateData}.

ensure_topic(Topic, StateData = #state{partitions = Partitions}) ->
    case maps:get(Topic, Partitions, undefined) of
        undefined ->
            Topics = [Topic | maps:keys(Partitions)],
            refresh_metadata(Topics, StateData);
        _ ->
            StateData
    end.

refresh_metadata(StateData = #state{partitions = Partitions}) ->
    Topics = maps:keys(Partitions),
    refresh_metadata(Topics, StateData).

refresh_metadata(
    Topics, StateData = #state{ref = Ref, metadata = Metadata, partitions = Partitions}
) ->
    ?LOG_DEBUG("Refreshing metadata for topics: ~p", [Topics]),
    kafine_metadata_cache:refresh(Ref, Topics),

    Brokers = kafine_metadata_cache:brokers(Ref),
    TopicPartitionInfo = kafine_metadata_cache:partitions(Ref, Topics),

    telemetry:execute(
        [kafine, producer, refresh_metadata],
        #{},
        Metadata#{
            topics => Topics,
            brokers => Brokers
        }
    ),

    NewPartitions = kafine_topic_partition_data:map(
        fun(T, P, #{leader := LeaderId}) ->
            case kafine_topic_partition_data:get(T, P, Partitions, undefined) of
                undefined ->
                    % Add the new topic partition
                    #partition_status{leader_id = LeaderId};
                PartitionData = #partition_status{leader_id = LeaderId} ->
                    % Leader unchanged
                    PartitionData;
                PartitionData ->
                    % Update the current leader
                    PartitionData#partition_status{leader_id = LeaderId}
            end
        end,
        TopicPartitionInfo
    ),

    StateData#state{brokers = Brokers, partitions = NewPartitions}.

get_node_producer(
    NodeId, StateData = #state{ref = Ref, brokers = Brokers, node_producers = NodeProducers}
) ->
    case maps:get(NodeId, NodeProducers, undefined) of
        undefined ->
            {ok, Pid} = kafine_node_producer_sup:start_child(
                Ref, self(), get_node_by_id(Brokers, NodeId)
            ),
            {Pid, StateData#state{node_producers = NodeProducers#{NodeId => Pid}}};
        Pid ->
            {Pid, StateData}
    end.

-spec get_node_by_id([Node], NodeId :: non_neg_integer()) -> Node when
    Node :: #{node_id := integer(), host := binary(), port := integer()}.

get_node_by_id(Nodes, NodeId) when is_list(Nodes), is_integer(NodeId) ->
    [Node] = [N || N = #{node_id := Id} <- Nodes, Id =:= NodeId],
    Node.

response_behaviour({error, {kafka_error, ErrorCode}}) ->
    case kafcod_error:is_retriable(ErrorCode) of
        true ->
            retry;
        false ->
            return
    end;
response_behaviour(_) ->
    return.
