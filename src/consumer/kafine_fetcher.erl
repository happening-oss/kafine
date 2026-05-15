-module(kafine_fetcher).

-feature(maybe_expr, enable).

-behaviour(gen_server).

-export([
    start_link/4,

    whereis/1,
    info/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-export([
    set_topic_partitions/2,
    fetch/6,
    pause/3
]).

-export([
    set_node_fetcher/3,
    request_job/3
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("kafcod/include/error_code.hrl").
-include("../kafine_eqwalizer.hrl").

-type ref() :: term().

id(Ref) -> {?MODULE, Ref}.

via(Ref) ->
    kafine_via:via(id(Ref)).

-spec whereis(Ref :: ref()) -> pid() | undefined.

whereis(Ref) ->
    kafine_via:whereis_name(id(Ref)).

-spec info(RefOrPid :: ref() | pid()) ->
    #{
        brokers := #{kafine:broker() => pid()},
        topic_partition_nodes := kafine_topic_partition_data:t(kafine:node_id()),
        pending_job_requests := [kafine:node_id()],
        topic_partition_states := kafine_topic_partition_data:t(topic_partition_state()),
        next_job_id := job_id()
    }.

info(RefOrPid) ->
    call(RefOrPid, info).

-spec set_topic_partitions(
    RefOrPid :: ref() | pid(),
    TopicPartitions :: kafine_topic_partitions:t()
) -> ok.

set_topic_partitions(RefOrPid, TopicPartitions) ->
    call(RefOrPid, {set_topic_partitions, TopicPartitions}).

-spec fetch(
    RefOrPid :: ref() | pid(),
    Topic :: kafine:topic(),
    Partition :: kafine:partition(),
    Offset :: kafine:offset() | kafine:offset_timestamp(),
    CallbackMod :: module(),
    CallbackArgs :: term()
) -> ok.

fetch(RefOrPid, Topic, Partition, Offset, CallbackMod, CallbackArgs) ->
    cast(RefOrPid, {fetch, Topic, Partition, {Offset, CallbackMod, CallbackArgs}}).

-spec pause(
    RefOrPid :: ref() | pid(),
    Topic :: kafine:topic(),
    Partition :: kafine:partition()
) -> ok.

pause(RefOrPid, Topic, Partition) ->
    cast(RefOrPid, {pause, Topic, Partition}).

-spec set_node_fetcher(
    RefOrPid :: ref() | pid(),
    Broker :: kafine:broker(),
    NodeFetcher :: pid()
) -> ok.

set_node_fetcher(RefOrPid, Broker, NodeFetcher) ->
    cast(RefOrPid, {set_node_fetcher, Broker, NodeFetcher}).

-spec request_job(RefOrPid :: ref() | pid(), NodeId :: kafine:node_id(), From :: pid()) -> ok.

request_job(RefOrPid, NodeId, From) ->
    cast(RefOrPid, {request_job, NodeId, From}).

-spec call(RefOrPid :: ref() | pid(), Request :: term()) -> dynamic().

call(Pid, Request) when is_pid(Pid) ->
    gen_server:call(Pid, Request);
call(Ref, Request) ->
    gen_server:call(via(Ref), Request).

-spec cast(RefOrPid :: ref() | pid(), Request :: term()) -> ok.

cast(Pid, Request) when is_pid(Pid) ->
    gen_server:cast(Pid, Request);
cast(Ref, Request) ->
    gen_server:cast(via(Ref), Request).

-spec start_link(
    Ref :: ref(),
    ConsumerOptions :: kafine:consumer_options(),
    TopicOptions :: #{kafine:topic() => kafine:topic_options()},
    Metadata :: telemetry:event_metadata()
) -> gen_server:start_ret().

start_link(Ref, ConsumerOptions, TopicOptions, Metadata) ->
    gen_server:start_link(
        via(Ref), ?MODULE, [Ref, ConsumerOptions, TopicOptions, Metadata], start_options()
    ).

start_options() ->
    [{debug, kafine_trace:debug_options(#{mfa => {?MODULE, handle_event, 4}})}].

-type fetch_request() :: {
    Offset :: kafine:offset() | kafine:offset_timestamp(),
    CallbackMod :: module(),
    CallbackArgs :: term()
}.

-type backoff_state() :: kafine_backoff:state() | undefined.

-type topic_partition_state() ::
    init
    | paused
    | {ready, fetch_request(), backoff_state()}
    | {busy, job_id(), fetch_request(), backoff_state()}
    | {backoff, fetch_request(), backoff_state()}.

-type job_id() :: pos_integer().

-record(state, {
    ref :: ref(),
    consumer_options :: kafine:consumer_options(),
    topic_options :: #{kafine:topic() => kafine:topic_options()},
    metadata :: telemetry:event_metadata(),
    brokers = #{} :: #{kafine:broker() => pid()},
    topic_partitions = kafine_topic_partitions:new() :: kafine_topic_partitions:t(),
    topic_partition_nodes = kafine_topic_partition_data:new() :: kafine_topic_partition_data:t(
        kafine:node_id()
    ),
    node_topic_partitions = #{} :: #{kafine:node_id() => kafine_topic_partitions:t()},
    pending_job_requests = #{} :: #{kafine:node_id() => pid()},
    topic_partition_states = kafine_topic_partition_data:new() :: kafine_topic_partition_data:t(
        topic_partition_state()
    ),
    job_req_ids = kafine_node_fetcher:reqids_new() :: kafine_node_fetcher:request_id_collection(),
    next_job_id = 1 :: job_id()
}).

-type state() :: #state{}.

init([Ref, ConsumerOptions, TopicOptions, Metadata]) ->
    Metadata2 = Metadata#{ref => Ref},
    logger:set_process_metadata(Metadata2),
    kafine_proc_lib:set_label({?MODULE, Ref}),
    State = #state{
        ref = Ref,
        consumer_options = ConsumerOptions,
        topic_options = TopicOptions,
        metadata = Metadata2
    },
    {ok, State}.

-spec handle_call(
    Request :: {set_topic_partitions, kafine_topic_partitions:t()} | info,
    From :: gen_server:from(),
    State :: state()
) ->
    {reply, dynamic(), state()}.

handle_call(
    info,
    _From,
    State = #state{
        brokers = Brokers,
        topic_partition_nodes = TopicPartitionNodes,
        pending_job_requests = PendingJobRequests,
        topic_partition_states = TopicPartitionStates,
        next_job_id = NextJobId
    }
) ->
    TopicPartitionStates2 = maps:map(
        fun(_T, Ps) ->
            maps:map(
                fun
                    (_P, St) when is_atom(St) -> St;
                    (_P, St) when is_tuple(St) -> element(1, St)
                end,
                Ps
            )
        end,
        TopicPartitionStates
    ),
    Info = #{
        brokers => Brokers,
        topic_partition_nodes => TopicPartitionNodes,
        pending_job_requests => maps:keys(PendingJobRequests),
        topic_partition_states => TopicPartitionStates2,
        next_job_id => NextJobId
    },
    {reply, Info, State};
handle_call(
    {set_topic_partitions, TopicPartitions},
    _From,
    State = #state{
        ref = Ref,
        topic_partition_states = TopicPartitionStates
    }
) ->
    ?LOG_DEBUG("Setting topic partitions to ~p", [TopicPartitions]),

    telemetry:execute(
        [kafine, fetcher, set_topic_partitions],
        #{count => kafine_topic_partitions:count(TopicPartitions)},
        #{ref => Ref}
    ),

    % Update topic partition states to reflect new topic partitions.
    % New partitions should be init, abandoned partitions should have their state dropped
    NewTopicPartitionStates = kafine_topic_partitions:map(
        fun(Topic, Partition) ->
            kafine_topic_partition_data:get(Topic, Partition, TopicPartitionStates, init)
        end,
        TopicPartitions
    ),

    State2 = State#state{
        topic_partition_states = NewTopicPartitionStates
    },

    State3 = update_node_mappings(TopicPartitions, State2),

    State4 = maybe_fulfil_jobs(State3),

    {reply, ok, State4}.

-spec handle_cast(
    Msg ::
        {fetch, kafine:topic(), kafine:partition(), fetch_request()}
        | {pause, kafine:topic(), kafine:partition()}
        | {set_node_fetcher, kafine:broker(), pid()}
        | {request_job, kafine:node_id(), pid()},
    State :: state()
) ->
    {noreply, state()}.

handle_cast(
    {fetch, Topic, Partition, FetchRequest = {Offset, _, _}},
    State = #state{
        topic_partition_nodes = TopicPartitionNodes,
        topic_partition_states = TopicPartitionStates
    }
) ->
    ?LOG_DEBUG("Received fetch request for ~s/~p at offset ~p", [Topic, Partition, Offset]),
    case kafine_topic_partition_data:get(Topic, Partition, TopicPartitionNodes, undefined) of
        undefined ->
            ?LOG_WARNING("Got a fetch request for an unknown topic partition ~s/~p, ignoring", [
                Topic, Partition
            ]),
            {noreply, State};
        NodeId ->
            NewTopicPartitionStates = kafine_topic_partition_data:put(
                Topic, Partition, {ready, FetchRequest, undefined}, TopicPartitionStates
            ),
            State1 = State#state{topic_partition_states = NewTopicPartitionStates},
            case maybe_fulfil_job(NodeId, true, State1) of
                false ->
                    {noreply, State1};
                {true, State2} ->
                    ?LOG_DEBUG("Fulfilled job request from node ~p", [NodeId]),
                    {noreply, State2}
            end
    end;
handle_cast(
    {pause, Topic, Partition},
    State = #state{
        topic_partition_nodes = TopicPartitionNodes,
        topic_partition_states = TopicPartitionStates
    }
) ->
    ?LOG_DEBUG("Received pause for ~s/~p", [Topic, Partition]),
    case kafine_topic_partition_data:get(Topic, Partition, TopicPartitionNodes, undefined) of
        undefined ->
            ?LOG_WARNING("Got a pause for an unknown topic partition ~s/~p, ignoring", [
                Topic, Partition
            ]),
            {noreply, State};
        NodeId ->
            NewTopicPartitionStates = kafine_topic_partition_data:put(
                Topic, Partition, paused, TopicPartitionStates
            ),
            State1 = State#state{topic_partition_states = NewTopicPartitionStates},
            case maybe_fulfil_job(NodeId, true, State1) of
                false ->
                    {noreply, State1};
                {true, State2} ->
                    ?LOG_DEBUG("Fulfilled job request from node ~p", [NodeId]),
                    {noreply, State2}
            end
    end;
handle_cast(
    {set_node_fetcher, Broker = #{node_id := NodeId}, Pid},
    State = #state{
        ref = Ref,
        brokers = Brokers
    }
) ->
    NewBrokers =
        case maps:get(Broker, Brokers, undefined) of
            undefined ->
                % Don't know anything about this broker, probably a restart racing a rebalance.
                % Tell the supervisor to terminate this pid if it still exists
                ?LOG_DEBUG("Terminating node fetcher ~p for unknown node ~p", [Pid, NodeId]),
                kafine_node_fetcher_sup:terminate_child(Ref, Pid),
                Brokers;
            Pid ->
                % This matches what we have, nothing to do
                ?LOG_DEBUG("Node fetcher ~p for node ~p already registered", [Pid, NodeId]),
                Brokers;
            _OldPid ->
                % Node fetcher has been restarted, save the new pid
                ?LOG_DEBUG("Monitoring new node fetcher ~p for node ~p", [Pid, NodeId]),
                maps:put(Broker, Pid, Brokers)
        end,
    {noreply, State#state{brokers = NewBrokers}};
handle_cast(
    {request_job, NodeId, From},
    State = #state{
        ref = Ref,
        pending_job_requests = PendingJobRequests
    }
) ->
    NewPendingJobRequests = PendingJobRequests#{NodeId => From},
    State1 = State#state{pending_job_requests = NewPendingJobRequests},
    case maybe_fulfil_job(NodeId, true, State1) of
        false ->
            ?LOG_DEBUG("Waiting for requests for node ~p", [NodeId]),
            telemetry:execute(
                [kafine, fetcher, wait_for_job],
                #{},
                #{ref => Ref, node_id => NodeId}
            ),
            {noreply, State1};
        {true, State2} ->
            ?LOG_DEBUG("Immediately fulfilled job request from node ~p", [NodeId]),
            {noreply, State2}
    end.

handle_info(
    {backoff_complete, Topic, Partition},
    State = #state{topic_partition_states = TopicPartitionStates}
) ->
    case kafine_topic_partition_data:get(Topic, Partition, TopicPartitionStates, undefined) of
        {backoff, Request, BackoffState} ->
            ?LOG_DEBUG("Backoff complete for ~s/~p, marking as ready", [Topic, Partition]),
            NewTopicPartitionStates = kafine_topic_partition_data:put(
                Topic, Partition, {ready, Request, BackoffState}, TopicPartitionStates
            ),
            State1 = State#state{topic_partition_states = NewTopicPartitionStates},
            State2 = handle_backoffs_complete(Topic, Partition, State1),
            {noreply, State2};
        _Other ->
            % State has changed since we initiated the backoff, ignore
            ?LOG_DEBUG("Backoff complete for ~s/~p but state has changed, ignoring", [
                Topic, Partition
            ]),
            {noreply, State}
    end;
handle_info(Msg, State = #state{job_req_ids = ReqIds}) ->
    case kafine_node_fetcher:check_response(Msg, ReqIds) of
        {{reply, {ok, TopicPartitionResults}}, {JobId, NodeId}, NewReqIds} ->
            NewState = complete_job(
                ?DYNAMIC_CAST(JobId),
                ?DYNAMIC_CAST(NodeId),
                TopicPartitionResults,
                State#state{job_req_ids = NewReqIds}
            ),
            {noreply, NewState};
        {{reply, {error, Reason}}, {JobId, NodeId}, NewReqIds} ->
            ?LOG_INFO("Job ~p failed with reason ~p", [JobId, Reason]),
            NewState = abort_job(JobId, NodeId, State#state{job_req_ids = NewReqIds}),
            {noreply, NewState};
        {{error, {Reason, _}}, {JobId, NodeId}, NewReqIds} ->
            ?LOG_INFO("Job ~p failed with reason ~p", [JobId, Reason]),
            NewState = abort_job(JobId, NodeId, State#state{job_req_ids = NewReqIds}),
            {noreply, NewState};
        _Other ->
            ?LOG_WARNING("Unexpected info: ~p", [Msg]),
            {noreply, State}
    end.

handle_backoffs_complete(
    Topic, Partition, State = #state{topic_partition_nodes = TopicPartitionNodes}
) ->
    case kafine_topic_partition_data:get(Topic, Partition, TopicPartitionNodes, undefined) of
        undefined ->
            State;
        NodeId ->
            case maybe_fulfil_job(NodeId, false, State) of
                {true, NewState} -> NewState;
                false -> State
            end
    end.

-spec complete_job(
    JobId :: job_id(),
    NodeId :: kafine:node_id(),
    TopicPartitionResults ::
        kafine_topic_partition_data:t(kafine_fetch:partition_result())
        | kafine_topic_partition_data:t(kafine_list_offsets:partition_result()),
    State :: state()
) -> state().

complete_job(
    JobId,
    NodeId,
    TopicPartitionResults,
    State = #state{
        ref = Ref,
        consumer_options = #{retry_backoff := RetryBackoff},
        topic_options = TopicOptions,
        topic_partitions = TopicPartitions,
        topic_partition_nodes = TopicPartitionNodes,
        topic_partition_states = TopicPartitionStates
    }
) ->
    telemetry:execute(
        [kafine, fetcher, job_complete],
        #{},
        #{ref => Ref, job_id => JobId, node_id => NodeId}
    ),
    % Find all the state updates resulting from this job completion
    TopicPartitionStateUpdates1 =
        kafine_topic_partition_data:filtermap(
            fun
                (_Topic, _Partition, completed) ->
                    % This means that the node fetcher got a response for this topic/partition.
                    % We leave the state alone, it'll be updated when the handler fetches
                    % again or pauses
                    false;
                (Topic, Partition, Result) ->
                    % This is the result of a successful list_offsets call. Update the offset
                    % and mark the partition as ready to fetch.
                    case
                        kafine_topic_partition_data:get(
                            Topic, Partition, TopicPartitionStates, undefined
                        )
                    of
                        undefined ->
                            % Not following this topic/partition any more, ignore
                            false;
                        {busy, JobId, Request, BackoffState} ->
                            case
                                handle_result(
                                    Topic,
                                    Partition,
                                    Result,
                                    Request,
                                    BackoffState,
                                    RetryBackoff,
                                    TopicOptions
                                )
                            of
                                {ok, NewState} ->
                                    {true, NewState};
                                {error, {kafka_error, ErrorCode}} ->
                                    ?LOG_ERROR(
                                        "Job for ~s/~B failed with non-retryable error ~B",
                                        [Topic, Partition, ErrorCode]
                                    ),
                                    exit({kafka_error, ErrorCode})
                            end;
                        _Other ->
                            % Must have been superseded, ignore
                            false
                    end
            end,
            TopicPartitionResults
        ),
    % If any topic/partition was part of this job, but isn't in the results, mark it as not busy
    TopicPartitionStateUpdates2 = kafine_topic_partition_data:filtermap(
        fun
            (Topic, Partition, {busy, J, Request, BackoffState}) when J =:= JobId ->
                case kafine_topic_partition_data:is_key(Topic, Partition, TopicPartitionResults) of
                    true ->
                        % Result already handled above
                        false;
                    false ->
                        % Mark as not busy
                        {true, {ready, Request, BackoffState}}
                end;
            (_Topic, _Partition, _Other) ->
                false
        end,
        TopicPartitionStates
    ),
    State1 = State#state{
        topic_partition_states = kafine_topic_partition_data:merge([
            TopicPartitionStates,
            TopicPartitionStateUpdates1,
            TopicPartitionStateUpdates2
        ])
    },
    % Did this node give away any partitions that we still think it owns?
    UpdateNodeMappingsRequired =
        kafine_topic_partition_data:any(
            fun(Topic, Partition, Result) ->
                Result =:= {error, {kafka_error, ?NOT_LEADER_OR_FOLLOWER}} andalso
                    NodeId =:=
                        kafine_topic_partition_data:get(
                            Topic, Partition, TopicPartitionNodes, undefined
                        )
            end,
            TopicPartitionResults
        ),
    State2 =
        case UpdateNodeMappingsRequired of
            true ->
                ?LOG_INFO("NodeId ~p reports moved partitions, updating node mappings", [NodeId]),
                kafine_metadata_cache:refresh(Ref, maps:keys(TopicPartitions)),
                update_node_mappings(TopicPartitions, State1);
            false ->
                State1
        end,
    % Doing a give away or updating node mappings may mean we can fulfil pending job requests
    maybe_fulfil_jobs(State2).

handle_result(
    Topic,
    Partition,
    Result,
    Request = {_, CallbackMod, CallbackArg},
    BackoffState,
    RetryBackoff,
    TopicOptions
) ->
    case Result of
        repeat ->
            {ok, {ready, Request, undefined}};
        {update_offset, NewOffset} ->
            {ok, {ready, {NewOffset, CallbackMod, CallbackArg}, undefined}};
        {error, {kafka_error, ?OFFSET_OUT_OF_RANGE}} ->
            #{offset_reset_policy := ResetPolicy} = maps:get(
                Topic, TopicOptions
            ),
            {ok, {ready, {ResetPolicy, CallbackMod, CallbackArg}, undefined}};
        {error, {kafka_error, ?NOT_LEADER_OR_FOLLOWER}} ->
            % We don't back off this this - instead we look up the new broker then retry
            % immediately. We preserve the backoff state in case something complicated is happening
            % on the kafka cluster that results in an immediate error
            {ok, {ready, Request, BackoffState}};
        {error, {kafka_error, ErrorCode}} ->
            case kafcod_error:is_retriable(ErrorCode) of
                true ->
                    case handle_backoff(BackoffState, RetryBackoff) of
                        {DelayMs, NewBackoffState} ->
                            erlang:send_after(
                                DelayMs, self(), {backoff_complete, Topic, Partition}
                            ),
                            {ok, {backoff, Request, NewBackoffState}};
                        limit_exceeded ->
                            {error, {kafka_error, ErrorCode}}
                    end;
                false ->
                    {error, {kafka_error, ErrorCode}}
            end
    end.

handle_backoff(undefined, RetryBackoff) ->
    BackoffState = kafine_backoff:init(RetryBackoff),
    handle_backoff(BackoffState, RetryBackoff);
handle_backoff(BackoffState, _RetryBackoff) ->
    kafine_backoff:backoff(BackoffState).

abort_job(
    JobId,
    NodeId,
    State = #state{
        ref = Ref,
        topic_partition_states = TopicPartitionStates
    }
) ->
    telemetry:execute(
        [kafine, fetcher, job_aborted],
        #{},
        #{ref => Ref, job_id => JobId, node_id => NodeId}
    ),
    NewTopicPartitionStates = kafine_topic_partition_data:map(
        fun
            (_Topic, _Partition, {busy, J, Request, BackoffState}) when J =:= JobId ->
                % Mark as not busy
                {ready, Request, BackoffState};
            (_Topic, _Partition, TopicPartitionState) ->
                TopicPartitionState
        end,
        TopicPartitionStates
    ),
    State#state{
        topic_partition_states = NewTopicPartitionStates
    }.

-spec update_node_mappings(TopicPartitions :: kafine_topic_partitions:t(), state()) -> state().

update_node_mappings(
    TopicPartitions,
    State = #state{
        ref = Ref,
        brokers = Brokers0,
        pending_job_requests = PendingJobRequests,
        metadata = Metadata
    }
) ->
    TopicPartitionInfo = kafine_metadata_cache:partitions(Ref, maps:keys(TopicPartitions)),

    TopicPartitionNodes = kafine_topic_partition_data:filtermap(
        fun(Topic, Partition, #{leader := Leader}) ->
            case kafine_topic_partitions:member(Topic, Partition, TopicPartitions) of
                true ->
                    {true, Leader};
                false ->
                    false
            end
        end,
        TopicPartitionInfo
    ),

    NodeTopicPartitions = kafine_topic_partition_data:fold(
        fun(Topic, Partition, NodeId, Acc) ->
            case maps:get(NodeId, Acc, undefined) of
                undefined ->
                    maps:put(NodeId, kafine_topic_partitions:single(Topic, Partition), Acc);
                NodeTopicPartitions ->
                    maps:put(
                        NodeId,
                        kafine_topic_partitions:add(Topic, Partition, NodeTopicPartitions),
                        Acc
                    )
            end
        end,
        #{},
        TopicPartitionNodes
    ),

    AllBrokers = kafine_metadata_cache:brokers(Ref),

    NewBrokers = lists:filter(
        fun(#{node_id := NodeId}) ->
            maps:is_key(NodeId, NodeTopicPartitions)
        end,
        AllBrokers
    ),

    {ToAdd, ToTerminate} = lists:foldl(
        fun(Broker, {ToAddAcc, ToTerminateAcc}) ->
            case maps:is_key(Broker, ToTerminateAcc) of
                false ->
                    {[Broker | ToAddAcc], ToTerminateAcc};
                true ->
                    {ToAddAcc, maps:remove(Broker, ToTerminateAcc)}
            end
        end,
        {[], Brokers0},
        NewBrokers
    ),

    maps:foreach(
        fun(_Broker, Pid) -> kafine_node_fetcher_sup:terminate_child(Ref, Pid) end,
        ToTerminate
    ),

    Brokers1 =
        lists:foldl(
            fun(Broker, BrokersAcc) ->
                {ok, Pid} = kafine_node_fetcher_sup:start_child(Ref, self(), Broker, Metadata),
                maps:put(Broker, Pid, BrokersAcc)
            end,
            Brokers0,
            ToAdd
        ),

    PendingJobRequests2 = maps:filter(
        fun(NodeId, _Pid) ->
            maps:get(NodeId, NodeTopicPartitions, undefined) =/= undefined
        end,
        PendingJobRequests
    ),

    State#state{
        brokers = Brokers1,
        topic_partitions = TopicPartitions,
        topic_partition_nodes = TopicPartitionNodes,
        node_topic_partitions = NodeTopicPartitions,
        pending_job_requests = PendingJobRequests2
    }.

-spec get_job_for_offset(Offset :: kafine:offset() | kafine:offset_timestamp()) ->
    fetch | list_offsets.

get_job_for_offset(Offset) when is_number(Offset) andalso Offset >= 0 ->
    fetch;
get_job_for_offset(_Offset) ->
    list_offsets.

maybe_fulfil_jobs(State = #state{pending_job_requests = PendingJobRequests}) ->
    maps:fold(
        fun(NodeId, _Pid, StateAcc) ->
            case maybe_fulfil_job(NodeId, true, StateAcc) of
                false -> StateAcc;
                {true, NewState} -> NewState
            end
        end,
        State,
        PendingJobRequests
    ).

maybe_fulfil_job(
    NodeId,
    SkipBackoffPartitions,
    State = #state{
        pending_job_requests = PendingJobRequests,
        node_topic_partitions = NodeTopicPartitions
    }
) ->
    case maps:take(NodeId, PendingJobRequests) of
        error ->
            false;
        {Target, NewPendingJobRequests} ->
            TopicPartitions = maps:get(NodeId, NodeTopicPartitions, kafine_topic_partitions:new()),
            case
                find_and_dispatch_job(NodeId, Target, TopicPartitions, SkipBackoffPartitions, State)
            of
                false ->
                    false;
                {true, NewState} ->
                    {true, NewState#state{pending_job_requests = NewPendingJobRequests}}
            end
    end.

-spec find_and_dispatch_job(
    NodeId :: kafine:node_id(),
    Target :: pid(),
    TopicPartitions :: kafine_topic_partitions:t(),
    SkipBackoffPartitions :: boolean(),
    State :: state()
) ->
    {true, NewState :: state()} | false.

find_and_dispatch_job(NodeId, Target, TopicPartitions, SkipBackoffPartitions, State) ->
    case find_list_offsets_job(NodeId, Target, TopicPartitions, SkipBackoffPartitions, State) of
        {true, NewState} ->
            {true, NewState};
        false ->
            find_fetch_job(NodeId, Target, TopicPartitions, SkipBackoffPartitions, State)
    end.

find_list_offsets_job(
    NodeId,
    Target,
    TopicPartitions,
    SkipBackoffPartitions,
    State = #state{topic_partition_states = TopicPartitionStates}
) ->
    maybe
        true ?= has_job(list_offsets, TopicPartitions, TopicPartitionStates, SkipBackoffPartitions),
        ListOffsetsBody =
            kafine_topic_partitions:filtermap(
                fun(Topic, Partition) ->
                    case kafine_topic_partition_data:get(Topic, Partition, TopicPartitionStates) of
                        {ready, {Offset, _, _}, _} ->
                            case get_job_for_offset(Offset) of
                                list_offsets ->
                                    {true, Offset};
                                _Other ->
                                    false
                            end;
                        _Other ->
                            false
                    end
                end,
                TopicPartitions
            ),
        NewState = dispatch_job(NodeId, Target, list_offsets, ListOffsetsBody, State),
        {true, NewState}
    end.

find_fetch_job(
    NodeId,
    Target,
    TopicPartitions,
    SkipBackoffPartitions,
    State = #state{topic_partition_states = TopicPartitionStates}
) ->
    maybe
        % Need all the topic partitions to have ready fetches, or be paused.
        % If we don't do this, we might only fetch from quiet partitions, which could significantly
        % delay fetching from busy partitions.
        true ?= has_job(fetch, TopicPartitions, TopicPartitionStates, SkipBackoffPartitions),
        FetchBody = kafine_topic_partitions:filtermap(
            fun(Topic, Partition) ->
                case kafine_topic_partition_data:get(Topic, Partition, TopicPartitionStates) of
                    {ready, Request, _} ->
                        {true, Request};
                    _ ->
                        false
                end
            end,
            TopicPartitions
        ),
        NewState = dispatch_job(NodeId, Target, fetch, FetchBody, State),
        {true, NewState}
    end.

-spec has_job(
    JobType :: fetch | list_offsets,
    TopicPartitions :: kafine_topic_partitions:t(),
    TopicPartitionStates :: kafine_topic_partition_data:t(topic_partition_state()),
    SkipBackoffPartitions :: boolean()
) -> boolean().

has_job(JobType, TopicPartitions, TopicPartitionStates, SkipBackoffPartitions) ->
    {_, Result} =
        kafine_topic_partitions:reduce_while(
            fun(Topic, Partition, HasJob) ->
                case
                    kafine_topic_partition_data:get(
                        Topic, Partition, TopicPartitionStates, undefined
                    )
                of
                    init ->
                        % No fetch at all for this partition, don't make a job until one arrives
                        {halt, false};
                    {ready, {Offset, _, _}, _} ->
                        % This partition is ready, is it of the correct job type?
                        HasJob2 = HasJob orelse get_job_for_offset(Offset) =:= JobType,
                        {cont, HasJob2};
                    {busy, _, _, _} ->
                        % We're waiting for a new fetch for this partition, don't request a job
                        % until one arrives
                        {halt, false};
                    paused ->
                        % This partition is paused, ignore it
                        {cont, HasJob};
                    {backoff, _, _} when SkipBackoffPartitions ->
                        % This partition is backing off, proceed without it
                        {cont, HasJob};
                    {backoff, {Offset, _, _}, _} ->
                        case get_job_for_offset(Offset) of
                            JobType ->
                                % This partition needs to be included in a job of the requested
                                % type, but is backing off. We care about backoff partitions here,
                                % so we can't send the job yet
                                {halt, false};
                            _ ->
                                % Not the correct job type, not relevant
                                {cont, HasJob}
                        end;
                    undefined ->
                        % shouldn't happen
                        {cont, HasJob}
                end
            end,
            false,
            TopicPartitions
        ),
    Result.

dispatch_job(
    NodeId,
    Target,
    JobType,
    JobBody,
    State = #state{
        next_job_id = JobId,
        topic_partition_states = TopicPartitionStates,
        job_req_ids = JobReqIds
    }
) ->
    NewTopicPartitionStates = kafine_topic_partition_data:map(
        fun
            (Topic, Partition, TopicPartitionState = {ready, Request, BackoffState}) ->
                case kafine_topic_partition_data:is_key(Topic, Partition, JobBody) of
                    true ->
                        {busy, JobId, Request, BackoffState};
                    false ->
                        TopicPartitionState
                end;
            (_Topic, _Partition, TopicPartitionState) ->
                TopicPartitionState
        end,
        TopicPartitionStates
    ),
    Job = {JobType, JobBody},
    NewJobReqIds = kafine_node_fetcher:job(Target, Job, {JobId, NodeId}, JobReqIds),
    State#state{
        next_job_id = JobId + 1,
        topic_partition_states = NewTopicPartitionStates,
        job_req_ids = NewJobReqIds
    }.
