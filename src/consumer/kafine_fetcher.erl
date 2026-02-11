-module(kafine_fetcher).

-feature(maybe_expr, enable).

-behaviour(gen_server).

-export([
    start_link/2,

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
    request_job/3,
    complete_job/4
]).

-export_type([job_id/0]).

-include_lib("kernel/include/logger.hrl").

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

-type topic_partition_result() ::
    completed
    | repeat
    | {update_offset, kafine:offset() | kafine:offset_timestamp()}
    | give_away.

-spec complete_job(
    RefOrPid :: ref() | pid(),
    JobId :: job_id(),
    NodeId :: kafine:node_id(),
    TopicPartitionResults :: kafine_topic_partition_data:t(topic_partition_result())
) -> ok.

complete_job(RefOrPid, JobId, NodeId, TopicPartitionResults) ->
    cast(RefOrPid, {complete_job, JobId, NodeId, TopicPartitionResults}).

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

-spec start_link(Ref :: ref(), Metadata :: telemetry:event_metadata()) -> gen_server:start_ret().

start_link(Ref, Metadata) ->
    gen_server:start_link(via(Ref), ?MODULE, [Ref, Metadata], start_options()).

start_options() ->
    [{debug, kafine_trace:debug_options(#{mfa => {?MODULE, handle_event, 4}})}].

-type fetch_request() :: {
    Offset :: kafine:offset() | kafine:offset_timestamp(),
    CallbackMod :: module(),
    CallbackArgs :: term()
}.

-type topic_partition_state() ::
    init | paused | {ready, fetch_request()} | {busy, job_id(), fetch_request()}.

-type job_id() :: pos_integer().

-record(state, {
    ref :: ref(),
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
    next_job_id = 1 :: job_id(),
    job_assignments = #{} :: #{pid() => job_id()}
}).

-type state() :: #state{}.

init([Ref, Metadata]) ->
    Metadata2 = Metadata#{ref => Ref},
    logger:set_process_metadata(Metadata2),
    kafine_proc_lib:set_label({?MODULE, Ref}),
    State = #state{ref = Ref, metadata = Metadata2},
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
    Info = #{
        brokers => Brokers,
        topic_partition_nodes => TopicPartitionNodes,
        pending_job_requests => maps:keys(PendingJobRequests),
        topic_partition_states => TopicPartitionStates,
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
        | {request_job, kafine:node_id(), pid()}
        | {complete_job, job_id(), kafine:node_id(),
            kafine_topic_partition_data:t(topic_partition_result())},
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
                Topic, Partition, {ready, FetchRequest}, TopicPartitionStates
            ),
            State1 = State#state{topic_partition_states = NewTopicPartitionStates},
            case maybe_fulfil_job(NodeId, State1) of
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
            case maybe_fulfil_job(NodeId, State1) of
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
                monitor(process, Pid),
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
    case maybe_fulfil_job(NodeId, State1) of
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
    end;
handle_cast(
    {complete_job, JobId, NodeId, TopicPartitionResults},
    State = #state{
        ref = Ref,
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
                (Topic, Partition, {update_offset, NewOffset}) ->
                    case
                        kafine_topic_partition_data:get(
                            Topic, Partition, TopicPartitionStates, undefined
                        )
                    of
                        undefined ->
                            % Not following this topic/partition any more, ignore
                            false;
                        {busy, JobId, {_, CallbackMod, CallbackArg}} ->
                            % Update the offset and mark as not busy
                            {true, {ready, {NewOffset, CallbackMod, CallbackArg}}};
                        _Other ->
                            % Must have been superseded, ignore
                            false
                    end;
                (Topic, Partition, Result) when Result =:= repeat orelse Result =:= give_away ->
                    case
                        kafine_topic_partition_data:get(
                            Topic, Partition, TopicPartitionStates, undefined
                        )
                    of
                        undefined ->
                            % Not following this topic/partition any more, ignore
                            false;
                        {busy, JobId, Request} ->
                            % Return this topic/partition from busy to ready
                            {true, {ready, Request}};
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
            (Topic, Partition, {busy, J, Request}) when J =:= JobId ->
                case kafine_topic_partition_data:is_key(Topic, Partition, TopicPartitionResults) of
                    true ->
                        % Result already handled above
                        false;
                    false ->
                        % Mark as not busy
                        {true, {ready, Request}}
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
                give_away =:= Result andalso
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
    State3 = maybe_fulfil_jobs(State2),
    {noreply, State3}.

handle_info(
    {'DOWN', _Ref, process, Pid, _Reason},
    State = #state{
        ref = Ref,
        topic_partition_states = TopicPartitionStates,
        job_assignments = JobAssignments
    }
) ->
    case maps:take(Pid, JobAssignments) of
        error ->
            ?LOG_DEBUG("Ignoring DOWN for ~p - no associated jobs", [Pid]),
            {noreply, State};
        {JobId, NewJobAssignments} ->
            ?LOG_DEBUG("Job ~p aborted due to node exit", [JobId]),
            telemetry:execute(
                [kafine, fetcher, job_aborted],
                #{},
                #{ref => Ref, job_id => JobId}
            ),
            NewTopicPartitionStates = kafine_topic_partition_data:map(
                fun
                    (_Topic, _Partition, {busy, J, Request}) when J =:= JobId ->
                        % Mark as not busy
                        {ready, Request};
                    (_Topic, _Partition, TopicPartitionState) ->
                        TopicPartitionState
                end,
                TopicPartitionStates
            ),
            NewState = State#state{
                topic_partition_states = NewTopicPartitionStates,
                job_assignments = NewJobAssignments
            },
            {noreply, NewState}
    end;
handle_info(Info, State) ->
    ?LOG_WARNING("Unexpected info: ~p", [Info]),
    {noreply, State}.

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
            fun(Broker = #{node_id := NodeId}, BrokersAcc) ->
                {ok, Pid} = kafine_node_fetcher_sup:start_child(Ref, self(), Broker, Metadata),
                ?LOG_DEBUG("Monitoring new node fetcher ~p for node ~p", [Pid, NodeId]),
                monitor(process, Pid),
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

-spec find_and_dispatch_job(
    Target :: pid(), TopicPartitions :: kafine_topic_partitions:t(), State :: state()
) ->
    {true, NewState :: state()} | false.

maybe_fulfil_jobs(State = #state{pending_job_requests = PendingJobRequests}) ->
    maps:fold(
        fun(NodeId, _Pid, StateAcc) ->
            case maybe_fulfil_job(NodeId, StateAcc) of
                false -> StateAcc;
                {true, NewState} -> NewState
            end
        end,
        State,
        PendingJobRequests
    ).

maybe_fulfil_job(
    NodeId,
    State = #state{
        pending_job_requests = PendingJobRequests,
        node_topic_partitions = NodeTopicPartitions
    }
) ->
    case maps:take(NodeId, PendingJobRequests) of
        error ->
            false;
        {From, NewPendingJobRequests} ->
            TopicPartitions = maps:get(NodeId, NodeTopicPartitions, kafine_topic_partitions:new()),
            case find_and_dispatch_job(From, TopicPartitions, State) of
                false ->
                    false;
                {true, NewState} ->
                    {true, NewState#state{pending_job_requests = NewPendingJobRequests}}
            end
    end.

find_and_dispatch_job(Target, TopicPartitions, State) ->
    case find_list_offsets_job(Target, TopicPartitions, State) of
        {true, NewState} ->
            {true, NewState};
        false ->
            find_fetch_job(Target, TopicPartitions, State)
    end.

find_list_offsets_job(
    Target, TopicPartitions, State = #state{topic_partition_states = TopicPartitionStates}
) ->
    maybe
        true ?= has_list_offsets_job(TopicPartitions, TopicPartitionStates),
        ListOffsetsBody =
            kafine_topic_partitions:filtermap(
                fun(Topic, Partition) ->
                    case kafine_topic_partition_data:get(Topic, Partition, TopicPartitionStates) of
                        {ready, {Offset, _, _}} ->
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
        NewState = dispatch_job(Target, list_offsets, ListOffsetsBody, State),
        {true, NewState}
    end.

-spec has_list_offsets_job(
    TopicPartitions :: kafine_topic_partitions:t(),
    TopicPartitionStates :: kafine_topic_partition_data:t(topic_partition_state())
) -> boolean().

has_list_offsets_job(TopicPartitions, TopicPartitionStates) ->
    Iterator = maps:iterator(TopicPartitions),
    has_list_offsets_job_1(maps:next(Iterator), TopicPartitionStates, false).

has_list_offsets_job_1(none, _TopicPartitionStates, HasListOffsets) ->
    HasListOffsets;
has_list_offsets_job_1({Topic, Partitions, Next}, TopicPartitionStates, HasListOffsets) ->
    case has_list_offsets_job_2(Topic, Partitions, TopicPartitionStates, false) of
        {true, HasListOffsets2} ->
            has_list_offsets_job_1(
                maps:next(Next), TopicPartitionStates, HasListOffsets or HasListOffsets2
            );
        false ->
            false
    end.

has_list_offsets_job_2(_Topic, [], _TopicPartitionStates, HasListOffsets) ->
    {true, HasListOffsets};
has_list_offsets_job_2(Topic, [Partition | Rest], TopicPartitionStates, HasListOffsets) ->
    case kafine_topic_partition_data:get(Topic, Partition, TopicPartitionStates) of
        init ->
            false;
        {ready, {Offset, _, _}} ->
            HasListOffsets2 = HasListOffsets orelse get_job_for_offset(Offset) =:= list_offsets,
            has_list_offsets_job_2(Topic, Rest, TopicPartitionStates, HasListOffsets2);
        _ ->
            has_list_offsets_job_2(Topic, Rest, TopicPartitionStates, HasListOffsets)
    end.

find_fetch_job(
    Target, TopicPartitions, State = #state{topic_partition_states = TopicPartitionStates}
) ->
    maybe
        % Need all the topic partitions to have ready fetches, or be paused.
        % If we don't do this, we might only fetch from quiet partitions, which could significantly
        % delay fetching from busy partitions.
        true ?= has_fetch_job(TopicPartitions, TopicPartitionStates),
        FetchBody = kafine_topic_partitions:filtermap(
            fun(Topic, Partition) ->
                case kafine_topic_partition_data:get(Topic, Partition, TopicPartitionStates) of
                    {ready, Request} ->
                        {true, Request};
                    paused ->
                        false
                end
            end,
            TopicPartitions
        ),
        NewState = dispatch_job(Target, fetch, FetchBody, State),
        {true, NewState}
    end.

-spec has_fetch_job(
    TopicPartitions :: kafine_topic_partitions:t(),
    TopicPartitionStates :: kafine_topic_partition_data:t(topic_partition_state())
) -> boolean().

has_fetch_job(TopicPartitions, TopicPartitionStates) ->
    Iterator = maps:iterator(TopicPartitions),
    has_fetch_job_1(maps:next(Iterator), TopicPartitionStates, false).

has_fetch_job_1(none, _TopicPartitionStates, HasFetch) ->
    HasFetch;
has_fetch_job_1({Topic, Partitions, Next}, TopicPartitionStates, HasFetch) ->
    case has_fetch_job_2(Topic, Partitions, TopicPartitionStates, false) of
        {true, HasFetch2} ->
            has_fetch_job_1(maps:next(Next), TopicPartitionStates, HasFetch or HasFetch2);
        false ->
            false
    end.

has_fetch_job_2(_Topic, [], _TopicPartitionStates, HasFetch) ->
    {true, HasFetch};
has_fetch_job_2(Topic, [Partition | Rest], TopicPartitionStates, HasFetch) ->
    case kafine_topic_partition_data:get(Topic, Partition, TopicPartitionStates) of
        {ready, {Offset, _, _}} ->
            HasFetch2 = HasFetch orelse get_job_for_offset(Offset) =:= fetch,
            has_fetch_job_2(Topic, Rest, TopicPartitionStates, HasFetch2);
        paused ->
            has_fetch_job_2(Topic, Rest, TopicPartitionStates, HasFetch);
        _ ->
            false
    end.

dispatch_job(
    Target,
    JobType,
    JobBody,
    State = #state{
        next_job_id = JobId,
        topic_partition_states = TopicPartitionStates,
        job_assignments = JobAssignments
    }
) ->
    NewTopicPartitionStates = kafine_topic_partition_data:map(
        fun
            (Topic, Partition, TopicPartitionState = {ready, Request}) ->
                case kafine_topic_partition_data:is_key(Topic, Partition, JobBody) of
                    true ->
                        {busy, JobId, Request};
                    false ->
                        TopicPartitionState
                end;
            (_Topic, _Partition, TopicPartitionState) ->
                TopicPartitionState
        end,
        TopicPartitionStates
    ),
    Job = {JobId, JobType, JobBody},
    kafine_node_fetcher:job(Target, Job),
    State#state{
        next_job_id = JobId + 1,
        topic_partition_states = NewTopicPartitionStates,
        job_assignments = JobAssignments#{Target => JobId}
    }.
