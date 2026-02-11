-module(kafine_parallel_subscription_impl).

-behaviour(gen_server).
-behaviour(kafine_resumer).

-export([
    start_link/2,
    stop/1,
    whereis/1,
    info/1
]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).
-export([resume/4]).
-export([set_restart_offset/3]).
-export([
    subscribe_partitions/2,
    unsubscribe_all/1
]).

-ifdef(TEST).
-export([check_restart/2]).
-endif.

-include_lib("kernel/include/logger.hrl").

-define(CHILD_EXIT_TIMEOUT_MS, 10_000).
-define(CHILD_RESTART_MAX_INTENSITY, 1).
-define(CHILD_RESTART_MAX_PERIOD_MS, 5_000).

-type ref() :: term().

id(Ref) -> {?MODULE, Ref}.

via(Ref) ->
    kafine_via:via(id(Ref)).

-spec whereis(Ref :: ref()) -> pid() | undefined.

whereis(Ref) ->
    kafine_via:whereis_name(id(Ref)).

-spec start_link(
    Ref :: kafine:consumer_ref(), Options :: kafine_parallel_subscription_callback:options()
) ->
    gen_server:start_ret().

start_link(Ref, Options) ->
    gen_server:start_link(via(Ref), ?MODULE, [Ref, Options], start_options()).

start_options() ->
    [{debug, kafine_trace:debug_options(#{mfa => {?MODULE, handle_event, 4}})}].

stop(Pid) when is_pid(Pid) ->
    monitor(process, Pid),
    exit(Pid, normal),
    receive
        {'DOWN', _, process, Pid, _} -> ok
    end.

-spec info(RefOrPid :: ref() | pid()) ->
    #{
        topic_options := #{kafine:topic() => kafine:topic_options()},
        callback_mod := module(),
        callback_arg := dynamic(),
        offset_callback := module(),
        children := #{pid() => kafine_parallel_handler:info()}
    }.

info(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, info);
info(Ref) ->
    gen_server:call(via(Ref), info).

-spec subscribe_partitions(
    RefOrPid :: ref() | pid(), AssignedPartitions :: kafine_topic_partitions:t()
) -> ok.

subscribe_partitions(RefOrPid, AssignedPartitions) ->
    cast(RefOrPid, {subscribe_partitions, AssignedPartitions}).

-spec unsubscribe_all(RefOrPid :: ref() | pid()) -> ok.

unsubscribe_all(RefOrPid) ->
    cast(RefOrPid, unsubscribe_all).

-spec cast(RefOrPid :: ref() | pid(), Msg :: term()) -> ok.

cast(Pid, Msg) when is_pid(Pid) ->
    gen_server:cast(Pid, Msg);
cast(Ref, Msg) ->
    gen_server:cast(via(Ref), Msg).

resume(Ref, Topic, Partition, Offset) ->
    kafine_parallel_handler:resume(Ref, Topic, Partition, Offset).

-spec set_restart_offset(
    RefOrPid :: ref() | pid(),
    TopicPartition :: {kafine:topic(), kafine:partition()},
    Offset :: kafine:offset()
) -> ok.

set_restart_offset(RefOrPid, TopicPartition, Offset) ->
    cast(RefOrPid, {set_restart_offset, TopicPartition, Offset}).

-type topic_partition() :: {kafine:topic(), kafine:partition()}.
-record(state, {
    ref :: term(),
    topic_options :: #{kafine:topic() => kafine:topic_options()},
    callback_mod :: module(),
    callback_arg :: term(),
    offset_callback :: module(),
    skip_empty_fetches :: boolean() | after_first,
    error_mode :: reset | retry | skip,
    topic_partition_handlers = #{} :: #{pid() => topic_partition()},
    restarts = [] :: [{topic_partition(), non_neg_integer()}],
    restart_offsets = #{} :: #{topic_partition() => kafine:offset()},
    req_ids = kafine_coordinator:reqids_new() :: kafine_coordinator:req_ids()
}).

init([
    Ref,
    #{
        topic_options := TopicOptions,
        callback_mod := CallbackMod,
        callback_arg := CallbackArg,
        offset_callback := OffsetCallback,
        skip_empty_fetches := SkipEmptyFetches,
        error_mode := ErrorMode
    }
]) ->
    process_flag(trap_exit, true),
    Metadata = #{ref => Ref},
    logger:set_process_metadata(Metadata),
    kafine_proc_lib:set_label({?MODULE, Ref}),
    ok = kafine_resumer:register(Ref, ?MODULE),

    % Link to the fetcher. We'd need to re-call set_topic_partitions if it dies, and if we die all
    % its state is bogus
    case kafine_fetcher:whereis(Ref) of
        undefined ->
            ?LOG_ERROR("No fetcher found for ref ~p", [Ref]),
            {stop, no_fetcher};
        FetcherPid ->
            link(FetcherPid),

            {ok, #state{
                ref = Ref,
                topic_options = TopicOptions,
                callback_mod = CallbackMod,
                callback_arg = CallbackArg,
                offset_callback = OffsetCallback,
                skip_empty_fetches = SkipEmptyFetches,
                error_mode = ErrorMode
            }}
    end.

handle_call(
    info,
    _From,
    State = #state{
        topic_options = TopicOptions,
        callback_mod = CallbackMod,
        callback_arg = CallbackArg,
        offset_callback = OffsetCallback,
        topic_partition_handlers = TopicPartitionHandlers
    }
) ->
    Info = #{
        topic_options => TopicOptions,
        callback_mod => CallbackMod,
        callback_arg => CallbackArg,
        offset_callback => OffsetCallback,
        children => maps:map(
            fun(Pid, _) -> kafine_parallel_handler:info(Pid) end, TopicPartitionHandlers
        )
    },
    {reply, Info, State};
handle_call(Request, _From, State) ->
    ?LOG_WARNING("Unexpected call: ~p", [Request]),
    {reply, {error, unexpected_call}, State}.

handle_cast(
    {set_restart_offset, TopicPartition, Offset},
    State = #state{restart_offsets = RestartOffsets}
) ->
    NewRestartOffsets = maps:put(TopicPartition, Offset, RestartOffsets),
    {noreply, State#state{restart_offsets = NewRestartOffsets}};
handle_cast(
    {subscribe_partitions, AssignedPartitions},
    State = #state{ref = Ref}
) ->
    kafine_fetcher:set_topic_partitions(Ref, AssignedPartitions),
    NewState = get_offset_and_start(AssignedPartitions, State),
    {noreply, NewState};
handle_cast(
    unsubscribe_all,
    State = #state{topic_partition_handlers = Handlers}
) ->
    maps:foreach(
        fun(Pid, _) ->
            unlink(Pid),
            kafine_parallel_handler:stop(Pid, normal)
        end,
        Handlers
    ),
    % TODO: not yet implemented
    {noreply, State#state{topic_partition_handlers = #{}}};
handle_cast(Request, State) ->
    ?LOG_WARNING("Unexpected cast: ~p", [Request]),
    {noreply, State}.

handle_info(
    {'EXIT', Pid, Reason},
    State = #state{
        topic_partition_handlers = Handlers,
        restarts = Restarts
    }
) ->
    case maps:take(Pid, Handlers) of
        {TopicPartition = {Topic, Partition}, Handlers2} ->
            {Continue, NewRestarts} = check_restart(TopicPartition, Restarts),
            State1 = State#state{
                topic_partition_handlers = Handlers2,
                restarts = NewRestarts
            },
            case Continue of
                true ->
                    ?LOG_DEBUG("Proc for ~p exited with ~p, restarting", [TopicPartition, Reason]),
                    RequiredOffsets = kafine_topic_partitions:single(Topic, Partition),
                    State2 = get_offset_and_start(RequiredOffsets, State1),
                    {noreply, State2};
                false ->
                    ?LOG_ERROR("Max restart intensity exceeded for ~p, shutting down", [
                        TopicPartition
                    ]),
                    {stop, shutdown, State1}
            end;
        error ->
            % Another linked pid exited. kafine_parallel_subscription_callback links us to the
            % subscriber, and we link to kafine_fetcher. It should be one of those, and we should
            % exit
            {stop, Reason, State}
    end;
handle_info(
    Msg,
    State = #state{req_ids = ReqIds}
) ->
    case kafine_coordinator:check_response(Msg, ReqIds) of
        Result when Result =:= no_request; Result =:= no_reply ->
            ?LOG_WARNING("Unexpected message ~p", [Msg]),
            {noreply, State};
        {{reply, {ok, Offsets}}, _Request, NewReqIds} ->
            NewHandlers = handle_offsets(Offsets, State),
            {noreply, State#state{req_ids = NewReqIds, topic_partition_handlers = NewHandlers}}
    end.

check_restart(TopicPartition, Restarts) ->
    % Clean restarts older than max period
    Now = erlang:monotonic_time(millisecond),
    CleanedRestarts = lists:filter(
        fun({_TP, Timestamp}) ->
            Timestamp + ?CHILD_RESTART_MAX_PERIOD_MS > Now
        end,
        Restarts
    ),
    % Add a new restart for this topic partition
    NewRestart = {TopicPartition, erlang:monotonic_time(millisecond)},
    NewRestarts = [NewRestart | CleanedRestarts],
    % Count restarts for this topic partition
    Count = lists:foldl(
        fun
            ({TP, _}, Acc) when TP =:= TopicPartition -> Acc + 1;
            (_, Acc) -> Acc
        end,
        0,
        NewRestarts
    ),
    {Count =< ?CHILD_RESTART_MAX_INTENSITY, NewRestarts}.

get_offset_and_start(
    TopicPartitions,
    State = #state{
        ref = Ref,
        topic_options = TopicOptions,
        callback_mod = CallbackMod,
        callback_arg = CallbackArg,
        skip_empty_fetches = SkipEmptyFetches,
        error_mode = ErrorMode,
        topic_partition_handlers = Handlers,
        restart_offsets = RestartOffsets,
        req_ids = ReqIds
    }
) ->
    HaveOffsetFetcher = kafine_coordinator:exists(Ref),
    {NewHandlers, NeedFetch} =
        kafine_topic_partitions:fold(
            fun(Topic, Partition, {NewHandlers, NeedFetch}) ->
                TopicPartition = {Topic, Partition},
                case
                    get_restart_offset(
                        TopicPartition, RestartOffsets, TopicOptions, HaveOffsetFetcher
                    )
                of
                    {ok, Offset} ->
                        % Don't need to fetch an offset for this partition, start handler now
                        Opts = #{
                            callback_mod => CallbackMod,
                            callback_arg => CallbackArg,
                            skip_empty_fetches => SkipEmptyFetches,
                            error_mode => ErrorMode
                        },
                        {ok, Pid} = kafine_parallel_handler:start_link(
                            Ref, {Topic, Partition}, Offset, Opts
                        ),
                        {[{Pid, {Topic, Partition}} | NewHandlers], NeedFetch};
                    fetch ->
                        {NewHandlers, kafine_topic_partitions:add(Topic, Partition, NeedFetch)}
                end
            end,
            {[], kafine_topic_partitions:new()},
            TopicPartitions
        ),
    % Fetch offsets for topic partitions restarting from committed
    ReqIds2 =
        case kafine_topic_partitions:is_empty(NeedFetch) of
            true ->
                ReqIds;
            false ->
                kafine_coordinator:offset_fetch(Ref, NeedFetch, ReqIds)
        end,
    % Clear any restart offsets we used
    RestartOffsets2 = maps:filter(
        fun({Topic, Partition}, _Offset) ->
            not kafine_topic_partitions:member(Topic, Partition, TopicPartitions)
        end,
        RestartOffsets
    ),
    NewHandlers2 = maps:merge(Handlers, maps:from_list(NewHandlers)),
    State#state{
        topic_partition_handlers = NewHandlers2,
        restart_offsets = RestartOffsets2,
        req_ids = ReqIds2
    }.

get_restart_offset(
    TopicPartition = {Topic, _},
    RestartOffsets,
    TopicOptions,
    HaveOffsetFetcher
) ->
    case maps:get(TopicPartition, RestartOffsets, undefined) of
        undefined ->
            case HaveOffsetFetcher of
                true ->
                    fetch;
                false ->
                    Options = maps:get(Topic, TopicOptions),
                    InitialOffset = maps:get(initial_offset, Options),
                    {ok, InitialOffset}
            end;
        Offset ->
            {ok, Offset}
    end.

handle_offsets(
    Offsets,
    #state{
        ref = Ref,
        topic_options = TopicOptions,
        callback_mod = CallbackMod,
        callback_arg = CallbackArg,
        skip_empty_fetches = SkipEmptyFetches,
        error_mode = ErrorMode,
        offset_callback = OffsetCallback,
        topic_partition_handlers = Handlers
    }
) ->
    % TODO: Do we need to handle missing offsets being in requests but not responses?
    kafine_topic_partition_data:fold(
        fun(Topic, Partition, Offset0, HandlersAcc) ->
            TopicPartition = {Topic, Partition},
            Offset1 =
                case Offset0 of
                    -1 ->
                        Options = maps:get(Topic, TopicOptions),
                        maps:get(initial_offset, Options);
                    Offset ->
                        OffsetCallback:adjust_committed_offset(Offset)
                end,
            Opts = #{
                callback_mod => CallbackMod,
                callback_arg => CallbackArg,
                skip_empty_fetches => SkipEmptyFetches,
                error_mode => ErrorMode
            },
            {ok, Pid} = kafine_parallel_handler:start_link(
                Ref, TopicPartition, Offset1, Opts
            ),
            maps:put(Pid, TopicPartition, HandlersAcc)
        end,
        Handlers,
        Offsets
    ).

terminate(Reason, #state{ref = Ref, topic_partition_handlers = Handlers}) ->
    maybe_unlink_fetcher(Reason, Ref),
    kafine_resumer:unregister(Ref),
    exit_all_children(Reason, maps:keys(Handlers)),
    ok.

% If we're doing a graceful shutdown, unlink from the fetcher so that it stops gracefully
maybe_unlink_fetcher(_Reason = shutdown, Ref) ->
    case kafine_fetcher:whereis(Ref) of
        undefined -> ok;
        FetcherPid -> unlink(FetcherPid)
    end;
maybe_unlink_fetcher({shutdown, _}, Ref) ->
    maybe_unlink_fetcher(shutdown, Ref);
maybe_unlink_fetcher(_Reason, _Ref) ->
    ok.

exit_all_children(Reason, Children) ->
    % send an exit to all the children in parallel
    lists:foreach(fun(Pid) -> exit(Pid, {shutdown, Reason}) end, Children),
    % Set a timer after which we brutal_kill any remaining children
    timer:send_after(?CHILD_EXIT_TIMEOUT_MS, kill_remaining_children),
    % wait for the children to exit
    await_children_exit(Children).

await_children_exit([]) ->
    ok;
await_children_exit(Children) ->
    receive
        {'EXIT', Pid, _} ->
            await_children_exit(lists:delete(Pid, Children));
        kill_remaining_children ->
            lists:foreach(fun(Pid) -> exit(Pid, brutal_kill) end, Children)
    end.
