-module(kafine_parallel_handler).

-behaviour(gen_server).

-export([
    start_link/4,
    stop/2,
    info/1
]).

-export([resume/4]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    handle_continue/2,
    terminate/2
]).

-export([handle_partition_data/5]).

-export_type([info/0]).

-include_lib("kernel/include/logger.hrl").

id(Ref, TopicPartition) -> {?MODULE, Ref, TopicPartition}.

via(Ref, TopicPartition) ->
    kafine_via:via(id(Ref, TopicPartition)).

-type opts() :: #{
    callback_mod := module(),
    callback_arg := term(),
    skip_empty_fetches := boolean() | after_first,
    error_mode := reset | retry | skip
}.

start_link(Ref, TopicPartition, Offset, Opts) ->
    gen_server:start_link(
        via(Ref, TopicPartition),
        ?MODULE,
        [Ref, TopicPartition, Offset, Opts],
        start_options()
    ).

start_options() ->
    [
        {spawn_opt, [
            % Disable the old heap for this process. This prevents holding references to large
            % binaries after we're done with them
            {fullsweep_after, 0}
        ]},
        {debug, kafine_trace:debug_options(#{mfa => {?MODULE, handle_event, 4}})}
    ].

stop(Pid, Reason) ->
    gen_server:cast(Pid, {stop, Reason}).

-type info() ::
    #{
        topic := kafine:topic(),
        partition := kafine:partition(),
        opts := opts(),
        callback_state := dynamic(),
        status := active | paused,
        next_offset := kafine:offset(),
        skip_empty := boolean()
    }.

-spec info(Pid :: pid()) -> info().

info(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, info).

handle_partition_data({_Pid, true}, _Topic, _PartitionData = #{records := []}, _FetchOffset, _Span) ->
    % Empty fetch, we're skipping those
    repeat;
handle_partition_data({Pid, _}, Topic, PartitionData, FetchOffset, Span) ->
    gen_server:cast(Pid, {partition_data, {Topic, PartitionData, FetchOffset, Span}}).

resume(Ref, Topic, Partition, Offset) ->
    gen_server:cast(via(Ref, {Topic, Partition}), {resume, Offset}).

-record(state, {
    ref :: kafine:consumer_ref(),
    fetcher :: term(),
    topic :: kafine:topic(),
    partition :: kafine:partition(),
    opts :: opts(),
    callback_state :: term(),
    status :: active | paused,
    next_offset :: kafine:offset(),
    skip_empty :: boolean(),
    fetch_span = undefined :: kafine_telemetry:span() | undefined
}).

init([
    Ref,
    {Topic, Partition},
    Offset,
    Opts = #{
        callback_mod := CallbackMod,
        callback_arg := CallbackArg,
        skip_empty_fetches := SkipEmptyFetches
    }
]) ->
    Metadata = #{ref => Ref, topic => Topic, partition => Partition},
    logger:set_process_metadata(Metadata),
    kafine_proc_lib:set_label({?MODULE, {Ref, Topic, Partition}}),
    FetcherPid =
        case kafine_fetcher:whereis(Ref) of
            undefined ->
                exit({no_fetcher, Ref});
            Pid ->
                monitor(process, Pid),
                Pid
        end,

    {CallbackStatus, CallbackState} = callback_init(
        Topic, Partition, CallbackMod, CallbackArg
    ),

    State = #state{
        ref = Ref,
        fetcher = FetcherPid,
        topic = Topic,
        partition = Partition,
        next_offset = Offset,
        skip_empty =
            case SkipEmptyFetches of
                after_first -> false;
                true -> true;
                false -> false
            end,
        opts = Opts,
        callback_state = CallbackState,
        status = CallbackStatus
    },

    State2 = handle_status(CallbackStatus, State),

    {ok, State2}.

callback_init(Topic, PartitionIndex, Callback, CallbackArg) ->
    callback_init_result(Callback:init(Topic, PartitionIndex, CallbackArg)).

callback_init_result({ok, State}) ->
    {active, State};
callback_init_result({pause, State}) ->
    {paused, State};
callback_init_result(Other) ->
    erlang:error({bad_callback_return, Other}).

handle_call(
    info,
    _From,
    State = #state{
        topic = Topic,
        partition = Partition,
        opts = Opts,
        callback_state = CallbackState,
        status = Status,
        next_offset = NextOffset,
        skip_empty = SkipEmpty
    }
) ->
    Info = #{
        topic => Topic,
        partition => Partition,
        opts => Opts,
        callback_state => CallbackState,
        status => Status,
        next_offset => NextOffset,
        skip_empty => SkipEmpty
    },
    {reply, Info, State};
handle_call(Request, From, State) ->
    ?LOG_WARNING("Unexpected call: ~p from ~p", [Request, From]),
    {reply, {error, unexpected_call}, State}.

handle_cast(
    {partition_data, {Topic, PartitionData, FetchOffset, _Span}},
    State = #state{
        topic = Topic,
        partition = Partition,
        opts = #{
            callback_mod := CallbackMod,
            skip_empty_fetches := SkipEmptyFetches,
            error_mode := ErrorMode
        },
        ref = Ref,
        callback_state = CallbackState1,
        fetch_span = Span
    }
) ->
    NextOffset = kafine_fetch_response_partition_data:find_next_offset(PartitionData),
    Measurements0 = maps:with(
        [high_watermark, last_stable_offset, log_start_offset], PartitionData
    ),
    Measurements =
        case NextOffset of
            undefined -> Measurements0#{next_offset => FetchOffset};
            N when is_number(N) -> Measurements0#{next_offset => NextOffset}
        end,

    kafine_telemetry:stop_span([kafine, parallel_handler, fetch], Span, Measurements, #{}),

    case ErrorMode of
        retry ->
            kafine_parallel_subscription_impl:set_restart_offset(
                Ref, {Topic, Partition}, FetchOffset
            );
        skip ->
            case NextOffset of
                undefined ->
                    ok;
                _ ->
                    kafine_parallel_subscription_impl:set_restart_offset(
                        Ref, {Topic, Partition}, NextOffset
                    )
            end;
        _ ->
            ok
    end,

    % NextFetchOffset may differ from NextOffset here:
    % NextOffset is the offset after the last offset of the last batch in PartitionData (or undefined if no records)
    % NextFetchOffset may be less than NextOffset if the callback pauses part way through processing
    {NextFetchOffset, CallbackStatus, CallbackState2} = kafine_fetch_response_partition_data:fold(
        Topic, PartitionData, FetchOffset, CallbackMod, CallbackState1
    ),

    SkipEmpty =
        case SkipEmptyFetches of
            false -> false;
            _ -> true
        end,
    State2 = State#state{
        next_offset = NextFetchOffset,
        callback_state = CallbackState2,
        fetch_span = undefined,
        skip_empty = SkipEmpty
    },
    State3 = handle_status(CallbackStatus, State2),
    case CallbackStatus of
        paused ->
            % if we just paused we're unlikely to unpause imminently. We should hibernate
            {noreply, State3, hibernate};
        active ->
            % We've just handled a fetch, which means we've handled a potentially hefty message. We've then
            % invoked a callback that potentially mutates a state for every single message in that fetch.
            % We potentially have a lot of temporary data that we can free up right now. We also know this
            % process is not going to receive any records for a little while, because we've only just
            % requested the next batch. So run a garbage collect right now. This also helps us get closer to
            % a fullsweep which is handy because we're prone to moving the actual partition data, and its
            % references to large binaries, onto the old heap.
            %
            % Note that we only do this if we're not paused. Pause should use hibernate instead
            {noreply, State3, {continue, gc}}
    end;
handle_cast(
    {resume, Offset},
    State = #state{ref = Ref, topic = Topic, partition = Partition, next_offset = NextOffset}
) ->
    NextOffset2 =
        case Offset of
            keep_current_offset -> NextOffset;
            _ -> Offset
        end,
    telemetry:execute(
        [kafine, parallel_handler, resume],
        #{next_offset => NextOffset2},
        #{ref => Ref, topic => Topic, partition => Partition}
    ),
    State2 = State#state{next_offset = NextOffset2},
    State3 = handle_status(active, State2),
    {noreply, State3};
handle_cast(
    {stop, Reason},
    State
) ->
    {stop, Reason, State};
handle_cast(Request, State) ->
    ?LOG_WARNING("Unexpected cast: ~p", [Request]),
    {noreply, State}.

handle_info({'DOWN', _Ref, process, Fetcher, Reason}, State = #state{fetcher = Fetcher}) ->
    % Fetcher process died, we should go down with it
    ?LOG_WARNING("Fetcher process died (~p), stopping handler", [Reason]),
    {stop, Reason, State};
handle_info(Info, State) ->
    ?LOG_WARNING("Unexpected info: ~p", [Info]),
    {noreply, State}.

handle_continue(gc, State) ->
    % elp:ignore W0047 (no_garbage_collect)
    erlang:garbage_collect(self(), [{type, minor}]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

handle_status(
    active,
    State = #state{
        ref = Ref,
        fetcher = Fetcher,
        topic = Topic,
        partition = Partition,
        next_offset = Offset,
        skip_empty = SkipEmpty
    }
) ->
    ?LOG_DEBUG("Fetching ~s/~p at offset ~p", [Topic, Partition, Offset]),
    ok = kafine_fetcher:fetch(
        Fetcher, Topic, Partition, Offset, ?MODULE, {self(), SkipEmpty}
    ),
    Span = kafine_telemetry:start_span(
        [kafine, parallel_handler, fetch],
        #{fetch_offset => Offset},
        #{
            ref => Ref,
            topic => Topic,
            partition => Partition,
            known_offset => is_number(Offset) andalso Offset >= 0
        }
    ),
    State#state{status = active, fetch_span = Span};
handle_status(
    paused,
    State = #state{
        ref = Ref,
        fetcher = Fetcher,
        topic = Topic,
        partition = Partition,
        next_offset = NextOffset
    }
) ->
    ?LOG_DEBUG("Paused"),
    ok = kafine_fetcher:pause(Fetcher, Topic, Partition),
    telemetry:execute(
        [kafine, parallel_handler, pause],
        #{next_offset => NextOffset},
        #{ref => Ref, topic => Topic, partition => Partition}
    ),
    State#state{status = paused}.
