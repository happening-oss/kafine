-module(kafine_metadata_cache).

-feature(maybe_expr, enable).

-behaviour(gen_statem).

-export([
    partitions/2,
    brokers/1,
    refresh/2,

    info/1
]).

-export([
    start_link/1,
    stop/1
]).

-export([
    init/1,
    callback_mode/0,
    handle_event/4,
    terminate/3
]).

-include_lib("kernel/include/logger.hrl").

-type topic_partition_metadata() ::
    #{leader := kafine:node_id(), replicas := [kafine:node_id()], isr := [kafine:node_id()]}.

-spec partitions(Ref :: ref(), TopicOrTopics :: binary() | [binary()]) ->
    #{kafine:topic() => #{kafine:partition() => topic_partition_metadata()}}.

partitions(Ref, Topic) when is_binary(Topic) ->
    partitions(Ref, [Topic]);
partitions(Ref, Topics) when is_list(Topics) ->
    TopicPartitionData0 = lookup_partitions(Ref, Topics),
    TopicCount = length(Topics),
    case maps:size(TopicPartitionData0) of
        TopicCount ->
            TopicPartitionData0;
        _ ->
            refresh(Ref, Topics),
            lookup_partitions(Ref, Topics)
    end.

-spec brokers(Ref :: ref()) -> [kafine:broker()].

brokers(Ref) ->
    case lookup(Ref, brokers) of
        {error, not_found} ->
            refresh(Ref, []),
            lookup(Ref, brokers);
        Brokers ->
            Brokers
    end.

-spec refresh(Ref :: ref(), Topics :: [kafine:topic()]) -> ok.

refresh(Ref, Topics) ->
    ok = gen_statem:call(via(Ref), {refresh, Topics}).

info(Ref) ->
    Table = persistent_term:get(table(Ref)),
    #{
        table => Table,
        brokers => lookup(Ref, brokers),
        partitions => lookup(Ref, topic_partitions)
    }.

lookup_partitions(Ref, Topics) ->
    case lookup(Ref, topic_partitions) of
        {error, not_found} ->
            #{};
        TopicPartitionData ->
            maps:with(Topics, TopicPartitionData)
    end.

table(Ref) ->
    {?MODULE, Ref}.

lookup(Ref, Key) ->
    Table = persistent_term:get(table(Ref)),
    case ets:lookup(Table, Key) of
        [] -> {error, not_found};
        [{Key, Value}] -> Value
    end.

-type ref() :: term().

id(Ref) -> {?MODULE, Ref}.

via(Ref) ->
    kafine_via:via(id(Ref)).

-spec start_link(ref()) -> gen_statem:start_ret().

start_link(Ref) ->
    gen_statem:start_link(via(Ref), ?MODULE, [Ref], start_options()).

start_options() ->
    [{debug, kafine_trace:debug_options(#{mfa => {?MODULE, handle_event, 4}})}].

-spec stop(RefOrPid :: ref() | pid()) -> ok.

stop(Pid) when is_pid(Pid) ->
    gen_statem:stop(Pid);
stop(Ref) ->
    gen_statem:stop(via(Ref)).

callback_mode() ->
    [handle_event_function, state_enter].

-record(data, {
    ref :: ref(),
    table :: ets:table(),
    req_id :: kafine_bootstrap:request_id() | undefined,
    reply_to = [] :: [gen_statem:from()],
    span = undefined :: kafine_telemetry:span() | undefined
}).

init([Ref]) ->
    Metadata = #{ref => Ref},
    logger:set_process_metadata(Metadata),
    kafine_proc_lib:set_label({?MODULE, Ref}),
    % I considered using persistent_term instead of an ETS here, but we don't actually read the
    % cache often enough for persistent_term's upside - very fast reads - to outweigh its downside -
    % having to scan every process for the term on the rare occasions it changes/gets deleted
    Table = ets:new(?MODULE, [set, protected]),
    % We can't use named_table because there might be multiple caches, and the table name must be an
    % atom so we can't put Ref in it. Instead, store the tid in a persistent term keyed by Ref.
    persistent_term:put(table(Ref), Table),
    {ok, ready, #data{ref = Ref, table = Table}}.

handle_event(enter, _, ready, _) ->
    keep_state_and_data;
handle_event({call, From}, {refresh, Topics}, ready, Data) ->
    {next_state, {refresh, Topics}, Data#data{reply_to = [From]}};
handle_event(enter, _, {refresh, Topics}, Data = #data{ref = Ref}) ->
    ?LOG_DEBUG("Refreshing metadata for topics ~p", [Topics]),
    Span = kafine_telemetry:start_span([kafine, metadata, refresh], #{
        topics => Topics, ref => Ref
    }),
    ReqId = kafine_bootstrap:get_metadata(Ref, Topics),
    {keep_state, Data#data{req_id = ReqId, span = Span}};
handle_event(
    info,
    Msg,
    {refresh, _},
    Data = #data{table = Table, req_id = ReqId, reply_to = ReplyTo, span = Span}
) ->
    case kafine_bootstrap:check_response(Msg, ReqId) of
        {reply, {Brokers, TopicPartitionData}} ->
            ets:insert(Table, {brokers, Brokers}),
            case ets:lookup(Table, topic_partitions) of
                [] ->
                    ets:insert(Table, {topic_partitions, TopicPartitionData});
                [{topic_partitions, ExistingTopicPartitions}] ->
                    NewTopicPartitions = maps:merge(ExistingTopicPartitions, TopicPartitionData),
                    ets:insert(Table, {topic_partitions, NewTopicPartitions})
            end,
            kafine_telemetry:stop_span([kafine, metadata, refresh], Span),
            NewData = Data#data{req_id = undefined, reply_to = [], span = undefined},
            {next_state, ready, NewData, reply(ok, ReplyTo)};
        {error, {Reason, _}} ->
            ?LOG_WARNING("Failed to refresh metadata: ~p", [Reason]),
            NewData = Data#data{req_id = undefined, reply_to = []},
            {next_state, ready, NewData, reply({error, Reason}, ReplyTo)};
        no_reply ->
            ?LOG_WARNING("Unexpected message: ~p", [Msg]),
            keep_state_and_data
    end;
handle_event(
    {call, From},
    {refresh, NewTopics},
    {refresh, CurrentlyRefreshingTopics},
    Data = #data{reply_to = ReplyTo}
) ->
    case NewTopics -- CurrentlyRefreshingTopics of
        [] ->
            % Requested refresh is covered by an active refresh request, respond when that refresh is done
            {keep_state, Data#data{reply_to = [From | ReplyTo]}};
        _ ->
            % This will require a new metadata request, postpone the request until the current one is finished
            {keep_state_and_data, postpone}
    end.

reply(Result, To) when is_list(To) ->
    [{reply, Target, Result} || Target <- To].

terminate(_Reason, _State, Ref) ->
    persistent_term:erase(table(Ref)).
