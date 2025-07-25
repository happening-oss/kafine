-module(kafine_node_consumer).
-moduledoc false.
-export([
    start_link/5,
    stop/1,

    info/1,

    subscribe/3,
    unsubscribe/2,
    unsubscribe_all/1,

    resume/3,
    resume/4,

    continue/6
]).
-behaviour(gen_statem).
-export([
    init/1,
    callback_mode/0,
    handle_event/4,
    terminate/3
]).
-export_type([
    start_ret/0
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("kafcod/include/error_code.hrl").
-include_lib("kafcod/include/api_key.hrl").
-include("kafine_topic_partition_state.hrl").

-type start_ret() :: gen_statem:start_ret().
-spec start_link(
    Ref :: kafine_consumer:ref(),
    Broker :: kafine:broker(),
    ConnectionOptions :: kafine:connection_options(),
    ConsumerOptions :: kafine:consumer_options(),
    Owner :: pid()
) ->
    start_ret().

start_link(
    Ref,
    Broker = #{host := _, port := _, node_id := _},
    ConnectionOptions,
    ConsumerOptions,
    Owner
) ->
    gen_statem:start_link(
        ?MODULE,
        [
            Ref,
            Broker,
            ConnectionOptions,
            ConsumerOptions,
            Owner
        ],
        start_options()
    ).

start_options() -> [{debug, kafine_trace:debug_options(#{mfa => {?MODULE, handle_event, 4}})}].

stop(Pid) ->
    gen_statem:stop(Pid).

info(Pid) ->
    call(Pid, info).

subscribe(Pid, TopicPartitionStates, TopicOptions) ->
    call(Pid, {subscribe, {TopicPartitionStates, TopicOptions}}).

unsubscribe_all(Pid) ->
    call(Pid, unsubscribe_all).

unsubscribe(Pid, TopicPartitions) ->
    call(Pid, {unsubscribe, TopicPartitions}).

resume(Pid, Topic, Partition) ->
    resume(Pid, Topic, Partition, keep_current_offset).

resume(Pid, Topic, Partition, Offset) ->
    call(Pid, {resume, {Topic, Partition, Offset}}).

continue(Pid, Topic, Partition, NextOffset, NextState, Span) ->
    call(Pid, {continue, {Topic, Partition, NextOffset, NextState, Span}}).

call(Pid, Request) ->
    gen_statem:call(Pid, Request).

callback_mode() ->
    [handle_event_function].

-record(state, {
    metadata :: telemetry:event_metadata(),
    broker :: kafine:broker(),
    connection :: kafine:connection() | undefined,
    connection_options :: kafine:connection_options(),

    consumer_options :: kafine:consumer_options(),

    topic_options :: #{kafine:topic() => kafine:topic_options()},
    topic_partition_states :: kafine_consumer:topic_partition_states(),
    subscribe_epoch :: non_neg_integer(),

    owner :: pid(),
    next_actions,
    req_ids :: kafine_connection:request_id_collection()
}).

init([
    Ref,
    Broker = #{node_id := NodeId},
    ConnectionOptions,
    ConsumerOptions,
    Owner
]) ->
    process_flag(trap_exit, true),
    Metadata = #{ref => Ref, node_id => NodeId},
    logger:set_process_metadata(Metadata),
    kafine_proc_lib:set_label({?MODULE, {Ref, NodeId}}),

    StateData = #state{
        metadata = Metadata,
        broker = Broker,
        connection = undefined,
        connection_options = ConnectionOptions,

        consumer_options = ConsumerOptions,

        topic_options = #{},
        topic_partition_states = #{},
        subscribe_epoch = 0,

        owner = Owner,
        next_actions = [],
        req_ids = kafine_connection:reqids_new()
    },
    {ok, disconnected, StateData, [{next_event, internal, connect}]}.

handle_event(
    internal,
    connect,
    _State = disconnected,
    StateData = #state{
        broker = Broker,
        connection_options = ConnectionOptions,
        metadata = Metadata
    }
) ->
    {ok, Connection} = kafine_connection:start_link(Broker, ConnectionOptions),
    telemetry:execute([kafine, node_consumer, connected], #{}, Metadata),
    StateData2 = StateData#state{connection = Connection},
    {next_state, idle, StateData2, [{next_event, internal, continue}]};
handle_event(
    internal,
    continue,
    State,
    StateData
) ->
    handle_continue(State, StateData);
handle_event(
    {call, From},
    info,
    State,
    StateData
) ->
    Info = build_info(State, StateData),
    {keep_state_and_data, [{reply, From, Info}]};
handle_event(
    {call, From},
    {subscribe, {TopicPartitionStates1, TopicOptions1}},
    State,
    StateData = #state{
        topic_partition_states = TopicPartitionStates0,
        topic_options = TopicOptions0,
        subscribe_epoch = SubscribeEpoch,
        next_actions = NextActions0,
        metadata = Metadata
    }
) ->
    TopicPartitionStates2 = kafine_topic_partition_states:merge_topic_partition_states(
        TopicPartitionStates0, TopicPartitionStates1
    ),
    TopicOptions2 = kafine_topic_options:merge_options(TopicOptions0, TopicOptions1),
    NextActions = add_next_actions(TopicPartitionStates2, NextActions0),
    StateData2 = StateData#state{
        topic_partition_states = TopicPartitionStates2,
        subscribe_epoch = SubscribeEpoch + 1,
        topic_options = TopicOptions2,
        next_actions = NextActions
    },
    PartitionCount = kafine_topic_partition_states:count(TopicPartitionStates2),
    telemetry:execute(
        [kafine, node_consumer, subscribe], #{partition_count => PartitionCount}, Metadata
    ),
    reply_and_continue(State, StateData2, {reply, From, ok});
handle_event(
    {call, From},
    unsubscribe_all,
    State,
    StateData = #state{
        subscribe_epoch = SubscribeEpoch
    }
) ->
    StateData2 = StateData#state{
        topic_partition_states = #{}, subscribe_epoch = SubscribeEpoch + 1
    },
    reply_and_continue(State, StateData2, {reply, From, ok});
handle_event(
    {call, From},
    {unsubscribe, TopicPartitions},
    State,
    StateData = #state{
        topic_partition_states = TopicPartitionStates0,
        subscribe_epoch = SubscribeEpoch
    }
) ->
    TopicPartitionStates1 = kafine_topic_partition_states:remove(
        TopicPartitions, TopicPartitionStates0
    ),
    StateData2 = StateData#state{
        topic_partition_states = TopicPartitionStates1, subscribe_epoch = SubscribeEpoch + 1
    },
    reply_and_continue(State, StateData2, {reply, From, ok});
handle_event(
    {call, From},
    {resume, {Topic, Partition, Offset}},
    State,
    StateData = #state{
        topic_partition_states = TopicPartitionStates,
        next_actions = NextActions0
    }
) ->
    case kafine_maps:get([Topic, Partition], TopicPartitionStates, undefined) of
        undefined ->
            % Bad (Topic, Partition) key
            reply_and_continue(
                State, StateData, {reply, From, {error, {badkey, [Topic, Partition]}}}
            );
        _ ->
            % Found it; mark it as active.
            TopicPartitionStates2 = kafine_maps:update_with(
                [Topic, Partition],
                fun
                    (PartitionState) when Offset =:= keep_current_offset ->
                        PartitionState#topic_partition_state{state = active};
                    (PartitionState) ->
                        PartitionState#topic_partition_state{state = active, offset = Offset}
                end,
                TopicPartitionStates
            ),
            NextActions = add_next_actions(TopicPartitionStates2, NextActions0),
            StateData2 = StateData#state{
                topic_partition_states = TopicPartitionStates2,
                next_actions = NextActions
            },
            reply_and_continue(State, StateData2, {reply, From, ok})
    end;
handle_event(
    {call, From},
    {continue, {Topic, Partition, NextOffset, NextState, Span}},
    State,
    StateData = #state{topic_partition_states = TopicPartitionStates}
) ->
    case kafine_maps:get([Topic, Partition], TopicPartitionStates, undefined) of
        undefined ->
            % Bad (Topic, Partition) key; we unsubscribed; ignore it.
            reply_and_continue(State, StateData, {reply, From, ok});
        _ ->
            % Found it; update it.
            TopicPartitionStates2 = kafine_maps:update_with(
                [Topic, Partition],
                fun(PartitionState = #topic_partition_state{}) ->
                    kafine_telemetry:stop_span(
                        [kafine, fetch, partition_data], Span, #{next_offset => NextOffset}, #{}
                    ),
                    PartitionState#topic_partition_state{state = NextState, offset = NextOffset}
                end,
                TopicPartitionStates
            ),
            StateData2 = StateData#state{
                topic_partition_states = TopicPartitionStates2
            },
            reply_and_continue(State, StateData2, {reply, From, ok})
    end;
handle_event(
    internal,
    fetch,
    _State = active,
    StateData = #state{
        connection = Connection,
        topic_partition_states = TopicPartitionStates,
        subscribe_epoch = SubscribeEpoch,
        consumer_options = ConsumerOptions,
        req_ids = ReqIds,
        metadata = Metadata
    }
) ->
    FetchRequest = kafine_fetch_request:build_fetch_request(TopicPartitionStates, ConsumerOptions),
    ?LOG_DEBUG("Fetching ~p", [summarise_fetch_request(FetchRequest)]),
    telemetry:execute([kafine, node_consumer, fetch], #{}, Metadata),
    ReqIds2 = kafine_connection:send_request(
        Connection,
        % Wireshark only supports up to v11.
        fun fetch_request:encode_fetch_request_11/1,
        FetchRequest,
        fun fetch_response:decode_fetch_response_11/1,
        {fetch, SubscribeEpoch},
        ReqIds,
        kafine_request_telemetry:request_labels(?FETCH, 11)
    ),
    {next_state, fetch, StateData#state{req_ids = ReqIds2}};
handle_event(
    internal,
    {list_offsets, TopicPartitionOffsets0},
    _State = active,
    StateData = #state{
        connection = Connection,
        consumer_options = #{isolation_level := IsolationLevel},
        req_ids = ReqIds
    }
) ->
    % Convert from {T, P, O} to T => P => O.
    TopicPartitionOffsets = lists:foldl(
        fun({Topic, Partition, Offset}, Acc) ->
            kafine_maps:put([Topic, Partition], Offset, Acc)
        end,
        #{},
        TopicPartitionOffsets0
    ),
    ListOffsetsRequest = kafine_list_offsets_request:build_list_offsets_request(
        TopicPartitionOffsets, IsolationLevel
    ),
    ReqIds2 = kafine_connection:send_request(
        Connection,
        fun list_offsets_request:encode_list_offsets_request_5/1,
        ListOffsetsRequest,
        fun list_offsets_response:decode_list_offsets_response_5/1,
        list_offsets,
        ReqIds,
        kafine_request_telemetry:request_labels(?LIST_OFFSETS, 5)
    ),
    {next_state, list_offsets, StateData#state{req_ids = ReqIds2}};
handle_event(
    internal,
    {reset_offsets, TopicPartitions0},
    _State = active,
    StateData = #state{
        connection = Connection,
        consumer_options = #{isolation_level := IsolationLevel},
        topic_options = TopicOptions,
        req_ids = ReqIds
    }
) ->
    TopicPartitionOffsets = collect_topic_partition_offsets(TopicPartitions0, TopicOptions),
    ListOffsetsRequest = kafine_list_offsets_request:build_list_offsets_request(
        TopicPartitionOffsets, IsolationLevel
    ),
    ReqIds2 = kafine_connection:send_request(
        Connection,
        fun list_offsets_request:encode_list_offsets_request_5/1,
        ListOffsetsRequest,
        fun list_offsets_response:decode_list_offsets_response_5/1,
        list_offsets,
        ReqIds,
        kafine_request_telemetry:request_labels(?LIST_OFFSETS, 5)
    ),
    {next_state, list_offsets, StateData#state{req_ids = ReqIds2}};
handle_event(
    internal,
    {give_away, TopicPartitions},
    _State,
    StateData = #state{owner = Owner, topic_partition_states = TopicPartitionStates}
) ->
    {Discard, TopicPartitionStates2} = kafine_topic_partition_states:take_partition_states(
        TopicPartitions, TopicPartitionStates
    ),
    Owner ! {give_away, Discard},
    {keep_state,
        StateData#state{
            topic_partition_states = TopicPartitionStates2
        },
        [{next_event, internal, continue}]};
handle_event(info, Info, State, StateData = #state{req_ids = ReqIds}) ->
    % We can't tell the difference between send_request responses and normal info messages, so we have to check them
    % first.
    check_response(kafine_connection:check_response(Info, ReqIds), Info, State, StateData).

terminate(_Reason, _State, _StateData = #state{connection = Connection}) when
    Connection =/= undefined
->
    kafine_connection:stop(Connection),
    ok;
terminate(_Reason, _State, _StateData) ->
    ok.

check_response(_Result = {Response, Label, ReqIds2}, _Info, State, StateData) ->
    handle_response(Response, Label, State, StateData#state{req_ids = ReqIds2});
check_response(_Other, Info, State, StateData) ->
    handle_info(Info, State, StateData).

handle_info(
    {'EXIT', Connection, _Reason},
    _State,
    StateData = #state{connection = Connection, metadata = Metadata}
) ->
    ?LOG_WARNING("Connection closed; reconnecting"),
    StateData2 = StateData#state{connection = undefined},
    telemetry:execute([kafine, node_consumer, disconnected], #{}, Metadata),
    {next_state, disconnected, StateData2, [{next_event, internal, connect}]};
handle_info(Info, _State, _StateData) ->
    % Normal info message; ignore it.
    ?LOG_WARNING("Ignoring ~p", [Info]),
    keep_state_and_data.

handle_response(
    {ok, FetchResponse},
    {fetch, SubscribeEpoch},
    _State = fetch,
    StateData = #state{
        topic_partition_states = TopicPartitionStates,
        subscribe_epoch = SubscribeEpoch,
        next_actions = NextActions0,
        metadata = Metadata
    }
) ->
    {TopicPartitionStates2, Errors} = kafine_fetch_response:fold(
        FetchResponse, TopicPartitionStates, Metadata
    ),
    StateData2 = StateData#state{
        topic_partition_states = TopicPartitionStates2,
        next_actions = NextActions0 ++ next_actions(Errors)
    },
    {next_state, active, StateData2, [{next_event, internal, continue}]};
handle_response(
    {ok, _FetchResponse},
    {fetch, _StaleEpoch},
    _State = fetch,
    StateData
) ->
    % Subscription changed while Fetch was in flight; drop the response.
    {next_state, active, StateData, [{next_event, internal, continue}]};
handle_response(
    {ok, ListOffsetsResponse},
    list_offsets,
    _State = list_offsets,
    StateData = #state{topic_partition_states = TopicPartitionStates}
) ->
    TopicPartitionStates2 = kafine_list_offsets_response:fold(
        ListOffsetsResponse, TopicPartitionStates
    ),
    StateData2 = StateData#state{
        topic_partition_states = TopicPartitionStates2
    },
    {next_state, active, StateData2, [{next_event, internal, continue}]}.

reply_and_continue(_State = idle, StateData, Reply) ->
    % If we we're idle, we need to issue a 'continue' event to wake ourselves up.
    {next_state, active, StateData, [Reply, {next_event, internal, continue}]};
reply_and_continue(_State, StateData, Reply) ->
    {keep_state, StateData, [Reply]}.

handle_continue(
    _State,
    StateData = #state{next_actions = [Action | NextActions]}
) ->
    % Actions pending: do the next action.
    {next_state, active, StateData#state{next_actions = NextActions}, [
        {next_event, internal, Action}
    ]};
handle_continue(
    _State,
    StateData = #state{topic_partition_states = TopicPartitionStates, metadata = Metadata}
) ->
    % No actions pending, do we have anything to fetch?
    NextState = maps:fold(
        fun(_Topic, PartitionStates, NextState) ->
            maps:fold(
                fun
                    (_PartitionIndex, #topic_partition_state{state = active}, _) -> active;
                    (_PartitionIndex, _TopicPartitionState, State) -> State
                end,
                NextState,
                PartitionStates
            )
        end,
        idle,
        TopicPartitionStates
    ),

    case NextState of
        idle ->
            % Nothing to fetch: hibernate.
            ?LOG_DEBUG("Nothing to fetch; hibernating; idle"),
            telemetry:execute([kafine, node_consumer, idle], #{}, Metadata),
            {next_state, idle, StateData, [hibernate]};
        _ ->
            % Do a fetch
            ?LOG_DEBUG("Something to fetch; continuing"),
            telemetry:execute([kafine, node_consumer, continue], #{}, Metadata),
            {next_state, active, StateData, [{next_event, internal, fetch}]}
    end.

add_next_actions(TopicPartitionStates, NextActions0) ->
    case need_offsets(TopicPartitionStates) of
        [] -> NextActions0;
        NeedOffsets -> NextActions0 ++ [{list_offsets, NeedOffsets}]
    end.

%% Do we need to issue an initial ListOffsets request for any of the partitions we're handling?
need_offsets(TopicPartitionStates) ->
    maps:fold(
        fun(Topic, PartitionStates, Acc1) ->
            maps:fold(
                fun
                    (
                        _PartitionIndex,
                        _PartitionState = #topic_partition_state{state = paused},
                        Acc2
                    ) ->
                        % Paused; do it later.
                        Acc2;
                    (
                        _PartitionIndex,
                        _PartitionState = #topic_partition_state{state = busy},
                        Acc2
                    ) ->
                        % Busy; not needed.
                        Acc2;
                    (
                        _PartitionIndex,
                        _PartitionState = #topic_partition_state{state = active, offset = Offset},
                        Acc2
                    ) when is_integer(Offset), Offset >= 0 ->
                        % Offset is a positive integer; continue.
                        Acc2;
                    (
                        PartitionIndex,
                        _PartitionState = #topic_partition_state{state = active, offset = Offset},
                        Acc2
                    ) ->
                        % Offset is something else (e.g. 'earliest'); we need to convert it to a real offset.
                        [{Topic, PartitionIndex, Offset} | Acc2]
                end,
                Acc1,
                PartitionStates
            )
        end,
        [],
        TopicPartitionStates
    ).

next_actions(Errors) ->
    lists:reverse(maps:fold(fun next_actions_for/3, [], Errors)).

next_actions_for(?OFFSET_OUT_OF_RANGE, TopicPartitions, Acc) ->
    [{reset_offsets, TopicPartitions} | Acc];
next_actions_for(?NOT_LEADER_OR_FOLLOWER, TopicPartitions, Acc) ->
    [{give_away, TopicPartitions} | Acc].

%% Convert from [{T, P}] to #{T => #{P => O}}
collect_topic_partition_offsets(TopicPartitions, TopicOptions) ->
    lists:foldl(
        fun({T, P}, Acc) ->
            #{offset_reset_policy := OffsetResetPolicy} = maps:get(T, TopicOptions),
            kafine_maps:put([T, P], OffsetResetPolicy, Acc)
        end,
        #{},
        TopicPartitions
    ).

%% Build the object returned in the 'info' call.
build_info(
    State,
    _StateData = #state{topic_partition_states = TopicPartitionStates}
) ->
    TopicPartitions = maps:fold(
        fun(Topic, PartitionStates, Acc) ->
            Partitions = maps:fold(
                fun(
                    Partition, #topic_partition_state{state = PartitionState, offset = Offset}, Acc2
                ) ->
                    Acc2#{Partition => #{state => PartitionState, offset => Offset}}
                end,
                #{},
                PartitionStates
            ),
            Acc#{Topic => Partitions}
        end,
        #{},
        TopicPartitionStates
    ),
    #{
        state =>
            case State of
                idle -> idle;
                _ -> active
            end,
        topic_partitions => TopicPartitions
    }.

%% For logging, we want to see Topic => Partition => Offset
summarise_fetch_request(_FetchRequest = #{topics := Topics}) ->
    lists:foldl(
        fun(#{topic := Topic, partitions := Partitions}, Acc) ->
            Acc#{
                Topic => lists:foldl(
                    fun(#{partition := Partition, fetch_offset := FetchOffset}, Acc2) ->
                        Acc2#{Partition => FetchOffset}
                    end,
                    #{},
                    Partitions
                )
            }
        end,
        #{},
        Topics
    ).
