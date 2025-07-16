-module(kafine_consumer).
-export([
    start_link/5,
    stop/1,

    info/1,

    subscribe/2,
    unsubscribe/2,
    unsubscribe_all/1,

    resume/3,
    resume/4
]).
-export([
    init_ack/4,
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
    ref/0,
    start_ret/0,
    subscription/0
]).

-include_lib("kernel/include/logger.hrl").
-include("kafine_topic_partition_state.hrl").
-include_lib("kafcod/include/api_key.hrl").
-include_lib("kafcod/include/error_code.hrl").

-type ref() :: any().

-type topic_partition_state() :: #topic_partition_state{}.

-type topic_partition_states() :: #{
    kafine:topic() =>
        #{kafine:partition() => topic_partition_state()}
}.

-export_type([topic_partition_states/0]).

-type partition_offsets() :: #{kafine:partition() => kafine:offset()}.

-spec start_link(
    Ref :: ref(),
    Bootstrap :: kafine:broker(),
    ConnectionOptions :: kafine:connection_options(),
    ConsumerCallback :: {ConsumerCallbackModule :: module(), ConsumerCallbackArgs :: term()},
    ConsumerOptions :: kafine:consumer_options()
) -> start_ret().
-type start_ret() :: gen_statem:start_ret().

start_link(Ref, Bootstrap, ConnectionOptions, ConsumerCallback, ConsumerOptions) ->
    gen_statem:start_link(
        via(Ref),
        ?MODULE,
        [
            Ref,
            Bootstrap,
            ConnectionOptions,
            ConsumerCallback,
            kafine_consumer_options:validate_options(ConsumerOptions)
        ],
        start_options()
    ).

start_options() -> [{debug, kafine_trace:debug_options(#{mfa => {?MODULE, handle_event, 4}})}].

-spec via(Ref :: ref()) -> {via, kafine_via, {module(), ref()}}.

via(Ref) ->
    {via, kafine_via, {?MODULE, Ref}}.

stop(Consumer) when is_pid(Consumer) ->
    gen_statem:stop(Consumer).

info(Consumer) ->
    call(Consumer, info).

-type subscription() :: #{kafine:topic() => {kafine:topic_options(), partition_offsets()}}.
-spec subscribe(
    Consumer :: kafine_consumer:ref() | pid(),
    Subscription :: subscription()
) -> ok.

subscribe(Consumer, Subscription) ->
    call(Consumer, {subscribe, Subscription}).

-spec unsubscribe(
    Consumer :: kafine_consumer:ref() | pid(),
    Unsubscription :: #{kafine:topic() => [kafine:partition()]}
) -> ok.

unsubscribe(Consumer, Unsubscription) ->
    call(Consumer, {unsubscribe, Unsubscription}).

unsubscribe_all(Consumer) ->
    call(Consumer, unsubscribe_all).

-spec resume(
    Consumer :: pid() | ref(),
    Topic :: kafine:topic(),
    Partition :: kafine:partition()
) -> ok.

resume(Consumer, Topic, Partition) when
    is_binary(Topic), is_integer(Partition), Partition >= 0
->
    resume(Consumer, Topic, Partition, keep_current_offset).

-spec resume(
    Consumer :: pid() | ref(),
    Topic :: kafine:topic(),
    Partition :: kafine:partition(),
    Offset :: kafine:offset() | keep_current_offset
) -> ok.

-define(IS_VALID_RESUME_OFFSET(X),
    ((is_integer(X) andalso X >= 0) orelse X =:= keep_current_offset)
).

resume(Consumer, Topic, Partition, Offset) when
    is_binary(Topic),
    is_integer(Partition),
    Partition >= 0,
    ?IS_VALID_RESUME_OFFSET(Offset)
->
    call(Consumer, {resume, {Topic, Partition, Offset}}).

% Continue fetching the given topic and partition from the specified offset. Called from the consumer callback process.
%
% The NextOffset is provided by the callback because only it knows how far we got through the partition data before
% (e.g.) pausing.
continue(Consumer, Topic, Partition, NextOffset, NextState, Span) ->
    call(Consumer, {continue, {Topic, Partition, NextOffset, NextState, Span}}).

call(Consumer, Request) when is_pid(Consumer) ->
    gen_statem:call(Consumer, Request);
call(Consumer, Request) ->
    gen_statem:call(via(Consumer), Request).

-spec init_ack(
    Consumer :: pid() | ref(),
    Topic :: kafine:topic(),
    Partition :: kafine:partition(),
    State :: active | paused
) -> ok.

init_ack(Consumer, Topic, Partition, State) when is_pid(Consumer) ->
    erlang:send(Consumer, {init_ack, {Topic, Partition}, State}),
    ok;
init_ack(Consumer, Topic, Partition, State) ->
    kafine_via:send({?MODULE, Consumer}, {init_ack, {Topic, Partition}, State}),
    ok.

callback_mode() ->
    [handle_event_function].

-record(state, {
    ref :: ref(),
    broker :: kafine:broker(),
    connection :: kafine:connection() | undefined,
    connection_options :: kafine:connection_options(),
    consumer_options :: kafine:consumer_options(),
    consumer_callback :: {module(), term()},
    topic_options :: #{kafine:topic() => kafine:topic_options()},
    node_consumers :: #{kafine:node_id() => pid()}
}).

init([Ref, Bootstrap, ConnectionOptions, ConsumerCallback, ConsumerOptions]) ->
    process_flag(trap_exit, true),
    Metadata = #{ref => Ref},
    logger:set_process_metadata(Metadata),
    kafine_proc_lib:set_label({?MODULE, Ref}),
    StateData = #state{
        ref = Ref,
        broker = Bootstrap,
        connection = undefined,
        connection_options = ConnectionOptions,
        consumer_options = ConsumerOptions,
        consumer_callback = ConsumerCallback,
        topic_options = #{},
        node_consumers = #{}
    },
    {ok, disconnected, StateData, [{next_event, internal, connect}]}.

handle_event(
    internal,
    connect,
    disconnected,
    StateData = #state{broker = Broker, connection_options = ConnectionOptions}
) ->
    {ok, Connection} = kafine_connection:start_link(Broker, ConnectionOptions),
    StateData2 = StateData#state{connection = Connection},
    {next_state, ready, StateData2};
handle_event(
    {call, From},
    info,
    _State,
    _StateData = #state{node_consumers = NodeConsumers}
) ->
    Info = #{node_consumers => NodeConsumers},
    {keep_state_and_data, [{reply, From, Info}]};
handle_event(
    {call, From},
    {subscribe, Subscription},
    _State,
    StateData = #state{
        ref = Ref,
        consumer_callback = ConsumerCallback,
        topic_options = TopicOptions0
    }
) ->
    TopicPartitionStates = init_topic_partition_states(
        Ref, ConsumerCallback, Subscription
    ),

    telemetry:execute([kafine, consumer, subscription], #{}, #{
        subscription => kafine_telemetry:subscribed_to(TopicPartitionStates)
    }),

    TopicOptions1 = maps:fold(
        fun(Topic, {Options, _}, Acc) ->
            Acc#{Topic => kafine_topic_options:validate_options(Options)}
        end,
        #{},
        Subscription
    ),
    TopicOptions = kafine_topic_options:merge_options(TopicOptions0, TopicOptions1),

    StateData2 = do_subscribe(TopicOptions, TopicPartitionStates, StateData),
    {keep_state, StateData2, [{reply, From, ok}]};
handle_event(
    {call, From},
    unsubscribe_all,
    _State,
    StateData = #state{node_consumers = NodeConsumers}
) ->
    % This is a separate event from {unsubscribe, _} because we didn't want to encourage {unsubscribe, AnythingYouWant}.
    % Ask each of the node consumers to unsubscribe_all; they'll ignore irrelevant topic/partitions.
    maps:foreach(
        fun(_NodeId, Pid) ->
            kafine_node_consumer:unsubscribe_all(Pid)
        end,
        NodeConsumers
    ),
    {keep_state, StateData, [{reply, From, ok}]};
handle_event(
    {call, From},
    {unsubscribe, Unsubscription},
    _State,
    StateData = #state{node_consumers = NodeConsumers}
) ->
    % Ask each of the node consumers to unsubscribe; they'll ignore irrelevant topic/partitions.
    maps:foreach(
        fun(_NodeId, Pid) ->
            kafine_node_consumer:unsubscribe(Pid, Unsubscription)
        end,
        NodeConsumers
    ),
    % Note that unsubscribing might leave an existing node consumer connected to the broker, but with no subscriptions.
    {keep_state, StateData, [{reply, From, ok}]};
handle_event(
    {call, From},
    {resume, {Topic, Partition, Offset}},
    _State,
    _StateData = #state{node_consumers = NodeConsumers}
) ->
    % Ask each of the node consumers to resume the topic/partition; if one of them returns 'ok', we're good.
    ok = maps:fold(
        fun
            (_NodeId, _Pid, ok) ->
                ok;
            (_NodeId, Pid, _) ->
                kafine_node_consumer:resume(Pid, Topic, Partition, Offset)
        end,
        {error, {badkey, [Topic, Partition]}},
        NodeConsumers
    ),
    {keep_state_and_data, [{reply, From, ok}]};
handle_event(
    {call, From},
    {continue, {Topic, Partition, NextOffset, NextState, Span}},
    _State,
    _StateData = #state{node_consumers = NodeConsumers}
) ->
    % Ask each of the node consumers to continue the topic/partition; ignore the results.
    maps:foreach(
        fun(_NodeId, Pid) ->
            kafine_node_consumer:continue(Pid, Topic, Partition, NextOffset, NextState, Span)
        end,
        NodeConsumers
    ),
    {keep_state_and_data, [{reply, From, ok}]};
handle_event(
    info,
    {give_away, TopicPartitionStates},
    _State,
    StateData = #state{topic_options = TopicOptions}
) ->
    StateData2 = do_subscribe(TopicOptions, TopicPartitionStates, StateData),
    {keep_state, StateData2};
handle_event(
    info,
    {'EXIT', Connection, _Reason},
    _State,
    StateData = #state{connection = Connection}
) ->
    StateData2 = StateData#state{connection = undefined},
    {next_state, disconnected, StateData2, [{next_event, internal, connect}]};
handle_event(
    info,
    _Info,
    _State,
    _StateData
) ->
    % Normal info message; ignore it.
    keep_state_and_data.

terminate(
    _Reason, _State, _StateData = #state{connection = Connection, node_consumers = NodeConsumers}
) ->
    maps:foreach(
        fun(_NodeId, Pid) ->
            kafine_node_consumer:stop(Pid)
        end,
        NodeConsumers
    ),
    kafine_connection:stop(Connection),
    ok.

do_subscribe(
    TopicOptions,
    TopicPartitionStates0,
    StateData = #state{
        ref = Ref,
        connection = Connection,
        connection_options = ConnectionOptions,
        consumer_options = ConsumerOptions,
        node_consumers = NodeConsumers0
    }
) ->
    % Get the metadata for the specified topics.
    Topics = maps:keys(TopicPartitionStates0),
    ?LOG_DEBUG("Subscribing to ~p", [Topics]),

    MetadataRequest = #{
        allow_auto_topic_creation => false,
        include_cluster_authorized_operations => false,
        include_topic_authorized_operations => false,
        topics => [#{name => Name} || Name <- Topics]
    },
    {ok, MetadataResponse} = kafine_connection:call(
        Connection,
        fun metadata_request:encode_metadata_request_9/1,
        MetadataRequest,
        fun metadata_response:decode_metadata_response_9/1,
        kafine_request_telemetry:request_labels(?METADATA, 9)
    ),

    #{brokers := Brokers, topics := TopicsMetadata} = MetadataResponse,
    ByLeader = kafine_metadata:fold(
        fun(
            Topic,
            #{partition_index := PartitionIndex, error_code := ?NONE, leader_id := LeaderId},
            Acc
        ) ->
            case kafine_maps:get([Topic, PartitionIndex], TopicPartitionStates0, undefined) of
                undefined ->
                    Acc;
                ConsumerState ->
                    kafine_maps:update_with(
                        [LeaderId, Topic],
                        fun(PartitionOffsets) ->
                            PartitionOffsets#{PartitionIndex => ConsumerState}
                        end,
                        #{PartitionIndex => ConsumerState},
                        Acc
                    )
            end
        end,
        #{},
        TopicsMetadata
    ),

    % BUG: If TopicPartitionStates refers to a partition that doesn't exist, we don't notice.
    % BUG: Given that this is different behaviour from when a topic doesn't exist (we crash), I think this is a bug.

    NodeConsumers = maps:fold(
        fun(LeaderId, TopicPartitionStates, Acc) ->
            case maps:get(LeaderId, Acc, undefined) of
                undefined ->
                    % We don't have a node consumer; start one.
                    Leader = get_node_by_id(Brokers, LeaderId),
                    {ok, NodeConsumer} = kafine_node_consumer:start_link(
                        Ref,
                        Leader,
                        ConnectionOptions,
                        ConsumerOptions,
                        self()
                    ),
                    ok = kafine_node_consumer:subscribe(
                        NodeConsumer, TopicPartitionStates, TopicOptions
                    ),
                    Acc#{LeaderId => NodeConsumer};
                NodeConsumer ->
                    ok = kafine_node_consumer:subscribe(
                        NodeConsumer, TopicPartitionStates, TopicOptions
                    ),
                    Acc
            end
        end,
        NodeConsumers0,
        ByLeader
    ),
    StateData#state{node_consumers = NodeConsumers, topic_options = TopicOptions}.

-spec init_topic_partition_states(
    Ref :: ref(),
    {Callback :: module(), CallbackArgs :: term()},
    Subscription :: subscription()
) -> topic_partition_states().

init_topic_partition_states(Ref, {Callback, CallbackArgs}, Subscription) ->
    maps:fold(
        fun(Topic, {_Options, PartitionOffsets}, Acc) ->
            PartitionStates = maps:map(
                fun(PartitionIndex, Offset) ->
                    {ok, ClientPid} = kafine_consumer_callback_process:start_link(
                        Ref, Topic, PartitionIndex, Callback, CallbackArgs
                    ),
                    receive
                        {init_ack, {Topic, PartitionIndex}, State} ->
                            ok
                    end,
                    #topic_partition_state{
                        offset = Offset,
                        state = State,
                        client_pid = ClientPid
                    }
                end,
                PartitionOffsets
            ),
            Acc#{Topic => PartitionStates}
        end,
        #{},
        Subscription
    ).

-spec get_node_by_id([Node], NodeId :: non_neg_integer()) -> Node when
    Node :: #{node_id := integer(), host := binary(), port := integer()}.

get_node_by_id(Nodes, NodeId) when is_list(Nodes), is_integer(NodeId) ->
    [Node] = [N || N = #{node_id := Id} <- Nodes, Id =:= NodeId],
    Node.
