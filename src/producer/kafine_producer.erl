-module(kafine_producer).
-export([
    start_link/3,
    stop/1,

    produce/5,
    produce_async/7,
    produce_batch/3,

    reqids_new/0
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
    start_ret/0
]).

-include_lib("kafcod/include/api_key.hrl").

-type ref() :: any().

-spec start_link(
    Ref :: ref(),
    Bootstrap :: kafine:broker(),
    ConnectionOptions :: kafine:connection_options()
) -> start_ret().
-type start_ret() :: gen_statem:start_ret().

start_link(Ref, Bootstrap, ConnectionOptions) ->
    gen_statem:start_link(
        via(Ref),
        ?MODULE,
        [
            Bootstrap,
            ConnectionOptions
        ],
        start_options()
    ).

start_options() -> [{debug, kafine_trace:debug_options(#{mfa => {?MODULE, handle_event, 4}})}].

-spec via(Ref :: ref()) -> {via, kafine_via, {module(), ref()}}.

via(Ref) ->
    {via, kafine_via, {?MODULE, Ref}}.

stop(Producer) when is_pid(Producer) ->
    gen_statem:stop(Producer).

produce(Pid, Topic, PartitionIndex, BatchAttributes, Messages) ->
    call(Pid, {produce, Topic, PartitionIndex, BatchAttributes, Messages}).

produce_async(Pid, Topic, PartitionIndex, BatchAttributes, Messages, Label, ReqIdCollection) ->
    send_request(
        Pid, {produce, Topic, PartitionIndex, BatchAttributes, Messages}, Label, ReqIdCollection
    ).

produce_batch(Pid, Batch, BatchAttributes) ->
    kafine_producer_batch:produce_batch(Pid, Batch, BatchAttributes).

call(Producer, Request) when is_pid(Producer) ->
    gen_statem:call(Producer, Request);
call(Producer, Request) ->
    gen_statem:call(via(Producer), Request).

send_request(Pid, Request, Label, ReqIdCollection) ->
    gen_statem:send_request(Pid, Request, Label, ReqIdCollection).

reqids_new() ->
    gen_statem:reqids_new().

callback_mode() ->
    [handle_event_function].

-record(state, {
    bootstrap :: kafine:broker(),
    brokers :: [kafine:broker()],
    connection :: kafine:connection() | undefined,
    connection_options :: kafine:connection_options(),
    partition_leaders :: #{kafine:topic() := #{kafine:partition() := kafine:node_id()}},
    node_producers :: #{kafine:node_id() := pid()},
    pending :: kafine_node_producer:request_id_collection()
}).

init([Bootstrap, ConnectionOptions]) ->
    process_flag(trap_exit, true),
    StateData = #state{
        bootstrap = Bootstrap,
        connection = undefined,
        connection_options = ConnectionOptions,
        brokers = [],
        node_producers = #{},
        partition_leaders = #{},
        pending = kafine_node_producer:reqids_new()
    },
    {ok, disconnected, StateData, [{next_event, internal, connect}]}.

handle_event(
    internal,
    connect,
    disconnected,
    StateData = #state{bootstrap = Broker, connection_options = ConnectionOptions}
) ->
    {ok, Connection} = kafine_connection:start_link(Broker, ConnectionOptions),
    StateData2 = StateData#state{connection = Connection},
    {next_state, ready, StateData2};
handle_event(
    internal,
    refresh_metadata,
    _State,
    StateData
) ->
    StateData2 = do_refresh_metadata(StateData),
    {keep_state, StateData2};
handle_event(
    {call, From},
    Req = {produce, Topic, PartitionIndex, BatchAttributes, Messages},
    _State,
    StateData
) ->
    case
        do_produce(
            Topic, PartitionIndex, BatchAttributes, Messages, From, StateData
        )
    of
        {ok, StateData2} ->
            {keep_state, StateData2, []};
        {error, missing_topic_metadata} ->
            StateData2 = add_topic(Topic, StateData),
            {keep_state, StateData2, [
                {next_event, internal, refresh_metadata}, {next_event, {call, From}, Req}
            ]};
        {error, invalid_partition_index} ->
            exit({produce_error, {invalid_partition_index, Topic, PartitionIndex}})
    end;
handle_event(info, Info, State, StateData = #state{pending = ReqIds}) ->
    % We can't tell the difference between send_request responses and normal info messages, so we have to check them
    % first.
    check_response(kafine_node_producer:check_response(Info, ReqIds), Info, State, StateData).

check_response(_Result = {Response, Label, ReqIds2}, _Info, State, StateData) ->
    handle_response(Response, Label, State, StateData#state{pending = ReqIds2});
check_response(_Other, Info, State, StateData) ->
    handle_info(Info, State, StateData).

handle_info(
    {'EXIT', Connection, _Reason},
    _State,
    StateData = #state{connection = Connection}
) ->
    StateData2 = StateData#state{connection = undefined},
    {next_state, disconnected, StateData2, [{next_event, internal, connect}]};
handle_info(
    _Info,
    _State,
    _StateData
) ->
    % Normal info message; ignore it.
    keep_state_and_data.

handle_response(
    ProduceResponse,
    {produce, From},
    _State,
    StateData
) ->
    {next_state, todo, StateData, [{reply, From, ProduceResponse}]}.

add_topic(Topic, StateData = #state{partition_leaders = PartitionLeaders}) ->
    case maps:get(Topic, PartitionLeaders, undefined) of
        undefined ->
            StateData#state{partition_leaders = PartitionLeaders#{Topic => #{}}};
        _ ->
            StateData
    end.

do_produce(
    Topic,
    PartitionIndex,
    BatchAttributes,
    Messages,
    From,
    StateData = #state{partition_leaders = PartitionLeaders}
) ->
    case maps:get(Topic, PartitionLeaders, undefined) of
        undefined ->
            {error, missing_topic_metadata};
        LeaderByPartition ->
            case maps:get(PartitionIndex, LeaderByPartition, undefined) of
                undefined ->
                    {error, invalid_partition_index};
                LeaderId ->
                    do_produce_to_leader(
                        Topic, PartitionIndex, BatchAttributes, Messages, From, LeaderId, StateData
                    )
            end
    end.

do_produce_to_leader(
    Topic,
    PartitionIndex,
    BatchAttributes,
    Messages,
    From,
    LeaderId,
    StateData = #state{pending = Pending}
) ->
    {NodeProducer, StateData2} = get_or_start_node_producer(LeaderId, StateData),
    Pending2 = kafine_node_producer:produce(
        NodeProducer, Topic, PartitionIndex, BatchAttributes, Messages, {produce, From}, Pending
    ),
    {ok, StateData2#state{pending = Pending2}}.

get_or_start_node_producer(
    LeaderId,
    StateData = #state{
        brokers = Brokers, node_producers = NodeProducers, connection_options = ConnectionOptions
    }
) ->
    case maps:get(LeaderId, NodeProducers, undefined) of
        undefined ->
            Leader = get_node_by_id(Brokers, LeaderId),
            {ok, NodeProducer} = kafine_node_producer:start_link(Leader, ConnectionOptions),

            {NodeProducer, StateData#state{
                node_producers = NodeProducers#{LeaderId => NodeProducer}
            }};
        NodeProducer ->
            {NodeProducer, StateData}
    end.

do_refresh_metadata(
    StateData = #state{connection = Connection, partition_leaders = PartitionLeaders}
) ->
    Topics = maps:keys(PartitionLeaders),

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
    PartitionLeaders2 = kafine_metadata:group_by_leader(
        fun(LeaderId, Topic, PartitionIndex, Acc) ->
            case maps:get(Topic, Acc, undefined) of
                undefined ->
                    Acc#{Topic => #{PartitionIndex => LeaderId}};
                PartitionMap ->
                    Acc#{Topic => PartitionMap#{PartitionIndex => LeaderId}}
            end
        end,
        #{},
        TopicsMetadata
    ),

    StateData#state{brokers = Brokers, partition_leaders = PartitionLeaders2}.

terminate(
    _Reason, _State, _StateData = #state{connection = Connection, node_producers = NodeProducers}
) ->
    kafine_connection:stop(Connection),
    maps:foreach(
        fun(_NodeId, Pid) ->
            kafine_node_producer:stop(Pid)
        end,
        NodeProducers
    ),
    ok.

-spec get_node_by_id([Node], NodeId :: non_neg_integer()) -> Node when
    Node :: #{node_id := integer(), host := binary(), port := integer(), _ := _}.

get_node_by_id(Nodes, NodeId) when is_list(Nodes), is_integer(NodeId) ->
    [Node] = [N || N = #{node_id := Id} <- Nodes, Id =:= NodeId],
    Node.
