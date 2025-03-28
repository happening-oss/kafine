-module(kafine_node_producer).
-moduledoc false.
-export([
    start_link/2,
    stop/1,

    reqids_new/0,
    produce/6,
    produce/8,
    check_response/2
]).
-behaviour(gen_statem).
-export([
    init/1,
    callback_mode/0,
    handle_event/4,
    terminate/3
]).
-export_type([
    start_ret/0,
    request_id_collection/0
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("kafcod/include/api_key.hrl").
-include_lib("kafcod/include/ack.hrl").

-type request_id_collection() :: gen_statem:request_id_collection().
-type start_ret() :: gen_statem:start_ret().
-spec start_link(
    Broker :: kafine:broker(),
    ConnectionOptions :: kafine:connection_options()
) ->
    start_ret().

start_link(
    Broker = #{host := _, port := _, node_id := _},
    ConnectionOptions
    % ConsumerOptions,
) ->
    gen_statem:start_link(
        ?MODULE,
        [
            Broker,
            ConnectionOptions
        ],
        start_options()
    ).

start_options() -> [{debug, kafine_trace:debug_options(#{mfa => {?MODULE, handle_event, 4}})}].

stop(Pid) ->
    gen_statem:stop(Pid).

produce(Pid, Topic, PartitionIndex, ProduceOptions, BatchAttributes, Messages) ->
    call(Pid, {produce, Topic, PartitionIndex, ProduceOptions, BatchAttributes, Messages}).

produce(
    Pid, Topic, PartitionIndex, ProduceOptions, BatchAttributes, Messages, Label, ReqIdCollection
) ->
    send_request(
        Pid,
        {produce, Topic, PartitionIndex, ProduceOptions, BatchAttributes, Messages},
        Label,
        ReqIdCollection
    ).

reqids_new() ->
    gen_statem:reqids_new().

call(Pid, Request) ->
    gen_statem:call(Pid, Request).

send_request(Pid, Request, Label, ReqIdCollection) ->
    gen_statem:send_request(Pid, Request, Label, ReqIdCollection).

-spec check_response(Msg, ReqIdCollection) -> Result when
    Msg :: term(),
    ReqIdCollection :: request_id_collection(),
    Result ::
        {Response, Label, ReqIdCollection2}
        | no_request
        | no_reply,
    Response :: {ok, Decoded :: map()} | {error, {Reason :: term(), gen_statem:server_ref()}},
    Label :: term(),
    ReqIdCollection2 :: request_id_collection().

check_response(Msg, ReqIdCollection) ->
    check_response(gen_statem:check_response(Msg, ReqIdCollection, true)).

check_response({{reply, {ok, Response}}, Label, ReqIdCollection2}) ->
    {{ok, Response}, Label, ReqIdCollection2};
check_response({{error, Reason}, Label, ReqIdCollection2}) ->
    {{error, Reason}, Label, ReqIdCollection2};
check_response(Result) when Result == no_request; Result == no_reply ->
    Result.

callback_mode() ->
    [handle_event_function].

-record(state, {
    metadata :: telemetry:event_metadata(),
    broker :: kafine:broker(),
    connection :: kafine:connection() | undefined,
    connection_options :: kafine:connection_options(),
    pending :: kafine_connection:request_id_collection()
}).

init([
    Broker = #{node_id := NodeId},
    ConnectionOptions
]) ->
    process_flag(trap_exit, true),
    Metadata = #{node_id => NodeId},
    logger:set_process_metadata(Metadata),
    kafine_proc_lib:set_label({?MODULE, NodeId}),

    StateData = #state{
        metadata = Metadata,
        broker = Broker,
        connection = undefined,
        connection_options = ConnectionOptions,
        pending = kafine_connection:reqids_new()
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
    telemetry:execute([kafine, node_producer, connected], #{}, Metadata),
    StateData2 = StateData#state{connection = Connection},
    {next_state, ready, StateData2, []};
handle_event(
    {call, From},
    {produce, Topic, PartitionIndex, ProduceOptions, BatchAttributes, Messages},
    _State,
    StateData
) ->
    StateData2 = send_messages(
        Topic, PartitionIndex, ProduceOptions, BatchAttributes, Messages, From, StateData
    ),
    {keep_state, StateData2};
handle_event(info, Info, State, StateData = #state{pending = ReqIds}) ->
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
    handle_response(Response, Label, State, StateData#state{pending = ReqIds2});
check_response(_Other, Info, State, StateData) ->
    handle_info(Info, State, StateData).

handle_info(
    {'EXIT', Connection, _Reason},
    _State,
    StateData = #state{connection = Connection, metadata = Metadata}
) ->
    ?LOG_WARNING("Connection closed; reconnecting"),
    StateData2 = StateData#state{connection = undefined},
    telemetry:execute([kafine, node_producer, disconnected], #{}, Metadata),
    {next_state, disconnected, StateData2, [{next_event, internal, connect}]};
handle_info(_Info, _State, _StateData) ->
    % Normal info message; ignore it.
    keep_state_and_data.

handle_response(
    ProduceResponse,
    {produce, From},
    _State,
    StateData
) ->
    {ok, #{responses := [TopicResponse]}} = ProduceResponse,
    #{partition_responses := [PartitionResponse]} = TopicResponse,
    {keep_state, StateData, [{reply, From, {ok, PartitionResponse}}]}.

send_messages(
    Topic,
    PartitionIndex,
    ProduceOptions,
    BatchAttributes,
    Messages,
    From,
    StateData = #state{connection = Connection, pending = Pending}
) ->
    Acks = encode_acks(maps:get(acks, ProduceOptions, full_isr)),
    ProduceRequest = #{
        transactional_id => null,
        acks => Acks,
        timeout_ms => 5_000,
        topic_data => [
            #{
                name => Topic,
                partition_data => [
                    #{
                        index => PartitionIndex,
                        records => kafcod_message_set:prepare_message_set(BatchAttributes, Messages)
                    }
                ]
            }
        ]
    },
    Pending2 = kafine_connection:send_request(
        Connection,
        fun produce_request:encode_produce_request_8/1,
        ProduceRequest,
        fun produce_response:decode_produce_response_8/1,
        {produce, From},
        Pending,
        kafine_request_telemetry:request_labels(?PRODUCE, 8)
    ),
    StateData#state{pending = Pending2}.

encode_acks(none) -> ?ACK_NONE;
encode_acks(leader) -> ?ACK_LEADER;
encode_acks(full_isr) -> ?ACK_FULL_ISR.
