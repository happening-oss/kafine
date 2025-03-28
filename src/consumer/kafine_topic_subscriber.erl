-module(kafine_topic_subscriber).

-export([
    start_link/5,
    stop/1
]).
-behaviour(gen_statem).
-export([
    init/1,
    callback_mode/0,
    handle_event/4,
    terminate/3
]).
-export_type([
    ref/0
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("kafcod/include/api_key.hrl").
-include_lib("kafcod/include/error_code.hrl").

-type ref() :: any().

start_link(Ref, Bootstrap, ConnectionOptions, SubscriberOptions, Topics) ->
    gen_statem:start_link(
        via(Ref),
        ?MODULE,
        [
            Bootstrap,
            ConnectionOptions,
            kafine_topic_subscriber_options:validate_options(SubscriberOptions),
            Topics
        ],
        start_options()
    ).

stop(Pid) ->
    gen_statem:stop(Pid).

start_options() -> [{debug, kafine_trace:debug_options(#{mfa => {?MODULE, handle_event, 4}})}].

-spec via(Ref :: ref()) -> {via, kafine_via, {module(), ref()}}.

via(Ref) ->
    {via, kafine_via, {?MODULE, Ref}}.

-record(state, {
    broker :: kafine:broker(),
    topics :: [kafine:topic()],
    connection :: kafine:connection() | undefined,
    connection_options :: kafine:connection_options(),
    subscription_callback :: {module(), term()},
    assignment_callback :: {module(), term()},
    req_ids :: kafine_connection:request_id_collection()
}).

init([
    Bootstrap,
    ConnectionOptions,
    #{
        subscription_callback := {SubscriptionCallback, MembershipArgs},
        assignment_callback := {AssignmentCallback, AssignmentArgs}
    },
    Topics
]) ->
    process_flag(trap_exit, true),

    {ok, SubscriptionState} = SubscriptionCallback:init(MembershipArgs),
    {ok, AssignmentState} = AssignmentCallback:init(AssignmentArgs),

    StateData = #state{
        broker = Bootstrap,
        topics = Topics,
        connection = undefined,
        connection_options = ConnectionOptions,
        subscription_callback = {SubscriptionCallback, SubscriptionState},
        assignment_callback = {AssignmentCallback, AssignmentState},
        req_ids = kafine_connection:reqids_new()
    },

    {ok, init, StateData, [
        {next_event, internal, connect}
    ]}.

callback_mode() ->
    [handle_event_function].

handle_event(
    internal,
    connect,
    init,
    StateData0 = #state{topics = Topics}
) ->
    StateData = do_connect(StateData0),
    {keep_state, StateData, [{next_event, internal, {metadata, Topics}}]};
handle_event(
    internal,
    connect,
    _State,
    StateData0
) ->
    StateData = do_connect(StateData0),
    {keep_state, StateData, []};
handle_event(
    internal,
    {metadata, Topics},
    _State,
    StateData = #state{connection = Connection, req_ids = ReqIds}
) ->
    MetadataRequest = #{
        topics => [#{name => T} || T <- Topics],
        allow_auto_topic_creation => false,
        include_cluster_authorized_operations => false,
        include_topic_authorized_operations => false
    },

    ReqIds2 = kafine_connection:send_request(
        Connection,
        fun metadata_request:encode_metadata_request_9/1,
        MetadataRequest,
        fun metadata_response:decode_metadata_response_9/1,
        metadata,
        ReqIds,
        kafine_request_telemetry:request_labels(?METADATA, 9)
    ),
    {keep_state, StateData#state{req_ids = ReqIds2}};
handle_event(info, Info, State, StateData = #state{req_ids = ReqIds}) ->
    % We can't tell the difference between send_request responses and normal info messages, so we have to check them
    % first.
    check_response(kafine_connection:check_response(Info, ReqIds), Info, State, StateData).

check_response(_Result = {Response, Label, ReqIds2}, _Info, State, StateData) ->
    % Important: Because we manipulate StateData here, you MUST NOT use keep_state_and_data.
    handle_response(Response, Label, State, StateData#state{req_ids = ReqIds2});
check_response(_Other, Info, State, StateData) ->
    handle_info(Info, State, StateData).

handle_info(
    {'EXIT', Connection, Reason},
    _State,
    StateData = #state{connection = Connection}
) when Reason =/= normal ->
    ?LOG_WARNING("Connection closed; reconnecting"),
    {keep_state, StateData, [{next_event, internal, connect}]};
handle_info(_Info, _State, _StateData) ->
    keep_state_and_data.

do_connect(StateData = #state{broker = Broker, connection_options = ConnectionOptions}) ->
    {ok, Connection} = kafine_connection:start_link(Broker, ConnectionOptions),
    StateData#state{connection = Connection}.

handle_response({ok, MetadataResponse}, metadata, State, StateData) ->
    handle_metadata_response(MetadataResponse, State, StateData);
handle_response({error, {closed, _PreviousConnection}}, _, _, StateData) ->
    % This response comes from before a disconnect event; by this point we have already
    % reconnected and made new requests.
    % Alteratively this response has arrived before the 'EXIT' event
    {keep_state, StateData, []}.

handle_metadata_response(
    _MetadataResponse = #{topics := TopicPartitions},
    _State,
    #state{
        connection = Connection,
        assignment_callback = {AssignmentCallback, AssignmentState0},
        subscription_callback = {SubscriptionCallback, SubscriptionState0}
    }
) ->
    AssignedTopicPartitions = convert_topic_partitions(TopicPartitions),
    kafine_assignment:handle_assignment(
        AssignedTopicPartitions,
        #{},
        Connection,
        {AssignmentCallback, AssignmentState0},
        {SubscriptionCallback, SubscriptionState0}
    ),
    % once kafine_consumer:subscribe has been called this process's job is done
    % and can release resources. The supervisor will not restart it; if it did
    % restart this process it would crash with a {error, already_subscribed}
    % when it calls kafine_consumer:subscribe.
    {stop, normal}.

terminate(
    _Reason,
    _State,
    _StateData = #state{connection = undefined}
) ->
    ok;
terminate(
    _Reason,
    _State,
    _StateData = #state{
        connection = Connection
    }
) ->
    kafine_connection:stop(Connection),
    ok.

%% Convert from Kafka list-of-maps to our preferred representation.
-type metadata_response_topic() :: metadata_response:metadata_response_topic_9().
-spec convert_topic_partitions(TopicPartitions :: [metadata_response_topic()]) ->
    #{kafine:topic() => [kafine:partition()]}.

convert_topic_partitions(TopicPartitions) ->
    lists:foldl(
        fun(#{name := T, partitions := Ps0, error_code := ?NONE}, Acc1) ->
            Ps = lists:sort(
                lists:map(fun(#{error_code := ?NONE, partition_index := P}) -> P end, Ps0)
            ),
            Acc1#{T => Ps}
        end,
        #{},
        TopicPartitions
    ).
