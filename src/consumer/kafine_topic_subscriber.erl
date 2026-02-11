-module(kafine_topic_subscriber).

-behaviour(gen_statem).

-export([
    start_link/3,

    whereis/1,
    info/1
]).

-export([
    init/1,
    callback_mode/0,
    handle_event/4
]).

id(Ref) -> {?MODULE, Ref}.

-spec via(Ref :: kafine:consumer_ref()) -> kafine_via:via().

via(Ref) ->
    kafine_via:via(id(Ref)).

-spec whereis(Ref :: kafine:consumer_ref()) -> pid() | undefined.

whereis(Ref) ->
    kafine_via:whereis_name(id(Ref)).

-spec start_link(
    Ref :: kafine:consumer_ref(),
    Topics :: [kafine:topic()],
    Options :: kafine_topic_subscriber_options:t()
) -> gen_statem:start_ret().

start_link(Ref, Topics, Options) ->
    gen_statem:start_link(
        via(Ref),
        ?MODULE,
        [
            Ref,
            Topics,
            kafine_topic_subscriber_options:validate_options(Options)
        ],
        start_options()
    ).

start_options() ->
    [{debug, kafine_trace:debug_options(#{mfa => {?MODULE, handle_event, 4}})}].

-spec info(RefOrPid :: kafine:consumer_ref() | pid()) ->
    #{
        state := dynamic(),
        topics := [kafine:topic()],
        subscription_callback := {module(), dynamic()},
        assignment_callback := {module(), dynamic()}
    }.

info(Pid) when is_pid(Pid) ->
    gen_statem:call(Pid, info);
info(Ref) ->
    gen_statem:call(via(Ref), info).

callback_mode() -> handle_event_function.

-record(data, {
    ref :: any(),
    topics :: [kafine:topic()],
    subscription_callback :: {module(), term()},
    assignment_callback :: {module(), term()}
}).

init([
    Ref,
    Topics,
    #{
        subscription_callback := {SubscriptionCallback, MembershipArgs},
        assignment_callback := {AssignmentCallback, AssignmentArgs}
    }
]) ->
    Metadata = #{ref => Ref},
    logger:set_process_metadata(Metadata),
    kafine_proc_lib:set_label({?MODULE, Ref}),

    {ok, SubscriptionState} = SubscriptionCallback:init(MembershipArgs),
    {ok, AssignmentState} = AssignmentCallback:init(AssignmentArgs),
    Data = #data{
        ref = Ref,
        topics = Topics,
        subscription_callback = {SubscriptionCallback, SubscriptionState},
        assignment_callback = {AssignmentCallback, AssignmentState}
    },
    {ok, init, Data, {next_event, internal, subscribe}}.

handle_event(
    internal,
    subscribe,
    init,
    Data = #data{
        ref = Ref,
        topics = Topics,
        assignment_callback = {AssignmentCallback, AssignmentState0},
        subscription_callback = {SubscriptionCallback, SubscriptionState0}
    }
) ->
    TopicPartitionInfo = kafine_metadata_cache:partitions(Ref, Topics),
    TopicPartitions = maps:map(
        fun(_Topic, Partitions) -> maps:keys(Partitions) end, TopicPartitionInfo
    ),
    {ok, SubscriptionState, AssignmentState} =
        kafine_assignment:handle_assignment(
            TopicPartitions,
            #{},
            undefined,
            {AssignmentCallback, AssignmentState0},
            {SubscriptionCallback, SubscriptionState0}
        ),

    NewData = Data#data{
        assignment_callback = {AssignmentCallback, AssignmentState},
        subscription_callback = {SubscriptionCallback, SubscriptionState}
    },
    {next_state, idle, NewData, hibernate};
handle_event(
    {call, From},
    info,
    State,
    #data{
        topics = Topics,
        subscription_callback = SubscriptionCallback,
        assignment_callback = AssignmentCallback
    }
) ->
    Info = #{
        state => State,
        topics => Topics,
        subscription_callback => SubscriptionCallback,
        assignment_callback => AssignmentCallback
    },
    {keep_state_and_data, [{reply, From, Info}, hibernate]}.
