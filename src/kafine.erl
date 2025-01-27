-module(kafine).
-export([
    start_topic_consumer/8,
    stop_topic_consumer/1,
    start_group_consumer/9,
    stop_group_consumer/1
]).

-export_type([
    node_id/0,
    client_id/0,
    topic/0,
    partition/0,
    offset/0,
    timestamp/0,
    error_code/0,
    isolation_level/0
]).
-export_type([
    broker/0,
    connection/0,
    connection_options/0,
    consumer_options/0,
    topic_options/0,
    offset_reset_policy/0,
    membership_options/0,
    subscriber_options/0
]).

-type broker() :: #{host := binary(), port := inet:port_number(), node_id => non_neg_integer()}.
-type connection() :: pid().

% default is whatever kcat does; probably earliest.
-type offset_reset_policy() :: earliest | latest | module().
-type isolation_level() :: read_uncommitted | read_committed.

-type connection_options() :: #{
    client_id := binary()
}.

% TODO: fetch_options, instead?
-type consumer_options() :: #{
    max_wait_ms := non_neg_integer(),
    min_bytes := non_neg_integer(),
    max_bytes := non_neg_integer(),

    partition_max_bytes := non_neg_integer(),

    isolation_level := isolation_level()
}.

-type topic_options() :: #{
    offset_reset_policy := offset_reset_policy()
}.

-type membership_options() :: #{
    assignor => module(),
    subscription_callback => {module(), term()},
    assignment_callback := {module(), term()},
    heartbeat_interval_ms => non_neg_integer(),
    session_timeout_ms => non_neg_integer(),
    rebalance_timeout_ms => non_neg_integer()
}.

-type subscriber_options() :: #{
    subscription_callback => {module(), term()},
    assignment_callback := {module(), term()}
}.

% TODO: These types should be in kafcod, maybe?
-type node_id() :: non_neg_integer().
-type client_id() :: binary().
-type topic() :: binary().
-type partition() :: non_neg_integer().
-type offset() :: non_neg_integer().
-type timestamp() :: non_neg_integer().
-type error_code() :: integer().

-define(CONSUMER_SUPERVISOR, kafine_consumer_sup_sup).

-export_type([
    consumer_ref/0,
    start_topic_consumer_ret/0,
    start_group_consumer_ret/0
]).

%% Ref is used to refer to the consumer later, e.g., in kafine:stop_topic_consumer/1.
%% Bootstrap is the bootstrap broker.
%% Topics is a list of topics.
%% Options is the consumer options; the defaults are probably fine for most purposes.
%% Callback must implement the kafine_consumer_callback behaviour.

-type consumer_ref() :: term().
-type start_topic_consumer_ret() :: {ok, undefined | pid()}.
% TODO: This isn't actually the correct type; it keeps eqalizer happy.
-type start_group_consumer_ret() :: {ok, undefined | pid()}.

-spec start_topic_consumer(
    Ref :: consumer_ref(),
    Bootstrap :: broker(),
    ConnectionOptions :: connection_options(),
    ConsumerOptions :: consumer_options(),
    SubscriberOptions :: subscriber_options(),
    {Callback :: module(), Args :: term()},
    Topics :: [topic()],
    TopicOptions :: #{topic() := topic_options()}
) -> start_topic_consumer_ret().

start_topic_consumer(
    Ref,
    Bootstrap = #{host := _, port := _},
    ConnectionOptions,
    ConsumerOptions,
    SubscriptionOptions0,
    Callback = {CallbackModule, _},
    Topics,
    TopicOptions
) when
    is_map(ConnectionOptions),
    is_map(ConsumerOptions),
    is_atom(CallbackModule),
    is_list(Topics),
    is_map(TopicOptions)
->
    kafine_topic_options:validate_options(Topics, TopicOptions),
    DefaultSubscriberOptions = #{
        assignment_callback => {kafine_noop_assignment_callback, undefined},
        subscription_callback =>
            {kafine_topic_consumer_subscription_callback, [
                Ref, TopicOptions
            ]}
    },
    SubscriberOptions = maps:merge(DefaultSubscriberOptions, SubscriptionOptions0),

    Id = make_consumer_id(Ref),
    ChildSpec = #{
        id => Id,
        start =>
            {kafine_consumer_sup, start_topic_consumer_linked, [
                Ref,
                Bootstrap,
                ConnectionOptions,
                SubscriberOptions,
                ConsumerOptions,
                Callback,
                Topics
            ]}
    },

    {ok, _GSup} = supervisor:start_child(?CONSUMER_SUPERVISOR, ChildSpec),
    Consumer = kafine_via:whereis_name({kafine_consumer, Ref}),

    {ok, Consumer}.

-spec stop_topic_consumer(Ref :: consumer_ref()) -> ok.

stop_topic_consumer(Ref) ->
    stop_consumer_sup(Ref).

%% Ref is used to refer to the consumer later, e.g., in kafine:stop_group_consumer/1.
%% Broker is the bootstrap broker.
%% GroupId is the name of the consumer group.
%% Topics is a list of topics.
%% Options is the consumer options; the defaults are probably fine for most purposes.
%% SubscriptionCallbackModule must implement the kafine_group_membership_callback behaviour.

-spec start_group_consumer(
    Ref :: consumer_ref(),
    Broker :: broker(),
    ConnectionOptions :: kafine:connection_options(),
    GroupId :: binary(),
    MembershipOptions :: membership_options(),
    ConsumerOptions :: kafine:consumer_options(),
    ConsumerCallback :: {module(), term()},
    Topics :: [kafine:topic()],
    TopicOptions :: #{topic() := topic_options()}
) -> start_group_consumer_ret().

start_group_consumer(
    Ref,
    Broker,
    ConnectionOptions,
    GroupId,
    SubscriberOptions0,
    ConsumerOptions,
    ConsumerCallback,
    Topics,
    TopicOptions
) ->
    kafine_topic_options:validate_options(Topics, TopicOptions),
    DefaultSubscriberOptions = #{
        assignment_callback => {kafine_noop_assignment_callback, undefined},
        subscription_callback =>
            {kafine_group_consumer_subscription_callback, [
                Ref, GroupId, TopicOptions, kafine_group_consumer_offset_callback
            ]}
    },
    SubscriberOptions = maps:merge(DefaultSubscriberOptions, SubscriberOptions0),

    Id = make_consumer_id(Ref),
    ChildSpec = #{
        id => Id,
        start =>
            {kafine_consumer_sup, start_group_consumer_linked, [
                Ref,
                Broker,
                ConnectionOptions,
                GroupId,
                SubscriberOptions,
                ConsumerOptions,
                ConsumerCallback,
                Topics
            ]}
    },
    {ok, _GSup} = supervisor:start_child(?CONSUMER_SUPERVISOR, ChildSpec),
    Consumer = kafine_via:whereis_name({kafine_consumer, Ref}),
    {ok, Consumer}.

-spec stop_group_consumer(Ref :: consumer_ref()) -> ok.

stop_group_consumer(Ref) ->
    stop_consumer_sup(Ref).

stop_consumer_sup(Ref) ->
    Id = make_consumer_id(Ref),
    ok = supervisor:terminate_child(?CONSUMER_SUPERVISOR, Id),
    ok = supervisor:delete_child(?CONSUMER_SUPERVISOR, Id),
    ok.

make_consumer_id(Ref) ->
    {kafine_consumer_sup, Ref}.
