-module(kafine).
-include("kafine_doc.hrl").
?MODULEDOC("""
Kafka client for Erlang.
""").

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
    offset_timestamp/0,
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

?DOC("""
Options that control the behaviour of `m:kafine_connection`.
Note that these set of options will be used for all connections of a given concern.
For example, the connection options passed into a `kafine_consumer` will be used for all connections made for that consumer.

- `client_id`: Optional; defaults to `<<"kafine">>`.
- `metadata`: Optional; defaults to `#{}`.

## Client ID

The client ID can be used to correlate requests in broker logging, which can be helpful for troubleshooting.
It also be used to enforce client quotas.

A client ID logically identifies an application making a request. This means that it should be the application name,
rather than the host name.

## Metadata

Allows for extra pieces of telemetry information to be made available for `kafine_connection`.
Kafine by default appends `host`, `port` and `node_id` to the `metadata`.
""").
-type connection_options() :: #{
    client_id := binary(),
    metadata := map()
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
    initial_offset := offset(),
    offset_reset_policy := offset_reset_policy()
}.

?DOC("""
Used when starting a group consumer.

- `assignors`: A list of modules that implement the `m:kafine_assignor` behaviour.
  The consumer group leader uses it to assign topics and partitions to members of the consumer group.
  If multiple assignors use the same protocol name then the module first in the list will be used.
  Use, e.g., `[kafine_range_assignor]`.
- `assignment_callback`: A tuple of `{Module, Args}`.
  Advanced use only; leave it unset to use the recommended default.
  The module is expected to implement the `m:kafine_assignment_callback` behaviour.
  It's called before and after partitions are assigned. Used, for example, to create/delete per-partition ETS tables.
- `subscription_callback`: A tuple of `{Module, Args}`.
  Advanced use only; leave it unset to use the recommended default.
  The module is expected to implement the `m:kafine_subscription_callback` behaviour.
- `heartbeat_interval_ms`, `session_timeout_ms`, `rebalance_timeout_ms`:
  Used to control the behaviour of the consumer coordinator.
  See the Kafka documentation for `heartbeat.interval.ms`, etc., for details.
""").
-type membership_options() :: #{
    assignors => [module()],
    assignment_callback := {module(), term()},
    subscription_callback => {module(), term()},
    heartbeat_interval_ms => non_neg_integer(),
    session_timeout_ms => non_neg_integer(),
    rebalance_timeout_ms => non_neg_integer()
}.

?DOC("""
Used when starting a topic consumer.

- `assignment_callback`: A tuple of `{Module, Args}`.
  Advanced use only; leave it unset to use the recommended default.
  The module is expected to implement the `m:kafine_assignment_callback` behaviour.
  It's called before and after partitions are assigned. Used, for example, to create/delete per-partition ETS tables.
- `subscription_callback`: A tuple of `{Module, Args}`.
  Advanced use only; leave it unset to use the recommended default.
  The module is expected to implement the `m:kafine_subscription_callback` behaviour.
""").
-type subscriber_options() :: #{
    assignment_callback := {module(), term()},
    subscription_callback => {module(), term()}
}.

% TODO: These types should be in kafcod, maybe?
-type node_id() :: non_neg_integer().
-type client_id() :: binary().
-type topic() :: binary().
-type partition() :: non_neg_integer().
-type offset() :: earliest | latest | non_neg_integer().
-type timestamp() :: non_neg_integer().
-type offset_timestamp() :: earliest | latest | neg_integer().
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
-type start_topic_consumer_ret() :: supervisor:startchild_ret().
% TODO: This isn't actually the correct type; it keeps eqalizer happy.
-type start_group_consumer_ret() :: supervisor:startchild_ret().

?DOC("""
Start a simple consumer for one or more topics.

## Arguments
- `Ref` is used to refer to the topic consumer in future calls, for example when stopping it.
- `Bootstrap` is any one of the brokers in the cluster. It is used to bootstrap the consumer.
- `ConnectionOptions` passes options to the network connections to the brokers. See `t:connection_options/0`.
""").

-spec start_topic_consumer(
    Ref :: consumer_ref(),
    Bootstrap :: broker(),
    ConnectionOptions :: connection_options(),
    ConsumerOptions :: consumer_options(),
    SubscriberOptions :: subscriber_options(),
    ConsumerCallback :: {CallbackModule :: module(), CallbackArgs :: term()},
    Topics :: [topic()],
    TopicOptions :: #{topic() => topic_options()}
) -> start_topic_consumer_ret().

start_topic_consumer(
    Ref,
    Bootstrap = #{host := _, port := _},
    ConnectionOptions,
    ConsumerOptions,
    SubscriptionOptions0,
    Callback = {CallbackModule, _},
    Topics,
    TopicOptions0
) when
    is_map(ConnectionOptions),
    is_map(ConsumerOptions),
    is_atom(CallbackModule),
    is_list(Topics),
    is_map(TopicOptions0)
->
    TopicOptions = kafine_topic_options:validate_options(Topics, TopicOptions0),
    DefaultAssignmentCallback =
        {kafine_noop_assignment_callback, undefined},
    DefaultSubscriptionCallback =
        {kafine_topic_consumer_subscription_callback, [Ref, TopicOptions]},

    DefaultSubscriberOptions = #{
        assignment_callback => DefaultAssignmentCallback,
        subscription_callback => DefaultSubscriptionCallback
    },
    SubscriberOptions = maps:merge(DefaultSubscriberOptions, SubscriptionOptions0),
    ok = kafine_behaviour:verify_callbacks_exported(kafine_consumer_callback, CallbackModule),

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

    supervisor:start_child(?CONSUMER_SUPERVISOR, ChildSpec).

-spec stop_topic_consumer(Ref :: consumer_ref()) -> ok.

stop_topic_consumer(Ref) ->
    stop_consumer_sup(Ref).

?DOC("""
Start a member of a consumer group.
""").
%% Ref is used to refer to the consumer later, e.g., in kafine:stop_group_consumer/1.
%% Broker is the bootstrap broker.
%% GroupId is the name of the consumer group.
%% Topics is a list of topics.
%% Options is the consumer options; the defaults are probably fine for most purposes.
%% SubscriptionCallbackModule must implement the kafine_group_membership_callback behaviour.

-spec start_group_consumer(
    Ref :: consumer_ref(),
    Bootstrap :: broker(),
    ConnectionOptions :: kafine:connection_options(),
    GroupId :: binary(),
    MembershipOptions :: membership_options(),
    ConsumerOptions :: kafine:consumer_options(),
    ConsumerCallback :: {module(), term()},
    Topics :: [kafine:topic()],
    TopicOptions :: #{topic() => topic_options()}
) -> start_group_consumer_ret().

start_group_consumer(
    Ref,
    Bootstrap,
    ConnectionOptions,
    GroupId,
    SubscriberOptions0,
    ConsumerOptions,
    ConsumerCallback = {CallbackModule, _CallbackArgs},
    Topics,
    TopicOptions0
) ->
    TopicOptions = kafine_topic_options:validate_options(Topics, TopicOptions0),
    DefaultSubscriberOptions = #{
        assignment_callback => {kafine_noop_assignment_callback, undefined},
        subscription_callback =>
            {kafine_group_consumer_subscription_callback, [
                Ref, GroupId, Topics, TopicOptions, kafine_group_consumer_offset_callback
            ]}
    },
    SubscriberOptions = maps:merge(DefaultSubscriberOptions, SubscriberOptions0),
    ok = kafine_behaviour:verify_callbacks_exported(kafine_consumer_callback, CallbackModule),

    Id = make_consumer_id(Ref),
    ChildSpec = #{
        id => Id,
        start =>
            {kafine_consumer_sup, start_group_consumer_linked, [
                Ref,
                Bootstrap,
                ConnectionOptions,
                GroupId,
                SubscriberOptions,
                ConsumerOptions,
                ConsumerCallback,
                Topics
            ]}
    },
    supervisor:start_child(?CONSUMER_SUPERVISOR, ChildSpec).

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
