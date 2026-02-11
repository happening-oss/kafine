-module(kafine).
-include("kafine_doc.hrl").
?MODULEDOC("""
Kafka client for Erlang.
""").

-export([
    start_topic_consumer/9,
    stop_topic_consumer/1,

    start_group_consumer/10,
    stop_group_consumer/1,

    start_producer/3,
    stop_producer/1
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
    parallel_handler_options/0,
    offset_reset_policy/0,
    membership_options/0,
    subscriber_options/0
]).

-type broker() :: #{
    host := binary(), port := inet:port_number(), node_id => non_neg_integer(), _ => _
}.
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
    transport => module,
    transport_options => any(),
    client_id => binary(),
    connect_timeout => infinity | non_neg_integer(),
    request_timeout_ms => non_neg_integer(),
    metadata => map(),
    % TODO: backoff isn't actually used by kafine_connection. This should live somewhere else
    backoff => kafine_backoff:config()
}.

% TODO: fetch_options, instead?
% Or nested fetch_options inside consumer_options -- you're passing them to the consumer (eventually), but they're used in the fetch.
-type consumer_options() :: #{
    max_wait_ms => non_neg_integer(),
    min_bytes => non_neg_integer(),
    max_bytes => non_neg_integer(),

    partition_max_bytes => non_neg_integer(),

    isolation_level => isolation_level()
}.

-type topic_options() :: #{
    initial_offset := offset(),
    offset_reset_policy := offset_reset_policy()
}.

?DOC("""
Used when handling fetches with kafine_parallel_subscription_callback.

- `callback_mod`: The module implementing the `m:kafine_consumer_callback` behaviour.
- `callback_arg`: An arbitrary term that will be passed to the callback module.
- `topic_options`: Optional per-topic options
- `offset_callback`: Optional module implementing the `m:kafine_adjust_fetched_offset_callback` behaviour.
- `skip_empty_fetches`: Whether to skip passing empty fetches to kafine_parallel_handler. Valid values are:
    - `true`: Always skip empty fetches.
    - `false`: Always send empty fetches.
    - `after_first`: Always send the first fetch (even if empty), then skip subsequent empty fetches.
- `error_mode`: How to behave if `callback_mod` produces an error while handling partition data. Valid
  values are:
    - `reset`: Restart from to the last committed offset (or according to the topic's initial offset if no
      committed offset is available)
    - `retry`: Retry the fetch from the offset that caused the error.
    - `skip`: Skip the fetch that caused the error and continue from the next offset.
""").
-type parallel_handler_options() :: #{
    callback_mod := module(),
    callback_arg := term(),
    topic_options => #{topic() => topic_options()},
    offset_callback => module(),
    skip_empty_fetches => boolean() | after_first,
    error_mode => reset | retry | skip
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
    assignment_callback => {module(), term()},
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
    ParallelHandlerOptions :: parallel_handler_options(),
    Topics :: [topic()],
    TopicOptions :: #{topic() => topic_options()},
    Metadata :: telemetry:event_metadata()
) -> start_topic_consumer_ret().

start_topic_consumer(
    Ref,
    Bootstrap = #{host := _, port := _},
    ConnectionOptions0,
    ConsumerOptions0,
    SubscriberOptions0,
    ParallelHandlerOptions0,
    Topics,
    TopicOptions0,
    Metadata
) when
    is_map(ConnectionOptions0),
    is_map(ConsumerOptions0),
    is_map(ParallelHandlerOptions0),
    is_list(Topics),
    is_map(TopicOptions0)
->
    TopicOptions = kafine_topic_options:validate_options(Topics, TopicOptions0),

    DefaultSubscriberOptions = #{
        assignment_callback => {kafine_noop_assignment_callback, undefined},
        subscription_callback => {kafine_parallel_subscription_callback, Ref}
    },
    SubscriberOptions1 = maps:merge(DefaultSubscriberOptions, SubscriberOptions0),
    SubscriberOptions = kafine_topic_subscriber_options:validate_options(SubscriberOptions1),
    ConnectionOptions = kafine_connection_options:validate_options(ConnectionOptions0),
    ConsumerOptions = kafine_consumer_options:validate_options(ConsumerOptions0),
    DefaultParallelHandlerOptions = #{topic_options => TopicOptions},
    ParallelHandlerOptions = kafine_parallel_subscription_callback:validate_options(
        maps:merge(DefaultParallelHandlerOptions, ParallelHandlerOptions0)
    ),

    ParallelCallbackChildSpec = parallel_subscription_child_spec(Ref, ParallelHandlerOptions),
    TopicSubscriberChildSpec = topic_subscriber_child_spec(
        Ref, Topics, SubscriberOptions
    ),

    ChildSpec = consumer_child_spec(
        Ref, Bootstrap, ConnectionOptions, ConsumerOptions, Topics, TopicOptions, Metadata, [
            ParallelCallbackChildSpec, TopicSubscriberChildSpec
        ]
    ),

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
    ParallelHandlerOptions :: parallel_handler_options(),
    Topics :: [kafine:topic()],
    TopicOptions :: #{topic() => topic_options()},
    Metadata :: telemetry:event_metadata()
) -> start_group_consumer_ret().

start_group_consumer(
    Ref,
    Bootstrap,
    ConnectionOptions0,
    GroupId,
    MembershipOptions0,
    ConsumerOptions0,
    ParallelHandlerOptions0,
    Topics,
    TopicOptions0,
    Metadata
) ->
    TopicOptions = kafine_topic_options:validate_options(Topics, TopicOptions0),
    DefaultMembershipOptions = #{
        assignment_callback => {kafine_noop_assignment_callback, undefined},
        subscription_callback => {kafine_parallel_subscription_callback, Ref}
    },
    MembershipOptions1 = maps:merge(DefaultMembershipOptions, MembershipOptions0),
    MembershipOptions = kafine_membership_options:validate_options(MembershipOptions1),
    ConnectionOptions = kafine_connection_options:validate_options(ConnectionOptions0),
    ConsumerOptions = kafine_consumer_options:validate_options(ConsumerOptions0),
    DefaultParallelHandlerOptions = #{topic_options => TopicOptions},
    ParallelHandlerOptions = kafine_parallel_subscription_callback:validate_options(
        maps:merge(DefaultParallelHandlerOptions, ParallelHandlerOptions0)
    ),

    CoordinatorChildSpec = coordinator_child_spec(
        Ref, GroupId, Topics, ConnectionOptions, MembershipOptions
    ),
    ParallelCallbackChildSpec = parallel_subscription_child_spec(Ref, ParallelHandlerOptions),
    EagerRebalanceChildSpec = eager_rebalance_child_spec(Ref, Topics, GroupId, MembershipOptions),

    ChildSpec = consumer_child_spec(
        Ref, Bootstrap, ConnectionOptions, ConsumerOptions, Topics, TopicOptions, Metadata, [
            CoordinatorChildSpec, ParallelCallbackChildSpec, EagerRebalanceChildSpec
        ]
    ),

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

consumer_child_spec(
    Ref,
    Bootstrap,
    ConnectionOptions,
    ConsumerOptions,
    Topics,
    TopicOptions,
    Metadata,
    AdditionalChildSpecs
) ->
    #{
        id => make_consumer_id(Ref),
        start =>
            {kafine_consumer_sup, start_link, [
                Ref,
                Bootstrap,
                ConnectionOptions,
                ConsumerOptions,
                Topics,
                TopicOptions,
                Metadata,
                AdditionalChildSpecs
            ]},
        restart => permanent,
        shutdown => 5000,
        type => supervisor,
        modules => [kafine_consumer_sup]
    }.

coordinator_child_spec(
    Ref,
    GroupId,
    Topics,
    ConnectionOptions,
    MembershipOptions
) ->
    #{
        id => coordinator,
        start =>
            {kafine_coordinator, start_link, [
                Ref, GroupId, Topics, ConnectionOptions, MembershipOptions
            ]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [kafine_coordinator]
    }.

eager_rebalance_child_spec(
    Ref,
    Topics,
    GroupId,
    MembershipOptions
) ->
    #{
        id => eager_rebalance,
        start =>
            {kafine_eager_rebalance, start_link, [Ref, Topics, GroupId, MembershipOptions]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [kafine_eager_rebalance]
    }.

topic_subscriber_child_spec(
    Ref,
    Topics,
    SubscriberOptions
) ->
    #{
        id => coordinator,
        start => {kafine_topic_subscriber, start_link, [Ref, Topics, SubscriberOptions]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [kafine_coordinator]
    }.

parallel_subscription_child_spec(Ref, Options) ->
    Options1 = kafine_parallel_subscription_callback:validate_options(Options),

    #{
        id => kafine_parallel_subscription,
        start => {kafine_parallel_subscription_impl, start_link, [Ref, Options1]},
        restart => permanent,
        shutdown => 5000,
        type => supervisor,
        modules => [kafine_parallel_subscription_impl]
    }.

start_producer(Ref, Bootstrap, ConnectionOptions) ->
    ConnectionOptions1 = kafine_connection_options:validate_options(ConnectionOptions),
    {ok, _} = kafine_producer_sup_sup:start_child(Ref, Bootstrap, ConnectionOptions1),
    {ok, Ref}.

stop_producer(Ref) ->
    kafine_producer_sup_sup:stop_child(Ref),
    ok.
