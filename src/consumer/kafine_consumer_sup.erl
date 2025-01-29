-module(kafine_consumer_sup).
-moduledoc false.
-export([
    start_group_consumer_linked/8,
    start_topic_consumer_linked/7
]).
-behaviour(supervisor).
-export([init/1]).

start_consumer(
    Ref,
    Broker,
    ConnectionOptions,
    ConsumerOptions,
    ConsumerCallback,
    SubscriberSpec
) ->
    {ok, Sup} = supervisor:start_link(?MODULE, [Ref]),

    ConsumerSpec = #{
        id => make_consumer_id(Ref),
        start =>
            {kafine_consumer, start_link, [
                Ref, Broker, ConnectionOptions, ConsumerCallback, ConsumerOptions
            ]}
    },
    {ok, _Consumer} = supervisor:start_child(Sup, ConsumerSpec),

    {ok, _S} = supervisor:start_child(Sup, SubscriberSpec),

    {ok, Sup}.

% TODO: Maybe 'Topics' can be a map OR a list?
start_group_consumer_linked(
    Ref,
    Broker,
    ConnectionOptions,
    GroupId,
    MembershipOptions,
    ConsumerOptions,
    ConsumerCallback,
    Topics
) ->
    SubscriberSpec = #{
        id => make_membership_id(Ref),
        start =>
            {kafine_eager_rebalance, start_link, [
                Ref, Broker, ConnectionOptions, GroupId, MembershipOptions, Topics
            ]}
    },
    start_consumer(
        Ref,
        Broker,
        ConnectionOptions,
        ConsumerOptions,
        ConsumerCallback,
        SubscriberSpec
    ).

start_topic_consumer_linked(
    Ref,
    Broker,
    ConnectionOptions,
    SubscriberOptions,
    ConsumerOptions,
    ConsumerCallback,
    Topics
) ->
    SubscriberSpec = #{
        id => make_subscriber_id(Ref),
        start =>
            {kafine_topic_subscriber, start_link, [
                Ref, Broker, ConnectionOptions, SubscriberOptions, Topics
            ]},
        % If this process stops normal then its job is done and doesn't
        % need to be restarted
        restart => transient
    },
    start_consumer(
        Ref,
        Broker,
        ConnectionOptions,
        ConsumerOptions,
        ConsumerCallback,
        SubscriberSpec
    ).

init([Ref]) ->
    kafine_proc_lib:set_label({?MODULE, Ref}),
    {ok, {{one_for_one, 1, 5}, []}}.

make_consumer_id(Ref) ->
    {kafine_consumer, Ref}.
make_membership_id(Ref) ->
    {kafine_membership, Ref}.
make_subscriber_id(Ref) ->
    {kafine_subscriber, Ref}.
