-module(kafine_consumer).

-export([
    resume/3,
    resume/4,

    all/0,
    info/0,
    info/1
]).

-export_type([ref/0]).

-type ref() :: term().

resume(Ref, Topic, Partition) ->
    kafine_resumer:resume(Ref, Topic, Partition).

resume(Ref, Topic, Partition, Offset) ->
    kafine_resumer:resume(Ref, Topic, Partition, Offset).

all() ->
    [
        Ref
     || {{kafine_consumer_sup, Ref}, _, _, _} <- supervisor:which_children(kafine_consumer_sup_sup)
    ].

info() ->
    #{Ref => info(Ref) || Ref <- all()}.

info(Ref) ->
    Info0 = #{
        bootstrap => kafine_bootstrap:info(Ref),
        metadata => kafine_metadata_cache:info(Ref),
        fetcher => kafine_fetcher:info(Ref),
        node_fetchers => node_fetchers_info(Ref)
    },
    Info1 =
        case kafine_coordinator:whereis(Ref) of
            undefined -> Info0;
            CoordinatorPid -> Info0#{coordinator => kafine_coordinator:info(CoordinatorPid)}
        end,
    Info2 =
        case kafine_parallel_subscription_impl:whereis(Ref) of
            undefined ->
                Info1;
            ParallelSubscriptionPid ->
                Info1#{
                    parallel_subscription => kafine_parallel_subscription_impl:info(
                        ParallelSubscriptionPid
                    )
                }
        end,
    Info3 =
        case kafine_eager_rebalance:whereis(Ref) of
            undefined ->
                Info2;
            EagerRebalancePid ->
                Info2#{eager_rebalance => kafine_eager_rebalance:info(EagerRebalancePid)}
        end,
    Info4 =
        case kafine_topic_subscriber:whereis(Ref) of
            undefined ->
                Info3;
            TopicSubscriberPid ->
                Info2#{topic_subscriber => kafine_topic_subscriber:info(TopicSubscriberPid)}
        end,
    Info4.

node_fetchers_info(Ref) ->
    NodeFetchers = kafine_node_fetcher_sup:list_children(Ref),
    Infos = [kafine_node_fetcher:info(Pid) || Pid <- NodeFetchers],
    #{NodeId => Info || #{node_id := NodeId} = Info <- Infos}.
