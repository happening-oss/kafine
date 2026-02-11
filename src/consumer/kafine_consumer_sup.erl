-module(kafine_consumer_sup).
-include("../kafine_doc.hrl").
?MODULEDOC("""
Generic supervision tree for a kafine consumer
""").

-behaviour(supervisor).

-export([
    start_link/8,
    stop/1
]).

-export([init/1]).

id(Ref) -> {?MODULE, Ref}.

via(Ref) ->
    kafine_via:via(id(Ref)).

?DOC("""
Start and link a supervision tree for a kafine consumer.

- `Ref` - Unique reference for this consumer
- `Bootstrap` - Connection information for the bootstrap broker
- `ConnectionOptions` - Options for establishing connections to brokers
- `ConsumerOptions` - Options for the consumer
- `Topics` - List of topics to consume from
- `TopicOptions` - Options for the topics
- `AdditionalChildSpecs` - Additional child specifications to include in the supervision tree.
                           It's likely you at least want a child spec for a subscriber in here.
""").

start_link(
    Ref,
    Bootstrap,
    ConnectionOptions,
    ConsumerOptions,
    Topics,
    TopicOptions,
    Metadata,
    AdditionalChildSpecs
) ->
    supervisor:start_link(
        via(Ref),
        ?MODULE,
        [
            Ref,
            Bootstrap,
            kafine_connection_options:validate_options(ConnectionOptions),
            kafine_consumer_options:validate_options(ConsumerOptions),
            kafine_topic_options:validate_options(Topics, TopicOptions),
            Metadata,
            AdditionalChildSpecs
        ]
    ).

init([
    Ref,
    Bootstrap,
    ConnectionOptions,
    ConsumerOptions,
    TopicOptions,
    Metadata,
    AdditionalChildSpecs
]) ->
    kafine_proc_lib:set_label({?MODULE, Ref}),
    Children =
        [
            #{
                id => bootstrap,
                start => {kafine_bootstrap, start_link, [Ref, Bootstrap, ConnectionOptions]},
                restart => permanent,
                shutdown => 5000,
                type => worker,
                modules => [kafine_bootstrap]
            },
            #{
                id => metadata_cache,
                start => {kafine_metadata_cache, start_link, [Ref]},
                restart => permanent,
                shutdown => 5000,
                type => worker,
                modules => [kafine_metadata_cache]
            },
            #{
                id => fetcher_sup,
                start =>
                    {kafine_fetcher_sup, start_link, [
                        Ref, ConnectionOptions, ConsumerOptions, TopicOptions, Metadata
                    ]},
                restart => permanent,
                shutdown => infinity,
                type => supervisor,
                modules => [kafine_fetcher_sup]
            }
        ] ++ AdditionalChildSpecs,
    SupFlags = #{
        strategy => one_for_all,
        intensity => 1,
        period => 5
    },
    {ok, {SupFlags, Children}}.

stop(Pid) when is_pid(Pid) ->
    monitor(process, Pid),
    exit(Pid, normal),
    receive
        {'DOWN', _, process, Pid, _} -> ok
    end.
