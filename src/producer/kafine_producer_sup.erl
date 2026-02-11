-module(kafine_producer_sup).

-behaviour(supervisor).

-export([start_link/3]).

-export([init/1]).

id(Ref) -> {?MODULE, Ref}.

via(Ref) ->
    kafine_via:via(id(Ref)).

start_link(Ref, Bootstrap, ConnectionOptions) ->
    supervisor:start_link(
        via(Ref),
        ?MODULE,
        [Ref, Bootstrap, kafine_connection_options:validate_options(ConnectionOptions)]
    ).

init([Ref, Bootstrap, ConnectionOptions]) ->
    kafine_proc_lib:set_label({?MODULE, Ref}),
    Children = [
        #{
            id => bootstrap,
            start => {kafine_bootstrap, start_link, [Ref, Bootstrap, ConnectionOptions]},
            restart => permanent,
            shutdown => 5_000,
            type => worker,
            modules => [kafine_bootstrap]
        },
        #{
            id => metadata,
            start => {kafine_metadata_cache, start_link, [Ref]},
            restart => permanent,
            shutdown => 5_000,
            type => worker,
            modules => [kafine_metadata_cache]
        },
        #{
            id => node_producer_sup,
            start => {kafine_node_producer_sup, start_link, [Ref, ConnectionOptions]},
            restart => permanent,
            shutdown => infinity,
            type => supervisor,
            modules => [kafine_node_producer_sup]
        },
        #{
            id => producer,
            start => {kafine_producer, start_link, [Ref]},
            restart => permanent,
            shutdown => 5_000,
            type => worker,
            modules => [kafine_producer]
        }
    ],
    SupFlags = #{
        strategy => one_for_all,
        intensity => 1,
        period => 5
    },
    {ok, {SupFlags, Children}}.
