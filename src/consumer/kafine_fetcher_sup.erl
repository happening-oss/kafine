-module(kafine_fetcher_sup).

-behaviour(supervisor).

-export([
    start_link/5,
    stop/1
]).
-export([init/1]).

start_link(Ref, ConnectionOptions, ConsumerOptions, TopicOptions, Metadata) ->
    supervisor:start_link(
        ?MODULE,
        [Ref, ConnectionOptions, ConsumerOptions, TopicOptions, Metadata]
    ).

init([Ref, ConnectionOptions, ConsumerOptions, TopicOptions, Metadata]) ->
    kafine_proc_lib:set_label({?MODULE, Ref}),
    Children = [
        #{
            id => node_fetcher_sup,
            start =>
                {kafine_node_fetcher_sup, start_link, [
                    Ref, ConnectionOptions, ConsumerOptions, TopicOptions
                ]},
            restart => permanent,
            shutdown => infinity,
            type => supervisor,
            modules => [kafine_node_fetcher_sup]
        },
        #{
            id => fetcher,
            start => {kafine_fetcher, start_link, [Ref, Metadata]},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [kafine_fetcher]
        }
    ],
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
