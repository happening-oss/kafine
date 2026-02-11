-module(kafine_producer_sup_sup).

-behaviour(supervisor).

-include("../kafine_eqwalizer.hrl").

-export([
    start_link/0,
    start_child/3,
    stop_child/1,
    stop/0
]).

-export([init/1]).

start_link() ->
    supervisor:start_link(
        {local, ?MODULE},
        ?MODULE,
        []
    ).

start_child(Ref, Bootstrap, ConnectionOptions) ->
    supervisor:start_child(
        ?MODULE,
        #{
            id => Ref,
            start => {kafine_producer_sup, start_link, [Ref, Bootstrap, ConnectionOptions]},
            restart => permanent,
            shutdown => infinity,
            type => supervisor,
            modules => [kafine_producer_sup]
        }
    ).

stop_child(Ref) ->
    supervisor:terminate_child(?MODULE, Ref),
    supervisor:delete_child(?MODULE, Ref).

init([]) ->
    kafine_proc_lib:set_label(?MODULE),
    Children = [],
    SupFlags = #{
        strategy => one_for_one,
        intensity => 1,
        period => 5
    },
    {ok, {SupFlags, Children}}.

stop() ->
    Pid = ?DYNAMIC_CAST(whereis(?MODULE)),
    monitor(process, Pid),
    exit(Pid, normal),
    receive
        {'DOWN', _, process, Pid, _} -> ok
    after 10_000 ->
        error(timeout)
    end.
