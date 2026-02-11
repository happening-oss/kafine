-module(kafine_sup).
-moduledoc false.
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok,
        {{one_for_one, 1, 5}, [
            #{
                id => kafine_consumer_sup_sup,
                start => {kafine_consumer_sup_sup, start_link, []},
                restart => permanent,
                shutdown => infinity,
                type => supervisor,
                modules => [kafine_consumer_sup_sup]
            },
            #{
                id => kafine_producer_sup_sup,
                start => {kafine_producer_sup_sup, start_link, []},
                restart => permanent,
                shutdown => infinity,
                type => supervisor,
                modules => [kafine_producer_sup_sup]
            }
        ]}}.
