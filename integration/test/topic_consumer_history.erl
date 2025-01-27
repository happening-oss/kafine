-module(topic_consumer_history).
-export([start_link/0, stop/1]).
-export([append/2, get_history/1, get_history/3, reset_history/1]).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/1, code_change/3]).

start_link() ->
    gen_server:start_link(?MODULE, [], []).

stop(Pid) ->
    gen_server:stop(Pid).

append(Pid, Event) ->
    gen_server:call(Pid, {append, Event}).

get_history(Pid) ->
    gen_server:call(Pid, get_history).

get_history(Pid, Topic, PartitionIndex) ->
    lists:filter(
        fun({{T, P}, _}) ->
            T == Topic andalso P == PartitionIndex
        end,
        gen_server:call(Pid, get_history)
    ).

reset_history(Pid) ->
    gen_server:call(Pid, reset_history).

init([]) ->
    {ok, []}.

handle_call({append, Event}, _From, State) ->
    {reply, ok, State ++ [Event]};
handle_call(get_history, _From, State) ->
    {reply, State, State};
handle_call(reset_history, _From, _State) ->
    {reply, ok, []}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(_, State) ->
    {noreply, State}.

terminate(_State) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.
