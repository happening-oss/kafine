-module(kafine_node_producer_sup).

-behaviour(supervisor).

-export([start_link/2]).

-export([
    start_child/3,
    terminate_child/2,
    list_children/1
]).

-export([init/1]).

id(Ref) -> {?MODULE, Ref}.

via(Ref) ->
    kafine_via:via(id(Ref)).

-spec start_link(
    Ref :: kafine:consumer_ref(),
    ConnectionOptions :: kafine:connection_options()
) ->
    supervisor:startlink_ret().

start_link(Ref, ConnectionOptions) ->
    supervisor:start_link(
        via(Ref),
        ?MODULE,
        [Ref, ConnectionOptions]
    ).

-spec start_child(Ref :: kafine:consumer_ref(), Owner :: pid(), Broker :: kafine:broker()) ->
    {ok, pid()} | {error, term()}.

start_child(Ref, Owner, Broker) ->
    case supervisor:start_child(via(Ref), [Owner, Broker]) of
        {ok, Pid} when Pid =/= undefined ->
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

-spec terminate_child(Ref :: kafine:consumer_ref(), Pid :: pid()) -> ok | {error, term()}.

terminate_child(Ref, Pid) ->
    supervisor:terminate_child(via(Ref), Pid).

-spec list_children(RefOrPid :: kafine:consumer_ref() | pid()) -> [pid()].

list_children(Ref) ->
    Children =
        case is_pid(Ref) of
            true -> supervisor:which_children(Ref);
            false -> supervisor:which_children(via(Ref))
        end,
    [Pid || {_Id, Pid, _Type, _Modules} <- Children, is_pid(Pid)].

init([Ref, ConnectionOptions]) ->
    kafine_proc_lib:set_label({?MODULE, Ref}),
    {
        ok,
        {
            #{
                strategy => simple_one_for_one,
                intensity => 1,
                period => 5
            },
            [
                #{
                    id => kafine_node_producer,
                    start => {kafine_node_producer, start_link, [Ref, ConnectionOptions]},
                    restart => transient,
                    shutdown => 5000,
                    type => worker,
                    modules => [kafine_node_producer]
                }
            ]
        }
    }.
