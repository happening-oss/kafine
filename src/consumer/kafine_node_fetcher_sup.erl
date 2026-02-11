-module(kafine_node_fetcher_sup).

-behaviour(supervisor).

-export([start_link/4]).

-export([
    start_child/4,
    terminate_child/2,
    list_children/1
]).

-export([init/1]).

id(Ref) -> {?MODULE, Ref}.

via(Ref) ->
    kafine_via:via(id(Ref)).

-spec start_link(
    Ref :: kafine:consumer_ref(),
    ConnectionOptions :: kafine:connection_options(),
    ConsumerOptions :: kafine:consumer_options(),
    TopicOptions :: #{kafine:topic() => kafine:topic_options()}
) ->
    supervisor:startlink_ret().

start_link(Ref, ConnectionOptions, ConsumerOptions, TopicOptions) ->
    supervisor:start_link(
        via(Ref),
        ?MODULE,
        [Ref, ConnectionOptions, ConsumerOptions, TopicOptions]
    ).

-spec start_child(
    Ref :: kafine:consumer_ref(),
    Owner :: pid(),
    Broker :: kafine:broker(),
    Metadata :: telemetry:event_metadata()
) ->
    {ok, pid()} | {error, term()}.

start_child(Ref, Owner, Broker, Metadata) ->
    case supervisor:start_child(via(Ref), [Owner, Broker, Metadata]) of
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

init([Ref, ConnectionOptions, ConsumerOptions, TopicOptions]) ->
    kafine_proc_lib:set_label({?MODULE, Ref}),
    SupFlags = #{strategy => simple_one_for_one, intensity => 1, period => 5},
    ChildSpecs = [
        #{
            id => kafine_node_fetcher,
            % We're a simple_one_for_one; kafine_node_fetcher:start_link() takes extra arguments in start_child(),
            % above.
            start =>
                {kafine_node_fetcher, start_link, [
                    Ref, ConnectionOptions, ConsumerOptions, TopicOptions
                ]},
            restart => transient,
            shutdown => 5000,
            type => worker,
            modules => [kafine_node_fetcher]
        }
    ],
    {ok, {SupFlags, ChildSpecs}}.
