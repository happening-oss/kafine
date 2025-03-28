-module(kafine_consumer_callback_process).
-export([
    start_link/5,
    stop/1,
    partition_data/4
]).
-behaviour(gen_server).
-export([
    init/1,
    handle_call/3,
    handle_cast/2
]).

-record(state, {
    callback,
    state_data
}).

start_link(Ref, Topic, PartitionIndex, Callback, CallbackArgs) ->
    gen_server:start_link(
        ?MODULE, [Ref, Topic, PartitionIndex, Callback, CallbackArgs], start_options()
    ).

start_options() -> [{debug, kafine_trace:debug_options(#{mfa => {?MODULE, handle_event, 4}})}].

stop(Pid) ->
    gen_server:stop(Pid).

partition_data(Pid, Topic, PartitionData, FetchOffset) ->
    gen_server:call(
        Pid, {partition_data, {Topic, PartitionData, FetchOffset}}, 3600000
    ).

init([Ref, Topic, PartitionIndex, Callback, CallbackArgs]) ->
    {State, StateData} = callback_init(
        Topic, PartitionIndex, Callback, CallbackArgs
    ),

    kafine_consumer:init_ack(Ref, Topic, PartitionIndex, State),
    {ok, #state{callback = Callback, state_data = StateData}}.

callback_init(Topic, PartitionIndex, Callback, CallbackArgs) ->
    callback_init_result(Callback:init(Topic, PartitionIndex, CallbackArgs)).

callback_init_result({ok, State}) ->
    {active, State};
callback_init_result({pause, State}) ->
    {paused, State};
callback_init_result(Other) ->
    erlang:error({bad_callback_return, Other}).

handle_call(
    {partition_data, {Topic, PartitionData, FetchOffset}},
    _From,
    State = #state{callback = Callback, state_data = StateData1}
) ->
    {NextOffset, State2, StateData2} = kafine_fetch_response_partition_data:fold(
        Topic, PartitionData, FetchOffset, Callback, StateData1
    ),
    {reply, {ok, {NextOffset, State2}}, State#state{state_data = StateData2}}.

handle_cast(_Req, State) ->
    {noreply, State}.
