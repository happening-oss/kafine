-module(kafine_consumer_callback_process).
-export([
    start_link/5,
    stop/1
]).
-behaviour(gen_server).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-record(state, {
    ref,
    topic,
    partition,
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

init([Ref, Topic, PartitionIndex, Callback, CallbackArgs]) ->
    Metadata = #{ref => Ref, topic => Topic, partition_index => PartitionIndex},
    logger:set_process_metadata(Metadata),
    kafine_telemetry:put_metadata(Metadata),
    kafine_proc_lib:set_label({?MODULE, {Ref, Topic, PartitionIndex}}),

    {State, StateData} = callback_init(
        Topic, PartitionIndex, Callback, CallbackArgs
    ),

    kafine_consumer:init_ack(Ref, Topic, PartitionIndex, State),
    {ok, #state{
        ref = Ref,
        topic = Topic,
        partition = PartitionIndex,
        callback = Callback,
        state_data = StateData
    }}.

callback_init(Topic, PartitionIndex, Callback, CallbackArgs) ->
    callback_init_result(Callback:init(Topic, PartitionIndex, CallbackArgs)).

callback_init_result({ok, State}) ->
    {active, State};
callback_init_result({pause, State}) ->
    {paused, State};
callback_init_result(Other) ->
    erlang:error({bad_callback_return, Other}).

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(
    {partition_data,
        {Topic, PartitionData = #{partition_index := PartitionIndex}, FetchOffset, Span}},
    State = #state{ref = Ref, callback = Callback, state_data = StateData1}
) ->
    {NextOffset, State2, StateData2} = kafine_fetch_response_partition_data:fold(
        Topic, PartitionData, FetchOffset, Callback, StateData1
    ),
    kafine_consumer:continue(Ref, Topic, PartitionIndex, NextOffset, State2, Span),
    {noreply, State#state{state_data = StateData2}}.

terminate(_Reason, _State = #state{ref = Ref, topic = Topic, partition = Partition}) ->
    Unsubscription = #{Topic => [Partition]},
    kafine_consumer:unsubscribe(Ref, Unsubscription),
    ok.
