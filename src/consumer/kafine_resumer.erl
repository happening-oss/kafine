-module(kafine_resumer).

-export([
    register/2,
    unregister/1
]).

-export([
    set_next_offset/4,

    resume/3,
    resume/4
]).

-export_type([ref/0]).

-type ref() :: term().

-callback resume(
    Ref :: ref(),
    Topic :: binary(),
    Partition :: non_neg_integer(),
    Offset :: kafine:offset() | kafine:offset_timestamp() | keep_current_offset
) ->
    ok | {error, dynamic()}.

key(Ref) ->
    {?MODULE, Ref}.

register(Ref, Module) ->
    case persistent_term:get(key(Ref), undefined) of
        Current when Current =:= undefined; Current =:= Module ->
            persistent_term:put(key(Ref), Module),
            ok;
        _ ->
            {error, already_registered}
    end.

unregister(Ref) ->
    persistent_term:erase(key(Ref)).

set_next_offset(Ref, Topic, Partition, Offset) ->
    case persistent_term:get(key(Ref), undefined) of
        undefined ->
            {error, not_registered};
        Module ->
            Module:set_next_offset(Ref, Topic, Partition, Offset)
    end.

resume(Ref, Topic, Partition) ->
    resume(Ref, Topic, Partition, keep_current_offset).

resume(Ref, Topic, Partition, Offset) ->
    case persistent_term:get(key(Ref), undefined) of
        undefined ->
            {error, not_registered};
        Module ->
            Module:resume(Ref, Topic, Partition, Offset)
    end.
