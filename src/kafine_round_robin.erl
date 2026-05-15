-module(kafine_round_robin).
-export([
    fold/4,

    new/1,
    out/1,
    reset/1,
    to_list/1
]).
-export_type([
    t/1
]).

% Implementation of generic round-robin. Note that it's not Kafka-oriented.
%
% Given a list of "targets" and a list of "items", assign the items to the targets in a round-robin fashion.
%
% For example, with targets [a, b, c] and items [1, 2, 3, 4, 5], assign a=1, b=2, c=3, a=4, b=5.
%
% If either the list of targets or the list of items is empty, the result will be "empty" (the initial accumulator will
% be returned).
%
% We allow skipping items, so that a particular target can ignore a particular item. In this case, the item will be
% assigned to the first target that takes the item. If no targets take the item, the item is dropped.
%
% Note: the ability to skip items can result in unbalanced assignments; this is expected.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

% Assign 'Items' to 'Targets' in a round-robin fashion.
fold(Fun, Acc, Targets = [_ | _], Items = [_ | _]) ->
    do_fold(Fun, Acc, new(Targets), Items);
fold(_Fun, Acc, _Targets = [], _Items) ->
    Acc;
fold(_Fun, Acc, _Targets, _Items = []) ->
    Acc.

do_fold(Fun, Acc, Targets, Items = [_ | _]) ->
    do_fold_next(Fun, Acc, out(Targets), Items);
do_fold(_Fun, Acc, _Targets, _Items = []) ->
    Acc.

do_fold_next(Fun, Acc, {{value, Target}, Targets2}, [Item | Items]) ->
    case Fun(Target, Item, Acc) of
        {take, Acc2} ->
            do_fold(Fun, Acc2, reset(Targets2), Items);
        skip ->
            do_fold(Fun, Acc, Targets2, [Item | Items])
    end;
do_fold_next(Fun, Acc, {empty, Targets2}, [_Item | Items]) ->
    % No-one wanted it. Drop it.
    do_fold(Fun, Acc, Targets2, Items).

% Round-robin through the targets is implemented here. Note that it's not just Elixir's 'Stream.cycle', because we need
% to know when we've got to the end.
%
% We implement it with two lists. The first list is potential targets; the second list is used targets.
% If we run out of potential targets, return 'empty', allowing the item to be dropped.
% If the target wants the item (it calls reset), move the used targets to the end of the potential targets.

-type t(T) :: {[T], [T]}.
-spec new(Targets :: nonempty_list(Target)) -> t(Target).

% Create a new round-robin structure with the specified list of targets.
new(Targets = [_ | _]) ->
    {Targets, []}.

-spec out(Targets) -> {{value, Target}, Targets2} | {empty, Targets2} when
    Targets :: t(Target),
    Targets2 :: t(Target).

% Take the first target (and put it at the back of the queue). If we run out of potential targets, return 'empty'.
out({[A | As], Bs}) ->
    {{value, A}, {As, [A | Bs]}};
out({[], Bs}) ->
    {empty, {lists:reverse(Bs), []}}.

% Reset the potential targets, preserving the round-robin position.
reset({As, Bs}) ->
    {lists:append(As, lists:reverse(Bs)), []}.

% Only (currently) used by the tests; might be useful later.
to_list({As, []}) ->
    As;
to_list({As, Bs}) ->
    lists:append(As, lists:reverse(Bs)).
