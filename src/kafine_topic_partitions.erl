-module(kafine_topic_partitions).

-export([
    new/0,
    single/2,
    add/3,
    is_empty/1,
    to_list/1,
    topics/1,
    count/1,
    member/3,
    any/2,
    all/2,
    fold/3,
    map/2,
    filtermap/2
]).

-export_type([t/0]).

-type t() :: #{kafine:topic() => [kafine:partition()]}.

-spec new() -> t().

new() ->
    #{}.

-spec single(Topic :: kafine:topic(), Partition :: kafine:partition()) -> t().

single(Topic, Partition) ->
    #{Topic => [Partition]}.

-spec add(
    Topic :: kafine:topic(),
    Partition :: kafine:partition(),
    TopicPartitions :: t()
) -> t().

add(Topic, Partition, TopicPartitions) ->
    maps:update_with(
        Topic,
        fun(Partitions) -> [Partition | Partitions] end,
        [Partition],
        TopicPartitions
    ).

-spec is_empty(TopicPartitions :: t()) -> boolean().

is_empty(TopicPartitions) ->
    is_empty_(maps:next(maps:iterator(TopicPartitions))).

is_empty_(none) ->
    true;
is_empty_({_Topic, _Partitions = [], Iterator}) ->
    is_empty_(maps:next(Iterator));
is_empty_({_Topic, _Partitions, _Iterator}) ->
    false.

-spec to_list(TopicPartitions :: t()) -> [{kafine:topic(), [kafine:partition()]}].

to_list(TopicPartitions) ->
    maps:to_list(TopicPartitions).

-spec topics(TopicPartitions :: t()) -> [kafine:topic()].

topics(TopicPartitions) ->
    maps:keys(TopicPartitions).

-spec count(TopicPartitions :: t()) -> non_neg_integer().

count(TopicPartitions) ->
    maps:fold(
        fun(_Topic, Partitions, Acc) -> Acc + length(Partitions) end,
        0,
        TopicPartitions
    ).

-spec member(Topic :: kafine:topic(), Partition :: kafine:partition(), TopicPartitions :: t()) ->
    boolean().

member(Topic, Partition, TopicPartitions) ->
    case maps:get(Topic, TopicPartitions, undefined) of
        undefined -> false;
        Partitions -> lists:member(Partition, Partitions)
    end.

-spec any(
    Pred :: fun((Topic :: kafine:topic(), Partition :: kafine:partition()) -> boolean()),
    TopicPartitions :: t()
) -> boolean().

any(Pred, TopicPartitions) ->
    any_(Pred, maps:next(maps:iterator(TopicPartitions))).

any_(_Pred, none) ->
    false;
any_(Pred, {Topic, Partitions, Iterator}) ->
    case lists:any(fun(Partition) -> Pred(Topic, Partition) end, Partitions) of
        true -> true;
        false -> any_(Pred, maps:next(Iterator))
    end.

-spec all(
    Pred :: fun((Topic :: kafine:topic(), Partition :: kafine:partition()) -> boolean()),
    TopicPartitions :: t()
) -> boolean().

all(Pred, TopicPartitions) ->
    not any(
        fun(Topic, Partition) -> not Pred(Topic, Partition) end,
        TopicPartitions
    ).

-spec fold(
    Fun :: fun(
        (Topic :: kafine:topic(), Partition :: kafine:partition(), Acc :: AccType) -> AccType
    ),
    Acc :: AccType,
    TopicPartitions :: t()
) -> AccType when
    AccType :: dynamic().

fold(Fun, Acc0, TopicPartitions) ->
    maps:fold(
        fun(Topic, Partitions, Acc1) ->
            lists:foldl(
                fun(Partition, Acc2) ->
                    Fun(Topic, Partition, Acc2)
                end,
                Acc1,
                Partitions
            )
        end,
        Acc0,
        TopicPartitions
    ).

-spec map(
    Fun :: fun((Topic :: kafine:topic(), Partition :: kafine:partition()) -> DataType),
    TopicPartitions :: t()
) -> kafine_topic_partition_data:t(DataType) when
    DataType :: dynamic().

map(Fun, TopicPartitions) ->
    maps:map(
        fun(Topic, Partitions) ->
            maps:from_list(
                lists:map(
                    fun(Partition) ->
                        {Partition, Fun(Topic, Partition)}
                    end,
                    Partitions
                )
            )
        end,
        TopicPartitions
    ).

-spec filtermap(
    Fun :: fun(
        (Topic :: kafine:topic(), Partition :: kafine:partition()) -> {true, DataType} | false
    ),
    TopicPartitions :: t()
) -> kafine_topic_partition_data:t(DataType) when
    DataType :: dynamic().

filtermap(Fun, TopicPartitions) ->
    maps:filtermap(
        fun(Topic, Partitions) ->
            FilterMappedPartitions =
                lists:filtermap(
                    fun(Partition) ->
                        case Fun(Topic, Partition) of
                            {true, Data} -> {true, {Partition, Data}};
                            false -> false
                        end
                    end,
                    Partitions
                ),
            case FilterMappedPartitions of
                [] -> false;
                _ -> {true, maps:from_list(FilterMappedPartitions)}
            end
        end,
        TopicPartitions
    ).
