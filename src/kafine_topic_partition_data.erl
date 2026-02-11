-module(kafine_topic_partition_data).

-export([
    new/0,
    single/3,
    put/4,
    get/3,
    get/4,
    is_key/3,
    remove/3,
    take/3,
    is_empty/1,
    foreach/2,
    fold/3,
    any/2,
    filter/2,
    map/2,
    filtermap/2,
    split_with/2,
    merge/2,
    merge/1,

    topic_partitions/1
]).

-export_type([t/1]).

-type t(DataType) :: #{kafine:topic() => #{kafine:partition() => DataType}}.

-spec new() -> t(dynamic()).

new() ->
    #{}.

-spec single(
    Topic :: kafine:topic(),
    Partition :: kafine:partition(),
    Data :: DataType
) -> t(DataType) when
    DataType :: dynamic().

single(Topic, Partition, Data) ->
    #{Topic => #{Partition => Data}}.

-spec put(
    Topic :: kafine:topic(),
    Partition :: kafine:partition(),
    Data :: any(),
    TopicPartitionData :: t(DataType)
) -> t(DataType) when
    DataType :: dynamic().

put(Topic, Partition, Data, TopicPartitionData) ->
    kafine_maps:put([Topic, Partition], Data, TopicPartitionData).

-spec get(
    Topic :: kafine:topic(),
    Partition :: kafine:partition(),
    TopicPartitionData :: t(DataType)
) -> DataType | undefined when
    DataType :: dynamic().

get(Topic, Partition, TopicPartitionData) ->
    get(Topic, Partition, TopicPartitionData, undefined).

-spec get(
    Topic :: kafine:topic(),
    Partition :: kafine:partition(),
    TopicPartitionData :: t(DataType),
    Default :: DefaultType
) -> DataType | DefaultType when
    DataType :: dynamic(), DefaultType :: dynamic().

get(Topic, Partition, TopicPartitionData, Default) ->
    kafine_maps:get([Topic, Partition], TopicPartitionData, Default).

-spec is_key(
    Topic :: kafine:topic(),
    Partition :: kafine:partition(),
    TopicPartitionData :: t(DataType)
) -> boolean() when
    DataType :: any().

is_key(Topic, Partition, TopicPartitionData) ->
    kafine_maps:is_key([Topic, Partition], TopicPartitionData).

-spec remove(
    Topic :: kafine:topic(),
    Partition :: kafine:partition(),
    TopicPartitionData :: t(DataType)
) -> t(DataType) when
    DataType :: dynamic().

remove(Topic, Partition, TopicPartitionData) ->
    kafine_maps:remove([Topic, Partition], TopicPartitionData).

-spec take(
    Topic :: kafine:topic(),
    Partition :: kafine:partition(),
    TopicPartitionData :: t(DataType)
) -> {DataType, t(DataType)} | error when
    DataType :: dynamic().

take(Topic, Partition, TopicPartitionData) ->
    kafine_maps:take([Topic, Partition], TopicPartitionData).

-spec is_empty(TopicPartitionData :: t(DataType)) -> boolean() when
    DataType :: dynamic().

is_empty(TopicPartitionData) ->
    TopicPartitionData =:= #{}.

-spec foreach(
    Fun :: fun(
        (Topic :: kafine:topic(), Partition :: kafine:partition(), Data :: DataType) -> any()
    ),
    TopicPartitionData :: t(DataType)
) -> ok when
    DataType :: dynamic().

foreach(Fun, TopicPartitionData) ->
    maps:foreach(
        fun(Topic, Partitions) ->
            maps:foreach(
                fun(Partition, Data) ->
                    Fun(Topic, Partition, Data)
                end,
                Partitions
            )
        end,
        TopicPartitionData
    ),
    ok.

-spec fold(
    Fun :: fun(
        (Topic :: kafine:topic(), Partition :: kafine:partition(), Data :: DataType, Acc :: AccType) ->
            AccType
    ),
    Acc0 :: AccType,
    TopicPartitionData :: t(DataType)
) -> AccType when
    DataType :: dynamic(), AccType :: dynamic().

fold(Fun, Acc0, TopicPartitionData) ->
    maps:fold(
        fun(Topic, Partitions, Acc1) ->
            maps:fold(
                fun(Partition, Data, Acc2) ->
                    Fun(Topic, Partition, Data, Acc2)
                end,
                Acc1,
                Partitions
            )
        end,
        Acc0,
        TopicPartitionData
    ).

-spec any(
    Pred :: fun(
        (Topic :: kafine:topic(), Partition :: kafine:partition(), Data :: DataType) -> boolean()
    ),
    TopicPartitionData :: t(DataType)
) -> boolean() when
    DataType :: dynamic().

any(Pred, TopicPartitionData) ->
    any_topic_(Pred, maps:next(maps:iterator(TopicPartitionData))).

any_topic_(_, none) ->
    false;
any_topic_(Pred, {Topic, Partitions, Iterator}) ->
    case any_partition_(Pred, Topic, maps:next(maps:iterator(Partitions))) of
        true -> true;
        false -> any_topic_(Pred, maps:next(Iterator))
    end.

any_partition_(_, _, none) ->
    false;
any_partition_(Pred, Topic, {Partition, Data, Iterator}) ->
    case Pred(Topic, Partition, Data) of
        true -> true;
        false -> any_partition_(Pred, Topic, maps:next(Iterator))
    end.

-spec filter(
    Pred :: fun(
        (Topic :: kafine:topic(), Partition :: kafine:partition(), Data :: DataType) -> boolean()
    ),
    TopicPartitionData :: t(DataType)
) -> t(DataType) when
    DataType :: dynamic().

filter(Pred, TopicPartitionData) ->
    maps:filtermap(
        fun(Topic, Partitions) ->
            FilteredPartitions =
                maps:filter(
                    fun(Partition, Data) ->
                        Pred(Topic, Partition, Data)
                    end,
                    Partitions
                ),
            case FilteredPartitions =:= #{} of
                true -> false;
                false -> {true, FilteredPartitions}
            end
        end,
        TopicPartitionData
    ).

-spec map(
    Fun :: fun(
        (Topic :: kafine:topic(), Partition :: kafine:partition(), Data :: DataType) -> NewDataType
    ),
    TopicPartitionData :: t(DataType)
) -> t(NewDataType) when
    DataType :: dynamic(), NewDataType :: dynamic().

map(Fun, TopicPartitionData) ->
    maps:map(
        fun(Topic, Partitions) ->
            maps:map(
                fun(Partition, Data) ->
                    Fun(Topic, Partition, Data)
                end,
                Partitions
            )
        end,
        TopicPartitionData
    ).

-spec filtermap(
    Fun :: fun(
        (Topic :: kafine:topic(), Partition :: kafine:partition(), Data :: DataType) ->
            {true, NewDataType} | false
    ),
    TopicPartitionData :: t(DataType)
) -> t(NewDataType) when
    DataType :: dynamic(), NewDataType :: dynamic().

filtermap(Fun, TopicPartitionData) ->
    maps:filtermap(
        fun(Topic, Partitions) ->
            FilterMappedPartitions =
                maps:filtermap(
                    fun(Partition, Data) ->
                        Fun(Topic, Partition, Data)
                    end,
                    Partitions
                ),
            case FilterMappedPartitions =:= #{} of
                true -> false;
                false -> {true, FilterMappedPartitions}
            end
        end,
        TopicPartitionData
    ).

-spec split_with(
    Pred :: fun(
        (Topic :: kafine:topic(), Partition :: kafine:partition(), Data :: DataType) -> boolean()
    ),
    TopicPartitionData :: t(DataType)
) -> {t(DataType), t(DataType)} when
    DataType :: dynamic().

split_with(Pred, TopicPartitionData) ->
    {True, False} = maps:fold(
        fun(Topic, Partitions, {TopicsTrueAcc, TopicsFalseAcc}) ->
            {TruePartitions, FalsePartitions} =
                maps:fold(
                    fun(Partition, Data, {PartitionsTrueAcc, PartitionsFalseAcc}) ->
                        case Pred(Topic, Partition, Data) of
                            true ->
                                {[{Partition, Data} | PartitionsTrueAcc], PartitionsFalseAcc};
                            false ->
                                {PartitionsTrueAcc, [{Partition, Data} | PartitionsFalseAcc]}
                        end
                    end,
                    {[], []},
                    Partitions
                ),
            {
                case TruePartitions of
                    [] -> TopicsTrueAcc;
                    _ -> [{Topic, TruePartitions} | TopicsTrueAcc]
                end,
                case FalsePartitions of
                    [] -> TopicsFalseAcc;
                    _ -> [{Topic, FalsePartitions} | TopicsFalseAcc]
                end
            }
        end,
        {[], []},
        TopicPartitionData
    ),
    {from_lists(True), from_lists(False)}.

-spec from_lists([{kafine:topic(), [{kafine:partition(), DataType}]}]) -> t(DataType) when
    DataType :: dynamic().

from_lists(Lists) ->
    #{
        Topic => #{Partition => Data || {Partition, Data} <- Partitions}
     || {Topic, Partitions} <- Lists
    }.

-spec merge(
    TopicPartitionData1 :: t(DataType),
    TopicPartitionData2 :: t(DataType)
) -> t(DataType) when
    DataType :: dynamic().

merge(TopicPartitionData1, TopicPartitionData2) when TopicPartitionData1 =:= #{} ->
    TopicPartitionData2;
merge(TopicPartitionData1, TopicPartitionData2) when TopicPartitionData2 =:= #{} ->
    TopicPartitionData1;
merge(TopicPartitionData1, TopicPartitionData2) ->
    maps:merge_with(
        fun(_Topic, Partitions1, Partitions2) ->
            maps:merge(Partitions1, Partitions2)
        end,
        TopicPartitionData1,
        TopicPartitionData2
    ).

-spec merge(
    TopicPartitionDataList :: [t(DataType)]
) -> t(DataType) when
    DataType :: dynamic().

merge(TopicPartitionDataList) ->
    lists:foldl(
        fun(TopicPartitionData, Acc) ->
            merge(Acc, TopicPartitionData)
        end,
        #{},
        TopicPartitionDataList
    ).

-spec topic_partitions(TopicPartitionData :: t(DataType)) -> kafine_topic_partitions:t() when
    DataType :: dynamic().

topic_partitions(TopicPartitionData) ->
    maps:map(
        fun(_Topic, PartitionInfo) -> maps:keys(PartitionInfo) end,
        TopicPartitionData
    ).
