-module(kafine_assignor_tests).

% Helpers for other kafine assignor tests.
-export([
    create_member_id/1,
    create_member_id/2,

    create_topic_partition_info/1,
    create_topic_partition_info/2,

    ensure_assignments_ordering/1,

    is_balanced/1,
    flatten_partitions/1,
    count_partitions/1
]).

% We also have tests for the kafine_assignor helpers.
-include_lib("eunit/include/eunit.hrl").

% Generate a member ID of the form <<"member-11223344-5566-7788-9900-000000000001">>, where 000000000001 is the
% zero-padded index.
create_member_id(Index) ->
    create_member_id(<<"member">>, Index).

% Generate a member ID of the form <<"prefix-11223344-5566-7788-9900-000000000001">>, where 000000000001 is the
% zero-padded index.
create_member_id(Prefix, Index) when is_binary(Prefix), is_integer(Index) ->
    % Member IDs are usually <prefix>-<uuid>, but we need them to be deterministic for tests.
    Fixed = <<"11223344-5566-7788-9900">>,
    create_member_id(Prefix, Fixed, Index).

create_member_id(Prefix, Fixed, Index) ->
    iolist_to_binary(io_lib:format("~s-~s-~12..0B", [Prefix, Fixed, Index])).

create_topic_partition_info(TopicPartitions) ->
    Replicas = [101, 102],
    create_topic_partition_info(TopicPartitions, Replicas).

create_topic_partition_info(TopicPartitions, Replicas = [LeaderId | _]) ->
    fun(_Cluster, Topics) when is_list(Topics) ->
        maps:map(
            fun(_Topic, Partitions) ->
                #{
                    P => #{leader => LeaderId, replicas => Replicas, isr => Replicas}
                 || P <- Partitions
                }
            end,
            TopicPartitions
        )
    end.

ensure_assignments_ordering(Assignments) when is_map(Assignments) ->
    maps:map(
        fun(_MemberId, Assignment = #{assigned_partitions := AssignedPartitions}) ->
            Assignment#{
                assigned_partitions := maps:map(
                    fun(_Topic, Partitions) -> lists:sort(Partitions) end, AssignedPartitions
                )
            }
        end,
        Assignments
    ).

is_balanced(Assignment) when is_map(Assignment), map_size(Assignment) > 0 ->
    % Taken from the Java. Assert that the minimum and maximum count of assigned partitions don't differ too much.
    {Min, Max} = min_max_of(
        fun(_MemberId, #{assigned_partitions := AssignedPartitions}) ->
            count_partitions(AssignedPartitions)
        end,
        Assignment
    ),
    Max - Min =< 1.

min_max_of(Fun, Map) when is_function(Fun, 2), is_map(Map) ->
    maps:fold(
        fun
            (Key, Value, undefined) ->
                Init = Fun(Key, Value),
                {Init, Init};
            (Key, Value, {Min0, Max0}) ->
                Next = Fun(Key, Value),
                Min = min(Min0, Next),
                Max = max(Max0, Next),
                {Min, Max}
        end,
        undefined,
        Map
    ).

flatten_partitions(TopicPartitions) ->
    lists:sort(
        maps:fold(
            fun(Topic, Partitions, Acc) ->
                lists:foldl(
                    fun(Partition, Acc1) ->
                        [{Topic, Partition} | Acc1]
                    end,
                    Acc,
                    Partitions
                )
            end,
            [],
            TopicPartitions
        )
    ).

count_partitions(TopicPartitions) ->
    length(flatten_partitions(TopicPartitions)).
