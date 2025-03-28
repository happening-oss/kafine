-module(kafine_kamock).
-export([
    produce/2
]).
-include_lib("eunit/include/eunit.hrl").

produce(FirstOffset, LastOffset) ->
    meck:expect(
        kamock_list_offsets_partition_response,
        make_list_offsets_partition_response,
        kamock_list_offsets_partition_response:range(FirstOffset, LastOffset)
    ),

    % If we send an empty fetch, that's a bad thing; we should have gone idle.
    % Note that there's a race here, so this won't always trigger. That's fine; it's double-checking.
    meck:expect(
        kamock_fetch,
        handle_fetch_request,
        fun(FetchRequest = #{topics := Topics}, Env) ->
            ?assertNotEqual([], Topics),
            meck:passthrough([FetchRequest, Env])
        end
    ),

    meck:expect(
        kamock_fetchable_topic,
        make_fetchable_topic_response,
        fun(FetchableTopic = #{topic := _Topic, partitions := FetchPartitions}, Env) ->
            ?assertNotEqual([], FetchPartitions),
            meck:passthrough([FetchableTopic, Env])
        end
    ),

    meck:expect(
        kamock_partition_data,
        make_partition_data,
        kamock_partition_data:range(FirstOffset, LastOffset, fun(_T, _P, O) ->
            Key = iolist_to_binary(io_lib:format("key~B", [O])),
            #{key => Key}
        end)
    ),
    ok.
