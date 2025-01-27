#!/usr/bin/env escript

-mode(compile).
-include_lib("kafcod/include/error_code.hrl").

-define(CLIENT_ID, <<"list_topics">>).

main(Args) ->
    {ok, _} = application:ensure_all_started(telemetry),

    argparse:run(
        Args,
        #{
            arguments => [
                #{
                    name => broker,
                    help => "Bootstrap broker (host[:port])",
                    type => {custom, fun parse_broker/1}
                },
                #{
                    name => topic,
                    nargs => nonempty_list,
                    action => extend,
                    type => binary
                }
            ],
            handler => fun delete_topics/1
        },
        #{progname => delete_topics}
    ).

-define(DEFAULT_BROKER_PORT, 9092).

parse_broker(Arg) when is_list(Arg) ->
    case string:split(Arg, ":") of
        [Host, Port] ->
            {Host, list_to_integer(Port)};
        [Host] ->
            {Host, ?DEFAULT_BROKER_PORT};
        _ ->
            error(badarg)
    end.

delete_topics(#{broker := {Host, Port}, topic := Topics}) ->
    process_flag(trap_exit, true),
    {ok, Connection} = kafine_connection:start_link(Host, Port, #{client_id => ?CLIENT_ID}),

    {ok, #{responses := Responses}} = kafine_connection:call(
        Connection,
        fun delete_topics_request:encode_delete_topics_request_4/1,
        #{
            timeout_ms => 15_000,
            topic_names => Topics
        },
        fun delete_topics_response:decode_delete_topics_response_4/1
    ),

    lists:foreach(fun(#{error_code := ?NONE}) -> ok end, Responses),

    % {ok, Metadata} = kafine_connection:call(
    %     Connection,
    %     fun metadata_request:encode_metadata_request_9/1,
    %     #{
    %         topics => null,
    %         include_topic_authorized_operations => false,
    %         include_cluster_authorized_operations => false,
    %         allow_auto_topic_creation => false
    %     },
    %     fun metadata_response:decode_metadata_response_9/1
    % ),
    % #{topics := Topics} = Metadata,
    % TopicNames = [Name || #{error_code := ?NONE, name := Name, is_internal := false} <- Topics],
    % lists:foreach(fun(T) -> io:format("~s~n", [T]) end, lists:sort(TopicNames)),
    ok.
