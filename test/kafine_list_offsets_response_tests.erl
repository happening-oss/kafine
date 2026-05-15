-module(kafine_list_offsets_response_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/error_code.hrl").

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun handle_response/0,
        fun handle_response_with_replay_request/0
    ]}.

setup() ->
    meck:new(kafine_fetcher, [stub_all]),
    ok.

cleanup(_) ->
    meck:unload(),
    ok.

handle_response() ->
    RequestedOffsets = #{
        <<"cats">> => #{
            61 => 34,
            62 => 75
        },
        <<"dogs">> => #{
            61 => 48,
            62 => 54
        },
        <<"fish">> => #{
            61 => 68,
            62 => 78
        }
    },

    % Note: the offsets here correspond to the offset reset policy. That is: we asked for the earliest, we get the
    % earliest offset, and so on.
    ListOffsetsResponse = #{
        topics => [
            #{
                name => <<"cats">>,
                partitions => [
                    #{partition_index => 61, error_code => ?NONE, offset => 44},
                    #{partition_index => 62, error_code => ?NONE, offset => 75}
                ]
            },
            #{
                name => <<"dogs">>,
                partitions => [
                    #{partition_index => 61, error_code => ?NONE, offset => 52},
                    #{partition_index => 62, error_code => ?NONE, offset => 54}
                ]
            },
            #{
                name => <<"fish">>,
                partitions => [
                    #{partition_index => 61, error_code => ?NONE, offset => 68},
                    #{partition_index => 62, error_code => ?NONE, offset => 90}
                ]
            }
        ]
    },

    {ok, Result} = kafine_list_offsets:handle_response(ListOffsetsResponse, RequestedOffsets),

    ?assertEqual(
        #{
            <<"cats">> => #{
                61 => {update_offset, 44},
                62 => {update_offset, 75}
            },
            <<"dogs">> => #{
                61 => {update_offset, 52},
                62 => {update_offset, 54}
            },
            <<"fish">> => #{
                61 => {update_offset, 68},
                62 => {update_offset, 90}
            }
        },
        Result
    ),
    ok.

handle_response_with_replay_request() ->
    RequestedOffsets = #{
        <<"cats">> => #{
            61 => -5,
            62 => -5
        },
        <<"dogs">> => #{
            61 => -10,
            62 => -10
        }
    },

    % Note: the offsets here correspond to the offset reset policy. That is: we asked for the earliest, we get the
    % earliest offset, and so on.
    ListOffsetsResponse = #{
        topics => [
            #{
                name => <<"cats">>,
                partitions => [
                    #{partition_index => 61, error_code => ?NONE, offset => 10},
                    #{partition_index => 62, error_code => ?NONE, offset => 5}
                ]
            },
            #{
                name => <<"dogs">>,
                partitions => [
                    #{partition_index => 61, error_code => ?NONE, offset => 2},
                    #{partition_index => 62, error_code => ?NONE, offset => 4}
                ]
            }
        ]
    },

    {ok, Result} = kafine_list_offsets:handle_response(ListOffsetsResponse, RequestedOffsets),

    ?assertEqual(
        #{
            <<"cats">> => #{
                61 => {update_offset, 5},
                62 => {update_offset, 0}
            },
            <<"dogs">> => #{
                61 => {update_offset, 0},
                62 => {update_offset, 0}
            }
        },
        Result
    ),
    ok.
