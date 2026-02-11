-module(kafine_maps).
-moduledoc false.
-export([
    get/2,
    get/3,
    is_key/2,
    put/3,
    remove/2,
    take/2,
    update_with/3,
    update_with/4
]).

-export_type([keys/0]).

-type key() :: term().
-type keys() :: [key()].

get(Keys, Map) when is_list(Keys), is_map(Map) ->
    get_(Keys, Map);
get(Keys, Map) ->
    error(badarg, [Keys, Map]).

get_([Key], Map) ->
    maps:get(Key, Map);
get_([Key | Keys], Map1) ->
    get_(Keys, maps:get(Key, Map1)).

get(Keys, Map, Default) when is_list(Keys), is_map(Map) ->
    get_(Keys, Map, Default);
get(Keys, Map, Default) ->
    error(badarg, [Keys, Map, Default]).

get_([Key], Map, Default) ->
    maps:get(Key, Map, Default);
get_([Key | Keys], Map1, Default) ->
    get_(Keys, maps:get(Key, Map1, #{}), Default).

is_key(Keys, Map) when is_list(Keys), is_map(Map) ->
    is_key_(Keys, Map);
is_key(Keys, Map) ->
    error(badarg, [Keys, Map]).

is_key_([Key], Map) ->
    maps:is_key(Key, Map);
is_key_([Key | Keys], Map) ->
    is_key_(Keys, maps:get(Key, Map, #{})).

put(Keys, Value, Map) when is_list(Keys), is_map(Map) ->
    update_with_(Keys, fun(_) -> Value end, Value, Map).

%% Remove the item identified by `Keys`, if it exists, and its associated value from `Map1` and returns a new map
%% `Map2`. If the item identified by `Keys` isn't present, the existing map is returned.
-spec remove(Keys, Map1) -> Map2 when Keys :: keys(), Map1 :: map(), Map2 :: map().

remove(Keys, Map) when is_list(Keys), is_map(Map) ->
    remove_(Keys, Map).

remove_([Key], Map) ->
    maps:remove(Key, Map);
remove_([Key | Keys], Outer) ->
    Inner = maps:get(Key, Outer, #{}),
    remove_result_(Key, remove_(Keys, Inner), Outer).

remove_result_(Key, Inner2, Outer) when Inner2 =:= #{} ->
    % Inner map is empty, remove the key.
    maps:remove(Key, Outer);
remove_result_(Key, Inner2, Outer) ->
    % Inner map is not empty, keep it.
    maps:update(Key, Inner2, Outer).

%% Removes the item identified by `Keys`, if it exists, and its associated value from `Map1` and returns a tuple with
%% the removed `Value` and the new map `Map2` without the item identified by `Key`. If the item does not exist `error` is
%% returned.
-spec take(Keys, Map1) -> {Value, Map2} | error when
    Keys :: keys(), Map1 :: map(), Map2 :: map(), Value :: term().

take(Keys, Map) when is_list(Keys), is_map(Map) ->
    take_(Keys, Map).

take_(_, Map) when Map =:= #{} ->
    error;
take_([Key], Map) ->
    maps:take(Key, Map);
take_([Key | Keys], Outer) ->
    Inner = maps:get(Key, Outer, #{}),
    take_result_(Key, take_(Keys, Inner), Outer).

take_result_(Key, {Value, Inner2}, Outer) when Inner2 =:= #{} ->
    % Inner map is empty, remove the key.
    Outer2 = maps:remove(Key, Outer),
    {Value, Outer2};
take_result_(Key, {Value, Inner2}, Outer) ->
    % Inner map is not empty, keep it.
    Outer2 = maps:update(Key, Inner2, Outer),
    {Value, Outer2};
take_result_(_Key, error, _Outer) ->
    error.

%% Update a value in `Map1` associated with `Key` by calling `Update` on the old value to get a new value. An exception
%% `{badkey, Keys}` is generated if `Keys` doesn't identify an item in the nested map.
update_with(Keys, Update, Map) when is_list(Keys), is_function(Update, 1), is_map(Map) ->
    update_with_(Keys, Update, Map).

update_with_([Key], Update, Map1) ->
    maps:update_with(Key, Update, Map1);
update_with_([Key | Keys], Update, Map1) ->
    maps:update_with(
        Key,
        fun(Map) -> update_with_(Keys, Update, Map) end,
        Map1
    ).

update_with(Keys, Update, Init, Map) when is_list(Keys), is_function(Update, 1), is_map(Map) ->
    update_with_(Keys, Update, Init, Map).

update_with_([Key], Update, Init, Map1) ->
    maps:update_with(Key, Update, Init, Map1);
update_with_([Key | Keys], Update, Init, Map1) ->
    maps:update_with(
        Key,
        fun(Map) -> update_with(Keys, Update, Init, Map) end,
        update_with(Keys, Update, Init, #{}),
        Map1
    ).
