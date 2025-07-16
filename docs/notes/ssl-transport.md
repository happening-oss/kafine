```erlang
{ok, C} = kafine_connection:start_link(
    #{host=> <<"localhost">>, port => 9192},
    #{transport => ssl, transport_options => [{verify, verify_none}]}).

kafine_connection:call(
    C,
    fun metadata_request:encode_metadata_request_3/1,
    #{topics => null},
    fun metadata_response:decode_metadata_response_3/1).
```
