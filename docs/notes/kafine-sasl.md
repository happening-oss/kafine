# Kafine SASL

Until it's integrated (somehow), you have to do it like this:

## Example without authentication

```erlang
{ok, C} = kafine_connection:start_link(
    #{host=> <<"localhost">>, port => 9292},
    #{}).

% This fails; the broker reports "Unexpected Kafka request of type METADATA during SASL handshake."
kafine_connection:call(
    C,
    fun metadata_request:encode_metadata_request_3/1,
    #{topics => null},
    fun metadata_response:decode_metadata_response_3/1).
```

## Example with PLAIN authentication

See [RFC 4616](https://datatracker.ietf.org/doc/html/rfc4616): The PLAIN Simple Authentication and Security Layer (SASL) Mechanism.

Run the Erlang shell with the following command:

```sh
rebar3 as sasl do auto
```

Then run the following Erlang snippets:

```erlang
{ok, C} = kafine_connection:start_link(
    #{host=> <<"localhost">>, port => 9292},
    #{}).

% We don't actually care that 'mechanisms' contains PLAIN; we only care about 'error_code'.
{ok, #{error_code := 0, mechanisms := _Mechanisms}} = kafine_connection:call(
    C,
    fun sasl_handshake_request:encode_sasl_handshake_request_1/1,
    #{mechanism => <<"PLAIN">>},
    fun sasl_handshake_response:decode_sasl_handshake_response_1/1).

% The RFC uses authorization identity (authzid), authentication identity (authcid) and password (passwd).
% We leave authzid empty.
AuthzId = <<>>.
AuthcId = Username = <<"admin">>.
Password = <<"secret">>.

{ok, #{error_code := 0}} = kafine_connection:call(
    C,
    fun sasl_authenticate_request:encode_sasl_authenticate_request_1/1,
    #{auth_bytes => <<AuthzId/binary, 0, AuthcId/binary, 0, Password/binary>>},
    fun sasl_authenticate_response:decode_sasl_authenticate_response_1/1).

% Get Metadata (this works):
kafine_connection:call(
    C,
    fun metadata_request:encode_metadata_request_3/1,
    #{topics => null},
    fun metadata_response:decode_metadata_response_3/1).
```

## Example with SCRAM-SHA-256 authentication

See [RFC 5802](https://www.rfc-editor.org/rfc/rfc5802): Salted Challenge Response Authentication Mechanism (SCRAM) SASL and GSS-API Mechanisms.

See [RFC 7677](https://www.rfc-editor.org/rfc/rfc7677): SCRAM-SHA-256 and SCRAM-SHA-256-PLUS Simple Authentication and Security Layer (SASL) Mechanisms.

Run the Erlang shell with the following command:

```sh
rebar3 as sasl do auto
```

Then run the following Erlang snippets:

```erlang
{ok, C} = kafine_connection:start_link(
    #{host=> <<"localhost">>, port => 9292},
    #{}).

{ok, #{error_code := 0, mechanisms := _Mechanisms}} = kafine_connection:call(
    C,
    fun sasl_handshake_request:encode_sasl_handshake_request_1/1,
    #{mechanism => <<"SCRAM-SHA-256">>},
    fun sasl_handshake_response:decode_sasl_handshake_response_1/1).

Username = <<"admin">>.
Password = <<"secret">>.

ClientFirst = sasl_auth_scram:client_first_message(Username).

{ok, #{error_code := 0, auth_bytes := ServerFirst}} = kafine_connection:call(
    C,
    fun sasl_authenticate_request:encode_sasl_authenticate_request_1/1,
    #{auth_bytes => ClientFirst},
    fun sasl_authenticate_response:decode_sasl_authenticate_response_1/1).

{continue, ClientFinal, ClientCache} =
    sasl_auth_scram:check_server_first_message(
        ServerFirst,
        #{
            client_first_message => ClientFirst,
            password  => Password,
            algorithm => sha256
        }).

{ok, #{error_code := 0, auth_bytes := ServerFinal}} = kafine_connection:call(
    C,
    fun sasl_authenticate_request:encode_sasl_authenticate_request_1/1,
    #{auth_bytes => ClientFinal},
    fun sasl_authenticate_response:decode_sasl_authenticate_response_1/1).

ok = sasl_auth_scram:check_server_final_message(ServerFinal, ClientCache#{algorithm => sha256}).

% Get Metadata (this works):
kafine_connection:call(
    C,
    fun metadata_request:encode_metadata_request_3/1,
    #{topics => null},
    fun metadata_response:decode_metadata_response_3/1).
```
