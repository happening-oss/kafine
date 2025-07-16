-module(kafine_connection_tls_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("public_key/include/public_key.hrl").

-elvis([{elvis_style, atom_naming_convention, disable}]).

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).

setup() ->
    % The TCP tests mock 'gen_tcp'; we _could_ do that, but we'd be missing much of the complexity in TLS. Instead,
    % we'll use kamock.
    {ok, _} = application:ensure_all_started(telemetry),

    CAKey = erl509_private_key:create_rsa(2048),
    CACert = erl509_certificate:create_self_signed(
        CAKey, <<"CN=kubernetes">>, erl509_certificate_template:root_ca()
    ),

    ServerKey = erl509_private_key:create_rsa(2048),
    ServerPub = erl509_public_key:derive_public_key(ServerKey),
    ServerCert = erl509_certificate:create(
        ServerPub, <<"CN=localhost">>, CACert, CAKey, erl509_certificate_template:server()
    ),

    ClientKey = erl509_private_key:create_rsa(2048),
    ClientPub = erl509_public_key:derive_public_key(ClientKey),
    ClientCert = erl509_certificate:create(
        ClientPub, <<"CN=client">>, CACert, CAKey, erl509_certificate_template:client()
    ),

    {ok, Broker} = kamock_broker:start_tls(?BROKER_REF, #{}, [
        {cert, erl509_certificate:to_der(ServerCert)},
        {key, {'RSAPrivateKey', erl509_private_key:to_der(ServerKey)}},
        % Client certs are optional.
        {verify, verify_peer},
        {fail_if_no_peer_cert, false},
        {cacerts, [erl509_certificate:to_der(CACert)]}
    ]),
    #{bootstrap => Broker, cacert => CACert, client_key => ClientKey, client_cert => ClientCert}.

cleanup(#{bootstrap := Broker}) ->
    kamock_broker:stop(Broker),
    meck:unload().

connect_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {with, [
            fun connect_ok/1
            % ,
            % fun send_request/0,
            % fun send_request_metadata/0,
            % fun call/0,
            % fun call_metadata/0
        ]}
    ]}.

connect_ok(#{
    bootstrap := Bootstrap, cacert := CACert, client_key := ClientKey, client_cert := ClientCert
}) ->
    {ok, Connection} = kafine_connection:start_link(Bootstrap, #{
        transport => ssl,
        transport_options => [
            {verify, verify_peer},
            {cert, erl509_certificate:to_der(ClientCert)},
            {key, {'RSAPrivateKey', erl509_private_key:to_der(ClientKey)}},
            {cacerts, [erl509_certificate:to_der(CACert)]}
        ]
    }),

    [#{transport := ranch_ssl, socket := SslSocket}] = kamock_broker:connections(Bootstrap),
    {ok, PeerCertDer} = ssl:peercert(SslSocket),
    #'OTPCertificate'{
        tbsCertificate = #'OTPTBSCertificate'{
            subject = PeerSubject
        }
    } = public_key:pkix_decode_cert(PeerCertDer, otp),
    ?assertEqual(
        {rdnSequence, [
            [{'AttributeTypeAndValue', ?'id-at-commonName', {printableString, "client"}}]
        ]},
        PeerSubject
    ),
    kafine_connection:stop(Connection),
    ok.
