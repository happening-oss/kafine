# Connecting with TLS

## Check that the broker is using TLS

```sh
openssl s_client \
    -connect localhost:9192 -CAfile docker/secrets/kafka-root.crt </dev/null
```

## Validate the broker's certificate

```sh
kcat -b localhost:9192 \
    -X security.protocol=SSL \
    -X ssl.ca.location=$(pwd)/docker/secrets/kafka-root.crt \
    -L
```

## Connect with client certificate

```sh
kcat -b localhost:9192 \
    -X security.protocol=SSL \
    -X ssl.ca.location=$(pwd)/docker/secrets/kafka-root.crt \
    -X ssl.certificate.location=$(pwd)/docker/secrets/kafka-client.crt \
    -X ssl.key.location=$(pwd)/docker/secrets/kafka-client.key \
    -L
```
