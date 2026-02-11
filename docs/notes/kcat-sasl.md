# Connecting with SASL

## Connect with PLAIN username/password

```sh
kcat -b localhost:9292 \
    -X security.protocol=sasl_plaintext \
    -X sasl.mechanism=PLAIN \
    -X sasl.username=admin \
    -X sasl.password=secret \
    -L
```

## Connect with SCRAM-SHA-256 username/password

You have to configure the user account as follows (they're kept in ZooKeeper):

```sh
kafka-configs \
    --bootstrap-server localhost:9092 \
    --alter \
    --add-config 'SCRAM-SHA-256=[password=secret]' \
    --entity-type users \
    --entity-name admin
```

Then you can connect using `kcat`, as follows:

```sh
kcat -b localhost:9292 \
    -X security.protocol=sasl_plaintext \
    -X sasl.mechanism=SCRAM-SHA-256 \
    -X sasl.username=admin \
    -X sasl.password=secret \
    -L
```
