# Kafka Container Options

The Kafka Containers are from Confluent; see <https://hub.docker.com/r/confluentinc/cp-kafka>.

Inside the container -- `docker exec -it kafka-default-101 /bin/sh`:

- The binaries are mixed in with everything else in `/usr/bin`
- The configuration files are in `/etc/kafka/`.

The `KAFKA_FOO_BAR` environment variables aren't validated; they're just turned into `foo.bar` entries in
`/etc/kafka/kafka.properties`, so make sure you get the keys right.
