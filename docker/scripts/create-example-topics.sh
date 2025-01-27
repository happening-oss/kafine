#!/bin/bash

BOOTSTRAP_CONTAINER=$1

# Create a reasonable default topic
docker exec "$BOOTSTRAP_CONTAINER" \
    /bin/kafka-topics --bootstrap-server localhost:9092 \
    --create --topic cars --partitions 10 --replication-factor 3

# Create a topic with 1 replica, minimal partitions. Might be handy for some things.
docker exec "$BOOTSTRAP_CONTAINER" \
    /bin/kafka-topics --bootstrap-server localhost:9092 \
    --create --topic highlander --partitions 1 --replication-factor 1

# Create a topic that rotates segments every minute and deletes expired segments every 5 minutes. Messages don't live
# long (hence "mayfly"). It's intended to demonstrate that log_start_offset can be non-zero. It's also useful for
# testing offset-reset-policy (or kcat's negative offsets) -- when the offset is large enough, but there are no messages
# before that point, for example.
docker exec "$BOOTSTRAP_CONTAINER" \
    /bin/kafka-topics --bootstrap-server localhost:9092 \
    --create --topic mayfly --partitions 10 --replication-factor 3 \
    --config cleanup.policy=delete \
    --config segment.ms=60000 \
    --config retention.ms=300000

# Some of the kafire examples use topics 'p' and 'q', 3 partitions, 1 replica.
docker exec "$BOOTSTRAP_CONTAINER" \
    /bin/kafka-topics --bootstrap-server localhost:9092 \
    --create --topic p --partitions 3 --replication-factor 1
docker exec "$BOOTSTRAP_CONTAINER" \
    /bin/kafka-topics --bootstrap-server localhost:9092 \
    --create --topic q --partitions 3 --replication-factor 1
