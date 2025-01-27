# Local Brokers

This directory contains docker compose files for a local Kafka cluster.

## Start the cluster

```sh
make up
```

This will start a "default" cluster, with 3 brokers, managed by Zookeeper. It also starts a kafka-ui instance -- browse
to <http://localhost:8080>.

## Add more brokers

```sh
make scale-up
```

This adds another 3 brokers to the "default" cluster.

## Start an extra cluster

```sh
make extra-up
```

## Monitoring

The compose file includes prometheus and grafana, but they're not started by default. To do that:

```sh
make monitoring-up
```

## Tear down

The docker environment is a bit brittle, and starting and stopping containers tends to fail awkwardly. For now, the best
option is to delete everything and start again. To do that, use `make purge`:

```sh
make purge
```

## Using the cluster

### From kafire

In your config file (this is `local.config`):

```erlang
    {kafire, [
        {bootstrap_servers, "localhost:9092"},
```

### From kcat

```sh
kcat -b localhost:9092 -L
```

### Using go-zkcli

Because you might want to poke around in ZooKeeper.

```sh
go install github.com/go-zkcli/zkcli@latest
$(go env GOPATH)/bin/zkcli --servers localhost:2181 -c ls /
$(go env GOPATH)/bin/zkcli --servers localhost:2181 -c get /cluster/id
```
