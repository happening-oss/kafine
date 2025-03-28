# Integration tests for kafine

This directory contains the integration test suites for `kafine`.

## Broker requirement

The integration tests assume the presence of a Kafka cluster.

- Locally, you can use minikube or docker compose. If using docker compose, there's a Kafka cluster defined in the
  `docker` directory.
- For Kubernetes, this is defined in the `k8s/kafka` directory. See the `README.md` file in that directory for details.
  Note: This is not included in the OSS release, because it's got details of our CI cluster in it.

## Running the tests locally

From the top-level directory:

```sh
# run all of the integration tests
make integration

# run a specific test
rebar3 as integration do ct --suite=integration/test/api_versions_SUITE

# run multiple specified tests
rebar3 as integration do ct --suite=integration/test/api_versions_SUITE,integration/test/direct_create_topic_SUITE
```
