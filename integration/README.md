# Integration tests for kafine

This directory contains the integration test suites for `kafine`.

## Broker requirement

The integration tests assume the presence of a Kafka cluster.

- For Kubernetes, this is defined in the `k8s/kafka` directory (not included in the OSS release).  See the `README.md`
  file in that directory for details.
- Locally, you can use minikube or docker compose. If using docker compose, there's a Kafka cluster defined in the
  `docker` directory.

## Running the tests locally

```sh
# run all of the tests
make tests

# run a specific test
make tests CT_SUITES=api_versions   # note: no _SUITE suffix

# run multiple specified tests
make tests CT_SUITES="api_versions create_topic"
```
