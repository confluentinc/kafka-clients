# Examples

Runnable programs that use the client against a real Kafka broker. Each `.rs`
file in this directory is one example, and its file name is the example name.

## Start a broker

A single-node Kafka in Docker. On a one-broker cluster the transaction-state
topic's replication factor must be lowered to 1, which the settings below do:

```
docker run -d --name kafka-examples -p 9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_LOG_DIRS=/tmp/kraft-combined-logs \
  apache/kafka:latest
```

Tear down with `docker rm -f kafka-examples`. This also deletes all topic data.

## Run an example

```
cargo run --example <name>
```

from the `rust/` directory, where `<name>` is the file name without `.rs`.

Every example connects to `localhost:9092` by default. Set
`KAFKA_BOOTSTRAP_SERVERS` to use another broker:

```
KAFKA_BOOTSTRAP_SERVERS=broker:9092 cargo run --example <name>
```
