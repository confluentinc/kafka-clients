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

## Start a Schema Registry

`avro_producer` and `avro_consumer` also need a Confluent Schema Registry:
the producer registers the Avro schema of its records there and the
consumer fetches it back by the id each record carries. The registry's own
Kafka client has to reach the broker above, which advertises
`localhost:9092`, so the container shares the host network:

```
docker run -d --name schema-registry-examples --network host \
  -e SCHEMA_REGISTRY_HOST_NAME=localhost \
  -e SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081 \
  -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://localhost:9092 \
  confluentinc/cp-schema-registry:latest
```

Tear down with `docker rm -f schema-registry-examples`. (`--network host`
needs Docker on Linux, or Docker Desktop 4.34 or later.)

These two examples use Robert Yokota's
[`schema-registry-client`](https://crates.io/crates/schema-registry-client)
crate for the registry round trip, wrapped in implementations of this
client's `Serializer` and `Deserializer` traits: the producer is a
`KafkaProducer<String, User>` and the consumer's `poll` returns decoded
`User` records. The crate's serializers are `async`, like the traits, so the
wrappers await them directly. Only the first record of a schema reaches the
registry; the registry client caches the schema and its id after that.

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

The Avro examples connect to the Schema Registry at `http://localhost:8081`
by default. Set `SCHEMA_REGISTRY_URL` to use another one:

```
SCHEMA_REGISTRY_URL=http://registry:8081 cargo run --example avro_producer
```
