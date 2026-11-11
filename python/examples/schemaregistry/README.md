# Schema Registry examples

> **Note:** For GA, Confluent will provide a supported Schema Registry client that works with this
> Python client. During the preview, use the third-party
> [`python-schema-registry-client`](https://pypi.org/project/python-schema-registry-client/) package,
> as these examples do.

| Example | What it does |
|---|---|
| `avro_producer.py` | Sends 10 `User` records as Avro to `avro-users`, registering the schema in Schema Registry under `avro-users-value`. |
| `avro_consumer.py` | Reads them back from `avro-users` (group `avro-users-group`), decoding each value with the schema fetched from Schema Registry. Stops after a poll interval (30 s) with no records. |

Run the producer first, then the consumer: the consumer reads the records the producer wrote.

## Why a third-party registry client

- This client, `confluent-kafka` 4.4.0b1, has no Schema Registry serdes yet.
- The `confluent-kafka` 2.x releases have them (`confluent_kafka.schema_registry`), but cannot be
  used with 4.4.0b1: they are the same PyPI package, so only one version can be installed at a time.
- `python-schema-registry-client` has no Kafka client dependency, so it installs cleanly beside
  4.4.0b1.

## Requirements

The examples need a Kafka broker on `localhost:9092` and a Schema Registry on `http://localhost:8081`.
The registry's own Kafka client reaches the broker on the container network:

```bash
docker network create kafka-examples
docker run -d --rm --name kafka --network kafka-examples -p 9092:9092 \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,INTERNAL://:29092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092,INTERNAL://kafka:29092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  apache/kafka:4.3.1
docker run -d --rm --name schema-registry --network kafka-examples -p 8081:8081 \
  -e SCHEMA_REGISTRY_HOST_NAME=schema-registry \
  -e SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081 \
  -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://kafka:29092 \
  confluentinc/cp-schema-registry:latest
```
