# Schema Registry examples

> **Note:** For GA, Confluent will provide a supported Schema Registry client that works with this
> Python client. During the preview, use the third-party
> [`python-schema-registry-client`](https://pypi.org/project/python-schema-registry-client/) package,
> as these examples do.

| Example | What it does |
|---|---|
| `avro_producer.py` | Sends 10 `User` records as Avro to `avro-users`, registering the schema in Schema Registry under `avro-users-value`. |
| `avro_consumer.py` | Reads them back from `avro-users` (group `avro-users-group`), decoding each value with the schema fetched from Schema Registry. Stops after a poll interval (30 s) with no records. |

They are the Python peers of `rust/examples/schemaregistry/avro_producer.rs` and `avro_consumer.rs`:
same topic, group, schema and output, and the records are interchangeable. The Python consumer reads
what the Rust producer writes.

## Why a third-party registry client

- This client, `confluent-kafka` 4.4.0b1, has no Schema Registry serdes yet.
- The `confluent-kafka` 2.x releases have them (`confluent_kafka.schema_registry`), but cannot be
  used with 4.4.0b1: they are the same PyPI package, so only one version can be installed at a time.
- `python-schema-registry-client` has no Kafka client dependency, so it installs cleanly beside
  4.4.0b1.

## How it works

- Each example wraps the registry package's `AvroMessageSerializer` in a serde of this client: a
  callable taking `topic=`, `data=` and `headers=` (plus `**kwargs`).
- `AvroSerializer` registers the schema under `<topic>-value` (the registry's conventional naming)
  and writes the Confluent wire format: a magic byte, the schema id, then the Avro binary payload.
- `AvroDeserializer` reads the schema id from each record, fetches the writer schema, decodes the
  payload and builds a `User`. So the producer is given `User` objects and `poll()` returns `User`
  values.
- This client calls serdes synchronously and rejects `async def` ones, so the wrappers use the
  package's synchronous `SchemaRegistryClient`.
- Only the first record of a schema reaches the registry: the registry client caches the schema and
  its id after that.
- A failure is raised as `SerializationError`, with the original error as its cause.

## Running them

Build the client into the repository's venv and install the registry client, from the repository
root:

```bash
make devel-build-python
venv/bin/pip install python-schema-registry-client
```

Start a broker and a Schema Registry. The registry's own Kafka client reaches the broker on the
container network, and the examples reach both on `localhost`:

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

Run the producer, then the consumer:

```bash
cd python/examples/schemaregistry
../../../venv/bin/python avro_producer.py
../../../venv/bin/python avro_consumer.py
```

They connect to `localhost:9092` and `http://localhost:8081` by default. Set
`KAFKA_BOOTSTRAP_SERVERS` and `SCHEMA_REGISTRY_URL` to use others:

```bash
KAFKA_BOOTSTRAP_SERVERS=broker:9092 SCHEMA_REGISTRY_URL=http://registry:8081 \
  ../../../venv/bin/python avro_producer.py
```

Tear down with `docker rm -f kafka schema-registry && docker network rm kafka-examples`. This also
deletes all topic data and registered schemas.

## Expected output

The producer (the client's preview warning first, on stderr):

```
Schema Registry client created for http://localhost:8081.
Kafka producer properties: {'bootstrap.servers': 'localhost:9092', 'client.id': 'client-…'}
Kafka producer created successfully.
Producing record: key=user-0, value=User(name='user-0', favorite_number=0, favorite_color='red')
Delivered record 0 to topic avro-users, partition 0 at offset 0
…
Producing record: key=user-9, value=User(name='user-9', favorite_number=9, favorite_color='red')
Delivered record 9 to topic avro-users, partition 0 at offset 9
```

The consumer:

```
Schema Registry client created for http://localhost:8081.
Kafka consumer created successfully.
Subscribed to topic: avro-users
Consumed record: key=user-0, user user-0 likes red and 0 (partition 0, offset 0)
…
Consumed record: key=user-9, user user-9 likes red and 9 (partition 0, offset 9)
No records consumed in this poll interval.
```

The registered schema:

```bash
curl -s http://localhost:8081/subjects/avro-users-value/versions/1
```
