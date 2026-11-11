# Schema Registry examples

> **Note:** For GA, we have plans to provide a Confluent supported Schema Registry client that works
> with this Python client. During the preview, use the third-party
> [`python-schema-registry-client`](https://pypi.org/project/python-schema-registry-client/) package,
> as these examples do.

| Example | What it does |
|---|---|
| `avro_producer.py` | Sends 10 `User` records as Avro to `avro-users`, registering the schema in Schema Registry under `avro-users-value`. |
| `avro_consumer.py` | Reads them back from `avro-users` (group `avro-users-group`), decoding each value with the schema fetched from Schema Registry. Stops after a poll interval (30 s) with no records. |

Run the producer first, then the consumer: the consumer reads the records the producer wrote.

## Why a third-party registry client

- This client, `confluent-kafka` 4.4.*, has no Schema Registry serdes yet.
- The `confluent-kafka` 2.x releases have them (`confluent_kafka.schema_registry`), but cannot be
  used with 4.4.*: they are the same PyPI package, so only one version can be installed at a time.
- `python-schema-registry-client` has no Kafka client dependency, so it installs cleanly beside
  4.4.*.

## Running them

1. Start Kafka. Skip this step if the broker from [`../README.md`](../README.md) is already running
   (`docker ps` lists `kafka`).
   ```bash
   docker run -d --rm --name kafka -p 9092:9092 apache/kafka:4.4.0
   ```
2. Start Schema Registry. It shares the host network, so its Kafka client reaches the broker at
   `localhost:9092` (Docker on Linux, or Docker Desktop 4.34+ with host networking turned on).
   ```bash
   docker run -d --rm --name schema-registry --network host \
     -e SCHEMA_REGISTRY_HOST_NAME=localhost \
     -e SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081 \
     -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://localhost:9092 \
     confluentinc/cp-schema-registry:latest
   ```
3. Install the registry client into the repository's venv.
   ```bash
   cd python/examples/schemaregistry
   ../../../venv/bin/pip install python-schema-registry-client==2.6.1
   ```
4. Run the producer, then the consumer.
   ```bash
   ../../../venv/bin/python avro_producer.py
   ../../../venv/bin/python avro_consumer.py
   ```

Notes:

- To reach a broker or registry elsewhere, set `KAFKA_BOOTSTRAP_SERVERS` and `SCHEMA_REGISTRY_URL`
  (defaults `localhost:9092` and `http://localhost:8081`).
- `docker stop schema-registry` stops the registry when you are done (`--rm` then removes it).
