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

## Admin examples

`examples/admin` has one example per `Admin` RPC. Each takes its input on the
command line and prints the outcome of each item it asked for. It exits with
code 0 when everything succeeded, 1 when an operation failed (the failures go
to stderr, after the successes are printed), and 2 on a usage error, which also
prints the example's usage line.

```
cargo run --example admin_create_topics -- orders:3:1 payments
cargo run --example admin_describe_topics -- orders
```

`-X key=value` sets any admin client property, as librdkafka's tools do; it
also overrides `KAFKA_BOOTSTRAP_SERVERS`:

```
cargo run --example admin_list_topics -- \
    -X bootstrap.servers=broker:9093 -X security.protocol=SASL_SSL \
    -X sasl.mechanism=PLAIN \
    -X 'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="u" password="p";'
```

Run an example with `--help` to see its usage line. The shared argument
parsing and client setup are in `examples/admin/common/mod.rs`.

| Area | Examples |
|---|---|
| Topics | `admin_create_topics`, `admin_list_topics`, `admin_describe_topics`, `admin_delete_topics`, `admin_create_partitions` |
| Records and offsets | `admin_delete_records`, `admin_list_offsets` |
| Cluster and configs | `admin_describe_cluster`, `admin_describe_configs`, `admin_incremental_alter_configs`, `admin_list_config_resources`, `admin_describe_features`, `admin_update_features`, `admin_unregister_controller` |
| Replicas | `admin_describe_log_dirs`, `admin_describe_replica_log_dirs`, `admin_alter_replica_log_dirs`, `admin_alter_partition_reassignments`, `admin_list_partition_reassignments`, `admin_elect_leaders` |
| Groups | `admin_list_groups`, `admin_describe_consumer_groups`, `admin_describe_classic_groups`, `admin_list_consumer_group_offsets`, `admin_alter_consumer_group_offsets`, `admin_delete_consumer_group_offsets`, `admin_delete_consumer_groups`, `admin_remove_members_from_consumer_group` |
| ACLs | `admin_create_acls`, `admin_describe_acls`, `admin_delete_acls` |
| Quotas and SCRAM | `admin_describe_client_quotas`, `admin_alter_client_quotas`, `admin_describe_user_scram_credentials`, `admin_alter_user_scram_credentials` |
| Delegation tokens | `admin_create_delegation_token`, `admin_describe_delegation_token`, `admin_renew_delegation_token`, `admin_expire_delegation_token` |
| Transactions | `admin_list_transactions`, `admin_describe_transactions`, `admin_describe_producers`, `admin_abort_transaction`, `admin_fence_producers`, `admin_force_terminate_transaction` |

The broker started above runs most of them. Some need more:

- ACLs: an authorizer, `KAFKA_AUTHORIZER_CLASS_NAME=org.apache.kafka.metadata.authorizer.StandardAuthorizer`,
  with the client's principal in `KAFKA_SUPER_USERS` (`User:ANONYMOUS` over
  PLAINTEXT).
- Delegation tokens: `KAFKA_DELEGATION_TOKEN_SECRET_KEY` on the broker, and a
  client authenticated over SASL or two-way TLS. The broker refuses token
  requests on PLAINTEXT and one-way TLS connections.
- Reassignments and preferred elections: several brokers, so a partition can
  have more than one replica.
- Replica moves between log directories: a broker with several, for example
  `KAFKA_LOG_DIRS=/tmp/kafka-logs-0,/tmp/kafka-logs-1`.
- Transactions: a running transactional producer. On a single broker set
  `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1` and
  `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1`.

`rust/tests/integration/admin_examples_test.rs` runs every example against
such clusters.
