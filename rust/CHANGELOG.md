# Changelog

All notable changes to the `confluent-kafka` crate are documented in this file.

Before 1.0 the public API is not stable, and any minor release may contain
breaking changes.

## [0.1.1] - 2026-10-01

First preview release of the Rust Kafka client: a translation of the Apache
Kafka Java client (4.3.1) that keeps its architecture, names and behavior,
adapted to Rust conventions and to async Rust on Tokio.

> **Preview:** not yet recommended for production use. The public API is not
> stable before the 1.0 GA release and may change. We welcome your feedback
> while the design is still open.

### Producer

- `KafkaProducer` behind the `Producer` trait, plus `DynProducer` for
  dynamic dispatch (`Box<dyn DynProducer<K, V>>`).
- `send` and `send_with_callback`, returning a future that resolves to the
  record's `RecordMetadata`; `flush`, `partitions_for`, `metrics`, `close` and
  `close_with_timeout`.
- Idempotent producer, with the Java client's sequence tracking, epoch bump and
  recovery.
- Transactions: `init_transactions`, `begin_transaction`,
  `send_offsets_to_transaction`, `commit_transaction` and `abort_transaction`.
- Built-in sticky partitioner, `RoundRobinPartitioner`, and custom partitioners
  through the `Partitioner` trait.
- Producer interceptors.
- Batching, linger, retries with exponential backoff, delivery timeout and
  in-flight request limits, as in the Java client.
- Zero-copy send path: keys, values and headers are written straight into the
  batch buffer, and requests go out with vectored I/O.
- `MockProducer` and `MockPartitioner` for tests.

### Consumer

- `KafkaConsumer::new` returns a `Box<dyn Consumer<K, V>>` backed by the
  KIP-848 consumer group protocol (`group.protocol=consumer`).
- Subscription by topic list or by `SubscriptionPattern` (a regular expression
  evaluated by the broker), with or without a `ConsumerRebalanceListener`;
  manual assignment with `assign`; `unsubscribe`.
- `poll`, returning `ConsumerRecords` together with the next offset to consume for each partition.
- Offset management: `commit_sync` and `commit_async` (with or without explicit
  offsets and an `OffsetCommitCallback`), `committed`, `position`, `seek_*`,
  `seek_to_beginning` and `seek_to_end`.
- Offset lookups: `beginning_offsets`, `end_offsets`, `offsets_for_times` and
  `current_lag`.
- `pause` / `resume`, `partitions_for`, `list_topics`, `group_metadata`,
  `metrics`, `enforce_rebalance`.
- Rebalance listeners run on the caller's task during `poll`, and can call back
  into the consumer through a `ConsumerHandle` (for example to `commit_sync`
  on revocation).
- `wakeup` to interrupt a blocking call, and `close` / `close_with_options`
  using `CloseOptions`.
- `read_committed` and `read_uncommitted` isolation levels.
- Consumer interceptors.
- Zero-copy receive path: deserializers borrow from the fetched buffer.
- `MockConsumer` for tests.

### Admin client

- `AdminClient::create` returns a `Box<dyn Admin>`. Every operation returns
  immediately with a result holding one `KafkaFuture` per key, as in Java, and
  has a `_with_options` variant.
- Topics and partitions: `create_topics`, `delete_topics`, `list_topics`,
  `describe_topics_*`, `create_partitions`, `delete_records`, `list_offsets`,
  `elect_leaders`, `alter_partition_reassignments`,
  `list_partition_reassignments`.
- Cluster and configuration: `describe_cluster`, `describe_configs`,
  `incremental_alter_configs`, `list_config_resources`, `describe_features`,
  `update_features`.
- Log directories: `describe_log_dirs`, `alter_replica_log_dirs`,
  `describe_replica_log_dirs`.
- Groups: `list_groups`, `describe_consumer_groups`, `describe_classic_groups`,
  `list_consumer_group_offsets_*`, `alter_consumer_group_offsets`,
  `delete_consumer_group_offsets`, `delete_consumer_groups`.
- Transactions and producers: `describe_producers`, `describe_transactions`,
  `list_transactions`, `abort_transaction`, `fence_producers`,
  `force_terminate_transaction`.
- Security: `create_acls`, `describe_acls`, `delete_acls`,
  `describe_client_quotas`, `alter_client_quotas`,
  `describe_user_scram_credentials`, `alter_user_scram_credentials`, and
  delegation tokens (`create`, `renew`, `expire`, `describe`).
- `MockAdminClient` for tests, mirroring the Java mock.

### Common

- Networking on Tokio non-blocking I/O, with one selector multiplexing all
  broker connections, as in the Java client.
- Wire protocol generated from the official Kafka JSON message definitions,
  including flexible versions and tagged fields.
- TLS through rustls with the aws-lc-rs crypto provider, including mutual TLS
  and hostname verification. Keys and certificates are PEM, either as files or
  inline in the configuration.
- SASL/PLAIN authentication over `SASL_PLAINTEXT` and `SASL_SSL`.
- Compression: gzip, snappy, lz4 and zstd.
- Serializers and deserializers for strings and byte arrays, a deserializer for
  `Bytes`, plus
  custom ones through the `Serializer` / `Deserializer` traits.
- A single `Error` type for every fallible API, with predicates that rebuild
  the Java exception hierarchy (`is_retriable_error`, `is_api_error`,
  `is_authorization_error`, ...).
- Client metrics and configuration keys use the Java client's names.
- Every client logs a preview warning at startup through the `log` crate.

### Known limitations

- Classic consumer group protocol (`group.protocol=classic`) and client-side
  partition assignors are not supported.
- Share groups (KIP-932) and the share consumer are not included.
- Of the SASL mechanisms, only `PLAIN` is supported (no SCRAM, GSSAPI or
  OAUTHBEARER).
- JKS and PKCS12 keystores and truststores are not supported; convert them to
  PEM.
- The C FFI (`ffi` feature) is not part of the published crate. The C and Python
  bindings build from the repository.

[0.1.1]: https://crates.io/crates/confluent-kafka/0.1.1
[0.1.0]: https://crates.io/crates/confluent-kafka/0.1.0
