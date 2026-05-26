# Phase 4 — Cluster Model & Metadata (Manager plan)

Phase 4 translates the data structures that represent the cluster
topology and the `Metadata` cache that `KafkaProducer` reads from.
See `design/history/Milestone-1/PLAN.md` lines 222–242 for the goal.

## Sub-phase split

The 3,352 lines of Java in this phase are split into three Actor/Critic
rounds so each commit stays small enough to review carefully.

### Phase 4a — Foundational data types (this round)

**Java sources** (≈1,285 lines):
- `common/Node.java` (161)
- `common/TopicPartition.java` (72)
- `common/TopicIdPartition.java` (106)
- `common/TopicCollection.java` (81)
- `common/PartitionInfo.java` (141)
- `common/TopicPartitionInfo.java` (150)
- `common/Cluster.java` (397)
- `common/ClusterResource.java` (63)
- `common/ClusterResourceListener.java` (53) — **interface/trait**
- `common/internals/ClusterResourceListeners.java` (61)

**Tests:**
- `common/TopicPartitionTest.java`
- `common/TopicIdPartitionTest.java`
- `common/PartitionInfoTest.java`
- `common/ClusterTest.java`
- `clients/admin/TopicCollectionTest.java`
- (`NodeTest`, `TopicPartitionInfoTest`, `ClusterResourceListenersTest` —
  these do not exist in the Java tree; nothing to translate.)

### Phase 4b — Metadata stack

- `clients/MetadataRecoveryStrategy.java` (44) — enum
- `clients/MetadataSnapshot.java` (261)
- `clients/StaleMetadataException.java` (35)
- `clients/Metadata.java` (854)
- `clients/producer/internals/ProducerMetadata.java` (171)
- Tests: `MetadataTest`, `MetadataSnapshotTest`, `ProducerMetadataTest`

### Phase 4c — Client utils & common configs

- `clients/CommonClientConfigs.java` (330)
- `clients/ClientDnsLookup.java` (39) — enum
- `clients/HostResolver.java` (26) — interface/trait
- `clients/DefaultHostResolver.java` (29)
- `clients/ClientUtils.java` (278)
- Tests: `ClientUtilsTest`, `CommonClientConfigsTest`

## Module layout decisions

### Per CLAUDE.md naming rules

`org.apache.kafka.clients` (the bare `clients` package, not its
sub-packages) MUST NOT have `clients` in its Rust path. The
top-level files (`Metadata`, `MetadataSnapshot`,
`MetadataRecoveryStrategy`, `StaleMetadataException`,
`CommonClientConfigs`, `ClientDnsLookup`, `ClientUtils`,
`HostResolver`, `DefaultHostResolver`) therefore land at the
**crate root** as sibling modules to `common/`:

```
src/
  common/...
  metadata.rs
  metadata_snapshot.rs
  metadata_recovery_strategy.rs
  stale_metadata_error.rs           // StaleMetadataException → StaleMetadataError
  common_client_configs.rs
  client_dns_lookup.rs
  client_utils.rs
  host_resolver.rs
  default_host_resolver.rs
  producer/
    internals/
      producer_metadata.rs           // org.apache.kafka.clients.producer.internals
```

### Phase 4a placement

```
src/common/
  node.rs
  topic_partition.rs
  topic_id_partition.rs
  topic_collection.rs
  partition_info.rs
  topic_partition_info.rs
  cluster.rs
  cluster_resource.rs
  cluster_resource_listener.rs       // trait
  internals/
    cluster_resource_listeners.rs
```

`common/internals/mod.rs` already exists; add the new module there.

## Hot-path identifier interning (CLAUDE.md rule 11 + Phase 4 DoD)

`TopicPartition`, `TopicIdPartition`, `PartitionInfo` are the keys of
the producer-side per-partition `HashMap<TopicPartition, Deque<ProducerBatch>>`
in `RecordAccumulator`. Topic names are cloned at every call site that
constructs a `TopicPartition`. Use `Arc<str>` for the topic-name field in
these types so cloning the key is cheap. Public getters return `&str`.

`Cluster` stores topic→partition tables that are rebuilt on each
metadata refresh — the build path is not hot, but the lookup path is.
Use `HashMap<Arc<str>, Vec<PartitionInfo>>` for the topic-indexed
tables so a producer-side lookup keyed by a `String` topic borrowed
as `&str` is one hash computation.

## Java equivalence guards

- `TopicPartition`'s `hashCode`/`equals` on Java mix partition + topic.
  The Rust `Hash`/`Eq` derive must produce a stable layout; if `Arc<str>`
  is used, hash-by-content (not by pointer identity) — derive `Hash`
  goes through `str`'s `Hash` impl and is identity-safe.
- `Node::noNode()` returns a sentinel with `id=-1`, `host=""`, `port=-1`.
  Translate as `Node::no_node()` returning a `&'static Node` (or a
  `OnceLock<Node>`).
- `Cluster::empty()` is similarly a sentinel; cache it.
- `ClusterResourceListeners.maybeAdd(Object)` uses `instanceof
  ClusterResourceListener` — Rust translation should accept any
  `Arc<dyn Any + Send + Sync>` and downcast, OR (preferred) accept
  `Arc<dyn ClusterResourceListener>` directly and drop the
  `instanceof` check (we control all call sites).
- `Cluster::isBootstrapConfigured()` exists and is asserted by
  `ClusterTest`; preserve it.

## Skip / defer notes

- `ClusterResourceListener::onUpdate(ClusterResource)` is an empty
  default in Java with no producer-side implementor today. Translate
  the trait + `ClusterResourceListeners::on_update` plumbing — Sender
  and Metadata will call it in Phase 4b/Phase 6.
- `TopicCollection.TopicNameCollection` and `TopicIdCollection` are
  consumer/admin types — the producer doesn't use them, but they're
  cheap to translate and the file is small.

## Future perf opportunities

- **Cache `TopicPartition::hashCode`.** Java caches the int hash in a
  benign-data-race instance field (`TopicPartition.java:28,46-54`)
  because the comment in `Node.java:35` notes `hashCode` "is called in
  performance sensitive parts of the code (e.g. RecordAccumulator.ready)".
  The same applies to `TopicPartition`, the *key* of the producer's
  `Map<TopicPartition, Deque<ProducerBatch>>`. The Rust translation
  derives `Hash` through `(*self.topic).hash` which iterates the entire
  topic-name string each call. Caching would need `OnceLock<u64>` (atomic,
  no `unsafe`) or `Cell<Option<u64>>` (one allocation-free word, but
  requires interior mutability). Defer until the producer is wired
  end-to-end and we can benchmark `RecordAccumulator.ready` with a
  realistic topic-name length — Java's cache pays off most for long
  topic names. Phase 4a Critic Issue 2.
