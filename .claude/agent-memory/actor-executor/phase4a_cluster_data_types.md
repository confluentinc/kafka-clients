---
name: Phase 4a cluster data types
description: Phase 4a translation notes — Arc<str> hot-path interning choices, null-topic handling, deferred ClusterResourceListeners
type: project
---

# Phase 4a — what's load-bearing for Phase 4b/Phase 5

## Hot-path Arc<str> interning

`TopicPartition::topic`, `TopicIdPartition.topicPartition.topic`, and
`PartitionInfo::topic` are all `Arc<str>` (CLAUDE.md rule 11). Constructors
take `impl Into<Arc<str>>` so callers can pass `&str`, `String`, or an
existing `Arc<str>` cheaply. `Cluster`'s `partitionsByTopic`,
`availablePartitionsByTopic`, `topicIds`, `unauthorizedTopics`,
`invalidTopics`, `internalTopics` are keyed by `Arc<str>`. The cluster
constructor shares the same `Arc<str>` instance from the source
`PartitionInfo` to its derived index keys (no per-key string allocation).

Public getters all return `&str` so callers don't see the `Arc` wrapper.
There's also a `topic_arc()` accessor on `TopicPartition` and
`PartitionInfo` for callers building further keyed structures.

## Null-topic divergence from Java

Java accepts `null` for the topic name in `TopicPartition` and
`TopicIdPartition`. The Rust translation uses `Arc<str>` (non-null) and
substitutes the empty string `""` for "no topic". `TopicIdPartition`'s
`Display` therefore prints `<topicId>:-1` instead of Java's
`<topicId>:null-1`. The semantics ("no topic name") are preserved.
The `TopicIdPartitionTest.testToString` translation has an updated
expected string that reflects this.

## ClusterResourceListeners is `pub(crate)` with `#![allow(dead_code)]`

`org.apache.kafka.common.internals` is a Java-internal package, so the
struct is `pub(crate)` per CLAUDE.md naming rules. None of the producer
or metadata stack lives in the crate yet (Phase 4b/Phase 6), so
`#[deny(warnings)]` would trip dead-code on its public methods. We use
`#![allow(dead_code)]` at the file level rather than peppering each
method, with a comment pointing to the future consumers.

## Cluster::bootstrap vs bootstrap_with_hosts

Java's `Cluster.bootstrap(List<InetSocketAddress>)` calls
`getHostString()` on each address — that returns the original textual
host (DNS name or IP literal), preserving the unresolved hostname for
later DNS lookup. Rust's `SocketAddr::ip().to_string()` returns the
numeric IP. To match Java's behavior we expose two bootstrap variants:
- `bootstrap(&[SocketAddr])` for already-resolved addresses
- `bootstrap_with_hosts(&[(String, u16)])` for unresolved hostnames
  (closer to Java's `InetSocketAddress` semantics)

The `ClusterTest.testBootstrap` translation uses
`bootstrap_with_hosts` because the test passes a hostname string
without resolving.

## Cluster::nodes() shuffle

Java's `Cluster` constructor shuffles the node list with
`Collections.shuffle`. We mirror that with `rand::seq::SliceRandom`.
Two consequences:
- `Cluster::equals` is order-dependent on the `nodes` vec, so
  `assertEqual(c1, c2)` between two freshly-constructed clusters with
  >=2 nodes is in principle order-flaky. Java has the same property;
  `ClusterTest.testEquals` uses 1 node so the shuffle is a no-op.
- The `partitions_by_node` map is built by id, not by index, so it is
  not affected by the shuffle.

## Equality scope

- `Node::equals` uses **all** fields (id, port, host, rack, isFenced) —
  the prompt's "id only" claim was wrong; verified against Java's
  `Node.equals` in 4.2.
- `Cluster::equals` mirrors Java's check list: isBootstrapConfigured,
  nodes, unauthorizedTopics, invalidTopics, internalTopics, controller,
  partitionsByTopicPartition, clusterResource, topicIds. Note: the
  three derivable indices (`partitionsByTopic`,
  `availablePartitionsByTopic`, `partitionsByNode`,
  `nodesById`, `topicNames`) are NOT in the equality check — Java
  excludes them too because they're recomputed.

## Test coverage caveats

- `TopicPartitionTest` is mostly Java-Serializable round-trip — we kept
  the accessor-round-trip test only and skipped the file-format
  compatibility test (no Rust analogue).
- `ClusterTest.testReturnUnmodifiableCollections` asserts
  `UnsupportedOperationException` on `.add(...)`. Rust gets static
  immutability via `&` borrows, so we replaced those assertions with
  positive assertions about reachability and per-topic counts.
- No `Node`, `TopicPartitionInfo`, `ClusterResource`,
  `ClusterResourceListeners` test files exist in the Java tree; we
  added small unit tests for these (round-trip / equals /
  add+dispatch) and marked them as our additions.
