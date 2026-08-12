---
name: m11-g1-admin-topics-multilanguage
description: Milestone 11 G1 (admin multilanguage topics & partitions) — the envelope's real exception set, converting committed tests in place via a partially-cfg'd macro, and the C-vs-Python error-variant heuristic divergence that only a differential run finds
metadata:
  type: project
---

Slice G1 of `design/history/Milestone-11/PLAN-multilanguage-admin.md`: the six
topic/partition RPCs across all four backends, and the slice that had to prove
G0's per-key envelope before G2..G6 depend on it.

## The envelope needed three shapes, not one

G0 assumed every multi-key RPC is `entries[] + oneof outcome {error, value}`.
Two of the six G1 RPCs do not fit, and both were answered with a shape rather
than by bending the RPC:

  - **Whole-value response.** `listTopics` has *no* per-key futures — Java's
    `ListTopicsResult` holds a single `KafkaFuture<Map<String, TopicListing>>` —
    so a per-key oneof would carry a permanently dead error arm. Later slices
    reuse this for `describeCluster` and the group listings.
  - **Value carries its own error.** Exactly three of the 46 resolve the per-key
    future *successfully* while reporting a failure inside the value:
    `createTopics` (`TopicMetadataAndConfig`), `describeLogDirs`
    (`LogDirDescription.error`, G2), `deleteAcls` (`FilterResult`, G5). For
    createTopics, `CreateTopicsResult::values()` maps the future with an
    infallible `then_apply(|_| ())`, so `values()`/`all()` succeed while
    `topic_id()`/`config()` return `Err`. Both bindings already model it
    (`kafka_admin_TopicMetadataAndConfig_error`, admin.py's
    `TopicMetadataAndConfig.error`), so omitting it would have produced *false
    four-way agreement* on an empty metadata with no error.

**That third state is unreachable on the harness's cluster.** It needs either a
broker older than CreateTopics v5 or a caller lacking DESCRIBE_CONFIGS on the
topic (`ReplicationControlManager.java` sets `topicConfigErrorCode` only in the
`!authorizedToReturnConfigs` branch), and `User:ANONYMOUS` is a super user with
no authorizer configured. `validateOnly` does *not* reach it either — the
controller still fills the successes map. `MockAdminClient` always completes with
real metadata. So the schema and all three servers carry it, and no G1 scenario
asserts it.

## Nothing new had to be invented for the trait

Every input, option and value type crosses as the **production public type**:
`NewTopic`, `NewPartitions`, `RecordsToDelete`, all six `*Options`,
`TopicListing`, `TopicDescription`, `TopicPartitionInfo`, `Node`,
`DeletedRecords`, `TopicMetadataAndConfig`, `Config`. All have public
constructors *and* public getters, so the harness needs no parallel value types
(DoD #7) — check this before writing one for a later slice.

The exception is `CreateTopicsResult::futures()`, which is `pub(crate)` (Java's
field is private too). `RustNativeAdmin` reassembles `TopicMetadataAndConfig`
from the four public per-field views instead; they are all `then_apply_try` over
the same source object, so they fail together, which *is* the
`TopicMetadataAndConfig(exception)` state.

`deleteTopics` and `describeTopics` need **two trait methods each** (by names /
by ids) rather than one taking a `TopicCollection`: both bindings already split
them, and the key type of the result changes with the collection kind so one
method could not type its return.

## Converting committed tests in place: cfg the macro's arms, not the module

`multilanguage_{test,consumer_test}_macro` are gated whole on
`multilanguage-tests`, which is why `mod producer_test` is too — so the producer
scenarios do **not** run under `make test-integration` / `verify-sandbox`.
Repeating that for admin would have moved committed coverage behind a feature.

Fix: put `#[cfg(feature = "multilanguage-tests")]` on the three container arms
*inside* the macro and move the macro module to `integration-tests`. An
invocation then expands to just `__rust` without the feature — 15 entries — and
to 60 with it. That is what makes replacing the originals (rather than keeping
two copies) lossless, since the `__rust` arm drives the same production trait
against the same broker.

The propagation helpers had to be duplicated, not changed:
`test_utils::{create_topic, wait_for_all_partitions_metadata,
try_partition_count}` take `&dyn Admin` and are shared with the producer and
consumer suites. `AdminBackend` twins live in `tests/common/admin_backend.rs`;
the `&dyn Admin` ones die with the last unconverted admin test.

## The C server guessed error variants differently from Python — 2 red arms, 0 defects

The C FFI does not expose the Rust `KafkaError` discriminator, so **both** servers
infer it from the message substring. They disagreed:

  - `grpc_translate.py::_guess_variant` runs for *every* error and covers 9
    variants, lowercased.
  - `server.cc` had the same idea as a **private member of
    `ProducerServiceImpl`**, covering 3 variants, case-sensitive, reachable only
    from the two `Send` call sites. All ~28 other `fill_proto_error` calls took
    its `variant_hint = VARIANT_GENERIC` default.

So an admin timeout crossed from Python as `Timeout` (`is_retriable() == true`)
and from C as `Generic(UnknownServerError)` (false, because the FFI reports code
-1 and `Errors::for_code(-1)` is `UnknownServerError`). Two scenarios failed on
the C backend alone with nothing wrong in the client.

Fixed by hoisting one `guess_variant(const char*)` into the anonymous namespace,
mirroring Python's patterns and order exactly, and making `fill_proto_error`
always apply it (the `variant_hint` parameter is gone — its default *was* the
bug). **General rule: any place where the two servers independently reconstruct
information the C boundary dropped must share one algorithm, or the oracle
manufactures disagreements.**

Standing limitation worth a real fix later: variant / `is_retriable()`
assertions on the gRPC backends rest on message substrings, not on transported
data.

## Scenarios worth having that no committed test had

  - `describe_and_delete_topics_by_ids` — the only coverage of
    `TopicCollection.ofTopicIds` and `ResultKey.topic_id`. The id comes from
    `list_topics`, the sole public route to it before a describe.
  - `create_topics_with_replica_assignment` /
    `create_partitions_with_assignment` — the assignment-carrying constructors
    send a *different* broker request and have their own path in every binding;
    without a scenario they are wired and never executed. The broker id is not
    fixed by config, so read it from a probe topic's `replicas()[0].id()`.
  - `create_topics_against_unreachable_broker_fails` — G0's create/close proves
    no connectivity (`new_admin_client` is lazy, `close` always succeeds). Point
    a client at `127.0.0.1:1` with a 3s api timeout; every other scenario would
    also fail there, which is the point.

## Environment

  - `cargo fmt` and `cargo clippy` are **absent** from the active 1.95.0
    toolchain, so `cargo xtask format-check` carries exactly the same caveat as
    `cargo xtask lint` — both only run under the nix 1.97.1 triple. Do not
    report format-check as unconditionally clean.
  - Clippy 1.97 has `cloned_ref_to_slice_refs`: `&[topic.clone()]` for a
    one-element slice wants `std::slice::from_ref(&topic)`. It fires a lot in
    scenario bodies.
  - Filtering `-- __grpc_python` also matches `__grpc_python_async`, and matches
    the producer and consumer suites too — one run covers the whole Python-side
    regression (88 entries). `-- __grpc_c` is 44.
  - Piping a `cargo test` run through `tail -N` loses the per-test lines you need
    for a results table; `tee` to a file instead.
