---
name: m11-phase1-admin-notes
description: Milestone 11 Tier 1 Phase 1 (Admin) — Rust core + unit + integration tests COMPLETE (no FFI/Python this task)
metadata:
  type: project
---

Milestone 11 (AdminClient), Tier 1 Phase 1 "Foundation + Topics CRUD", Actor N=1.
Plan: `design/history/Milestone-11/PLAN.md` (SCOPE BANNER: Rust core + tests ONLY,
NO C FFI, NO Python bindings, for any phase). Design rules: `.claude/rules/admin-client.md`.

**STATUS: Phase 1 COMPLETE and green.** build + `cargo test --lib` (2105 passing) +
`cargo xtask format-check` + `cargo xtask lint` all clean. 4 real-broker integration
tests pass via testcontainers.

**Key architectural realization that unblocked the network engine:** with
`bootstrap.controllers=false` (Phase 1) AND describeTopics using the Metadata-API
fallback path, ALL FOUR RPCs reuse already-wired wire types (CreateTopics,
DeleteTopics, Metadata). NO new wire types (DescribeCluster / DescribeTopicPartitions)
were needed — those are deferred to a later tier. This collapsed a large chunk of the
"remaining work" the earlier session feared.

**How the network engine is wired (mirrors producer `Sender<C>`):**
- `src/admin/internals/call.rs`: `Call` retry unit (boxed sync closures for
  create_request/handle_response/handle_failure/handle_unsupported_version) +
  `NodeProvider` enum (Controller/LeastLoaded/MetadataUpdate only — Constant &
  LeastLoadedBrokerOrActiveKController deferred) + `HandleResult` enum
  (Done / NewCall(quota-retry) / Retry(route-through-fail)).
- `src/admin/internals/admin_metadata_manager.rs`: `AdminMetadataManager` (Clone via
  `Arc<Mutex<Inner>>`) + `AdminMetadataUpdater` impl of `MetadataUpdater`. Both share
  ONE `Arc<Mutex<Inner>>` (cluster). The updater is passive (fetch_nodes only;
  maybe_update→MAX). `NetworkClient::with_metadata_updater(mm.updater(), ...)` is the
  integration point — Admin does NOT use a producer/consumer `Metadata`. The
  metadata_manager is ALSO cloned into `Shared` so create/delete handle_response
  closures can call `clear_controller`/`request_update` (handleNotControllerError).
- `src/admin/internals/admin_client_runnable.rs`: `AdminClientRunnable<C: KafkaClient>`,
  ONE tokio::spawn. `run_once` translates `processRequests` phase-for-phase.
  `fail_call` is the verbatim `Call.fail` retry logic (UV downgrade doesn't spend a
  try; backoff uses pre-increment tries). Queues: pending_calls Vec, calls_to_send
  HashMap<i32 node_id, NodeCalls>, calls_in_flight HashSet<String>,
  correlation_id_to_calls HashMap<i32, InFlightCall>. Internal metadata call: after
  Done, `unassign_unsent_calls(|_|true)` forces reassignment against new metadata.
- `src/admin/kafka_admin_client.rs`: `KafkaAdminClient` (app side owns
  `Shared{ admin_tx: mpsc::UnboundedSender<Call>, wakeup: Arc<Notify>, shutdown,
  metadata_manager, ...}`). RPC methods build per-key `KafkaFutureImpl` handles, box
  a `Call` capturing clones, `submit()` (channel send + wakeup.notify_one). Free
  functions `get_create_topics_call`/`get_delete_topics_call`(+with_ids) enable
  quota-retry to rebuild a fresh Call over the SAME futures Arc. `new_admin_client()`
  factory returns `Box<dyn Admin>`; `from_config` builds NetworkClient like
  KafkaProducer::from_config but PLAINTEXT-only + `with_metadata_updater`, seeds the
  bootstrap cluster, spawns the runnable.

**Deviations (documented in code + defensible to Critic):**
1. describeTopics by NAME uses the Metadata-API fallback (Java's
   generateDescribeTopicsCallWithMetadataApi), NOT DescribeTopicPartitions — module
   doc-comment explains. describeTopics by ID fails futures with unsupported_version
   (DescribeTopicPartitions deferred). All noted in `kafka_admin_client.rs` module docs.
2. quota-retry-on-timeout nuance (`maybeCompleteQuotaExceededException`) simplified to
   completeAllExceptionally on failure — the quota-timeout test slice was NOT translated.
   Straight quota-RETRY (resubmit retry topics) IS implemented.
3. CreateTopics config in TopicMetadataAndConfig uses `ConfigEntry::new(name,value)`
   (source/sensitive metadata simplified) — Phase-1 tests don't assert those.

**Unit-test harness gotcha (cost ~15 min):** `create_for_test` does NOT spawn; the test
drives `runnable.run_once()` manually (like SenderTest) and prepares responses on
`runnable.client_mut()` (MockClient). MUST start MockTime at a POSITIVE value (I use
1000) — MockClient's `not_throttled(0)==false` means a node never becomes ready at
t=0, so futures never complete and every test HANGS. (Same reason SenderTest starts at
1000.) 12 unit tests in `kafka_admin_client.rs` tests mod.

**Integration:** `tests/integration/admin_topics_test.rs` (registered in
`tests/integration/main.rs`), 4 `#[tokio::test]`s. Uses `new_admin_client` +
`TestContext`; `wait_until_listed` retry loop absorbs metadata-propagation lag. Left
`TestContext::cleanup` a no-op (shared infra; tests delete their own topics) — did NOT
wire it to admin to avoid destabilizing other suites.

**Testing infra reminder:** NEVER run two `cargo test` concurrently — they contend on
the build lock and appear to hang forever. macOS has no `timeout` (use `gtimeout` or
the harness). `cargo xtask lint` does NOT compile integration tests (feature-gated);
verify those with `cargo test --features integration-tests --no-run`.

**NEXT (Manager will run Critic N=1):** watch items for review — describe metadata
deviation, TopicDescription PartialEq excludes topic_id, id-XOR-name closed enum,
KafkaFuture::join_map helper. Future tiers add the AdminApiDriver/Handler/LookupStrategy
engine (listOffsets Phase 5), DescribeTopicPartitions, DescribeCluster, and the
remaining ~140 Admin RPCs.
