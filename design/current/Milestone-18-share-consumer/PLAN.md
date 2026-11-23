# Milestone 18 — Share consumer (KIP-932), Rust only

Status: **P0–P11 done** (2026-11-23); the milestone PR (`PR-BODY.md`) awaits the user's review of §8.1,
the open decisions and the rules proposal (`rules-promotion.md`). Drafted 2026-11-16. CLAUDE.md is
changed only through the agent-roles process.

## 1. Goal and scope

Translate `org.apache.kafka.clients.consumer.ShareConsumer` (`KafkaShareConsumer`, `MockShareConsumer`)
and its full dependency closure from **Java 4.4.0** (tag `4.4.0` = commit `1156b2752a`; the final tag points at the same commit as 4.4.0-rc4, so the submodule already pinned on master *is* 4.4.0) to Rust. Every
Java unit test and every `clients-integration-tests` share test is translated. C FFI, Python, .NET and
the multilanguage harness are **out of scope**: they come in a later milestone.

Base: `origin/master` @ `81ddfa9c0`, branch `milestone-18-share-consumer`, worktree
`example-confluent-kafka-rust-m18-share`.

### 1.1 What already exists on master

| Piece | State |
|---|---|
| 24 share wire specs (`rust/generator/messages/Share*.json`, `*ShareGroup*`) | generated message types exist |
| `ApiKeys::SHARE_FETCH` etc. | exist |
| `InvalidShareSessionEpochError`, `ShareSessionLimitReachedError`, `ShareSessionNotFoundError`, `InvalidRecordStateError` | exist (`rust/src/common/errors/`) |
| `ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE_CONFIG` / `SHARE_ACQUIRE_MODE_CONFIG` + validators | exist (`consumer_config.rs`) |
| `WakeupTrigger` share-fetch hooks | partial (mentions share) |
| `common/requests` wrappers for ShareFetch / ShareAcknowledge / ShareGroupHeartbeat / ShareGroupDescribe | **missing** |
| Admin share-group RPCs (`describeShareGroups`, `list/alter/deleteShareGroupOffsets`, `deleteShareGroups`) | **missing** (M11 Tier 4 deferral) |
| Everything in `consumer` named `Share*` / `Acknowledg*` | **missing** |

### 1.2 Java inventory (4.4.0)

| Group | Java main lines | Java test lines |
|---|---|---|
| Wire wrappers (`common/requests/Share*`, `ShareRequestMetadata`) | 1,178 | (in `RequestResponseTest`) |
| Public value types, config, acknowledgements (`AcknowledgeType`, `AcknowledgementCommitCallback`, `ShareAcknowledgementMode`, `ShareAcquireMode`, `ShareConsumerConfig`, `Acknowledgements`, `AcknowledgementBatch`, `NodeAcknowledgements`, `AcknowledgementCommitCallbackHandler`) | ~960 | ~880 |
| Session and fetch data path (`ShareSessionHandler`, `ShareInFlightBatch`(+Exception), `ShareCompletedFetch`, `ShareFetchConfig`, `ShareFetchException`, `ShareFetchBuffer`, `ShareFetch`, `ShareFetchCollector`, `ShareFetchMetrics{Registry,Manager,Aggregator}`) | ~2,235 | ~3,030 |
| Membership and heartbeat (`ShareMembershipManager`, `ShareHeartbeatRequestManager`, `ShareRebalanceMetricsManager`, `ShareConsumerMetadata`) | ~585 | ~2,360 |
| `ShareConsumeRequestManager` | 1,799 | 3,955 |
| Events (10 `Share*Event`, `ShareAcknowledgementEventHandler`) + processor/`RequestManagers` wiring | ~520 + edits | (in processor tests) |
| `ShareConsumerImpl`, `KafkaShareConsumerMetrics`, `ShareConsumerMetrics` | ~1,540 | 1,113 |
| Public API (`ShareConsumer`, `KafkaShareConsumer`, `MockShareConsumer`, `ShareConsumerDelegate(Creator)`) | ~1,310 (mostly javadoc) | ~945 |
| **Total client closure** | **~10,100** | **~13,300 unit** |
| Integration (`clients-integration-tests/.../ShareConsumer*Test`) — 140 `@ClusterTest`s | — | ~6,500 |
| Optional: Admin share-group RPCs (options/results/handlers) | ~1,850 | ~1,010 |

Java's share code moved fast: 4.3.1 → 4.4.0 changed 73 share files (+8,340 / −4,471), so translate
from 4.4.0 only. Checked 2026-11-16: `git diff 4.4.0 1156b2752a -- clients` is empty, and the `4.4`
branch has 73 commits after 4.4.0 but none touching the share consumer or share requests.

## 2. Size and cost estimate

### 2.1 Rust size

Ratios measured on the existing consumer: Rust main ≈ 0.8–1.0× Java main (rustdoc carried over per
CLAUDE.md §6), Rust tests ≈ 1.9× Java tests.

| | Estimated Rust lines |
|---|---|
| Main code (incl. ~1.5–2k of edits to shared consumer infra) | 9–12k |
| Unit tests | 21–27k |
| Integration tests + harness | 8–10k |
| **Core total** | **~40–50k** |
| Optional Admin share-group slice | +4–6k |

### 2.2 Cost, from measured Milestone 16

Milestone 16 (AK 4.4 bump, 14 Actor/Critic phases, 185 commits, 2026-10-06 → 10-09) is the best
measured precedent. Its transcripts (main + 36 subagents, all Opus 5.5) sum to:

| Token class | Tokens | Opus 5.5 price | Cost |
|---|---|---|---|
| Cache reads | 3.25 B | $0.20 / MTok | ~$650 |
| Cache writes (1 h TTL) | 80.6 M | $8 / MTok (2× input) | ~$645 |
| Output | ≥ 0.74 M logged (transcripts under-count streamed output) | $20 / MTok | ~$50–150 |
| **Total** | | | **~$1.35–1.45k** |

It changed ~32k Rust lines (27.9k inserted, 4.4k deleted in `rust/src` + `rust/tests`), so about **$45
per 1k Rust lines**. Cache reads and writes dominate: the cost scales with agent turns × context size,
so context hygiene matters more than output length.

| | Estimate |
|---|---|
| Core share consumer (40–50k lines × $45/kloc, ×1.3 contingency: fresh state-machine code needs more Critic rounds than a version delta) | **~$2.3–3.0k** |
| + Admin share-group slice | +$0.2–0.3k |
| Wall-clock (loop time, excluding human review) | ~5–8 days; the critical path (P1→P3→P5→P6→P7→P8→P10) is mostly serial |

These are list API prices. Under a subscription or enterprise plan they measure usage and are not an
invoice.

## 3. Design decisions to fix up front (become `share-consumer.md`, §6.1)

1. **Public API shape: same as `Consumer`.** The public surface is a `ShareConsumer<K, V>` trait whose
   blocking methods return `impl Future + Send`, a sealed `DynShareConsumer<K, V>` companion, and a
   concrete `KafkaShareConsumer<K, V>` holding the private `ShareConsumerImpl` delegate. Each type is
   `Send + !Sync` (`PhantomData<Cell<()>>`), and receivers are `&mut self` where Java calls
   `acquireAndEnsureOpen()`. `MockShareConsumer` implements the trait.
   - Async (they block in Java): `subscribe` (`addAndGet`), `unsubscribe`, `poll`,
     `commit_sync[_with_timeout]`, `close[_with_timeout]`.
   - Sync: `subscription`, `acknowledge*`, `commit_async`, `set_acknowledgement_commit_callback`,
     `acquisition_lock_timeout_ms`, `metrics`, and `wakeup(&self)`.
   - `ShareConsumerDelegate` / `ShareConsumerDelegateCreator` collapse into the constructor, as
     `ConsumerDelegate` did. This is a DoD §7 justification.
2. **One background task, shared with the consumer.** Java runs `ShareConsumerImpl` on the same
   `ConsumerNetworkThread` / `ApplicationEventHandler` / `ApplicationEventProcessor` /
   `RequestManagers` classes. Rust does the same: add share `Option` fields to `RequestManagers` in
   Java's `entries()` order (share heartbeat, share membership, share consume), share variants to
   `ApplicationEvent`, and share arms to the processor. No second network thread. §10 of
   `consumer-threading.md` (never race the poll) applies verbatim.
3. **Callback thread.** `AcknowledgementCommitCallback::on_complete` runs on the caller's task, inside
   the exact Java call sites of `handleCompletedAcknowledgements()`: `poll`, `commitSync`,
   `commitAsync` and `close`. `processBackgroundEvents()` runs in `poll`, `unsubscribe` and `close`.
   The javadoc says "any thread calling `poll`", but the code also calls it from commit and close, and
   the code is the contract (lesson VC-01). Exactly one callback per completed acknowledgement batch
   (CLAUDE.md §11.5).
4. **Receive path.** `ShareCompletedFetch` follows `consumer-threading.md` §27: one owned buffer,
   borrowed slices, no per-record topic `String`. It reuses the consumer's batch decode, including
   k-raina's chunked decompression (#242) if that lands first. An allocation-budget test is required
   (share fetch is a per-record hot path, CLAUDE.md §13).
5. **KIP-714 methods** (`clientInstanceId`, `register/unregisterMetricForSubscription`) follow
   whatever `Consumer` does today, which is not translated. **Decision D2.**

## 4. Phases

Each phase runs one Actor/Critic loop. Per-phase line counts are estimates. "∥" marks phases that can
run in a parallel worktree.

| Phase | Content | Java main / test | Est. Rust | Depends on |
|---|---|---|---|---|
| **P0** | Apply the approved rule changes (§6). Build the Java→Rust file and test map (the coverage ledger). Harness spike: share-group broker config (`group.share.*`, `share.coordinator.*`, `share.version`), and which tests need a 4.4 broker versus the default 4.2.0. Audit whether the Rust `AbstractMembershipManager` / `AbstractHeartbeatRequestManager` / `AbstractFetch` are still generic or have been specialised to the consumer. | — | small | — |
| **P1** | Wire wrappers: ShareFetch / ShareAcknowledge / ShareGroupHeartbeat / ShareGroupDescribe request+response, `ShareRequestMetadata`, `ConcreteRequest` / `ConcreteResponse` arms, builders (`super(...)` translated literally), byte-level vectors. | 1.2k / — | ~3k | P0 |
| **P2** ∥P1 | Public value types and acknowledgements: `AcknowledgeType`, `AcknowledgementCommitCallback`, `ShareAcknowledgementMode`, `ShareAcquireMode`, `ShareConsumerConfig` (unsupported-keys check with Java's message), `Acknowledgements`, `AcknowledgementBatch`, `NodeAcknowledgements`, `AcknowledgementCommitCallbackHandler` + tests. | 1.0k / 0.9k | ~3k | P0 |
| **P3a** | `ShareSessionHandler`, `ShareInFlightBatch`(+Exception), `ShareCompletedFetch`, `ShareFetchConfig`, `ShareFetchException` + tests, allocation budget. | 1.2k / 1.7k | ~5k | P1, P2 |
| **P3b** | `ShareFetchBuffer`, `ShareFetch`, `ShareFetchCollector`, the three share-fetch metrics classes + tests. | 1.0k / 1.3k | ~4k | P3a |
| **P4** ∥P3 | `ShareMembershipManager`, `ShareHeartbeatRequestManager`, `ShareRebalanceMetricsManager`, `ShareConsumerMetadata` + tests. Any refactor of the abstract managers goes in its own behaviour-neutral commit. | 0.6k / 2.4k | ~5k | P1 |
| **P5a** | `ShareConsumeRequestManager`: share-fetch building, the session, leader change, node acknowledgements. Tests split by `ShareConsumeRequestManagerTest` groups. | 1.8k / 4.0k (split) | ~5k | P3b, P4 |
| **P5b** | `ShareConsumeRequestManager`: sync / async / on-close acknowledge, renew (KIP-1222), error mapping, callbacks. | (cont.) | ~5k | P5a |
| **P6** | Events (10 `Share*Event` + `ShareAcknowledgementEventHandler`), processor arms, `RequestManagers` share supplier, `SubscriptionState::subscribe_to_share_group`, `WakeupTrigger` share action + processor tests. | 0.5k + edits | ~2k | P5b |
| **P7** | `ShareConsumerImpl` (acquire/release, poll loop, ack-mode rules, `handleCompletedAcknowledgements`, close sequence), `KafkaShareConsumerMetrics` + `ShareConsumerImplTest`. | 1.5k / 1.1k | ~5k | P6 |
| **P8** | Public API: `ShareConsumer` trait, `DynShareConsumer`, `KafkaShareConsumer`, `MockShareConsumer`, `api_promises.rs`, extended `check-consumer-receiver` lint, audience allow-list, plus `KafkaShareConsumerTest`, `MockShareConsumerTest`, `KafkaShareConsumerMetricsTest`. | 1.3k / 0.9k | ~3.5k | P7 |
| **P9** ∥P3–P8 | *(In scope: D1 decided 2026-11-18.)* Admin share-group RPCs, handlers, results/options, `MockAdminClient` parity per `admin-client.md` §9, plus tests. | 1.9k / 1.0k | ~5k | P1 |
| **P10a** | Harness (cluster configs, version gating) + `ShareConsumerTestBase` + `ShareConsumerTest` (71 tests). | — / 2.9k | ~4.5k | P8 (+P9) |
| **P10b** | `ShareConsumerDeliveryTest` (27), `DLQTest` (14), `LagTest` (9), `CallbackTest` (9), `RenewTest` (6), `RackAwareTest` (4). | — / 3.6k | ~4.5k | P10a |
| **P11** | Close-out: coverage-ledger audit (every Java share file and test mapped or skipped with a reason), deferral-ledger audit (§6.2 R2), check the `4.4` branch for share fixes landed after 4.4.0, `make verify`, full integration on 4.2.0 and 4.4, docs (README, CHANGELOG, MILESTONES). | — | small | all |

Total: 14 loop phases plus P0 and P11, about the size of Milestone 16.

### 4.1 P8 test plan for the Rust-only types (translation-checklist R8)

P8 adds three public types with no Java class to diff. Each behaviour below names the test that pins
it; "mock" rows are the `MockShareConsumer` variant (R8: the mock variant is planned too, lesson
C5-24).

**`DynShareConsumer`** (sealed, dyn-compatible companion of `ShareConsumer`, as `DynConsumer` is of
`Consumer`; `dyn_share_consumer.rs`):

| # | Behaviour | Test |
|---|---|---|
| D1 | `dyn DynShareConsumer` and its box are `Send + !Sync` | `dyn_share_consumer_is_send_and_not_sync`; `api_promises.rs` `share_consumers_are_send_but_not_sync` |
| D2 | Different implementations live behind one type (`Vec<&mut dyn …>`, `Vec<Box<dyn …>>`) and each call reaches its own consumer | `test_different_share_consumers_in_one_vec`, `test_boxed_share_consumers_in_one_vec` |
| D3 | `dyn DynShareConsumer` implements `ShareConsumer`: generic code over `C: ShareConsumer + ?Sized` accepts a box, including the method with a borrowed argument (`acknowledge_with_record`) | `test_dyn_share_consumer_is_a_share_consumer` |
| D4 | Errors pass through unchanged (message and variant) | `test_errors_pass_through_unchanged` |
| D5 | The sync methods (`wakeup`, `handle`, `metrics`, `acquisition_lock_timeout_ms`, `set_acknowledgement_commit_callback`) and `close` / `close_with_timeout` reach the consumer, each observed on its own (each forward made a no-op fails a test) | `test_sync_methods_reach_the_share_consumer` (mock: `wakeup`, `handle`, `close`, `metrics`); `dyn_share_consumer_forwards_reach_a_kafka_share_consumer` (`metrics`, `wakeup`, `close_with_timeout`, and `acquisition_lock_timeout_ms` / `set_acknowledgement_commit_callback` through the closed check; the mock's are Java's constants) |
| D6 | The boxed futures are `Send`: a boxed consumer can be driven from a spawned task | `test_dyn_share_consumer_futures_can_be_spawned` |
| D7 | It cannot be implemented outside the crate (sealed) | doc test `compile_fail` on the trait |

**`ShareConsumerHandle`** (decision D6: the `Send + Sync` view exposing only `wakeup`, because `poll`
holds `&mut self` and the consumer is `!Sync`; `share_consumer_handle.rs`):

| # | Behaviour | Test |
|---|---|---|
| H1 | `Send + Sync + Clone`; `Debug` prints no state | `share_consumer_handle_is_send_sync_and_clone`; `api_promises.rs` |
| H2 | A `wakeup` from another task ends a `KafkaShareConsumer::poll` that is waiting, which returns the `WakeupError` (`is_wakeup_error`), as Java's `wakeup()` from another thread does | `handle_wakeup_from_another_task_ends_a_waiting_poll` (`kafka_share_consumer.rs`, over `MockClient`) |
| H3 | A `wakeup` with no call waiting makes the next `poll` return the `WakeupError`, and only that one (Java's `WakeupTrigger` clears the flag when it throws) | `handle_wakeup_before_poll_fails_only_the_next_poll` |
| H4 | A `wakeup` from another task also ends a waiting `commit_sync` (Java's `setActiveTask(commitFuture)`) | `handle_wakeup_from_another_task_ends_a_waiting_commit_sync` |
| H5 | Clones share the consumer: a wakeup through a clone wakes the consumer | `handle_clones_wake_the_same_consumer` |
| H6 | The handle outlives `close`: `wakeup` after `close` does nothing and does not panic (Java's `wakeup` does not check `closed`) | `handle_wakeup_after_close_does_not_panic` |
| H7 | mock: the handle's `wakeup` sets the mock's flag, as `MockShareConsumer::wakeup` does (Java's mock never reads it) | `mock_handle_wakeup_sets_the_mock_flag` (`mock_share_consumer.rs`) |
| H8 | Wakeups are disabled once `close` starts (`disable()` runs before close's first `.await`, as Java's `disableWakeups()` is close's first step): a wakeup pending then is discarded and a later one, also through the handle, does nothing, including after a dropped `close` that left the consumer open (row 19 of §8.1, open question) | `wakeup_is_disabled_once_close_starts` (`share_consumer_impl.rs`; fails if `wakeup_trigger.disable()` is removed from `close_internal`) |

**`AcknowledgeOptions` / `AcknowledgeOptionsBuilder`** (the CLAUDE.md §2 `Options` form of
`acknowledge(String, int, long, AcknowledgeType)`; `acknowledge_options.rs`):

| # | Behaviour | Test |
|---|---|---|
| O1 | `build` with the four parameters set returns options carrying them, read through the public getters `topic`, `partition`, `offset`, `r#type` | `build_with_every_parameter` |
| O2 | `build` names the first unset mandatory parameter (`topic`, `partition`, `offset`, `type`) in a `LocalIllegalArgumentError`, exact message asserted | `build_names_the_first_unset_parameter` |
| O3 | `acknowledge_with_options` keeps Java's misuse errors: implicit mode → "Implicit acknowledgement of delivery is being used.", explicit mode with no failed batch → "The record cannot be acknowledged.", closed → "This consumer has already been closed." | `acknowledge_with_options_reports_java_misuse_errors` (`kafka_share_consumer.rs`) |
| O4 | `Send + Sync` | `api_promises.rs` |

## 5. Risks

1. **Shared-infra conflicts.** P6/P7 touch `request_managers.rs`, `application_event_processor.rs`,
   `subscription_state.rs` and `wakeup_trigger.rs`, the same files the consumer fixes keep changing.
   Mitigation: merge `origin/master` (merge commits, no rebase) before P4 and before P6, and keep
   share edits additive (new arms and fields, no reshaping).
2. **Abstract managers may be consumer-specialised** (the KAFKA-20106 reshape in M13, one reconcile
   driver in #279). The P0 audit decides between reuse and a behaviour-neutral refactor before P4.
3. **Broker support.** The harness defaults to `apache/kafka:4.2.0`, and some 4.4 tests (renew, DLQ,
   lag) may need a 4.4 broker. The P0 spike sets per-test version gating.
4. **Acknowledgement invariants.** The risk is lost or doubled callbacks, and records acknowledged
   twice or never. The Critic gets an explicit checklist (§6.1), and the integration Delivery and
   Callback suites are the backstop.
5. **Post-4.4.0 fixes.** None exist on the `4.4` branch today. P11 re-checks for share fixes landed later (a 4.4.1).
   **P11 (2026-11-23):** the `4.4` branch still has none in the client (one test-only DLQ commit); trunk has
   three that apply, not ported: KAFKA-21558 (leadership-change resilience; acknowledgements whose
   callback never runs), the renew-acknowledgement check accumulated across nodes, and KAFKA-19804
   (initial heartbeat interval, shared with the consumer). See `post-4.4.0-java.md`; the user decides
   whether to port them before the PR (§7 D11).

## 6. Rule changes proposed before starting (need approval)

### 6.1 New `.claude/rules/share-consumer.md` (analogous to `admin-client.md`)

Contents: §3 items 1–5 above, plus:
- **Event table**: each of the 10 share events classified completable or bare from its Java `extends`,
  per `consumer-threading.md` §28.
- **Acknowledgement-state ownership**: what lives app-side (in-flight batches,
  `Acknowledgements` per partition), what lives bg-side (per-node acks in `ShareConsumeRequestManager`),
  and which locks guard them (std `Mutex`, never held across `.await`).
- **Ack-mode semantics**: implicit versus explicit; Java's `IllegalStateException` texts asserted
  verbatim.
- **Critic checklist**: exactly-once callback, call sites of `handleCompletedAcknowledgements`,
  acknowledgement ordering, leader-change re-routing.
- **Citations by `Class#method`**, not line numbers (see R7).

It also amends `consumer-threading.md`:
- §20 removes the share-consumer files and `Share*Test` from the out-of-scope lists.
- §28 drops the "Share per §20" skip.
- CLAUDE.md drops "`ShareConsumerConfig` is not translated while the share consumer is not" (Python
  section; the Python half stays deferred).

### 6.2 Cross-cutting amendments from the consumer-audit learnings

Evidence: the 84-finding consumer parity audit (`consumer-audit-prs/consumer-findings.json`,
`why_missed` field). Each rule targets a cause that recurred.

| # | Proposed rule (target file) | Audit findings it would have caught |
|---|---|---|
| R1 | **Assertion fidelity** (DoD §3): every Java assertion has a Rust counterpart. A translated test may not swap inputs (record magic, list order), widen accepted outcomes in a fixture, or drop `assertFalse(future.isDone())` / timing checks without a deviation note at the test. The Critic diffs assertions per test. | C1-27, C2-01, C3-02, C3-03, C4-02, C4-06, C4-07, TX3-02 (the most frequent cause) |
| R2 | **Deferral ledger** (DoD §8): "deferred to Phase N", "documented carry-over" or a stub closure counts as a FIXME. Each one needs an entry in the milestone PLAN's ledger, and a phase cannot close with an unowned entry. | C1-04, C1-12, C3-03, C3-05, C3-06, C5-01, C5-04, C5-09 |
| R3 | **Production-path construction** (extends DoD §12): every config key and every Java constructor parameter is exercised by at least one test that builds through the production constructor, not a test-only setter. | C2-07, C3-06, C1-16, C5-01 |
| R4 | **Callback → async checklist** (CLAUDE.md §11): read the clock at completion, not at spawn; preserve completion order (no unordered per-callback spawn); wake the bg task when re-enqueuing; don't collapse a `CompletableFuture` chain so that `isDone()` timing changes; map every nested lock path when Java's reentrant monitors become a non-reentrant `Mutex`; a loop translated from a Java thread loop must yield, tested on a `current_thread` runtime. | C1-01, C2-01, C2-05, C2-06, C4-03, C4-08, C4-09, C4-14, C3-04 |
| R5 | **Java semantics table** (CLAUDE.md §13 or a new §): `LinkedHashMap` → `IndexMap` + `shift_remove` (never `swap_remove`); `TreeSet` / `SortedSet` → `BTreeSet`; sorted outputs stay sorted; `long` ms arithmetic saturates (test `Duration::MAX`); `String.trim` ≠ `str::trim`; `Matcher.matches` ≠ `Regex::find`; `equalsIgnoreCase`. Tests use non-trivial orders. | C3-01, C2-03, C2-04, C1-03, C1-08, C4-11, C5-06 |
| R6 | **`close()` is explicit**: steps of Java's `close()` stay in `close`, not in `Drop`. | C1-30 |
| R7 | **Rules are checked against Java**: a rule's claim about Java call sites cites `Class#method` and is verified before adoption. When Java contradicts a rule, the agent escalates and does not follow the rule. Separately, apply or convert the M17 citation errata: 36 of 46 line cites have drifted and the M13 errata were never applied. | VC-01, C5-03 |
| R8 | **Rust-only types get a test plan**: a type with no Java body to diff (a handle, an adapter) needs an explicit test plan in the phase PLAN, reviewed by the Critic. | C1-15, C5-24 |

### 6.3 Orchestration (`agent-roles.md` / `.claude/agents/project-manager.md`)

These lessons are currently only in personal memory; put them where every Manager run sees them:
- **Commit gating.** `--no-verify` commits with targeted tests only; `make verify` and the full suites
  run once at phase end; prefix commands with `env -u RUSTUP_TOOLCHAIN`.
- **Brokers and timeouts.** One broker cluster at a time
  (`lockf -t 120 -k <lock> timeout 1800 …`), and a `timeout` on every long command.
- **Worktrees.** Parallel tracks use separate worktrees and merge commits. Use `cp -c` warm target
  clones and init submodules with `-c` flags, never shared config.
- **Scratch and verification.** Each agent gets its own scratch prefix. The Critic verifies from a
  `git archive HEAD` export, because `.gitignore` excludes `*.json`.
- **Master and pushing.** Re-check `origin/master` before every merge-back. Push only with approval,
  via `git push-external`.
- **Review files.** `COMMENTS.FP.md` / `COMMENTS.FN.md` are referenced but have never existed: create
  them per milestone or drop the step. Keep `COMMENTS.<N>.md` under the milestone directory, not the
  repo root, where stale files from earlier milestones still sit.

### 6.4 Adoption approach: pilot, then promote

Rules have caused regressions here before. VC-01 came from `consumer-threading.md` §31 misstating
Java's call sites, and C5-03 came from §20's "accept silently" line. So R1–R8 are piloted rather than
added to CLAUDE.md or the DoD:

1. **Scope.** R1–R8 live in this milestone's rules (`share-consumer.md` or a milestone checklist it
   links to), applying to share-consumer files only. CLAUDE.md and the DoD are shared with
   teammates' agents working on producer, admin, .NET and JavaScript, so they stay unchanged until
   the milestone ends.
2. **Each rule states its boundary**: "applies to / does not apply to" and an anti-pattern for
   over-application. The main over-application risks:
   - **R4, "loops must yield":** only a translated Java thread loop that can spin without awaiting
     I/O. Never `yield_now()` in per-record decode or send loops.
   - **R4, "wake the bg task":** use `ApplicationEventHandler::wake_background_task`, never
     `WakeupTrigger`. The wrong primitive returns a spurious `Wakeup` (`consumer-threading.md` §31).
   - **R5:** follow Java's exact semantics. Saturate only where Java clamps; where Java throws
     (`Math.addExact`), return the error. No retrofitting existing code outside the share files.
   - **R6:** `close()` does the Java steps explicitly. A `Drop` safety net may stay.
   - **R7:** "Java wins" applies to claims about Java behaviour, not to documented deviations that
     have a *Why* (async API, panics policy). On a contradiction the agent escalates; it does not act
     alone.
   - **R1:** a Java timing assertion may be adapted to the Rust test clock with a note at the test.
     It is not ported as a literal sleep.
   - **R3:** "production constructor" means the public constructor over `MockClient`. It does not
     mean adding broker-backed unit tests or test seams to production code.
3. **Backtest before P1** (~$50–150). Run a Critic with the new rules over two or three historical
   commits with known bugs (the pre-fix state of the #277 / #279 findings) and one known-good commit.
   Count what it catches and how many false positives the new rules produce.
4. **Canary.** P1 and P2 are small. Every Critic comment that cites R1–R8 is tagged, and the Manager
   logs rule-driven false positives in `COMMENTS.FP.md`. Any rule that produces them gets reworded
   before P3a.
5. **Promote** the rules that held up into CLAUDE.md and the DoD at P11, with team review.

## 7. Open decisions

- **D1** — **Decided 2026-11-18: yes, P9 is in scope.** Admin share-group RPCs: translate them in P9 (recommended: about 8 integration tests call
  `describeShareGroups` or the share-group offsets APIs, and the slice is small), or skip those tests
  with a reason.
- **D2** — **Decided 2026-11-20 (user): leave them out, mirroring `Consumer`.** KIP-714 methods on
  `ShareConsumer` (`clientInstanceId`, `registerMetricForSubscription`,
  `unregisterMetricFromSubscription`) are not on the trait, as `Consumer` has none of them;
  `MockShareConsumer#setClientInstanceId` goes with them. The 6 `KafkaShareConsumerMetricsTest`
  cases over the registration methods are skipped with that reason (ledger §2.1). Implemented in P8.
- **D3** — **Decided 2026-11-20: one milestone PR** (replaces the 2026-11-18 four-PR split), so the
  whole share consumer is tested end to end before review. The branch is pushed as phases land; the
  PR body must list the consumer behaviour changes (`revokePartitions`, §8), since fixup messages are
  lost on squash.
  (Options were one milestone PR, as M16, or PRs at tier boundaries.)
- **D4** — Which §6.2 / §6.3 amendments to adopt now. They are cross-cutting, so they also bind
  producer and admin work.

### 7.1 Share-specific design decisions (no consumer precedent)

- **D5 — Record retention for RENEW (KIP-1222).** **Decided 2026-11-20: option (c), provisional; team review at
  <https://claude.ai/artifact/CJQEkgXPFm9XCgno1S4yEZ>.** Implemented in P3a with these constraints: the whole scheme
  lives behind one crate-private seam inside `ShareInFlightBatch` (nothing outside the type knows how a
  redelivered record is produced); one refcounted `Bytes` handle per in-flight batch, offset-keyed
  `inFlightRecords` / `renewingRecords` / `renewedRecords`; copy-on-renew-only memory policy (once a batch's only
  remaining in-flight records are renewed ones, its raw bytes are copied into an owned buffer and the response
  handle is dropped); a deserialization error on redelivery takes the normal per-record path; the deviations
  (deserializer runs again, the redelivered record is a new value) are documented at the type citing
  `ShareInFlightBatch.java:35-37,176-179`. The `AcknowledgeType::Renew` rustdoc that tells users about it is a P8
  deferral row in `ledger.md`. Original analysis follows. Java's `ShareInFlightBatch` keeps the
  `ConsumerRecord` objects it hands to the user (`inFlightRecords`). A record acknowledged with `RENEW`
  moves `renewingRecords` → `renewedRecords` → back into `inFlightRecords` (`takeRenewals`), and the
  next `poll` returns the same object again (`ShareFetch` collects `getInFlightRecords()`). The Rust
  `ConsumerRecords` owns its records and is consumed by value, so the batch cannot keep them.
  - (a) Bound `K: Clone, V: Clone`. Rejected: the bound isn't in Java, and it deep-copies every
    record.
  - (b) Store `Arc<ConsumerRecord>` inside `ConsumerRecords`. Rejected: it breaks the consumer's
    owned iteration.
  - (c) **Recommended:** each in-flight batch keeps one refcounted handle on its raw record-batch
    bytes (compressed or not, no copy) plus the in-flight offsets it already tracks. When a renewed
    record is delivered again, the batch is re-decoded and the record re-deserialized. For a
    compressed batch that means decompressing again up to the record, because the Rust decoder
    decompresses one record at a time into pooled buffers (`completed_fetch.rs`), so decompressed
    bytes never persist. Deviation to document: the deserializer runs again for renewed records,
    while Java delivers the same object.
    - **Cost when nothing is renewed:** about zero. One handle per batch, never one per record, and
      no per-record copy. Copying every record's decompressed bytes so they could be redelivered was
      considered and rejected: it is a hot-path copy (CLAUDE.md §14).
    - **Cost on renew:** one re-decode, decompress and deserialize per renewed record, at most once
      per renew cycle (about one acquisition-lock period, `group.share.record.lock.duration.ms`,
      default 30 s). That is microseconds, up to ~1 ms to re-decompress a large batch. It is
      negligible next to processing slow enough to need a renew.
    - **Memory risk:** a `Bytes` slice keeps its whole underlying allocation alive. If that
      allocation is the whole fetch response, one long-renewed record could pin up to
      `fetch.max.bytes`, where Java pins only the record object. Mitigation: once a batch's only
      remaining in-flight records are renewed ones, copy that batch's raw bytes into an owned buffer
      and drop the shared handle. This copy happens only on the renew path. P3a confirms the buffer
      granularity of `PartitionData` and adds a test that a renewed record doesn't keep the response
      buffer alive.
  `acknowledge(record)` itself needs only the offset (`ShareInFlightBatch#acknowledge` looks up by
  `record.offset()`).
- **D6 — Cross-thread `wakeup`.** **Decided 2026-11-20 (user): the recommended `ShareConsumerHandle`.**
  Implemented in P8 (`consumer/share_consumer_handle.rs`): `Clone + Send + Sync`, one public method
  `wakeup`, obtained with `ShareConsumer::handle` (as `Consumer::handle`); test plan §4.1. Original
  analysis: `poll` holds `&mut self` and the type is `!Sync`, so no other
  thread can call `wakeup(&self)` during a poll. That is why `ConsumerHandle` exists. Recommended:
  a Rust-only `ShareConsumerHandle` (`Send + Sync`) exposing only `wakeup()`, with a test plan per
  R8. Java forbids every other consumer call from inside the acknowledgement callback
  (`ShareConsumerImpl#acquire` throws *"KafkaShareConsumer methods are not accessible from
  user-defined acknowledgement commit callback"*). Rust enforces this at compile time, so the Java
  tests that assert it are skipped with that reason.
- **D7 — `acknowledge*` misuse: `Result` or panic** (CLAUDE.md §12.5). **Decided (user; recorded 2026-11-20 in the P8
  fix round): `Result<(), Error>`** with Java's `IllegalStateException` texts as `LocalIllegalStateError`, per
  CLAUDE.md §12.2; implemented in P7 (`ShareConsumerImpl`) and P8 (`ShareConsumer::acknowledge_*`). The cases are *"Implicit
  acknowledgement of delivery is being used."* and *"The record cannot be acknowledged."*, the latter
  also reachable from `acknowledge(topic, partition, offset, type)` with arbitrary input.
  Recommended: `Result<(), Error>` (`IllegalStateError`, Java's text). Whether a record is still in
  flight isn't readable from the signature, so the panic carve-out doesn't fit. The explicit-mode
  *"All records must be acknowledged…"* check is already inside `poll`'s `Result`.
- **D8 — `AcknowledgementCommitCallback` shape.** **Decided 2026-11-18** (now `share-consumer.md` §3): `#[async_trait]` like
  `OffsetCommitCallback`,
  `on_complete(&self, offsets: &HashMap<TopicIdPartition, HashSet<i64>>, error: Option<&Error>)`.
  `commit_sync` returns `HashMap<TopicIdPartition, Option<Error>>`.

- **D9 — P10 harness and broker-version gates.** **Decided 2026-11-20 (user): all three approved.**
  (a) The share fixture reads broker meters over JMX (`with_broker_jmx` / `broker_meter_count`: the image's
  JmxTool run by `docker exec` against the broker container), where Java reads the broker's in-process Yammer
  registry; it keeps every meter assertion of the DLQ and Renew suites. (b)
  `ShareConsumerTest#testAddedPartitionsOfKnownTopicStartAtOffsetZero` runs only on a 4.4+ broker: it tests the
  broker fix KAFKA-20742, and a 4.2 broker starts the added partition at its log end. (c)
  `ShareConsumerLagTest#testSharePartitionLagAfterAlterShareGroupOffsets` runs only on a 4.3+ broker: it came with
  the broker fix KAFKA-20034, and a 4.2 broker reports no lag after the alter. Ledger §2.4 rows. A fourth P10
  proposal, retrying a cluster start whose broker misses the 90 s readiness deadline, was **rejected** (both cases
  were host overload, which a retry only hides) and reverted (`780a04bbf`).

- **D10 — The tests that needed Kafka's test jars, and a CI leg on a 4.4 broker.** **Decided 2026-11-21 (user),
  after Critic 310 Issues 3 and 4.** (a) Translate `ShareConsumerRackAwareTest` and the two tiered-storage
  `ShareConsumerDLQTest` cases instead of skipping them: the harness downloads the published 4.4.0 test jars
  (`kafka-clients-integration-tests:test` for `RackAwareTestAssignor`, `kafka-storage:test-fixtures` for
  `LocalTieredStorage`, `kafka-clients:test-fixtures` for the `TestUtils` it calls) from Maven Central on first
  use, caches them under the gitignored `target/test-jars`, checks pinned SHA-256s, and copies them into every node
  with `INCLUDE_TEST_JARS=true` (`tests/common/test_jars.rs`, `ClusterConfig::broker_libs`); per-broker properties
  (`broker.rack` per id) through `ClusterConfig::broker_properties`. No jar is committed. The tests are gated on a
  4.4 broker, the release the jars are built for. (b) Add the CI leg harness-spike §6 item 6 planned, so the tests
  gated off on the default 4.2.0 image run somewhere: the job `verify-rust (Linux amd64) — broker 4.4.0 (gated
  tests)` runs `make -C rust test-integration-broker-4.4`, i.e. the share suites plus the other 4.3/4.4-gated
  integration tests (the consumer max-size bounce arms, the quota-config test) against `apache/kafka:4.4.0`, on
  the PLAINTEXT listener only. Scope choice: Java gates by broker release, not by protocol, and the whole binary
  three times over on a second broker version would double the jobs' broker memory (the share suites alone peak at
  ~6 GiB of containers, §8 "Bugs found by P10"). Verified locally (134 passed); the GitHub/Semaphore run itself was
  not observable from the agent.

### 7.2 Which decision gates which phase

| Decision | Needed before | Why |
|---|---|---|
| D4 (rules) | P0 | P0 applies the approved rules |
| D8 (callback shape) | P2 | `AcknowledgementCommitCallback` is a P2 type |
| D3 (PR shape) | — | Decided: one milestone PR |
| **D5 (renew retention)** — decided 2026-11-20, (c) provisional | **P3a** | `ShareInFlightBatch` / `ShareCompletedFetch` own the record-retention shape; P3b, P5b, P7 and `ShareConsumerRenewTest` build on it |
| D1 (admin slice) | P9, P10 | |
| D6 (wakeup handle), D7 (`Result` vs panic), D2 (KIP-714) | P7 / P8 | all three decided: D2 and D6 on 2026-11-20, D7 (`Result`) recorded in the P8 fix round; implemented in P7 / P8 |

P0, P1, P2 and P4 (and P9, once D1 is settled) don't depend on D5. That is ~11–16k of the estimated
Rust lines, so they can run while the team discusses renew. Keep P1/P2/P4 free of any record-ownership
shape: `Acknowledgements` and the wire types are offset-based, so this holds naturally.

Checked and *not* a decision: sharing the background task. `ConsumerNetworkThread` is generic only
over `KafkaClient`, and `ApplicationEventProcessor` is not consumer-typed, so the share managers plug
in as Java does (§3 item 2).

## 8. Consumer gaps found during M18

These are pre-existing divergences from Java in the consumer, found while preparing the share consumer.

| Item | Found in | Disposition |
|---|---|---|
| `AbstractMembershipManager#revokePartitions` never translated. The reconcile path only marks partitions pending revocation; it skips `retainAll(assignedPartitions)`, the paused-partition log (`signalPartitionsBeingRevoked`) and the FATAL abort. The leave path (`ConsumerMembershipManager#signalMemberLeavingGroup`, epoch > 0) doesn't mark pending revocation at all. | Critic 240 (P4a) | **Fixed in P4b** in the shared base, as its own commit labelled as a consumer behaviour change (user, 2026-11-19). The share manager reaches the same method. |
| `AsyncKafkaConsumer::new` builds its network client inline without Java's `ClientUtils#createNetworkClient` wrap, `KafkaException("Failed to create new NetworkClient", t)`: a channel-builder failure reaches the caller as "Failed to construct kafka consumer" over the bare `ConfigError` | Critic 280 Issue 5 (P7) | Consumer follow-up, out of M18: build the consumer's client through `NetworkClientDelegate::supplier`, which has the wrap since P7 (only the share consumer calls it) |
| `NetworkClientDelegate` fails a disconnected request (and an unsent one whose connection failed) with `NetworkError` where Java's `NetworkClientDelegate.FutureCompletionHandler#onComplete` / `#checkDisconnects` use `DisconnectException` (`FutureCompletionHandler::on_complete`, `check_disconnects`). `Errors.forException(DisconnectException)` is `UNKNOWN_SERVER_ERROR` (the class has no code; Rust's `DisconnectError` has none either), so the share acknowledgement errors differ: `ShareConsumeRequestManager#handleShareAcknowledgeFailure` completes with `UnknownServerException` in Java, `NETWORK_EXCEPTION` in Rust; `#handleShareFetchFailure` passes `DisconnectException` through in Java, `NetworkError` in Rust. The user's acknowledgement commit callback sees the difference | Actor 260 (P5); Critic 260 Issues 1, 5 | **Share side fixed in P5** (§8.1 row 2): `ShareConsumeRequestManager::handle_completed_requests` maps the delegate's node-addressed `NetworkError` (and a dropped request) to `Error::Disconnect`, so both share paths match Java and the two tests assert Java's classes. The delegate change stays a consumer follow-up; it must also update `coordinator_request_manager.rs`, which matches `Errors::NetworkError` as the disconnect analog (the admin runnable already uses `Error::Disconnect`) |
| `SubscriptionState::assign_from_subscribed` collects the assignment into a `HashMap` and `PartitionStates::set` takes a `HashMap`, so the assignment (and `fetchable_partitions`) order is random per run; Java's `SubscriptionState#assignFromSubscribed` uses a `LinkedHashMap` and keeps the caller's order (translation-checklist R5). For the share consumer in `record_limit` mode this decides which node fetches first (`ShareConsumeRequestManager#poll` picks the node of the first fetchable partition); `testFetchOneNodeAtATimeForRecordLimitMode` failed about half the runs until the test pinned the order | Actor 260 (P5); Critic 260 question (b), Issue 6 | **Fixed (user decision, 2026-11-20)** in `50e83a71f` and its fixups, one consumer-behaviour-change commit. Java's order is restored end to end, for the consumer and the share consumer: the reconciliation sorts the resolved assignment by topic name, then partition (`TopicIdPartitionSet#topicPartitions()`, whose sort ledger G5 had dropped); `ApplyAssignment` carries it as a `Vec` (the `HashSet` hop is gone); `assign_from_subscribed` builds an `IndexMap`; and `PartitionStates::set` keeps the input order. Tests through both membership paths pin it. The P5 test pin is dropped; CHANGELOG entry under `### Consumer`. See §8.1 row 3 |
| `signalReconciliationCompleting` runs after `transitionTo(ACKNOWLEDGING)`; Java runs it before (`AbstractMembershipManager#maybeReconcile` completion), and Java's `reconciliationInProgress &&` guard is missing. | Critic 240 (P4a) | Consumer follow-up (the share hook is a no-op), not M18 |
| Backtest candidate S2-4 (`backtest-report.md`): `assignPartitions` trims the topic-name cache unconditionally in Java; Rust trims only after a successful `onPartitionsAssigned` (`continue_after_assign`) | Critic 202; **confirmed by Critic 245** (P4b) | Consumer follow-up; share unaffected (the share assignment applies inline) |
| Backtest candidates S1-2/3/4, S2-3, S5-1, S5-2 (`backtest-report.md`) | Critic 202 | Unverified; consumer follow-up backlog |
| `ConsumerIntegrationTest#testRackAwareAssignment` is not translated; `consumer_test.rs` gave as its reason that the Docker broker cannot load `RackAwareTestAssignor`, which stopped being true in P10: `ClusterConfig::broker_libs` with `TestJar::ClientsIntegrationTests` loads it (`share_consumer_rack_aware_test.rs`). The substitute `test_client_rack_reaches_the_broker` covers only the client side | Critic 310 Issue 10 (P10 fix round 2) | Consumer follow-up, out of M18: translate the test on a 4.4 broker with the test jar; the skip reason now says so |
| `AsyncKafkaConsumer::close` disables wakeups before its first `.await`, like the share consumer's `close`, so a `close` dropped after that leaves the consumer open with every wakeup (the `ConsumerHandle`'s included) disabled. Java's `close` cannot be abandoned. The consumer's tests cover only wakeups after a completed `close` (`close_disables_wakeups`), not this state | Critic 300 Issue 9 (P8 fix round) | Consumer follow-up, out of M18: document and test the state on `Consumer::close` / `ConsumerHandle::wakeup`, or re-enable wakeups when a `close` is dropped, for both consumers, if the user decides so (§8.1 row 19's open question) |
| `ConsumerNetworkThread::run_once` polls every `entries()` manager and the fetch manager in Phase 2, before Phase 2.4 (`drive_pending_release`) and Phase 2.5 (`ConsumerMembershipManager::reconcile(now, false)`); Java's `entries()` walk runs the membership manager's `poll` (`maybeReconcile(false)`) between the heartbeat and the offsets / topic-metadata / fetch managers. Only the `addAll` processing is split around the reconcile, so the old Phase 2 comments claiming the later polls saw its effects in the same iteration were false (corrected in `c0ddf76bb`). | Actor 270 (P6), reading `run_once`; **verdict by Critic 270: real, Low.** No fetch from a partition pending revocation can come from it: the background `maybeReconcile(false)` returns before `markPendingRevocationToPauseFetching` whenever the reconciliation revokes or auto-commit is on (`AbstractMembershipManager#maybeReconcile`), so revocation only starts from the AsyncPoll arm in Phase 1, before every poll, as in Java; an additions-only background reconciliation changes no `SubscriptionState` in that iteration, since 4.4 applies the assignment only through the app-driven `ApplyAssignmentEvent` (`ConsumerMembershipManager#applyAssignment`, KAFKA-20106); and `OffsetsRequestManager#poll` only drains `requestsToSend`, `TopicMetadataRequestManager#poll` only prunes expired requests. Two latency-only corners remain, both because Phases 2.4 / 2.5 also drive the stored callback continuations Java runs in Phase 1: (1) a prefetch `CreateFetchRequests` landing in the iteration whose Phase 2.5 consumes the `onPartitionsAssigned` ack omits, for that one prefetch round, a partition the listener `seek`ed; (2) after a fence, fatal or stale transition the release tail (`clearAssignment`) runs after the fetch poll, so one fetch can go out for partitions just lost, and its records are discarded on arrival | Consumer follow-up, out of M18: poll the offsets, topic-metadata and fetch managers after Phase 2.4 / 2.5, in Java's `entries()` order, as a labelled consumer behaviour change (§8.1 row 10's `impl RequestManager for Arc<…>` is a template for the synchronous reconcile half; the release drive awaits, so it stays its own phase). The share consumer is unaffected: its membership manager is an `entries()` item between heartbeat and share consume (§8.1 row 10) |

**Why `revokePartitions` was missed.** Java has had the method since at least 4.1, and Milestone 8
translated from 4.2. The name appears in no Rust file, plan or review before M18, so it was never
consciously deferred.
- **M8 Phase 11** moved `markPendingRevocation` into the rebalance-callback path when it translated
  listener invocation. The method's other steps had no home in the new shape and were dropped
  silently.
- **Later reshapes** — Phase 41 (non-blocking acks), M13's KAFKA-20106 handshake and #279 (one
  reconcile driver) — were each written against the existing Rust code rather than Java's method,
  so the gap carried forward.
- **The tests didn't catch it.** The translated
  `testRevokePartitionsUsesTopicNamesLocalCacheWhenMetadataNotAvailable` exercises the path but
  asserts only the topic-name cache.
- **The process didn't catch it.** DoD §2 ("are all methods implemented?") is checked by reading,
  class by class, and a method that is restructured away leaves nothing to notice.

**Process fix (pilot, from P4b on).** Each phase's Critic gets a per-class **method-coverage
table**: every Java method of each translated class maps to a Rust function (by name or
`#[doc(alias)]`) or to a stated reason. An unmapped method is a finding. A later `xtask` report could
mechanise this.

### Bugs found by P10

**None in the client.** All 115 Java methods of the seven `clients-integration-tests` share classes were run end to
end (113 translated, 2 skipped with reasons, ledger §2.4) on apache/kafka 4.2.0 and 4.4.0; no failure traced back
to the share consumer, the consumer or the admin share RPCs.

**Harness finding (environment, fixed in the tests):** the first run of the whole share suite on 4.4.0 failed 36
of 126 tests when every client lost the pooled default share cluster at once. Cause: the Docker VM ran out of
memory and its kernel OOM killer (`global_oom`) killed that cluster's broker JVM, at the second the clients saw the
disconnect (VM `dmesg`). The share suites start about a dozen distinct cluster configurations at once; in Java's
`KRAFT` shape (broker plus isolated controller) they peaked at 30 containers / 7.0 GiB of the VM's 7.75 GiB. Fix
`97ed22567`: pooled share clusters are combined nodes, Java's shape only for the tests that stop brokers (§8.1 row
27); peak 20 containers / 6.2 GiB, and the whole suite passes. Two broker-start timeouts under load were seen in the
same phase (a cold VM, and many clusters booting together); same root cause, host overload, not retried (D9).

### 8.1 Share-consumer deviations from the consumer design (review at the full-PR stage)

The user reviews these at the full-PR stage and keeps or reverts each one. Only the reasons are
summarised here; where the reasoning already lives elsewhere, the row links to it.

| # | Deviation | Why | Commit |
|---|---|---|---|
| 1 | **Response handlers run on the background task, not in spawned tasks.** `ShareConsumeRequestManager` keeps each request's response receiver and runs Java's `whenComplete` bodies in `handle_completed_requests`, called at the top of `poll` and of every event entry point, and (P6) by the background loop right after every network poll, before reaping (row 9). There is no per-request `tokio::spawn`. This departs from CLAUDE.md §11.3 ("use `task::spawn`" for callbacks the method does not await). The fetch manager (`FetchRequestManager`) and `AbstractHeartbeatRequestManager` already run their handler bodies on the background task, in a drain at the top of `poll`, but each keeps a spawned forwarder that moves the result into an mpsc channel and wakes the task; P5 drops only the forwarder. The commit, coordinator, offsets and topic-metadata managers do spawn, and run their handlers on the spawned task. | Critic 260's answer to question (c) (`COMMENTS.DONE.260.md`): Java runs these bodies inside `NetworkClientDelegate#poll` on the network thread, never concurrently with the manager's `poll` or with event processing. The class relies on that confinement (plain maps, no locks), and polling keeps it. A spawned handler would run after the next event drain and reap on the dedicated `current_thread` runtime, or concurrently with them on a multi-thread one. That opens four hazards: (i) the reaper expiring a `commitSync` / close event whose response already arrived; (ii) `acknowledgeOnClose` building on a session a FINAL_EPOCH response is about to remove; (iii) `fetch` setting "fetch more records" before the response clears it; (iv) the callback-registration flag read for responses Java handled under the old value. It would also put nearly the whole manager behind one shared lock (against `share-consumer.md` §6), add lock orders to audit, need a completion wake (the fetch, heartbeat and commit managers each had to add one), let handlers run after `close()`, and make callback order racy on a multi-thread runtime. The tests would also need a wait after every network poll. Polling needs none of that: the delegate completes the oneshots synchronously inside its poll | `96df47d45` (+ fixup `b451c8813`: three tests pin the entry-point ordering); P6 wiring `f22581ee2`, `231625c64`, `1073c7194` |
| 2 | **A share-local `NetworkError` → `Error::Disconnect` mapping.** `handle_completed_requests` turns the delegate's node-addressed `NetworkError`, and a dropped request, into a message-less `Error::Disconnect`, Java's `DisconnectException.INSTANCE` (§8 row on `NetworkClientDelegate`). | The Rust `NetworkClientDelegate` reports `NetworkError` where Java's `NetworkClientDelegate.FutureCompletionHandler#onComplete` / `#checkDisconnects` use `DisconnectException`. Without the mapping, the user's acknowledgement commit callback sees a different error than Java's (`UnknownServerException` on a failed ShareAcknowledge, `DisconnectException` on piggy-backed acknowledgements). Every share request names its node, so the mapping is exact. The delegate itself is unchanged: changing it is a consumer follow-up, and that change must also update `coordinator_request_manager.rs`, which matches `Errors::NetworkError` as the disconnect analog | `5287f8238`, `7a4d7abe1` (fixups of `96df47d45`) |
| 3 | **Consumer behaviour change: Java's assignment order, end to end.** The reconciliation in `AbstractMembershipManager` sorts the resolved assignment by topic name, then partition, as Java's `TopicIdPartitionSet#topicPartitions()` does. The consumer's `ApplyAssignment` hop carries it as a `Vec`. `assign_from_subscribed` builds an `IndexMap` and `PartitionStates::set` keeps the input order, as Java's `SubscriptionState#assignFromSubscribed` (`LinkedHashMap`) and `PartitionStates#set` do. After a rebalance, both consumers hold their partitions in Java's sorted order instead of a random one; a manual `assign()` is unchanged. | Share `record_limit` mode fetches from the node of the first fetchable partition, so a random order made `testFetchOneNodeAtATimeForRecordLimitMode` fail about half the time (§8 row). The user chose to fix it in shared infra rather than pin the order in the test | `50e83a71f` and its fixups (CHANGELOG `### Consumer`) |
| 4 | **The record-batch decoder is shared with `CompletedFetch`** (`batch_cursor.rs`), extracted in a behaviour-neutral commit rather than duplicated. | §3 item 4; `infra-audit.md` §4 ("`CompletedFetch` / batch decoder — refactor first (P3a)") | `7deea5d8a` |
| 5 | **RENEW record retention (D5, option (c), provisional).** A renewed record is decoded and deserialized again on redelivery, where Java hands back the same `ConsumerRecord`. The public doc (`AcknowledgeType::Renew`) carries a `WARNING:`: a **proposed contract change, awaiting acceptance** in `design/current/doc-contract-changes.md` (P8). | §7.1 D5 | `48d86cf59` (decision), `f406c1107`; doc `a34d82e8f` and its fixups |
| 6 | **The abstract membership and heartbeat managers were reshaped for a second subclass** (`MembershipManagerHooks`, the moved reconcile, leave and fence chains; `RebalanceMetricsManager` as a trait; `TopicIdPartitionSet` not translated, ledger G3 / G5), in behaviour-neutral commits. | PLAN §4 P4 and §5 risk 2; ledger §3 G3, G5 | `3923672ad`..`50c4735f6` (P4a) |
| 7 | **Consumer behaviour change: `AbstractMembershipManager#revokePartitions` translated** in the shared base, which the share manager also reaches. | §8 first row | `5b7977401` |
| 8 | **The share consume request manager listens to membership events through `impl MemberStateListener for Mutex<ShareConsumeRequestManager>`**, where `CommitRequestManager` keeps its listener state behind its own inner `Mutex`. | Java's `onMemberEpochUpdated` mutates the manager, which is otherwise driven through `&mut self` (`share-consumer.md` §6: plain fields); P6 holds it as `Arc<Mutex<_>>` and registers that. Ledger deferral row "P6 wiring of `ShareConsumeRequestManager`" | `96df47d45` |
| 9 | **A generic response hook after every network poll.** `RequestManager::handle_completed_requests` (Rust-only, default no-op) is routed by `RequestManagers::handle_completed_requests` to every manager of `entries()` without allocating; `ConsumerNetworkThread::run_once` calls it right after the network poll (Phase 4.5, inside the lock Phase 5 already takes, before `maximumTimeToWait` and the reaper), and `cleanup` after each close poll. Only the share consume request manager overrides it. | Java runs `whenComplete` inside `NetworkClientDelegate#poll`, so a share `commitSync` future completes in the `runOnce` iteration of its response (row 1). A hook in the loop keeps that timing; making it a trait method routed by `RequestManagers` (Critic 260's ask) means a consumer manager can adopt it later without touching `run_once`. Behaviour-neutral for the consumer (no consumer manager overrides it; no extra lock in `run_once`). Pinned by `run_once_handles_completed_requests_after_the_network_poll_and_before_the_reaper`, `cleanup_handles_completed_requests_after_each_close_poll` and the two `run_once_completes_*` share tests | `f22581ee2`, `1073c7194` |
| 10 | **The two `Arc`-shared share managers are `entries()` items**, through `impl RequestManager for Arc<ShareMembershipManager>` and `for Arc<Mutex<ShareConsumeRequestManager>>`, where the consumer's `Arc`-shared managers (coordinator, commit, membership) are skipped by `entries()` and driven explicitly by `run_once`. | Java's share `entries()` is coordinator → share heartbeat → share membership → share consume, and the order matters: the membership poll (`maybeReconcile(true)`) applies an assignment that the share consume poll fetches in the same iteration. As entries, the share managers are polled, asked for `maximumTimeToWait`, given `pollOnClose` and the response hook in Java's order with no share code in `run_once`. (The consumer's explicit drive polls offsets and fetch before its reconcile; see §8.) | `231625c64` |
| 11 | **The share supplier returns the managers.** `RequestManagers::supplier_with_share_acknowledgement_event_handler` builds and returns `RequestManagers` instead of Java's `Supplier` (the network thread's deferred creation); it takes `GroupRebalanceConfig`'s group id and rack id (G8), drops the `LogContext` and `ClientTelemetryReporter` (G16), and adds a Rust-only `completion_notify` that wakes the network poll from the coordinator's and heartbeat manager's spawned forwarders. The consumer has no supplier at all: `AsyncKafkaConsumer::new` builds its managers inline. **Naming (Critic 270 Issue 3):** `supplier_with_share_acknowledgement_event_handler` and `with_share_consume_request_manager` name only their first discriminating parameter. That is a deliberate exception to the letter of CLAUDE.md §2 for crate-private constructors, following the precedent of `ShareHeartbeatRequestManager::with_state` and `RequestManagers::new` (which a strict reading would rename too). The strict names, for the PR review: `supplier_with_share_acknowledgement_event_handler_share_fetch_metrics_manager` (the untranslated consumer overload would be `supplier_with_options`) and `with_share_consume_request_manager_share_heartbeat_request_manager_share_membership_manager`. | The network thread is constructed with built managers in Rust (no `Supplier` anywhere in `ConsumerNetworkThread`); a supplier function, rather than inline construction in P7's `ShareConsumerImpl`, keeps Java's wiring (listener registration included) in the class Java puts it in, where the share test can call it | `231625c64` |
| 12 | **`SharePollEvent#isComplete` is a bare `Arc<AtomicBool>` on the variant**, where `AsyncPollEvent`'s three flags live in an `AsyncPollState` struct. | One flag needs no struct (DoD §7); the application side keeps a clone, and its own deadline for `isExpired` (P7) | `c381dfa3e` |
| 13 | **A dropped share `poll` keeps what it collected; a dropped consumer `poll` gives it back.** The share consumer collects into a `ShareFetchCollectState` it keeps (`ShareFetchCollector::collect_with_state`, `ShareCompletedFetch::fetch_records_into`), so the next `poll` returns every record a dropped one decoded. Implicit acknowledgement and the explicit-mode check skip records the application was not handed (`acknowledge_all_handed_out`, `check_all_handed_out_are_acknowledged`), and the next `poll` returns those records. The acknowledgements `pollForFetches` took are sent by a drop guard (`PendingAcknowledgements`). Each completed-acknowledgement entry leaves its queue before its callback runs. The consumer instead drops what a cancelled call decoded and fetches it again (`FetchCollector`'s `CollectState`). | A share record is acquired for this consumer and cannot be fetched again; a dropped record waits out its acquisition lock and uses up a delivery attempt, and an implicit ACCEPT of a record never returned is lost data (ledger deferral rows, closed in P7). With every record handed out (always, unless a call was dropped), the handed-out methods are Java's `acknowledgeAll` / `checkAllInFlightAreAcknowledged`. Every drop test checks the next call's result and the acknowledgements sent, and was verified by mutation | `85df12051`, `a2bb05fa4` |
| 14 | **The share consumer keeps Java's separate background-event reaper**, a plain field confined to the application task; `AsyncKafkaConsumer` reaps background events with its application reaper. | Java's `ShareConsumerImpl` has both reapers; the share consumer's background events carry no completion, so the field is all Java's code needs | `a2bb05fa4` |
| 15 | **`commit_sync` waits for its event without a deadline of its own**, where `AsyncKafkaConsumer::commit_sync` bounds its wait by the event's deadline. | Java's `ShareConsumerImpl#commitSync(Duration)` waits with the unbounded `ConsumerUtils.getResult(commitFuture)`; the share consume manager and the reaper complete the event by its deadline (`share-consumer.md` §5). The wakeup still ends the wait | `a2bb05fa4` |
| 16 | **Constructors.** `new` (Java's configuration constructor), `with_client` (`#[cfg(test)]`, Java's visible-for-testing constructor over a `KafkaClient`) and `with_components` (Java's 19-argument one, taking a `ShareConsumerImplComponents` struct); Java's 9-argument constructor with factories for Mockito mocks is not translated (the tests use `new` and `with_components`). `NetworkClientDelegate::supplier` builds the network client the consumer builds inline, and passes the share fetch throttle sensor, which the consumer does not pass (its documented carry-over, `FetchMetricsManager::throttle_time_sensor`). **Naming:** `with_client` and `process_background_events_with_future_deadline_ms_ignore_error_event_error` follow row 11's crate-private convention for the first; the second is the full CLAUDE.md §2 name | The consumer precedent (`AsyncKafkaConsumerComponents`); `with_client` is what P8's `KafkaShareConsumer` test constructor delegates to, as Java's does | `41ed87c26`, `a2bb05fa4` |
| 17 | **Java loses the acknowledgements `pollForFetches` took when the collector throws; Rust does the same.** `pollForFetches` takes them with `currentFetch.takeAcknowledgedRecords()`, which clears the in-flight records and resets each batch's `Acknowledgements`, so the local `acksToSend` is their only reference. `fetchCollector.collect(fetchBuffer)` throws only when it collected nothing: `initialize` fails (`TopicAuthorizationException`, or a `KafkaException` for `CORRUPT_MESSAGE` or an unexpected error code), or the first batch carries an exception (`ShareFetchException`). The throw leaves `collect`, `pollForFetches` and `poll`, and nothing sends the map. A RENEW is never among the lost acknowledgements (a fetch with renewals does not call the collector), so the loss is ACCEPT, RELEASE, REJECT and GAP: an accepted record is redelivered once its acquisition lock times out (a duplicate, and a used-up delivery attempt), a rejected one comes back instead of being archived or dead-lettered, a released one is only late. `ShareConsumerImpl::collect` discards them the same way (`PendingAcknowledgements::discard`), pinned by `collector_failure_does_not_send_the_acknowledgements_taken`. | **Keep Java; candidate upstream report; user to decide at PR review** (Critic 280 verdict 6(a)). Sending them instead, as the drop guard of row 13 does, is a one-line change in both clients, and the pinning test flips | `a2bb05fa4` |
| 18 | **`clientInstanceId` always returns Java's no-reporter `IllegalStateException`** ("Telemetry is not enabled. Set config `enable.metrics.push` to `true`."), whatever `enable.metrics.push` says. | KIP-714 telemetry is not translated (ledger G16), so the consumer never has a reporter; the public method is decision D2 (P8) | `a2bb05fa4` |
| 19 | **A dropped share `close` is resumed, not restarted.** `close` sets `closed` only after its last step (as `AsyncKafkaConsumer::close`), and keeps a `CloseProgress`: a retried `close` awaits the `ShareAcknowledgeOnCloseEvent` the dropped one sent instead of sending another, sends the `ShareUnsubscribeEvent` again, and goes straight to the join once the background task was told to stop. A retried consumer `close` runs every step again. **Wakeups stay disabled:** `close` disables them before its first `.await`, so a `close` dropped after that leaves the consumer open with every wakeup, the handle's included, disabled; documented on `ShareConsumer::close` / `close_with_timeout` and `ShareConsumerHandle::wakeup`, pinned by `wakeup_is_disabled_once_close_starts`. **Open question for the user:** re-enable wakeups when a `close` is dropped? That would change both consumers (`AsyncKafkaConsumer::close` disables first too). | Java's `close` cannot be abandoned and makes each step once. A second `ShareConsumeRequestManager#acknowledgeOnClose` fails the close requests still in flight ("Attempt to call close() when there is an existing close request for node N") and completes their piggy-backed acknowledgements a second time, so the callback would see one acknowledgement twice with contradictory results (Critic 280 Issues 1, 7, 8). Re-sending the leave is harmless; re-signalling a stopping task is pointless. Pinned by `dropped_close_is_finished_by_the_next_close`, `dropped_close_does_not_send_the_acknowledge_on_close_again`, `dropped_close_at_the_join_only_joins_on_retry`, `dropped_close_after_the_join_is_finished_by_the_next_close` | `a2bb05fa4` and its fixups |
| 20 | **One public constructor, over a string map.** `KafkaShareConsumer::new(&HashMap<String, String>, key, value)` stands for Java's `(Map, Deserializer, Deserializer)` and `(Properties, Deserializer, Deserializer)`; the two constructors without deserializers are not translated. A `ShareConsumerConfig` failure (an unsupported key) is returned unwrapped, a construction failure wrapped as "Failed to construct Kafka share consumer", as Java. `KafkaConsumer::new` takes a built `ConsumerConfig` instead. | The deserializers would come from class-valued keys, which the Rust configuration does not have (CLAUDE.md §2). `ShareConsumerConfig` is crate-private (Java's class is package-private), and a `ConsumerConfig` rejects the share keys, so the share consumer cannot take a built configuration the way `KafkaConsumer` does | `aafedd8af` |
| 21 | **`ShareConsumerHandle` and `ShareConsumer::handle`** (D6). Only `wakeup`, where `ConsumerHandle` also carries the listener-reentrant operations. | §7.1 D6: `poll` holds `&mut self`; Java forbids every other call from inside the acknowledgement commit callback (`ShareConsumerImpl#acquire`), so there is nothing to reenter | `aafedd8af` |
| 22 | **`acknowledge_with_options(AcknowledgeOptions)`** for Java's `acknowledge(String, int, long, AcknowledgeType)`, with `AcknowledgeOptionsBuilder` (`build` names the first unset mandatory parameter in a `LocalIllegalArgumentError`). | CLAUDE.md §2: more than three parameters in the method name. The `Consumer` trait has no four-parameter method; the precedent is `ConsumerRecord::with_options` | `aafedd8af` |
| 23 | ~~**`ShareConsumer::subscribe(&[String])`**, where `Consumer::subscribe_with_topics` takes `Vec<String>`.~~ **Removed** (Critic 300 Issue 2): `subscribe` takes `Vec<String>` and moves the topics into the subscription, as `Consumer::subscribe_with_topics` does. | The `Consumer` trait's convention (`consumer/mod.rs`) is "a `Vec` when the implementation keeps the collection", and both share consumers keep the topics; the first version applied it backwards and cloned every topic. No divergence remains | `aafedd8af` and its fixup |
| 24 | **`MockShareConsumer`'s closed checks.** `subscription` and `metrics` panic on a closed mock with Java's message, `subscribe` / `unsubscribe` / `poll` / `add_record` return it; Java throws `IllegalStateException` from all six. | CLAUDE.md §12.5 and the consumer precedent (`MockConsumer::metrics` panics); the methods with no error channel keep Java's throw as a panic | `aafedd8af` |
| 25 | **`ShareConsumer::poll` with a timeout over `i64::MAX` ms** waits without limit, where Java throws `ArithmeticException`. Proposed contract change, `design/current/doc-contract-changes.md`, awaiting the user's acceptance (two `doc-baseline.txt` lines until then). | The accepted `Consumer::poll` entry; `ConsumerUtils::duration_to_millis` saturates for both consumers | `aafedd8af` |
| 26 | **`KafkaShareConsumerTest` runs over a `SharedMockClient` wrapper** (test code only) that shares one `MockClient` between the background task and the test, queues responses a matcher prepares, replays the last metadata on an update request (Java's `DefaultMockMetadataUpdater`), and treats a mismatched matcher as a pending response. | Java's test keeps mutating the `MockClient` it handed the consumer, including from inside a matcher; the Rust `MockClient` is owned by the background task, and it removes a prepared response before asserting the matcher, on the background task, where the consumer turns the panic into a failed request. Without the mismatch record the matchers of [B] / [C] could not fail the test (checked by mutation) | `aafedd8af` |
| 27 | **The pooled share integration clusters are combined broker+controller nodes** (`Type::CoKraft`, `share_cluster_config`), where Java's share suites run `@ClusterTestDefaults(types = KRAFT)`, a broker plus an isolated controller; only the tests that stop or restart brokers get Java's shape (`lifecycle_cluster`, dedicated). Test harness only; no assertion changes. | With an isolated controller per pooled configuration the share suites exhausted the Docker VM's memory and the kernel OOM killer killed the pooled default broker mid-run (§8 "Bugs found by P10"). Every other pooled suite already runs combined nodes, and the topology is invisible to a test that never stops a broker | `97ed22567` |
