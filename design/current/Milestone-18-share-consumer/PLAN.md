# Milestone 18 — Share consumer (KIP-932), Rust only

Status: **DRAFT for approval** (2026-11-16). Nothing in this milestone has started. The rule changes in
§6 need human approval before Phase 1. CLAUDE.md is changed only through the agent-roles process.

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
- **D2** — KIP-714 methods on `ShareConsumer`: mirror `Consumer` and leave them untranslated
  (recommended).
- **D3** — **Decided 2026-11-18: four PRs** — PR-A = P1–P2, PR-B = P3–P5, PR-C = P6–P9, PR-D = P10.
  (Options were one milestone PR, as M16, or PRs at tier boundaries.)
- **D4** — Which §6.2 / §6.3 amendments to adopt now. They are cross-cutting, so they also bind
  producer and admin work.

### 7.1 Share-specific design decisions (no consumer precedent)

- **D5 — Record retention for RENEW (KIP-1222).** Java's `ShareInFlightBatch` keeps the
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
- **D6 — Cross-thread `wakeup`.** `poll` holds `&mut self` and the type is `!Sync`, so no other
  thread can call `wakeup(&self)` during a poll. That is why `ConsumerHandle` exists. Recommended:
  a Rust-only `ShareConsumerHandle` (`Send + Sync`) exposing only `wakeup()`, with a test plan per
  R8. Java forbids every other consumer call from inside the acknowledgement callback
  (`ShareConsumerImpl#acquire` throws *"KafkaShareConsumer methods are not accessible from
  user-defined acknowledgement commit callback"*). Rust enforces this at compile time, so the Java
  tests that assert it are skipped with that reason.
- **D7 — `acknowledge*` misuse: `Result` or panic** (CLAUDE.md §12.5). The cases are *"Implicit
  acknowledgement of delivery is being used."* and *"The record cannot be acknowledged."*, the latter
  also reachable from `acknowledge(topic, partition, offset, type)` with arbitrary input.
  Recommended: `Result<(), Error>` (`IllegalStateError`, Java's text). Whether a record is still in
  flight isn't readable from the signature, so the panic carve-out doesn't fit. The explicit-mode
  *"All records must be acknowledged…"* check is already inside `poll`'s `Result`.
- **D8 — `AcknowledgementCommitCallback` shape.** **Decided 2026-11-18** (now `share-consumer.md` §3): `#[async_trait]` like
  `OffsetCommitCallback`,
  `on_complete(&self, offsets: &HashMap<TopicIdPartition, HashSet<i64>>, error: Option<&Error>)`.
  `commit_sync` returns `HashMap<TopicIdPartition, Option<Error>>`.

### 7.2 Which decision gates which phase

| Decision | Needed before | Why |
|---|---|---|
| D4 (rules) | P0 | P0 applies the approved rules |
| D8 (callback shape) | P2 | `AcknowledgementCommitCallback` is a P2 type |
| D3 (PR shape) | end of P2 | PR-A would be P1–P2 |
| **D5 (renew retention)** | **P3a** | `ShareInFlightBatch` / `ShareCompletedFetch` own the record-retention shape; P3b, P5b, P7 and `ShareConsumerRenewTest` build on it |
| D1 (admin slice) | P9, P10 | |
| D6 (wakeup handle), D7 (`Result` vs panic), D2 (KIP-714) | P7 / P8 | |

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
| `signalReconciliationCompleting` runs after `transitionTo(ACKNOWLEDGING)`; Java runs it before (`AbstractMembershipManager#maybeReconcile` completion), and Java's `reconciliationInProgress &&` guard is missing. | Critic 240 (P4a) | Consumer follow-up (the share hook is a no-op), not M18 |
| Backtest candidates S1-2/3/4, S2-3, S2-4, S5-1, S5-2 (`backtest-report.md`) | Critic 202 | Unverified; consumer follow-up backlog |

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
