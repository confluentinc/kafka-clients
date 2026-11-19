# Plan: `subscribe(SubscriptionPattern …)` on the .NET consumers (family S2)

Status: **done 2026-11-09** (approved 2026-11-08, Pranav: "I agree with the plan"; every lean as picked; ledger
rows 410–430, M23/P2). Manager N=121. Mode A (no Rust, C or Python change; no
C-ABI change). Worktree `TempDir/subscription-pattern`, branch `prashah_dev_dotnet_subscription_pattern`, on
`3afba0a7` (PR #325 head, stacked on PR #324). Proposed number: **M23/P2** (M23/P1 is the serializer
`ReadOnlyMemory` phase, N=120, ledger 400–409, on its own branch; its folder is not in this stack). The
alternative is M24/P1. Pranav picks (N1).

## Goal

Java's `subscribe(SubscriptionPattern)` and `subscribe(SubscriptionPattern, ConsumerRebalanceListener)` on
`IConsumer`, `IAsyncConsumer`, `KafkaConsumer`, `AsyncKafkaConsumer`, `MockConsumer` and `AsyncMockConsumer`,
plus Java's `SubscriptionPattern` type, Java- and Python-faithful. Item (1) of ledger row 148. Done when
`cargo xtask dotnet-java-name-baseline` prints `Wrote 942 line(s) … 12 removed, 0 added` (954 entries today)
and both gRPC pattern scenarios pass on the `__grpc_dotnet` and `__grpc_dotnet_async` arms.

## 1. Parity anchor (the ceiling on public surface)

**Java 4.4.0** (read from `TempDir/example-confluent-kafka-rust/kafka/`, the same submodule commit `1156b275`):

| Member | Cite |
|---|---|
| `Consumer.subscribe(SubscriptionPattern pattern, ConsumerRebalanceListener callback)`, `subscribe(SubscriptionPattern pattern)` | `Consumer.java:81`, `:86` |
| `KafkaConsumer` same two (parameter `listener`); Javadoc: IAE if pattern null or empty or listener null; ISE if subscribed with topics or assigned; a non-RE2/J pattern fails later in `poll` with `InvalidRegularExpression` | `KafkaConsumer.java:806`, `:830` |
| `AsyncKafkaConsumer`: listener null → IAE "RebalanceListener cannot be null"; then `acquireAndEnsureOpen` → `throwIfGroupIdNotDefined` → null → "Topic pattern to subscribe to cannot be null" → `pattern().isEmpty()` → "… cannot be empty" | `internals/AsyncKafkaConsumer.java:2173-2182`, `:2261-2284` |
| `MockConsumer`: listener null → IAE; then `pattern == null \|\| pattern.toString().isEmpty()` → "Topic pattern cannot be null/empty", **before** `ensureNotClosed()` | `MockConsumer.java:199-216` |
| `SubscriptionPattern`: public non-final class; ctor `(String pattern)` stores as given; `pattern()`; `toString()` returns the field; `hashCode()` = `pattern.hashCode()` (NPE on null); `equals` = `instanceof` + `Objects.equals` | `SubscriptionPattern.java:30-68` |

**Python** (the tie-breaker): `consumer/subscription_pattern.py` (stores `None`; `__str__` → `"None"` for it;
`__hash__` raises `TypeError("pattern must not be None")`; `__eq__` null-safe); `consumer.py:100-101`
(`SUBSCRIBE_FORMS`), `:140-173`, `:556-570` (`Pattern()` null → `TypeError("SubscriptionPattern.pattern must not be
None")` after the closed check; a `None` pattern crosses as `NULL`, the core answers); `async_consumer.py:118-150`
(the pattern form takes a plain **or async** listener), `:388-403`; `_mock_core.py:340-356` (mock: listener, then
`Pattern()` null, then "Topic pattern cannot be None/empty", then the closed check); the C extension builds the C
handle from the string at the call and destroys it after (`_confluentkafka.c:6160-6211`).

**C ABI** (verified in `rust/src/ffi/consumer/`; header lines from the clone's
`rust/target/include/confluent_kafka_bindings.h`, built from the same Rust sources):
`kafka_consumer_SubscriptionPattern_new_err` / `_destroy_err` (BH:45938, :45989; `_pattern_err`, `_to_string_err`
not needed); `kafka_consumer_Consumer_subscribe_with_pattern`, `_cb`, `_with_pattern_listener`, `_listener_cb`
(BH:37852-37950; `mod.rs:1246`, `:1304`, `:1364`, `:1438`). Each clones the pattern before it returns
(`mod.rs:1257`, `:1317`, `:1382`, `:1458`), so the handle is freed right after the P/Invoke, `_cb` forms included.
They take the shared `kafka_consumer_Consumer_t`, so the mock is covered through `MockConsumer__as_Consumer`
(`NativeMethods.cs:297`; `SafeMockConsumerHandle : SafeConsumerHandle`) and the core mock's
`subscribe_with_pattern(_listener)` (`mock_consumer.rs:707-716`). The core checks closed → `group.id` → empty on the
real consumer (`async_kafka_consumer.rs:4937-4946`) and empty → closed on the mock (`mock_consumer.rs:546-550`).

**.NET precedent to mirror** (topics `Subscribe`): declarations `IConsumer.cs:83,111`, `IAsyncConsumer.cs:100,131,159`;
classes forward to `NativeConsumer` (`KafkaConsumer.cs:88-92`, `AsyncKafkaConsumer.cs:90-107`, `MockConsumer.cs:79-83`,
`AsyncMockConsumer.cs:82-99`); sync path `NativeConsumer.cs:521-562` (listener check → `CheckAfterAcquire` →
`RefuseInsideACallback` → `CallSync` over the blocking form → `SettleListener`); async path
`NativeConsumer.Async.cs:92-179` (`Submit` over the `_cb` form, `ListenerRegistration.Root`, `SettleListener` on the
completion); argument snapshot with Java's order and per-flavour texts `NativeConsumer.cs:600-633`; small C values
built and freed per call by `NativeCallArguments` (`Interop/NativeCallArguments.cs`, `TopicPartition`,
`OffsetAndMetadata`, `Node`).

**Surface to add (and nothing else):**

```csharp
namespace Confluent.Kafka.Consumer;                       // Consumer/SubscriptionPattern.cs
public sealed class SubscriptionPattern {                 // Java SubscriptionPattern
    public SubscriptionPattern(string pattern);           // stores the argument as given, null too
    public string Pattern();                              // a method (CKRDA0013 rule 2)
    public override bool Equals(object? obj);             // ordinal, null-safe
    public override int GetHashCode();                    // D9
    public override string ToString();                    // the pattern; D3 for null
}
// IConsumer, KafkaConsumer, MockConsumer:
void Subscribe(SubscriptionPattern pattern);
void Subscribe(SubscriptionPattern pattern, IConsumerRebalanceListener callback);
// IAsyncConsumer, AsyncKafkaConsumer, AsyncMockConsumer:
Task Subscribe(SubscriptionPattern pattern, CancellationToken cancellationToken = default);
Task Subscribe(SubscriptionPattern pattern, IConsumerRebalanceListener callback, CancellationToken cancellationToken = default);
Task Subscribe(SubscriptionPattern pattern, IAsyncConsumerRebalanceListener callback, CancellationToken cancellationToken = default); // D4
```

Six P/Invokes (prelink count 967 → 973): `SubscriptionPattern_new_err`, `_destroy_err`, and the four
`Consumer_subscribe_with_pattern*`.

**NOT adding:** the `java.util.regex.Pattern` overloads (family S1; `dotnet/CLAUDE.md` §3 Java names rule 8, the
analyzer skips them); row 148 (2) `ConsumerRecords.NextOffsets()` and (3) `ConsumerRecord.DeliveryCount()` (N5);
pattern subscribe on `ConsumerHandle` (KIP-1306's view has none, `consumer-threading.md` §41); an async-listener
form on the sync `IConsumer` (as the topics forms); `IEquatable<SubscriptionPattern>`, `==` / `!=` (D10); a
`Pattern` property; a SafeHandle type over `kafka_consumer_SubscriptionPattern_t` (D2); P/Invokes for
`SubscriptionPattern_pattern_err` / `_to_string_err` (would be dead); new IntegrationTests (Python has none; the two
gRPC scenarios cover the broker path); any Rust, C, Python or C-ABI change.

## 2. Design

- **Type.** `Confluent.Kafka.Consumer.SubscriptionPattern` in `Consumer/SubscriptionPattern.cs` (Java package
  `org.apache.kafka.clients.consumer`, `dotnet/CLAUDE.md` §2). A managed value, as `TopicPartition` stores its
  arguments as given (`Common/TopicPartition.cs:19-29`). `Equals` = `obj is SubscriptionPattern other &&
  string.Equals(_pattern, other._pattern, StringComparison.Ordinal)`. `java-api.txt:2047-2052` lists its ctor and
  `equals`, `hashCode`, `pattern`, `toString`: with this shape CKRDA0013 reports nothing for it.
- **Marshalling (D2).** Built at the call, as Python's C extension does and as the binding builds `TopicPartition`
  and `OffsetAndMetadata`: `NativeCallArguments` gains `Kind.SubscriptionPattern` and
  `IntPtr SubscriptionPattern(string pattern)` (`_new_err` over `Utf8(pattern)`; freed with `_destroy_err` when the
  arguments are disposed). No SafeHandle: the handle lives only for one call.
- **Sync path.** `NativeConsumer.Subscribe(SubscriptionPattern)` / `(SubscriptionPattern, IConsumerRebalanceListener)`
  copy the topics methods: `RequireListener` → pattern checks (D7/D8) → `RefuseInsideACallback` → `CallSync` over
  `ConsumerSubscribeWithPattern[Listener]` → `SettleListener`.
- **Async path.** `SubscribeWithCallback(SubscriptionPattern …)` ×3 copy the topics ones over the `_cb` forms
  (`ConsumerCompletion.Void`, `ListenerRegistration.Root` for both listener kinds, `Finish(…,
  AsyncOperation.Subscribe, …)`).
- **Checks.** On `KafkaConsumer` / `AsyncKafkaConsumer` (D7): listener → closed / callback / owner →
  `group.id` → null pattern (IAE, Java's text) → `Pattern()` null (D6) → empty (the core, Java's text). On the
  mocks (D8): listener → null pattern, `Pattern()` null, empty (Java's `MockConsumer` texts) → closed.
- **Listener lifetime** as the topics forms: a successful subscribe replaces and releases the previous listener;
  `Unsubscribe` and `Close` keep it (`IAsyncConsumer.cs:103-112`).
- **gRPC servicers.** `ConsumerServiceImpl.cs:99-103` and `AsyncConsumerServiceImpl.cs:122-126` subscribe with
  `new SubscriptionPattern(request.Pattern)`, with the `LoggingRebalanceListener` when `WithListener`, as Python's
  `grpc_translate.py:402-412`; `Translate.PatternSubscribeUnsupported` goes.

## 3. Rulings needed

### 3.1 Fact refreshes — one approval

- F1. Row 148's claim holds: every C function is in the header (cites in §1). No Rust work.
- F2. The C functions act on the shared consumer handle, so the mocks need nothing extra (§1).
- F3. The C pattern can be freed right after each P/Invoke (`mod.rs:1257,1317,1382,1458`), as Python does.
- F4. The core keeps Java's order on both consumers (§1); the binding adds only what C cannot see.
- F5. Row 148 item (4), the mock extras, was done in PR #325 (`ba8574ba`).
- F6. `STATUS.md:105`, `:125`, `:194` are past entries and stay as written; the live text is `dotnet/CLAUDE.md:587`
  (and the stale comment block `rust/Makefile:212-258`). `ffi-marshalling.md:1946-1952` already names the pattern
  checks. `design/history/M9/P6/PLAN.md:45` ("not in the ABI; out of the whole roadmap") is superseded by row 148.
- F7. Bare `Subscribe(null!)` calls become ambiguous (CS0121) between the collection and pattern overloads, as in Java
  (`MockConsumerTest` writes `(SubscriptionPattern) null`): 8 test lines get a cast
  (`PublicConsumerCheckOrderTests.cs:274,276,694,695,705`, `PublicConsumerRebalanceListenerTests.cs:844`,
  `PublicSyncConsumerPreconditionTests.cs:66,183`).

### 3.2 Leans that are Python's way — one table

| # | Item | Python | Java | .NET (lean) |
|---|---|---|---|---|
| D2 | How the pattern crosses C | builds the C handle from the string at the call, frees it after (`_confluentkafka.c:6170-6182`) | n/a | same as Python, through `NativeCallArguments` |
| D3 | `ToString()` of a null pattern | `"None"` (`subscription_pattern.py`) | returns the null field (prints `null`) | `"null"`, as Python prints its null word and as `TopicPartition.ToString` prints a null topic |
| D4 | Async-listener pattern form on the async trio (+3 lines in the allowlist group "The async forms of a callback-taking overload", `java-name-allowlist.txt:68`) | the async consumer's pattern form takes a plain or `async def` listener (`async_consumer.py:124-127`) | none | add it, as the topics form (ledgers 47, 48) |
| D5 | Null and empty pattern texts | FFI clients: the core's Java texts; mocks: "Topic pattern cannot be None/empty" (`_mock_core.py:349-351`) | `KafkaConsumer`: "Topic pattern to subscribe to cannot be null/empty"; `MockConsumer`: "Topic pattern cannot be null/empty" | Java's text per flavour, `IllegalArgumentException` |
| D6 | `Pattern()` null at `Subscribe` (Java NPE) | `TypeError("SubscriptionPattern.pattern must not be None")` (`consumer.py:559-564`, `_mock_core.py:345-348`) | NPE (`pattern().isEmpty()`, mock `toString().isEmpty()`) | `ArgumentNullException`, `ParamName` "pattern", message "SubscriptionPattern.pattern must not be null." (the .NET form of a Java NPE, `dotnet/CLAUDE.md` §3 error rows) |
| D8 | Mock check order | pattern checks before the closed check (`_mock_core.py:340-356`) | same as Python (`MockConsumer.java:210-213`) | same as both: the binding checks null, `Pattern()` null and empty **before** the closed check on the mocks, because `Close` tears the native mock down (`NativeConsumer.cs:1782`) and only the binding can answer first; the core mock's own empty check then never fires for .NET |
| D9 | `GetHashCode()` of a null pattern | raises `TypeError("pattern must not be None")` | NPE | throws `ArgumentNullException("pattern")`. Caveat: .NET guidance says `GetHashCode` should not throw; if CA1065 fires under `TreatWarningsAsErrors`, the Actor stops and reports, and the fallback is `0` (the `TopicPartition` shape) |
| D10 | Equality surface | `__eq__` / `__hash__` only | `equals(Object)` / `hashCode()` only | `Equals(object)` / `GetHashCode()` only: no `IEquatable<T>`, no `==` (the newest precedent, PR #325's `OffsetAndTimestamp` / `ConsumerGroupMetadata`) |
| D11 | Tests | `test_mock_consumer.py:217-237`, `test_kafka_consumer.py:270-273`, `test_consumer_types.py:264-269`, `test_none_stored.py:279-292`, `test_none_dereference.py:228-262`, `test_none_is_null_consumer.py:120-137`, `test_consumer_family.py:365-376` | `MockConsumerTest.testRe2JPatternSubscription`; `AsyncKafkaConsumerTest.testSubscribeToRe2JPatternValidation`, `…ThrowsIfNoGroupId` | translate all of them (§4 S1/S2). Skipped, with the reason in the test file: `testSubscribeToRe2JPatternGeneratesEvent` (needs a stubbed event handler; core: `async_kafka_consumer.rs:9890`), `testSubscribePatternAgainstBrokerNotSupportingRegex` (needs `MockClient`), `KafkaConsumerTest.testSubscribeToRe2jPatternNotSupportedForClassicConsumer` (classic is refused at construction; Python skips it too, `test_kafka_consumer.py:57`) |

### 3.3 Items with no Python way — one at a time

- **D1. `sealed`.** (a) sealed, as `OffsetAndMetadata`, `CloseOptions` and `ConsumerGroupMetadata`, all non-final in
  Java and sealed here; (b) unsealed as Java, so a user may derive and `Equals` must then follow Java's `instanceof`.
  Lean (a). The analyzer does not flag it (`JavaNameAnalyzer.cs:484` checks only abstract classes).
- **D7. Check order on the real consumers.** (a) Java's order, as the topics precedent (`NativeConsumer.cs:611-633`):
  closed → `group.id` → null pattern → `Pattern()` null → empty (the core); a missing `group.id` wins over a null
  pattern (`InvalidGroupIdException`). (b) Python's: pass `NULL`, the core answers before its `group.id` check (an
  accepted Python deviation, root `CLAUDE.md` "Order"). Lean (a).
- **N1. Logistics.** M23/P2 (or M24/P1); N=121; plan folder `dotnet/design/history/M23/P2-subscription-pattern/`;
  dotnet-actor / dotnet-critic, `dotnet/COMMENTS.121.md` in the worktree, a Critic pass after every Actor commit;
  squash decided at the end. Next ledger row: 410.
- **N2. The hook on S3.** S3 edits `rust/Makefile` (outside `dotnet/`), so the pre-commit hook runs the Rust core
  (P1.5 precedent: 737 s and 851 s, Docker). (a) run it, with `RUST_TEST_THREADS=6`; (b) `--no-verify`, the skip list
  being the only change and the arms the real gate. Lean (a).
- **N3. `kafka/` for the Actor.** `$W/kafka` is empty. It is needed by the unit tests (`PublicNamespaceLayoutTests`'
  two per-row theories read `kafka/clients/src` and `rust/xtask/src/error_hierarchy.rs` through it; about 385 cases
  fail without it, M23/P1 record) and by `cargo xtask check-generated` (`java-api --check`, the generated error and
  config tables). `dotnet-java-name-baseline` does not need it (it reads the committed `java-api.txt`). (a) Pranav runs
  the local, no-network init before S0: `git -C "$W" -c protocol.file.allow=always -c
  submodule.kafka.url=/Users/pranavshah/WorkSpace/Confluent/TempDir/example-confluent-kafka-rust/.git/modules/kafka
  submodule update --init kafka` (it writes the module's git dir under the clone's
  `.git/worktrees/subscription-pattern/modules/`, outside `$W`, and checks out `1156b275`); (b) allow the Actor to run
  it; (c) skip those gates here and run them at the merge. Lean (a).
- **N4. Breaking changes (pre-publish).** New members on `IConsumer` / `IAsyncConsumer` break an external implementer
  (accepted pre-publish, `dotnet/CLAUDE.md` §3); a bare `Subscribe(null)` no longer compiles (F7), as in Java. Approve.
- **N5. Bundle row 148 (2) / (3)?** `NextOffsets()` and `DeliveryCount()` (C functions present). Default **no**: their
  own phase.
- **N6. Rule drafts R1–R3 (§5), word for word.**

## 4. Steps (one commit each; every commit is dotnet-only and goes in with `--no-verify` after the .NET gates, except S3)

Gates for every code step: `dotnet build -c Release` of the library (netstandard2.0, net8.0, net10.0), the unit, gRPC
server and analyzer projects, 0 warnings / 0 errors; `dotnet format --verify-no-changes`; unit tests on net8.0 and
net10.0 with the expected count, 0 failed, and no `Test Run Aborted` in the log. Counts are deltas from **B**, the
per-TFM count S0 measures at `3afba0a7` (this worktree has no native yet, so no count could be taken while planning).

- **S0 — setup and rules.** Build the native (`make -C rust build`; `--features ffi`), record B on both TFMs and the
  gRPC-server unit count G. Commit R1–R3 verbatim.
- **S1 — `SubscriptionPattern`.** The type, its `PublicNamespaceLayoutTests` row, `PublicSubscriptionPatternTests`
  (5 cases: getter and `ToString`; equal and unequal patterns with their hashes; ordinal case-sensitivity; a null
  pattern's `Pattern()`, `ToString`, `Equals` and `GetHashCode`; an empty pattern stored as given). Unit: **B+7** per
  TFM (5 + 2 layout rows). Baseline xtask: `0 removed, 0 added`. Prelink: 967.
- **S2 — the overloads.** Six P/Invokes; `NativeCallArguments`; the sync and async `NativeConsumer` paths; the 12
  members; docs on the two interfaces only (`<inheritdoc/>` on the classes), with the topics forms' `exception` docs
  per flavour plus `ArgumentNullException`, `InvalidGroupIdException` and the "mutually exclusive"
  `IllegalStateException`; 3 allowlist lines (D4); the 12 baseline lines removed by the xtask; F7's 8 casts. New
  `PublicConsumerPatternSubscribeTests`: Java's `testRe2JPatternSubscription` on both mocks (2), with the exact texts
  and "Subscription to topics, partitions and pattern are mutually exclusive"; `testSubscribeToRe2JPatternValidation`
  on both real consumers, the async-listener form included (2); `…ThrowsIfNoGroupId`, a null pattern included (2);
  `Pattern()` null on 4 flavours × 2 overloads (8); mock pattern + listener → `Rebalance` fires `OnPartitionsAssigned`,
  then a plain pattern subscribe releases it, sync, async and async-listener (3); after `Dispose` (4); a closed mock
  answers the pattern errors first (2, D8). Rows added to the existing lists in `PublicConsumerCheckOrderTests`
  (closed, concurrent, no `group.id`) and `PublicConsumerCallbackRoutingTests` (refused inside a callback). Unit:
  **B+7+23** per TFM (plus any `InlineData` rows those two files need; the Actor reports the exact figure). Baseline
  xtask: **`Wrote 942 line(s) … 12 removed, 0 added`**. Prelink: **973**. `NativeAbiConformanceTests` green.
- **S3 — gRPC.** Both servicers subscribe with the pattern; `ConsumerServiceTests.cs:42-58` (4 cases, the -31 answer)
  becomes a 4-case test that the pattern subscribe succeeds on a mock and `Subscription()` is empty; `rust/Makefile`
  drops the two pattern skips and its comment says three tests are skipped. Gates: `make -C dotnet
  test-grpc-server-dotnet` = **G** (unchanged); `make test-integration-dotnet-native` with both pattern scenarios run
  and passing on `__grpc_dotnet` and `__grpc_dotnet_async` (**+4** arms, 0 failed; quiet Docker first); the hook (N2).
- **S4 — comments and docs.** The comment-only pass over the phase's files (`dotnet/CLAUDE.md` §4 Comments rule 8),
  checked by comment-stripped token identity; the `dotnet/CLAUDE.md` text R2 did not cover, if any.
- **S5 — close-out.** The STATUS entry (evidence: commits, B and G, final counts, xtask line, arms, Critic passes);
  this `PLAN.md` and the archived `COMMENTS.DONE.121.md` committed in this folder (`dotnet/CLAUDE.md` §8.4).
  Final gate on macOS: `make test-dotnet-macos-docker`, `make test-integration-dotnet-native`, and the
  IntegrationTests on both TFMs with 0 skipped (the macOS `verify-dotnet` self-skips them).

## 5. Rule drafts (applied in S0 only if approved word for word)

- **R1** — `dotnet/.claude/rules/ffi-marshalling.md` §B5 **Precondition**, replace "a null or empty
  `SubscriptionPattern` ("Topic pattern to subscribe to cannot be null" / "… cannot be empty");" with: "a null
  `SubscriptionPattern` ("Topic pattern to subscribe to cannot be null"; on a mock Java's `MockConsumer` text, "Topic
  pattern cannot be null"), and on a mock an empty one ("Topic pattern cannot be empty"), checked on a mock before the
  closed check, as Java's `MockConsumer` checks the pattern before `ensureNotClosed()` (on a `KafkaConsumer` the core
  refuses an empty pattern after its closed and `group.id` checks, with Java's "Topic pattern to subscribe to cannot
  be empty");". In the `ArgumentNullException` list add "a `SubscriptionPattern` whose `Pattern()` is null".
- **R2** — `dotnet/CLAUDE.md` §3: the §1 sketch lines added to the `IAsyncConsumer` and `IConsumer` sketches and the
  `SubscriptionPattern` sketch after them; in the "Already wired" paragraph add "pattern subscribe
  (`Subscribe(SubscriptionPattern[, listener])`, M23/P2)"; line 587 "Still to come: pattern subscribe and headers for
  the deserializers" → "Still to come: headers for the deserializers"; the §4 `ConsumerRebalanceListener` row
  "Registered by the `Subscribe(topics, listener)` overloads" → "… and `Subscribe(pattern, listener)` overloads".
- **R3** — `ffi-marshalling.md` §B5 **Tests required**: "a null pattern or a null listener" → "a null pattern (each
  flavour's text), an empty pattern on a mock, or a null listener", and add "a pattern whose `Pattern()` is null →
  `ArgumentNullException`".

## 6. Close-out

- STATUS entry at the top of `dotnet/design/current/STATUS.md` (not here).
- Ledger proposals for Pranav (the Manager cannot write the ledger): rows 410+ for D1–D11 and N1–N6 as ruled; row 148
  note: "Item (1) done in M23/P2 (N=121, commits …); item (4) done in PR #325 (`ba8574ba`); (2) and (3) open."
- `rust/Makefile` comment, `dotnet/CLAUDE.md` (R2), `ffi-marshalling.md` (R1, R3) as above.

## 7. Briefs for the Actor and Critic (recorded rules)

- Follow `dotnet/CLAUDE.md`, `dotnet/.claude/rules/*`, root `definition-of-done.md`; work only in the worktree,
  `git -C` and `&&`; shell traps (clobbered `PATH`, `ugrep` → `/usr/bin/grep`, `cat` → `/bin/cat`, zsh globbing and
  `:r` modifiers, zero-match test filters and aborted runs exit 0, `--features ffi`).
- Comment rule (ledger 38): doc comments only on public API, on the declaring interface; implementations use
  `<inheritdoc/>`; one concise summary per type; no other comments except a recorded decision; no Java or Python line
  cites in code; no phase or review IDs.
- Critic lens: any public surface not in §1; divergence from the topics-`Subscribe` precedent without a written
  reason; dead P/Invokes; a check order or message that differs from §2 / §3; a test that asserts only a type where a
  message exists.
- Known flake in a touched file: `PublicConsumerCheckOrderTests` async-mock null-argument (M23/P1 record); a failure
  there is re-run once and reported, not hidden.

## 8. Before the Actor starts (Pranav)

1. The clone's `git config user.name` is the placeholder `YOUR_FULL_NAME` (every PR #325 commit carries it). Set it
   before S0; whether to amend PR #325's authors is Pranav's call. The Manager changes no git config.
2. N3 (the `kafka/` checkout).
3. Docker running for S3 and the hook.
