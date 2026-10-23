# M9/P1 — "Complete the .NET `ConsumerRecord<TKey,TValue>` accessor surface"

Status: **APPROVED (maintainer 2026-08-12).** N=25. §4 resolved: **EXCLUDE `DeliveryCount`** (Python-parity). Scope = the three accessors.
Branch: new stacked branch **`prashah_dev_dotnet_consumer_record_accessors`** off `prashah_dev_dotnet_binding_consumer` (`afbd79c3`, PR #150 head). M9 is a new milestone; M8 (multilanguage harness) is closed.

---

## 0 · Identity

- **Binding:** `.NET` (`bindings/dotnet/`).
- **Milestone / Phase:** **M9 / P1** — first phase of a new milestone. Completes the `ConsumerRecord<TKey,TValue>` accessor surface to match Java 4.2 + the already-exposed C ABI.
- **Assigned N (monotonic):** **25**.
- **Branch (resolved at approval):** a **new stacked branch `prashah_dev_dotnet_consumer_record_accessors`** created off `prashah_dev_dotnet_binding_consumer` (`afbd79c3`, PR #150 head). Do NOT commit onto `prashah_dev_dotnet_binding_consumer` itself — keep PR #150 stable; M9/P1 is a stacked follow-up.
- **Mode:** **Mode A** — every accessor is already exposed at the C ABI (`target/include/confluent_kafka.h`); **no `confluent_kafka.h` / `src/ffi` / Rust-core change**. New code = `NativeMethods` decls + receive-path reads + record properties + one `Translate` line — all header-down (`bindings/dotnet/CLAUDE.md §6.2`).

## 1 · Objective & context

The .NET `ConsumerRecord<TKey,TValue>` today exposes only `Topic, Partition, Offset, Timestamp, TimestampType, Key, Value, Headers` (`bindings/dotnet/src/Confluent.Kafka/ConsumerRecord.cs:91-123`). It is **missing accessors Java's `ConsumerRecord` has and the C ABI already exposes** — `leaderEpoch()`, `serializedKeySize()`, `serializedValueSize()` (and `deliveryCount()`, see §4). This gap surfaced during **M8** as a documented divergence: the multilanguage gRPC backend's `Translate.RecordToProto` **omits** the consumer proto's `optional int32 leader_epoch` because the .NET record had no leader-epoch accessor (recorded in `design/history/M8/P1-sync-consumer-grpc-backend/PLAN.md §2.1` and carried into the M8/P2 STATUS entry). M9/P1 closes the gap: add the accessors and wire the harness to forward `leader_epoch` like the Python sibling.

## 2 · Verified findings (against current code — `file:line`)

1. **C ABI already exposes all four accessors** (`target/include/confluent_kafka.h`) — Mode A holds:
   - `int32_t kafka_consumer_ConsumerRecord_serialized_key_size(record)` — `:760` (doc: `-1` if key null).
   - `int32_t kafka_consumer_ConsumerRecord_serialized_value_size(record)` — `:770` (doc: `-1` if value null).
   - `bool kafka_consumer_ConsumerRecord_leader_epoch(record, int32_t *out_epoch)` — `:782` — **presence-style**: returns `true` and writes `*out_epoch` if present; `false` (leaving `*out_epoch` untouched) if absent.
   - `bool kafka_consumer_ConsumerRecord_delivery_count(record, int32_t *out_count)` — `:795` — presence-style; doc explicitly: "**KIP-932 share consumer**".
2. **Java `ConsumerRecord` (Kafka 4.2, `kafka/clients/src/main/java/org/apache/kafka/clients/consumer/ConsumerRecord.java`)** — all four are **public members**:
   - `public int serializedKeySize()` — `:229` (−1 if key null).
   - `public int serializedValueSize()` — `:237` (−1 if value null).
   - `public Optional<Integer> leaderEpoch()` — `:246` ("empty for legacy record formats").
   - `public Optional<Short> deliveryCount()` — `:256` ("the delivery count or empty when deliveries not counted … counted for records delivered by **share groups**"). ⚠ **Divergence from the pre-pass** — see §4.
3. **Current .NET record** — internal ctor `ConsumerRecord(topic, partition, offset, timestamp, timestampType, key, value, headers)` (`ConsumerRecord.cs:70-88`), poll-output-only, no public ctor; the 8 properties at `:91-123`. Missing the four §2.1/§2.2 accessors.
4. **Receive-path builder** — `Internal/Interop/ConsumerRecordsMarshal.cs`: `CopyRecord<TKey,TValue>(IntPtr record, …)` (`:122-151`) reads each scalar via `NativeMethods.ConsumerRecordX(record)` (partition `:127`, offset `:128`, timestamp `:129`, timestamp_type `:130`), marshals the length-delimited topic (`:136-137`), deserializes key/value in place (`:142-146`), copies headers (`:148`), then builds the record at `:150`. **This is the one place the three new scalar reads slot in.**
5. **`NativeMethods` current `ConsumerRecord` decls** (`Internal/Interop/NativeMethods.cs:358-441`) — partition/offset/timestamp/timestamp_type/topic/key/value/header_*; **missing** serialized_key_size, serialized_value_size, leader_epoch, delivery_count. **Presence-accessor precedent already exists**: `OffsetAndMetadata_leader_epoch` (`:1260`) and `OffsetAndTimestamp_leader_epoch` (`:1284`) are already declared as `bool` + `out int32` DllImports — the new `ConsumerRecord_leader_epoch` (and, if in scope, `_delivery_count`) mirror that exact shape.
6. **Harness forwarding** — the consumer proto `ConsumerRecord` carries `optional int32 leader_epoch = 9` but **no** serialized sizes and **no** delivery count (`multilanguage-test-server/proto/consumer_service.proto:159-169`). `grpc-server/Translate.cs` `RecordToProto` (`:259-290`) sets topic/partition/offset/timestamp/timestamp_type/key/value/headers but **not** `leader_epoch`. Both the sync `ConsumerServiceImpl` and async `AsyncConsumerServiceImpl` call the **same** `Translate.RecordToProto`, so **one** line wires both. Python's sibling forwards it (`bindings/python/grpc_translate.py:196`).
7. **Mock injection limit** — `kafka_consumer_MockConsumer_add_record(consumer, topic, partition, offset, key, key_len, value, value_len)` (`confluent_kafka.h:870-877`) takes **no `leader_epoch`** (and no delivery_count); the managed `NativeConsumer.AddRecord` (`Internal/NativeConsumer.cs:1906`) and `MockConsumer.AddRecord` / `AsyncMockConsumer.AddRecord` (`:263` / `:261`) mirror it. ⇒ the mock can only ever produce records with an **absent** leader epoch / delivery count (see §6 Testing).

## 3 · In scope / Out of scope

**In scope — exactly the three accessors:**
- `public int? LeaderEpoch` — Java `Optional<Integer>` → nullable `int?` (present → value, absent → `null`).
- `public int SerializedKeySize` — Java `int` (−1 if key null).
- `public int SerializedValueSize` — Java `int` (−1 if value null).
- Their `NativeMethods` `[DllImport]`s; the receive-path reads in `CopyRecord`; the extended internal `ConsumerRecord` ctor.
- `Translate.RecordToProto` forwards `leader_epoch` into the proto (harness parity with Python).

**Out of scope:**
- **`DeliveryCount` — EXCLUDED** (maintainer decision, §4).
- Any `confluent_kafka.h` / `src/ffi` / Rust-core change (Mode A). In particular, **no** extended `MockConsumer_add_record` ABI (that would be Mode B — see §6).
- A consumer-proto extension for serialized sizes (proto lacks them; they stay **binding-only**, unit-tested, not harness-forwarded).
- Any `ConsumerRecord` public constructor (it stays poll-output-only, internal ctor — `ConsumerRecord.cs:62-69`).
- Share-consumer classes / KIP-932 protocol (`consumer-threading.md §20`).

## 4 · `DeliveryCount` — EXCLUDED (maintainer decision, resolved)

**Resolved: EXCLUDE `DeliveryCount`** — the .NET record mirrors the **Python sibling**, which does not expose it.

The decision criterion the maintainer applied is **"include only if the Python sibling exposes it."** Python's `ConsumerRecord` C-extension getters (`bindings/python/_confluentkafka.c:1251-1262`) expose exactly `topic, partition, offset, timestamp, timestamp_type, key, value, **serialized_key_size**, **serialized_value_size**, **leader_epoch**, headers` — and **no `delivery_count`**. So .NET adds the same **three** (`serialized_key_size`, `serialized_value_size`, `leader_epoch`) and no delivery count.

For the record, both facts that were flagged in the draft remain true — `ConsumerRecord.deliveryCount()` IS public on Java 4.2 (`ConsumerRecord.java:256`, `Optional<Short>`) and the C ABI DOES expose `kafka_consumer_ConsumerRecord_delivery_count` (`confluent_kafka.h:795`) — so a future phase could add `short? DeliveryCount` non-breakingly if cross-binding parity ever moves. It is simply **not** in M9/P1: cross-binding consistency with Python wins here, and for the in-scope KIP-848 consumers the value is always empty anyway.

## 5 · Work items

- **(NativeMethods)** add three `[DllImport]`s (mirror the existing `ConsumerRecord_*` block `:358-441` and the presence-accessor shape at `:1260`):
  - `ConsumerRecordSerializedKeySize(IntPtr record) → int`, `ConsumerRecordSerializedValueSize(IntPtr record) → int` (plain `int32_t` returns).
  - `ConsumerRecordLeaderEpoch(IntPtr record, out int epoch) → [return: MarshalAs(UnmanagedType.I1)] bool` (presence-style — the `bool` return MUST be `[MarshalAs(I1)]`, matching the C `bool`, exactly as `OffsetAndMetadata_leader_epoch` at `:1260`).
- **(record)** extend the internal `ConsumerRecord` ctor + add three properties: `int? LeaderEpoch`, `int SerializedKeySize`, `int SerializedValueSize`. Rustdoc→XML-doc mirrors Java's javadoc; getters → properties (`bindings/dotnet/CLAUDE.md §3`).
- **(receive path)** in `CopyRecord` (`ConsumerRecordsMarshal.cs:122-151`), read the new scalars before the `:150` build: two plain reads for the sizes; the presence read for leader epoch — `int? leaderEpoch = NativeMethods.ConsumerRecordLeaderEpoch(record, out int le) ? le : (int?)null;`. Scalar reads only — the copy-out / zero-copy contract (§B4) is untouched (no new byte copies).
- **(harness)** `Translate.RecordToProto` (`grpc-server/Translate.cs:259-290`): after the scalar block, `if (record.LeaderEpoch is int le) { proto.LeaderEpoch = le; }` (proto `optional int32 leader_epoch`). One change serves the sync + async servicers. Serialized sizes / delivery count are **not** forwarded (proto lacks the fields).

## 6 · Testing

Unit tests (`MockConsumer` / `AsyncMockConsumer` round-trip, no broker — `bindings/dotnet/CLAUDE.md §7`):

- **`SerializedKeySize` / `SerializedValueSize`:** `AddRecord(topic, 0, 0, key: 3 bytes, value: 5 bytes)` → poll → assert `SerializedKeySize == 3`, `SerializedValueSize == 5`; `AddRecord(… key: null, value: null)` → assert `-1` / `-1`. ⚠ **Verify-at-implementation:** confirm the mock path actually computes sizes from `key_len`/`value_len`; if the core returns `-1` for mock-added records regardless, keep only the null→`-1` assertion via mock and note the positive-size case as integration-only (do NOT change core behavior — Mode A).
- **`LeaderEpoch` three-state:** the mock produces only the **absent** case (§2.7 — `MockConsumer_add_record` carries no epoch), so unit tests assert `LeaderEpoch == null` for mock-added records. The **present** case is **not mock-injectable in Mode A** (see the sub-decision below) — cover it, if at all, via the opt-in multilanguage harness (§8), not a unit test.
- **Allocation budget unaffected** — the new reads are scalar (no per-record heap allocation added); the existing receive-path allocation-budget test (`consumer-threading.md §27`, DoD §10) must still pass unchanged.

**Sub-decision — no `AddRecord` overload (Mode A).** Injecting a leader epoch to unit-test the *present* case would require an extended `MockConsumer_add_record` ABI parameter — a **Mode B** Rust-core change, **out of scope**. So **do NOT add a `MockConsumer.AddRecord` overload** this phase. Accept the present-case unit-test gap (mirrors how M8 documented the leader_epoch omission); a future Mode B phase could extend the mock ABI if a present-case unit test is later deemed necessary.

## 7 · Definition of Done (`.claude/rules/definition-of-done.md` + `bindings/CLAUDE.md`)

- `dotnet build` — 0 warnings / 0 errors across the TFM matrix (netstandard2.0 / net8.0 / net10.0) under `Directory.Build.props` analyzers.
- `dotnet test -f net10.0` — green; the new accessor unit tests included; the receive-path allocation-budget test still green.
- `dotnet format --verify-no-changes` — clean.
- **Mode A:** **no** `confluent_kafka.h` / `src/ffi/**` / Rust-core change — confirm `git diff --stat` shows changes only under `bindings/dotnet/src/**` (+ `grpc-server/Translate.cs`), never `target/include/confluent_kafka.h` or `src/ffi/**`.
- **Shape-only:** the three new getters mirror Java's `ConsumerRecord` public API (names, nullability: `Optional<Integer>`→`int?` for `LeaderEpoch`, `int` for the two sizes); no Kafka logic added (`bindings/CLAUDE.md §2.6`). Review ground truth = the C ABI header + Java `ConsumerRecord` (`§8.2`).
- **Optional / opt-in (CI-Docker):** re-verify the multilanguage `…__dotnet` / `…__dotnet_async` arms still build and the backend forwards `leader_epoch` (`Translate` change). Not gating — Docker/CI only.

## 8 · Validation steps

1. Confirm the base branch (§0) with the maintainer; branch M9/P1 off it.
2. `cargo build --features ffi --release` (native + header — unchanged; a Mode-A sanity build).
3. `dotnet build` across the TFM matrix; `dotnet test -f net10.0`; `dotnet format --verify-no-changes`.
4. `git diff --stat` — assert zero churn under `target/include/confluent_kafka.h` and `src/ffi/**` (Mode A).
5. *(opt-in)* `make build-grpc-images` + the `…__dotnet*` multilanguage arms to confirm `leader_epoch` forwarding — CI/Docker only.

## 9 · Phase checklist

- [ ] (branch) new `prashah_dev_dotnet_consumer_record_accessors` off `prashah_dev_dotnet_binding_consumer` (`afbd79c3`)
- [ ] (NativeMethods) three `[DllImport]`s: serialized_key_size, serialized_value_size, leader_epoch — mirror `:1260` presence shape, `[return: MarshalAs(UnmanagedType.I1)] bool` on the epoch accessor
- [ ] (record) extended internal ctor + `int? LeaderEpoch` / `int SerializedKeySize` / `int SerializedValueSize` with Java-mirrored XML docs
- [ ] (receive path) `CopyRecord` reads the new scalars + presence epoch before the `:150` build; zero-copy/copy-out contract untouched
- [ ] (harness) `Translate.RecordToProto` forwards `leader_epoch` (one line; serves sync + async servicers)
- [ ] (tests) serialized-size (present + null→−1, per the mock-behavior caveat) + `LeaderEpoch==null` (mock absent-case); allocation-budget test still green
- [ ] DoD §7 green; Mode A `git diff --stat` clean; `COMMENTS.DONE.25` records the no-AddRecord-overload sub-decision + any mock serialized-size behavior finding

## 10 · Comment workflow & handoff (Manager, N=25)

`dotnet-actor N=25` (Mode A, header-down; per-path staging — never `git add` the root `.claude/agents/dotnet-*.md` discovery copies or the `COMMENTS.*25.md` working files, `bindings/dotnet/CLAUDE.md §8.4`) → DoD §7 → `dotnet-critic N=25` (Java-`ConsumerRecord` shape fidelity, `Optional`→nullable mapping, `[MarshalAs(I1)]` on the presence accessors, no byte-copy added to the receive path, no production/ABI churn) → fix cycle until `COMMENTS.25.md` empty + DoD passes → archive `COMMENTS.DONE.25.md` under `design/history/M9/P1-consumer-record-accessors/`, update `design/current/STATUS.md` (M9/P1 DONE), reset `COMMENTS.25.md`. **APPROVED** — §4 (`DeliveryCount` EXCLUDED) and §0 (base branch) are resolved; the loop starts on the new stacked branch.
