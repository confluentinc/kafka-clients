# COMMENTS.20 — dotnet-critic review of M6/P1b "Typed consumers"

Branch `prashah_dev_public_consumer_serdes_poc`, base `99c72ec5` (P1a HEAD).
Commits reviewed: `296476f0` (generic-only conversion + zero-copy typed poll),
`175b91c3` (test-corpus migration), `ec617286` (typed-poll tests), `a7910ff0`
(doc-sync). Reviewed against the C ABI header (`target/include/confluent_kafka.h`)
and the Java `Consumer<K,V>` / `ConsumerRecord` / `MockConsumer` shape.

## Verdict: CLEAN — no blocking findings.

All priority axes verified sound; DoD gates re-run and pass. No real defects
(span-escape / UAF / destroy-before-read / exception-into-native / missing wrap /
wrong three-state / intermediate byte[] / shadow non-generic type / weakened
migrated assertion / undocumented deviation / shape violation) found.

### (a) Zero-copy typed poll span-safety + deserialize-before-destroy — SOUND
- `ConsumerRecordsMarshal.DeserializeSpan` (`ConsumerRecordsMarshal.cs:205`)
  constructs `new ReadOnlySpan<byte>((void*)ptr, length)`, hands it to
  `IDeserializer<T>.Deserialize`, and returns an owned `T`. The `ref struct` span
  is never stored/boxed/captured/returned; the `unsafe` is contained to
  `Internal/Interop/`. Split out of `DeserializeField` so no span lives across the
  `catch` (correct ref-struct discipline).
- Sync `PollTyped` (`NativeConsumer.cs:1097`): `CopyOut` fully materializes owned
  records, then `ConsumerRecordsDestroy` runs in the `finally` — copy-out strictly
  before destroy, on the caller thread. Null-safe destroy makes the failure path a
  no-op.
- Async `TypedPollCallbacks.OnPoll` (`TypedPollCallbacks.cs:68`): `CopyOut` runs on
  the dispatcher thread; the batch is destroyed in the `finally` after
  `CompleteWithResult`. Safe because `marshalled` holds only owned values (no
  borrowed pointer), and `RunContinuationsAsynchronously` keeps the awaiter's
  continuation off the dispatcher thread — identical to the shipped owned-handle
  bridges (`OnCommitted` etc.).

### (b) Async dispatcher-thread exception safety + mandatory SerializationException wrap — SOUND
- `DeserializeField` (`ConsumerRecordsMarshal.cs:159`) catches EVERY throw from the
  user deserializer and wraps it in `SerializationException` with inner + topic /
  partition / offset (PLAN §6, ffi §B6).
- `OnPoll`'s outer `try/catch(Exception)` → `TrySetException` is the no-throw
  backstop; the `finally` still destroys the batch (no leak) and frees the per-op
  `GCHandle` on every path. No managed exception can unwind into native. Sync path
  surfaces the same wrap as a synchronous throw.
- Empirically: the throwing-deserializer churn test (50 polls) plus 12× isolated
  loops of the typed-poll race-shaped suite showed no crash / SIGSEGV / double-free.

### (c) null → default(T) three-state — CORRECT (matches ABI)
- ABI (`confluent_kafka.h:715-739`): key/value are `(ptr,len)` or `(null,-1)` if
  absent. `DeserializeField`: `ptr==Zero || len<0` → `default(T)`, deserializer NOT
  invoked; `len==0, ptr!=Zero` → 0-length span (deserialized); `len>0` → the span.
  Header value uses the same absent sentinel via `CopyBytes`. Tests
  (`PublicConsumerTypedPollTests.cs`) prove all three, incl. a throwing/counting
  deserializer that is not called on absent, and `long?` distinguishing tombstone
  from `0L`.

### (d) No shadow non-generic types + migration construction-only — CONFIRMED
- Grep confirms only generic `<TKey,TValue>` forms of the six clients + records +
  two interfaces exist; the non-generic dead poll path (`SubmitOperation`,
  non-generic `ConsumerCallbacks.OnPoll`/`Poll`, non-generic `CopyOut`/`CopyRecord`)
  is removed, not shadowed. `IConsumerCommon` correctly stays non-generic (only
  `Poll` retypes on the two generic interfaces).
- Migration diff (`175b91c3`) is construction + type-refs only; no assertion
  altered. The one more-than-construction touch (Interop poll tests pass
  `Serdes.ByteArray` and drive `TypedPollCallbacks<byte[],byte[]>.Poll`) is forced
  by §5 and documented in the commit message.
- No intermediate per-record `byte[]` on the key/value path (only `CopyBytes` for
  header values); alloc-budget test asserts marginal per-record allocation stays
  <1 KiB between a 16-byte and a 64-KiB value.

### DoD gates (re-run locally)
- `cargo build --features ffi` — no `confluent_kafka.h` delta (Mode A confirmed).
- `dotnet build` — 0 warnings / 0 errors on ns2.0 + net8.0 + net10.0.
- `dotnet test -f net10.0` — 445 passed, 0 failed; race-shaped typed-poll subset
  12× clean in isolation.
- `dotnet format --verify-no-changes` — clean.
- MockConsumer / AsyncMockConsumer deviations (ctor-takes-deserializers, bytes-in
  `AddRecord`) documented in the type xmldoc (PLAN §7). Java-shape fidelity holds
  (3-param ctor per `KafkaConsumer.java:601`; `ConsumerRecord<K,V>` typed
  Key/Value + materialized Headers; `ConsumerRecords<K,V> : IReadOnlyCollection`).

### Non-blocking nit (not a finding; informational)
- `tests/.../Interop/ConsumerPollWakeupCancelTests.cs:88` — a `//` comment still
  names `SubmitOperation` (renamed/removed this phase; the poll submit is now
  `SubmitTypedPollOperation`). Prose only (not a `<see cref>`, build is green);
  optional to refresh.

**M6/P1b meets the Definition of Done.**
