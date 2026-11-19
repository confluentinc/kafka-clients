# COMMENTS.DONE.25 — M9/P1 "Complete the .NET `ConsumerRecord<TKey,TValue>` accessor surface"

Local working record (NEVER committed at the binding root — `bindings/dotnet/CLAUDE.md §8.4`).
Captures decisions/deviations made during execution; the Manager archives a copy under
`design/history/M9/P1-consumer-record-accessors/`.

## Implementation (first pass — no Critic comments yet)

Mode A held: no `target/include/confluent_kafka.h`, `src/ffi/**`, or Rust-core change.
`git diff --stat` vs `prashah_dev_dotnet_binding_consumer` = 5 files only
(NativeMethods.cs, ConsumerRecord.cs, ConsumerRecordsMarshal.cs, Translate.cs,
+ the new PublicConsumerRecordAccessorTests.cs). Commits `988ab38d`, `1610d0a3`.

- NativeMethods: `ConsumerRecordSerializedKeySize` / `ConsumerRecordSerializedValueSize`
  (plain `int32`) + `ConsumerRecordLeaderEpoch` (presence-style, `[return: MarshalAs(I1)]`
  `bool` + `out int`, mirroring `OffsetAndMetadata_leader_epoch`).
- ConsumerRecord: extended internal (poll-output-only) ctor + `int? LeaderEpoch`,
  `int SerializedKeySize`, `int SerializedValueSize`; Java-mirrored XML docs.
- ConsumerRecordsMarshal.CopyRecord: three scalar reads before the record build; the
  leader epoch via the presence read (`... ? le : (int?)null`). No byte copy added — the
  §B4 copy-out/zero-copy contract is untouched.
- Translate.RecordToProto: `if (record.LeaderEpoch is int le) { proto.LeaderEpoch = le; }`
  — one line serving both the sync + async servicers. Serialized sizes not forwarded
  (consumer proto has no fields for them).

## Sub-decision — no `MockConsumer.AddRecord` overload (Mode A, PLAN §6)

The present-case leader epoch and positive serialized sizes are NOT mock-injectable without
extending the `MockConsumer_add_record` ABI — a **Mode B** Rust-core change, out of scope.
So NO `AddRecord` overload was added. The present-epoch / positive-size cases are accepted as
integration-only (mirrors how M8 documented the leader_epoch omission). A future Mode B phase
could extend the mock ABI if a present-case unit test is later deemed necessary.

## Verified finding — mock serialized-size / leader-epoch behavior (PLAN §6 verify-at-implementation)

The mock computes NEITHER size and carries NO leader epoch:
`kafka_consumer_MockConsumer_add_record` → `ConsumerRecord::new` (Rust core,
`src/consumer/consumer_record.rs`) hard-codes **both** `serialized_key_size` and
`serialized_value_size` to `NULL_SIZE` (`-1`) **regardless** of key/value length, and sets
`leader_epoch = None`. So every mock-added record reports:
`SerializedKeySize == -1`, `SerializedValueSize == -1`, `LeaderEpoch == null` — even with a
present, non-empty key/value.

Consequence for the unit tests (as PLAN §6 directs when "the core returns −1 regardless"):
- The null→`-1` size assertion is a genuine reachable contract test (Java + mock agree).
- The present-key/value case is asserted `-1` too, as a **characterization** of the mock
  limit (clearly commented), pinning the accessor plumbing; the positive-size path is
  integration-only.
- `LeaderEpoch == null` (absent) is the only mock-reachable epoch state; the present case is
  integration-only.

## DoD

- `cargo build --features ffi` clean (native + header, unchanged — Mode A sanity).
- `dotnet build` 0W/0E across netstandard2.0 / net8.0 / net10.0 (library) and net462 /
  net8.0 / net10.0 (tests); grpc-server (net8.0) 0W/0E.
- `dotnet test -f net10.0`: 421 passed / 0 failed (4 new accessor tests included; the
  receive-path allocation-budget test still green — the new reads are scalar value types).
- `dotnet format --verify-no-changes` clean (library / tests / grpc-server).
- Multilanguage `…__dotnet*` re-verify: opt-in / CI-Docker, skipped locally.
