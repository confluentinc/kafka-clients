# COMMENTS.DONE.28 — M11/P2 ".NET producer async PERIPHERALS (Flush / Close / PartitionsFor)"

Resolved issues moved here from `COMMENTS.28.md` by the Actor (N=28).

---

## Review 1 (Critic N=28) — commits `b122a423` + `50b1ea98`

### [LOW · test-coverage] Precondition tests assert only the exception *type*, not `ParamName` / message content — DoD §3 gap vs. the consumer precedent — RESOLVED

**Resolution (fixup! of `50b1ea98`, test-only — no production change):** strengthened
the producer precondition tests in
`tests/Confluent.Kafka.UnitTests/PublicProducerPeripheralTests.cs` to pin the
contract-bearing `ParamName` / `Message`, matching the shipped consumer norm.
Verified each asserted value against the *actual* production guard before pinning it:

- **`CloseTimeout_NegativeTimeout_ThrowsArgumentOutOfRange`** — now captures the
  exception and asserts `ex.ParamName == "timeout"` **and**
  `Assert.Contains("Timeout must not be negative.", ex.Message, StringComparison.Ordinal)`.
  These are the exact `paramName`/`message` the production guard throws
  (`AsyncKafkaProducer.Close(TimeSpan)`, `AsyncKafkaProducer.cs:99` —
  `ArgumentOutOfRangeException(nameof(timeout), timeout, "Timeout must not be negative.")`).
  Mirrors the consumer's `Close_NegativeTimeout_ThrowsArgumentOutOfRange`
  (`PublicSyncConsumerPreconditionTests.cs:150-157`).
- **`PartitionsFor_NullTopic_ThrowsArgumentNull`** — now asserts
  `ex.ParamName == "topic"` (`NativeProducer.PartitionsForWithCallback`,
  `NativeProducer.cs:305` — `ArgumentNullException(nameof(topic))`). No custom message
  is set by the guard (default `ArgumentNullException` message), so **only `ParamName`
  is a contract to pin** — noted in the test comment. Mirrors the consumer's
  `PartitionsFor` `ParamName` assertion (`PublicSyncConsumerQueryTests.cs:411-412`).
- **Optional case added** — `PartitionsFor_NullTopic_ThrownBeforeDisposedCheck_EvenWhenDisposed`:
  a disposed producer + null topic surfaces `ArgumentNullException` (`ParamName == "topic"`),
  NOT `ObjectDisposedException`, because the null guard (`NativeProducer.cs:303-306`)
  precedes the disposed check in `SubmitOwnedHandleOperation` (`ThrowIfDisposed`,
  `NativeProducer.cs:402`). Mirrors the consumer's `*_ThrownBeforeNativeCall_EvenWhenClosed`
  precedent. Ordering verified in the production source before adding.

**Deviation from the Critic's suggested fix (post-dispose assertion strength):** the
Critic listed the `ObjectDisposedException` precondition tests as also asserting
`ParamName` / message. The instruction was to "mirror the consumer norm." The consumer
norm for post-dispose is **type-only** — a repo-wide grep found **zero** `ObjectName`
assertions across the whole test suite (the consumer's post-dispose tests,
`PublicSyncConsumerQueryTests.cs:459-513`, assert only
`Assert.Throws<ObjectDisposedException>(...)`). So the producer's post-dispose tests
were left type-only, which already matches the shipped norm's strength. Strengthening
them beyond the consumer norm would have introduced an assertion the sibling surface
does not make.

**Production behavior confirmed correct** — this was a test-strength gap only; no
`src/**` / `src/ffi/**` / header / Rust-core change.

**Verify:** `cargo build --features ffi` clean; `dotnet build` 0W/0E across
net462/net8.0/net10.0; `dotnet test -f net10.0` → **470 passed / 0 failed** (was 469;
+1 the new "even when disposed" case; net8/net462 CI-only — runtimes not installed
locally); `dotnet format --verify-no-changes` clean.
