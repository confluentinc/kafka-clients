# Critic 122: resolved

### 122.1 · Low · doc · `dotnet/CLAUDE.md` "After close"

**Resolution.**
- `FenceProducer` is dropped from the list of calls that deviate after close.
- The parenthetical now reads "Java's mock checks for close only in `send`, `flush`, `fenceProducer` and the transaction methods".
- `ShouldThrowOnFenceProducerIfProducerIsClosed` already pins Java's throw.

### 122.2 · Low · doc · `SetBeginTransactionException` on `MockProducer` / `AsyncMockProducer`

**Resolution.**
- Both summaries now say the injected exception is thrown "after the closed, fenced and initialized checks, before the open-transaction check", matching Java's `beginTransaction` and the core.
- The new theory `SetBeginTransactionException_IsThrownBeforeTheOpenTransactionCheck` (sync and async flavors) pins the order:
  - with a transaction open and an exception injected, `BeginTransaction` throws the injected instance twice and the transaction stays in flight;
  - once the exception is cleared, it throws `IllegalStateException("Transaction already started")`.

### Open-question follow-ups

- **Setter ownership.** `MockSetError` now has a comment: the core resolves the mock handle before it takes the error, so the error could leak only on a NULL-handle panic, which a held use rules out.
- **No change.** `Closed` stays unexposed and `GroupOffsetsMarshal` stays as is, per the Critic's view.
