# DRAFT for the maintainer to apply — RD7, RD8, RD11 → `ffi-marshalling.md`

**Status: NOT APPLIED. Handed to the maintainer 2026-09-29.**

`bindings/dotnet/.claude/rules/ffi-marshalling.md` is a rule file; agents do not edit it
(root `CLAUDE.md`). This is the Manager's draft of M17/P1's rule changes to it (PLAN D15,
§7.2, Q12). Apply, edit or decline each item; the phase close does not wait. **No agent
has modified the file.** Line numbers were measured (`command grep -n`, `sed -n`) on the
current 2383-line file before any item is applied: apply bottom-up (RD7c first, RD11 and
RD8 last) or re-locate each target by its quoted text.

⚠ **Coupled to RD2** in the `CLAUDE.md` draft: RD7b and RD8 remove `IsFatal` /
`is_fatal`, which is the same pre-existing drift RD2 corrects. The header has no such
symbol: `command grep -c 'is_fatal' target/include/confluent_kafka.h` prints `0` (header
SHA-256 prefix `e8d39f09f4ac`). Decide the three together.

## RD7a — §A2: a fourth handle category, the transient owned input

**Reason.** D12: `SendOffsetsToTransaction` builds a `kafka_consumer_ConsumerGroupMetadata_t`
from managed values, lends it to one call and destroys it in the same frame
(`NativeProducer.cs:1536` new, `:1516` destroy, in the CP4 worktree). None of §A2's three
categories covers a handle the binding *creates as input*.

**Insertion point 1.** `ffi-marshalling.md:448`. Current text:
```text
**Decision:** Three ownership categories:
```
Replace with `**Decision:** Four ownership categories:`, and after line 457 (the end of
item 3, `**borrow into it**, so it must outlive every borrow taken from it.`) insert:
```markdown
4. **Transient owned input** (the group metadata `SendOffsetsToTransaction` passes,
   M17/P1) → built by the binding from managed values, lent to exactly **one** call, and
   destroyed in the same frame, in a `finally`. Never a `SafeHandle`, never cached.
```

**Insertion point 2.** After `ffi-marshalling.md:467`, the last table row, which begins:
```text
| `kafka_consumer_PartitionInfoList_t` — **owned here** (a consumer type by name; the handle/accessors are shared with the consumer FFI) | 3 — owned result (borrow-root) |
```
Insert the row:
```markdown
| `kafka_consumer_ConsumerGroupMetadata_t` — **built here** (a consumer type by name) | 4 — transient owned input | the binding: `kafka_consumer_ConsumerGroupMetadata_new`, its three strings pinned for that call only (the core copies them) | the binding: `kafka_consumer_ConsumerGroupMetadata_destroy`, in a `finally` around the one `Producer_send_offsets_to_transaction[_async]` call it is lent to |
```

**Insertion point 3.** Before `ffi-marshalling.md:513`, whose current text is:
```text
  - **Parent outlives children:** the producer must not be destroyed while the
```
Insert the bullet:
```markdown
  - **Category 4 — transient owned input** (M17/P1 D12). Build it, pass it to the one
    call, destroy it in a `finally` around that call — for the `_async` submit too,
    because the header says its inputs are marshalled **on the calling thread, before it
    returns** (`send_offsets_to_transaction_async`), so nothing is held until the
    callback. On the async path nothing that can throw may run after the submit
    P/Invoke returns: `SubmitVoidOperation`'s `catch` assumes native never ran, and a
    throw there would run `AbandonBeforeSubmit` for an operation native already owns
    (releasing the span-the-op ref and freeing the `GCHandle` while the callback can still
    fire). `_new` documents no NULL return; guard one anyway, throwing before any submit,
    and say the guard is unreachable. Caching the handle in the managed value type is the
    anti-pattern: a plain value would then own a native resource and need `IDisposable`.
```

## RD7b — §A5: the predicate rows, and `IsFatal` removed from the producer part

**Reason.** D6 reads five hierarchy predicates beside `IsRetriable` in `FromBorrowedHandle`
(`KafkaException.cs:398` in the CP6 worktree). §A5 lists `_is_fatal` / `IsFatal`, which neither side has
(see the RD2 coupling above). The header exports 16 error predicates
(`command grep -c '^bool kafka_common_Error_is_' target/include/confluent_kafka.h` prints
`16`); the binding binds the six below (`NativeMethods.cs:110`, `:129`, `:139`, `:149`,
`:159`, `:169`, by `EntryPoint`). §B5's identical text (`:1773`, `:1782`, `:1795`) is the
consumer part: D15 files it forward (item 6), so it is **not** drafted here. The table's
`KafkaError_*` heading is the stale type name of file-forward item 16, also left as is.

**Insertion point 1.** `ffi-marshalling.md:745-746` (Decision). Current text:
```text
`kafka_common_KafkaError_t` handle → one flat `KafkaException` (code + retriable +
fatal + message), mirroring the Python sibling. **Precondition errors** (bad
```
Proposed:
```markdown
`kafka_common_KafkaError_t` handle → one flat `KafkaException` (code + message +
the retriable and hierarchy predicates), mirroring the Python sibling. **Precondition errors** (bad
```

**Insertion point 2.** `ffi-marshalling.md:754` (table row). Current text:
```text
| `_is_retriable` / `_is_fatal` | `IsRetriable` / `IsFatal` |
```
Proposed (six rows replacing one, then a note after the table's last row, `:755`):
```markdown
| `_is_retriable_error` | `IsRetriable` (Java `RetriableException`) |
| `_is_transaction_abortable_error` | `IsTransactionAbortableError` (M17/P1) |
| `_is_application_recoverable_error` | `IsApplicationRecoverableError` (M17/P1) |
| `_is_invalid_configuration_error` | `IsInvalidConfigurationError` (M17/P1) |
| `_is_authorization_error` | `IsAuthorizationError` (M17/P1) |
| `_is_out_of_order_sequence_error` | `IsOutOfOrderSequenceError` (M17/P1) |
```
```markdown

All six are `bool` (`I1`, §0.1) and are read eagerly in `FromBorrowedHandle`, before the
free, so the exception owns copied answers only. They encode Java's `extends` chain and are
**not** complements (root `CLAUDE.md` §10.4). There is **no** `_is_fatal` / `IsFatal`: the
core keeps fatality off the ABI (contextual — `is_fatal_error` is not public). A leaf class
needs no predicate; `ProducerFencedException` is `Code == 90`.
```

**Insertion point 3.** `ffi-marshalling.md:765-766` (Rule). Current text:
```text
    `FromHandle`. Keep it **one flat `KafkaException` for now** (the ABI exposes
    only code/retriable/fatal); typed subclasses can be added under it later,
```
Proposed:
```markdown
    `FromHandle`. Keep it **one flat `KafkaException` for now** (the ABI identifies
    an error by its code and answers Java's class questions through predicates);
    typed subclasses can be added under it later,
```

## RD7c — §A7: transaction control operations

**Reason.** D3 (drain before control), D4 (completion barrier), D5 (cancellation) and D12
(reuse of `SubmitVoidOperation`) are boundary mechanics with no §A7 rule. The
`CLAUDE.md` draft's RD6 states the user-visible contract and points here.
(Checked 2026-09-29 against the CP6 worktree: `DrainPendingAsync` at NativeProducer.cs:1445,
`EnqueueBarrier()` at :1448 and SendCompletionPump.cs:364, `SubmitVoidOperation` at
NativeProducer.cs:2146.)

**Insertion point.** Between `ffi-marshalling.md:1158` and `:1160`. Current text:
```text
defers the consumer's copy-out-vs-keep-alive).

**Anti-patterns:**
```
Insert after line 1158, followed by a blank line:
```markdown

**Rule (transaction control operations, M17/P1):**

  - **Over `SubmitVoidOperation`, unchanged.** The four `Task` control operations submit
    through it: the span-the-op `DangerousAddRef`, `AbandonBeforeSubmit` on a submit
    throw, the rooted `ProducerCallbacks.Operation`, the release in `FreeGcHandle` (§A2).
    No new delegate, completion source type or free site. The five sync forms pass the
    `SafeProducerHandle` (§A2). `begin_transaction_async` is **not** bound: Java's
    `beginTransaction` does not block, so no `Task` member backs it.
  - **Drain before control.** Every control entry point first drains the binding's send
    accumulator, so every `Send` that had returned reaches the core via `send_batch`
    before the control call does. The FFI's `with_txn_control` drains only its own
    `send_async` outbox (`producer-transactions.md` §13), which .NET never uses, so this
    is the binding's mirror of that rule for its own buffer. `Task` forms await the drain,
    bounded only by the token. Blocking forms use the bounded helper shared with `Flush`
    (30 s) and, on expiry, throw the public message-only `KafkaException` (`Code == 0`)
    without calling native. No accumulator (the sync types always) → no-op. Preconditions
    run in a non-`async` wrapper so they throw synchronously, and with no accumulator the
    submit is reached in the calling frame (the `FlushWithCallback` shape).
  - **Completion barrier for commit / abort.** After the drain and **before** the native
    submit, enqueue `SendCompletionPump.EnqueueBarrier()`; await it only after native
    **success**. FIFO plus the drain is what makes it sufficient: the accumulator enqueues
    a chain's group on the pump before it clears its draining flag, so every returned
    send's group is ahead of the barrier. A barrier is a `get_all` coalescing boundary,
    reads nothing, counts no send, and is completed with **success** by the stop path.
    Where `Enqueue` would fault a group in place (gate closed, pump stopped),
    `EnqueueBarrier` returns a completed task and promises no ordering. No pump → no
    barrier.
  - **Cancellation abandons the wait, not the operation** (the best-effort rule above,
    applied to control): at entry → `OperationCanceledException`, synchronously; during
    the drain → the `Task` is cancelled and nothing is submitted; after submit → the
    awaiter is cancelled and the operation runs to completion, so a retry meanwhile gets
    the core's Code -2; during the barrier wait → the `Task` completes **successfully**,
    because the commit or abort happened.
```
And append to this section's **Anti-patterns**, after `:1221` — the bullet that ends
"holding a managed lock across `get_all`.":
```markdown
  - A control operation that reaches native before the drain; a barrier awaited after a
    native failure; a barrier-wait cancellation that faults a `Task` whose commit
    succeeded; a managed lock or flag around control operations — the core's CAS rejects
    an overlap with Code -2, surfaced verbatim.
```
And to its **Tests required**, after `:1229` — the bullet that ends "thread pool.":
```markdown
  - Control operations: records still buffered at the call are included by commit,
    discarded by abort and not swept in by a later begin; the barrier completes only after
    every group ahead of it; each cancellation case, with "never submitted" asserted
    through the submit seam, not inferred from core state.
```

## RD8 — §0.1 Tests required: the bool-returning predicates

**Reason.** D15's RD8 row: add the five predicates to the `I1` list. The same list names
`is_fatal`, which the header does not export (see the RD2 coupling); the draft drops it.
If RD2 is declined, keep `is_fatal` and apply only the addition.

**Insertion point.** `ffi-marshalling.md:115-116`. Current text:
```text
  - Each `bool`-returning fn (`is_done`/`is_retriable`/`is_fatal`) is correct
    (guards a missing `I1`).
```
Proposed:
```markdown
  - Each `bool`-returning fn (`is_done`, `is_retriable_error`, and the hierarchy
    predicates `is_transaction_abortable_error` / `is_application_recoverable_error` /
    `is_invalid_configuration_error` / `is_authorization_error` /
    `is_out_of_order_sequence_error`) is correct (guards a missing `I1`).
```

## RD11 — §0.1 Tests required: the structural P/Invoke sweep pins names

**Reason.** Critic 84, CP1, finding 84.4: S1's shape sweep compared types and `out` marks
only, so a same-typed parameter reorder stayed green in 7 of the 18 declarations. P/Invoke
passes arguments by position. Added after approval.

**Insertion point.** After `ffi-marshalling.md:117`, the last §0.1 test bullet:
```text
  - `NativeMethods` loads on net462, net8.0, net10.0 (TFM smoke test).
```
Insert:
```markdown
  - A structural P/Invoke sweep pins each parameter's **name** as well as its type and
    its `out` / `[Out]` mark, and its mutation list includes a **swap of two same-typed
    names**. P/Invoke passes arguments by position, so where parameters share a type the
    names are the declaration's only statement of which header parameter sits where; a
    type-only sweep passes the swap.
```
