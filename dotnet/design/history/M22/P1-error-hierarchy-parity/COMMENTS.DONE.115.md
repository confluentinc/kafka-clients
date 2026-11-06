# Critic 115: resolved findings

## 1. [Medium] An async `Get()` on a pending send future now allocates an extra async state machine for every record, and the allocation budgets were moved off `Get()`, so nothing guards it

**Files:**

- `src/Confluent.Kafka/Internal/KafkaFutureImpl.cs:482-509` (`JavaGetAsync`, the `_ => Wrapped(raw)` arm at `:502`) and `:622` (`Wrapped`);
- `tests/Confluent.Kafka.UnitTests/ProducerSendStages.cs:34` (`DeliveryStage`);
- `PublicProducerSendAllocationBudgetTests.cs:152` and `PublicProducerDeliveryCallbackAllocationBudgetTests.cs:240` (`.Delivery()` became `.DeliveryStage()`).

**Problem:** the documented async send is `RecordMetadata metadata = await future.Get();`
(`IAsyncProducer.cs:41`). Before Phase 6, `Get()` returned the future's own TCS task. Now, on a
pending future (always the case with a real broker), it returns `Wrapped(raw)`, which allocates:

- the raw TCS and its `Task`;
- an `async` state-machine box and its `Task`.

I measured this with a scratch console app against the built `Confluent.Kafka.dll` (net10.0):
`AsyncMockProducer(autoComplete: false)`, 64 pending sends, best of 8 runs.

| Call on the pending future | Bytes per call |
|---|---|
| `ToCompletionStage()` (what `Get()` returned before Phase 6) | 96 |
| `Get()` | 216 |

That is about 120 B more on every record. The fast-path budget is measured at 464 B and capped at
480 B (`PublicProducerSendAllocationBudgetTests.cs:50-53`). Its comment says the budget is set so
that "one more Task-sized allocation per send exceeds" it (`:108`). Read through `Get()`, the user's
path would now cost about 584 B per record and fail that budget.

Instead of the budget catching this, both budget tests now read the delivery through
`ToCompletionStage()`. STATUS.md says this was done "so they measure the send, not `Get()`'s
wrapper". So the per-record regression on the public path is no longer measured anywhere.

DoD #10 forbids avoidable per-message heap allocations on the send path. This one is avoidable:

- the wrapper does not have to be an `async` method awaiting the raw task;
- `_javaTask` can be a second TCS that `SetTask` / the completion path completes directly. It faults
  with `new ExecutionException(Failure)` (or passes the result or the cancellation through), with no
  continuation box.
- when `Get()` comes first, the raw TCS need not be created at all: it is only needed by
  `ToCompletionStage()` and the binding's internal waits. A `Get()`-only user then pays for one TCS,
  the 96 B of before.

**Fix:**

- Build the `Get()` task without an `async` continuation, and create it lazily so that `Get()` alone
  allocates no more than before.
- Keep one budget that reads through `Get()`, the documented path, beside the `ToCompletionStage()`
  ones. Alternatively, if the extra cost is accepted, record it under DoD #10 with the measured
  figure and raise the `Get()` budget explicitly. The test must not be moved to a different call.

**Resolution:** the `Get()` task is a second `TaskCompletionSource`, made on the first `Get()` taken
while the future is pending and set by `Complete` / `Fail` directly (`SetJavaTask`: a failure inside an
`ExecutionException`, a value or a cancellation as the raw task gets it). There is no `async`
continuation, and `Wrapped` is gone. Taken after the future is done, `Get()` is the raw task for a
value or a cancellation and a `Task.FromException` for a failure, cached by `Interlocked.CompareExchange`.
The raw completion source is no longer made by `Get()`, so a `Get()`-only caller makes one completion
source, as before the phase, and a `ToCompletionStage()`-only caller makes no second one. The cache is
one `object? _java` field (the source, or the finished task), so the future grows by 8 B only.

The three send budgets read through `Get()` again (`.Delivery()`; `ProducerSendStages`,
`PublicProducerSendAllocationBudgetTests` and `PublicProducerDeliveryCallbackAllocationBudgetTests`
restored to the base commit), at their original budgets (480 B fast path, 512 B plain). Measured per
send through `Get()`, net8.0 and net10.0, three repetitions each, all identical:

| | fast-path send | fast-path callback send | plain send |
|---|---|---|---|
| before this phase | 464 B | 464 B | 464 B |
| first Phase 6 version (`async Wrapped`) | 560 B | 569 B | 578 B |
| two cache fields, no `async` | 480 B | 480 B | 480 B |
| now (one cache field) | 472 B | 472 B | 472 B |

The 8 B left is the field that holds the `Get()` task's source; every budget holds.

## 2. [Low] A send's future failed with an `OperationCanceledException` throws it unwrapped. Java's `FutureRecordMetadata` wraps every failure, and the async outcome depends on timing

**Files:**

- `src/Confluent.Kafka/Internal/KafkaFutureImpl.cs:789` (`JavaAnswer`, the `is not OperationCanceledException` test), `:501` (`TaskStatus.Faulted => raw`) and `:622-631` (`Wrapped`'s `when` filter);
- `dotnet/CLAUDE.md:728`, and the STATUS.md Phase 6 entry ("and so does a stored failure that is an `OperationCanceledException` on a future that cannot be cancelled (a send's)");
- `tests/Confluent.Kafka.UnitTests/Interop/KafkaFutureJavaWaitTests.cs:116`, which pins this behaviour.

**Reference:** a stored failure that is an OCE can only be a stored failure on a
`mayCancel: false` future, and the only such future is `ForSend()`
(`KafkaFutureImpl.cs:321`). On every other future an OCE makes it Cancelled. A send's future is
Java's `org.apache.kafka.clients.producer.internals.FutureRecordMetadata`, and Java's
`MockProducer#send` builds the same class.

- Its `get()` and `get(long, TimeUnit)` end in `valueOrError()`, which does
  `if (this.result.error(batchIndex) != null) throw new ExecutionException(...)`, with no
  `CancellationException` special case.
- Its `isCancelled()` is always `false`.

The "rethrow a stored `CancellationException`" rule cited is
`org.apache.kafka.common.internals.KafkaFutureImpl#maybeThrowCancellationException`. It belongs to
the class that `FutureRecordMetadata` is not.

**Scenario:** I checked this on the built DLL.

1. Call `mock.ErrorNext(new OperationCanceledException("injected"))` on
   `MockProducer(autoComplete: false)`, after a `Send`.
2. `future.Get()` throws `System.OperationCanceledException`, not wrapped, while
   `future.IsCancelled` is `false`.

Java's `errorNext(new CancellationException())` then `get()` throws
`ExecutionException(CancellationException)`.

The .NET docs contradict the code:

- `MockProducer.ErrorNext` (`MockProducer.cs:132-135`) promises "`Get()` throws an
  `ExecutionException` around it";
- `AsyncMockProducer.ErrorNext` (`:162-165`) promises the same for its `Get()`.

On the async struct the result also depends on timing:

- `Get()` taken while the future is pending: `Wrapped` rethrows the OCE from an `async` method, so the
  `Task` ends **Canceled**;
- `Get()` taken after the failure: it returns the raw task, which is **Faulted**.

I observed both, `status=Canceled` and `status=Faulted`, for the same injected failure.

**Fix:**

- Make the OCE exemption apply only when the future is cancellable, which means removing it, since a
  cancellable future never stores an OCE as a failure: a stored OCE on a send's future goes inside
  the `ExecutionException` like any other failure.
- Fix the `KafkaFutureJavaWaitTests:116` test to assert the wrapper with the exact cause.
- Correct `dotnet/CLAUDE.md:728` and the STATUS.md sentence.

**Resolution:** `JavaAnswer` and the `Get()` task wrap every stored failure. An
`OperationCanceledException` given as an ordinary (cancellable) future's error cancels it, so it is
still thrown as it is, as Java's `KafkaFutureImpl` throws a stored `CancellationException`. On a send's
future, which cannot be cancelled, it is a failure like any other and goes inside the
`ExecutionException`, as `FutureRecordMetadata.valueOrError()` wraps every error. Since the `Get()` task
is now set by the completion, its outcome no longer depends on when `Get()` was taken: it is faulted with
the wrapper in both cases.

Tests: `KafkaFutureJavaWaitTests` replaces the old test with an ordinary-future case (cancelled, the
same instance thrown) and a send-future case (wrapped, with `Get()` taken before and after the failure,
both `Faulted`). It adds `ErrorNext(new OperationCanceledException("injected"))` on `MockProducer`
(`Get` and `GetNow` wrap, `IsCancelled` false, the callback gets the instance) and on
`AsyncMockProducer` (`Get()` taken before and after the failure, both faulted with the wrapper).
`PublicAsyncKafkaFutureTests`' "canceled" delivery cases now expect the wrapper. `dotnet/CLAUDE.md` §4
Futures and the STATUS.md entry say so. The `MockProducer` / `AsyncMockProducer` `ErrorNext` docs
already promised the wrapper and now hold.

## 3. [Low] `ERROR-CATEGORIZATION.md` still says futures rethrow the raw failure

**Files:** the repository root `ERROR-CATEGORIZATION.md` (unchanged by this diff):

- `:505`: `ExecutionException` is "Not thrown yet: futures rethrow the raw failure until the futures phase.";
- `:561-562`: "`KafkaFuture.Get()` rethrows the **original** exception, not an `ExecutionException`. This deviates from both Java and Python until the futures phase.";
- `:703-704`: open item 5, "Futures rethrow the raw failure".

**Problem:** Phase 6 is the futures phase that these lines wait for, and the code now throws
`ExecutionException`. Its .NET section (§6, §7, §10, as Phase 5 updated it) now contradicts the code
and `dotnet/CLAUDE.md` §4. The Phase 5 plan names this file as one of the error-categorization docs
to keep current.

**Fix:**

- Update the `ExecutionException` row (thrown by the public `Get` / `GetNow`).
- Replace the §6.3 bullet with the D13 behaviour, including `ToCompletionStage()` and the timeout
  texts.
- Close open item 5.

**Resolved (Manager):** the `ExecutionException` row and the §6.3 bullet now state the D13
behaviour, including `ToCompletionStage()` and both timeout texts; open item 5 is removed from §10
and the items after it are renumbered.

## 4. [Low] The two absolute-budget comments still give 464 B and a 16 B margin; Phase 6 measures 472 B

**Files:**

- `tests/Confluent.Kafka.UnitTests/PublicProducerSendAllocationBudgetTests.cs:50-52`
- `tests/Confluent.Kafka.UnitTests/PublicProducerDeliveryCallbackAllocationBudgetTests.cs:56-58`

**Problem:** both comments derive the 480 B budget as "the measured 464 B … plus 16 B, less than one
more object (24 B)".

- Phase 6 adds the `_java` field to every send's `KafkaFutureImpl`.
- The STATUS.md Phase 6 entry records the new figure as 472 B per send.
- I measured 472 B on net10.0, for `Send` + `Get()`.

So each comment now misstates the figure the budget is set against. It also claims a 16 B margin
where 8 B is left. The budgets still hold, and one more 24 B object still exceeds them. But the next
reader who re-derives the margin starts from a wrong number. This file states the figure precisely
so that drift is visible.

**Fix:** in both comments, replace "464 B" with "472 B" and list the `KafkaFutureImpl`'s Java `Get()`
field among the parts. Replace "plus 16 B" with "plus 8 B", or re-derive the budget. Keep both at
480 B if 8 B of headroom is intended.

**Resolved (Manager):** both comments now say the measured 472 B, name the `KafkaFutureImpl`
field that caches the `Get()` task, and give the margin as 8 B.
