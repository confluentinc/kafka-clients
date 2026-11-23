# Plan: KafkaFuture types become classes (M25/P3)

Status: **approved 2026-11-23** (Pranav, relayed by the coordinator: approved "with every one of your
recommendations"): decisions D1–D11 of §15 and the release rule, as recommended, recorded as ledger rows
626–637 (Agreed). N = 135. Follow-ups F1–F5 (§17) stay out of this phase.
Revised on 2026-11-23 for Pranav's release rule (§1.1): we are close to a release, so this phase makes the
struct-to-class change and nothing that can safely wait.
Branch `prashah_dev_dotnet_kafka_future_classes`, made on top of `prashah_dev_dotnet_additional_rules` at
`20c9c27db` (which holds M25/P2's squash `2066b2cec`), main checkout, no push.
Mode A: .NET only. No change to the Rust core, the C ABI, the C header, Python, the gRPC proto or `rust/xtask`.
Design: Option A, Way 3 of the design page `kafkafuture-class-design.html`, which Pranav and Emanuele agreed.
It is not re-opened here.

Path prefixes: `N/` = `dotnet/src/Confluent.Kafka/`, `NT/` = `dotnet/tests/Confluent.Kafka.UnitTests/`,
`D/` = `dotnet/`, `J/` = `kafka/clients/src/main/java/org/apache/kafka/`, `P/` = `python/confluent_kafka/`.
"NPE" = Java's `NullPointerException`; "IAE" = `IllegalArgumentException`.

**Checked with a probe.** Some claims below were checked with a small scratch program, compiled and run on
net10.0 (macOS arm64), outside the repository: the overload choice of D7, the equality of D8, the object
sizes of §6, and the lock cases of D6. Each such claim says "(probe)".

## 0. Words used in this plan

M25/P2's plan §0 explains struct and class. This plan adds:

- **Engine.** The object that holds one future's result, the threads waiting for it and its callbacks.
  Today it is the internal class `KafkaFutureImpl<T>` (848 lines, `N/Internal/KafkaFutureImpl.cs`).
- **Flavor.** Sync: `Get()` blocks and returns `T`. Async: `Get()` returns a `Task<T>` you `await`.
- **Abstract class.** A class you cannot create with `new`; only a class that derives from it can be created.
  Rust: like a trait. Python: like a base class whose `__init__` raises.
- **`private protected` constructor.** Only a class in this assembly that derives from the class can call it.
  Rust: a sealed trait (a trait only this crate can implement).
- **`abstract` / `override`.** An `abstract` member has no body; each derived class writes it (`override`).
  Calling it picks the object's own version at run time. Rust: a `dyn Trait` method. Python: every method.
- **Hiding with `new`.** A derived class can declare a member with the same name and parameters as one in its
  base class but a different return type. C# needs the word `new` for that. The base version still exists:
  code that holds the object as the base type calls the base version.
- **A struct copy.** In C#, `var s = _state;` copies a struct, every field, silently. Rust: a `Copy` type,
  where `let mut s = self.state; s.complete();` changes the copy and leaves `self.state` alone. C# has no
  borrow checker, so nothing stops it.
- **`lock (x)`.** C#'s `synchronized (x)`: only one thread at a time runs inside a `lock` on the same object.
  Any code that can reach the object can lock on it.

## 1. TL;DR

1. `KafkaFuture`, `KafkaFuture<T>`, `AsyncKafkaFuture` and `AsyncKafkaFuture<T>` change from `public readonly
   struct` to `public abstract class`, as Java's `public abstract class KafkaFuture<T>`.
2. `KafkaFuture<T>` derives from `KafkaFuture`, and `AsyncKafkaFuture<T>` from `AsyncKafkaFuture`, as .NET's
   `Task<TResult>` derives from `Task` (D2). `KafkaFuture plain = typed;` keeps compiling, now with no
   conversion method.
3. **The engine is the future.** Two internal sealed engines: `KafkaFutureImpl<T> : KafkaFuture<T>` (sync) and
   `AsyncKafkaFutureImpl<T> : AsyncKafkaFuture<T>` (async). Each future is still one object.
4. Today's 848 lines of logic move into an internal struct, `FutureState<T>` (D4), stored inside each engine
   (Way 3). §3.5 lists the rules that keep the struct from being copied.
5. Every place that makes a future picks its flavor (§4).
6. `default(KafkaFuture<T>)` is `null`. The "default future" and its `InvalidOperationException` go.
7. **Kept as they are, so they do not break users:** the equality members (`Equals`, `==`, `IEquatable<…>`,
   now plain identity, D8) and `AllOf<T>(params KafkaFuture<T>[])` (D7).
8. A `null` future given to `AllOf`, a Result's `All()` or `ListOffsetsResult.PartitionResult` gets Java's
   and Python's answer, never a `NullReferenceException` (§5, D9).
9. The future locks on itself, as today's engine does; a user's `lock (future)` can now reach that lock. This
   phase documents it, and a later internal change fixes it (D6, F1).
10. No new object per record: every allocation budget keeps its number (§6, probe).
11. The four `kind` lines in `D/analyzers/java-name-baseline.txt` go away (§8).
12. A breaking change for users, but only where the struct-to-class change forces it (§10).

### 1.1 The release rule: what is done now, and what waits

Pranav, 2026-11-23: we are close to a release. Make no ground-breaking change now. What is not right today but
can be corrected later is corrected later. Option A Way 3 stays.

Each item of the plan is in one of three groups:

- **(i) Needed now:** the struct-to-class change cannot be done without it.
- **(ii) Can be done later without breaking users:** an internal change, a new analyzer rule, a test, a doc.
  Deferred, and listed as follow-ups (§17).
- **(iii) Would break users if done later:** decided now, with the smallest change.

| Item | Group | In this phase? |
|---|---|---|
| S1: the engine-size test and the recorded budget bytes | (i): it proves Option A's one promise, no new object | yes |
| S2: `FutureState<T>`, `IKafkaFutureImpl<T>`, the renamed internal reads | (i): Way 3 needs the shared struct; the renames stop a silent wrong call (§3.6) | yes |
| S3: the interface replaces the internal base class | (i): an engine cannot have two base classes | yes |
| The do-not-copy analyzer rule CKRDA0017 (D5) | (ii) | **no: F2** |
| S4, S5: the sync, then the async pair become classes | (i) | yes |
| D2: `KafkaFuture<T> : KafkaFuture` | (iii): the smallest change that keeps `KafkaFuture plain = typed;` and a mixed `AllOf` compiling, with no object made | yes |
| D3: an internal constructor | (iii): it keeps today's surface (users cannot derive today); making it `protected` later is additive | yes |
| D4: internal names | (ii): internal; they only need to be chosen | yes (a choice) |
| D6: the future is its own lock | (ii): any fix is internal | documented now; **fix: F1** |
| D7: keep `AllOf<T>` | (iii): removing it later would break; keeping it is harmless (probe) | kept |
| D8: keep the equality members | (iii): removing them later would break; keeping them changes no answer (probe) | kept |
| D9, N2 and N4: the null checks in `AllOf` and `All()` | (i): without them a `null` is a `NullReferenceException` with no message | yes |
| D9, N5: `ListOffsetsResult.PartitionResult` of a null value | (iii): answering `null` now and throwing later would change behaviour; Java's IAE now is one line per flavor | yes |
| D9, N3, N6, N7: a null stored as given | nothing to write: a class stores it as given | no code; tests: F5 |
| D10: `public abstract` members | (ii): invisible to users while they cannot derive | yes (a choice) |
| D10: the plain forms a typed future inherits | forced by D2 | accepted |
| The engine tests on both engines | test-only; the main guard against a struct copy in the new engine | yes |
| S6: the docs that would be wrong after the change, STATUS, the ledger rows | (i) | yes |
| The two async-only fields in the sync engine | (ii) | **no: F3** |

## 2. Why, and what Java and Python do

The analyzer rule of `0091556a6` (CKRDA0013 `kind`) says "a Java class is a C# class, never a struct". The four
futures sit in its baseline until this phase. The design page gives the user-visible reasons:

- A struct cannot be `null`, so a "dead" future exists. `KafkaFuture<RecordMetadata> last;` compiles, and
  `last.Get()` throws `InvalidOperationException` at run time. With a class, `last` is `null` and the
  compiler's nullable warnings catch it.
- `KafkaFuture<T>?` means `Nullable<KafkaFuture<T>>` (with `.HasValue` / `.Value`), not "may be null".

| Question | Java | Python | .NET today | .NET after |
|---|---|---|---|---|
| Kind | `public abstract class KafkaFuture<T>`; the engine `KafkaFutureImpl<T>` (in `common.internals`) extends it | two classes, `KafkaFuture` and `AsyncKafkaFuture` | four `readonly struct`s over one internal engine | four abstract classes; two internal sealed engines that *are* the futures |
| A future of no value | `KafkaFuture<Void>` | `KafkaFuture[None]` | separate struct `KafkaFuture`, with an implicit conversion from `KafkaFuture<T>` | `KafkaFuture<T> : KafkaFuture` (D2) |
| Can a user make one? | yes in theory (public constructor), but `allOf` casts `toCompletionStage()` to `CompletableFuture`, "Safe since KafkaFuture's only subclass is KafkaFuture[Impl]" (`J/common/KafkaFuture.java`, `allOf`) | no: `KafkaFuture(...)` raises `TypeError("KafkaFuture cannot be constructed directly; …")` (`P/common/kafka_future.py`) | no (internal constructor) | no (D3) |
| Can a variable be null? | yes | yes (`None`) | no; a `default` value throws | yes |
| Equality | identity (no `equals` override) | identity (no `__eq__`) | identity, hand-written | identity, hand-written members kept (D8) |
| What the future locks on | nothing: `CompletableFuture` is lock-free | its own `threading.Lock()` (`_KafkaFutureImpl`), or the producer's shared lock (`_AsyncRecord`) | the internal engine, which users cannot reach | the engine, which is now the public future (D6) |
| Objects per future | the future, plus its `CompletableFuture` | two (the `KafkaFuture` and its `_FutureState`), except the asyncio producer's record, which is one (`_AsyncRecord`) | one (the engine; the struct is free) | one (the engine) |
| How the shared logic is reused | one class | `_FutureState`, "Java's `KafkaFutureImpl` as a mixin" over fields the class stores | one class | `FutureState<T>`, a struct stored in each engine (Way 3) |

Python's `_FutureState` is the same idea as Way 3: shared logic over fields stored inside the future object.
Python uses a mixin, which C# does not have (a class has one base class), so C# uses a struct field.

## 3. The new types

### 3.1 The sync pair

```csharp
// before (today: N/Common/KafkaFuture.cs, 353 lines)
public readonly struct KafkaFuture<T> : IEquatable<KafkaFuture<T>>
{
    private readonly KafkaFutureImpl<T>? _impl;                       // null in a default value
    public T Get() => Impl.GetWrapped();
    public static implicit operator KafkaFuture(KafkaFuture<T> future) => future.Erase();
    public bool Equals(KafkaFuture<T> other) => ReferenceEquals(_impl, other._impl);
    internal KafkaFutureImpl<T> Impl => _impl ?? throw new InvalidOperationException(KafkaFuture.DefaultValueMessage);
    // … ThenApply, WhenComplete, Cancel, Get(TimeSpan), GetNow, IsDone, …
}

public readonly struct KafkaFuture : IEquatable<KafkaFuture> { private readonly KafkaFutureImpl? _impl; … }
```

```csharp
// after
public abstract class KafkaFuture : IEquatable<KafkaFuture>   // Java's KafkaFuture<Void>
{
    private protected KafkaFuture() { }                       // D3: only the binding's engines derive

    public delegate TB BaseFunction<in TA, out TB>(TA a);      // unchanged
    public delegate void BiConsumer<in TA, in TB>(TA a, TB b); // unchanged

    public static KafkaFuture<TU> CompletedFuture<TU>(TU value);
    public static KafkaFuture AllOf(params KafkaFuture[] futures);
    public static KafkaFuture AllOf<T>(params KafkaFuture<T>[] futures);   // kept (D7)

    public abstract KafkaFuture<TR> ThenApply<TR>(Func<TR> function);
    public abstract KafkaFuture WhenComplete(Action<Exception?> action);
    public abstract bool Cancel(bool mayInterruptIfRunning);
    public void Get() => WaitForGet();                        // not virtual: see 3.3
    public void Get(TimeSpan timeout) => WaitForGet(timeout);
    public void Get(CancellationToken cancellationToken) => WaitForGet(cancellationToken);
    public abstract void GetNow();
    public abstract bool IsCancelled();
    public abstract bool IsCompletedExceptionally();
    public abstract bool IsDone();

    // kept (D8), now plain identity and null-safe
    public static bool operator ==(KafkaFuture? left, KafkaFuture? right) => ReferenceEquals(left, right);
    public static bool operator !=(KafkaFuture? left, KafkaFuture? right) => !ReferenceEquals(left, right);
    public bool Equals(KafkaFuture? other) => ReferenceEquals(this, other);
    public override bool Equals(object? obj) => ReferenceEquals(this, obj);
    public override int GetHashCode() => RuntimeHelpers.GetHashCode(this);

    private protected abstract void WaitForGet();             // the hook behind the plain Get (3.3)
    private protected abstract void WaitForGet(TimeSpan timeout);
    private protected abstract void WaitForGet(CancellationToken cancellationToken);
}

public abstract class KafkaFuture<T> : KafkaFuture, IEquatable<KafkaFuture<T>>   // Java's KafkaFuture<T> (D2)
{
    private protected KafkaFuture() { }

    public abstract KafkaFuture<TR> ThenApply<TR>(BaseFunction<T, TR> function);
    public abstract KafkaFuture<T> WhenComplete(BiConsumer<T?, Exception?> action);
    public new abstract T Get();                              // hides KafkaFuture.Get()
    public new abstract T Get(TimeSpan timeout);
    public new abstract T Get(CancellationToken cancellationToken);
    public abstract T GetNow(T valueIfAbsent);

    public static bool operator ==(KafkaFuture<T>? left, KafkaFuture<T>? right) => ReferenceEquals(left, right);
    public static bool operator !=(KafkaFuture<T>? left, KafkaFuture<T>? right) => !ReferenceEquals(left, right);
    public bool Equals(KafkaFuture<T>? other) => ReferenceEquals(this, other);

    private protected sealed override void WaitForGet() => Get();      // the plain Get waits as the typed one
    private protected sealed override void WaitForGet(TimeSpan timeout) => Get(timeout);
    private protected sealed override void WaitForGet(CancellationToken cancellationToken) => Get(cancellationToken);
}
```

- Every Java member keeps its name, its parameters and its behaviour (ledger 32's member list, 45, 365).
- `Cancel`, `IsCancelled()`, `IsCompletedExceptionally()` and `IsDone()` are declared once, on `KafkaFuture`,
  and the typed class inherits them: their signature and return type are the same in both.
- The equality members keep today's answers. Today's `==` calls `left.Equals(right)`, which would throw on a
  `null` left side once the types are classes, so the bodies become `ReferenceEquals` (null-safe). If an
  analyzer asks `KafkaFuture<T>` for its own `Equals(object)` override, it is one more `ReferenceEquals` line.
- The hook names (`WaitForGet`) are only a sketch. The Actor picks names that clash with no public member.
- A plain future the binding makes (`AllOf`, a Result's `All()`) is in fact a `KafkaFuture<object?>` engine whose
  value is `null`, as Java's is a `KafkaFuture<Void>`. Users see it as a `KafkaFuture`.

### 3.2 The async pair

The same shape, with the same kept members. Three members differ only in their return type between the two
classes, so they hide with `new`: `Get()`, `Get(TimeSpan)`, `Get(CancellationToken)` (`Task` / `Task<T>`) and
`ToCompletionStage()` (`Task` / `Task<T>`).

```csharp
public abstract class AsyncKafkaFuture : IEquatable<AsyncKafkaFuture>   // the async KafkaFuture<Void>
{
    private protected AsyncKafkaFuture() { }
    public static AsyncKafkaFuture<TU> CompletedFuture<TU>(TU value);
    public static AsyncKafkaFuture AllOf(params AsyncKafkaFuture[] futures);
    public static AsyncKafkaFuture AllOf<T>(params AsyncKafkaFuture<T>[] futures);   // kept (D7)
    public Task ToCompletionStage() => CompletionStage();
    public Task Get() => GetTask();                             // + the TimeSpan and token forms
    public abstract AsyncKafkaFuture<TR> ThenApply<TR>(Func<TR> function);
    public abstract AsyncKafkaFuture WhenComplete(Action<Exception?> action);
    // Cancel, GetNow(), IsCancelled(), IsCompletedExceptionally(), IsDone(), the equality members: as KafkaFuture
    private protected abstract Task CompletionStage();
    private protected abstract Task GetTask();                  // + the TimeSpan and token forms
}

public abstract class AsyncKafkaFuture<T> : AsyncKafkaFuture, IEquatable<AsyncKafkaFuture<T>>
{
    public new abstract Task<T> ToCompletionStage();
    public new abstract Task<T> Get();                          // + the TimeSpan and token forms
    public abstract AsyncKafkaFuture<TR> ThenApply<TR>(KafkaFuture.BaseFunction<T, TR> function);
    public abstract AsyncKafkaFuture<T> WhenComplete(KafkaFuture.BiConsumer<T?, Exception?> action);
    public abstract T GetNow(T valueIfAbsent);
    private protected sealed override Task CompletionStage() => ToCompletionStage();   // a Task<T> is a Task
    private protected sealed override Task GetTask() => Get();
}
```

Today the plain view's `ToCompletionStage()` and `Get()` hand out the same `Task` objects as the typed future's
(the struct erases to the same engine). The sealed hooks keep that: the plain view returns the typed one's task.

### 3.3 Why the plain `Get()` is not virtual

C# cannot have `void Get()` and `T Get()` in one class: a class may not hold two members that differ only in
return type. So the typed class *hides* the plain `Get()` with `new`. Hiding has one catch: code that holds a
typed future as a plain `KafkaFuture` calls the plain `Get()`, not the typed one.

```csharp
KafkaFuture<int> typed = KafkaFuture.CompletedFuture(5);
KafkaFuture plain = typed;          // the same object, no conversion
int five = typed.Get();             // the typed Get
plain.Get();                        // the plain Get: must wait for the same future
```

So the plain `Get()` is a normal method that calls a hook, and `KafkaFuture<T>` fills the hook by calling its
own typed `Get()`. Both calls wait for the same future, in the same way, with the same `ExecutionException`.

.NET does the same: `Task<TResult>` hides members of `Task` this way. C# 9 "covariant returns" would let
`Task<T> Get()` override `Task Get()` directly, but they need .NET 5 or later at run time, and this library
also targets netstandard2.0 and net462.

### 3.4 The two engines

```csharp
// N/Internal/KafkaFutureImpl.cs
internal sealed class KafkaFutureImpl<T> : KafkaFuture<T>, IKafkaFutureImpl<T>
{
    private FutureState<T> _state;                     // inside this object; never readonly, never copied (3.5)

    internal KafkaFutureImpl(bool mayCancel = true, IFutureValueSource<T>? source = null)
    {
        _state = new FutureState<T>(mayCancel, source, send: false);   // the one assignment
    }

    internal static KafkaFutureImpl<T> ForSend() => …;                // Java's FutureRecordMetadata, as today

    public override T Get() => _state.GetWrapped(this);
    public override bool IsDone() => _state.IsDone();

    public override KafkaFuture<TR> ThenApply<TR>(BaseFunction<T, TR> function)
    {
        if (function is null) throw new ArgumentNullException(nameof(function));
        var dependant = new KafkaFutureImpl<TR>();      // a dependant of the same flavor
        _state.ThenApply(this, dependant, function);    // the shared logic links the two
        return dependant;
    }

    public bool Complete(T value) => _state.Complete(this, value);   // for the binding (IKafkaFutureImpl<T>)
    public override string ToString() => _state.ToString(this);
    // … every other member is one line like these
}

// N/Internal/AsyncKafkaFutureImpl.cs
internal sealed class AsyncKafkaFutureImpl<T> : AsyncKafkaFuture<T>, IKafkaFutureImpl<T>
{
    private FutureState<T> _state;
    public override Task<T> Get() => _state.GetWrappedAsync(this);
    public override Task<T> ToCompletionStage() => _state.GetAsync(this);
    // … the same one-line members, making AsyncKafkaFutureImpl dependants
}
```

- Each engine is thin. All the threading logic stays in one place, `FutureState<T>`.
- Each engine makes dependants of its own flavor, so `ThenApply` and `WhenComplete` keep the flavor.
- The sync engine never calls the async waits. It still carries their two fields (F3).

### 3.5 The shared logic: `FutureState<T>` (Way 3)

Today's fields (`_state`, `_value`, `_failure`, `_listeners`, `_task`, `_source`, `_mayCancel`, `_send`,
`_propagated`, `_wrapped`) and their logic (`Complete`, `CompleteWithError`, the waits, `Cancel`, the listeners,
`ToString`) move into `N/Internal/FutureState.cs`:

```csharp
[StructLayout(LayoutKind.Auto)]                       // let the runtime pack the fields (§6)
internal struct FutureState<T>
{
    private volatile int _state;
    private T _value;
    // … the other eight fields, as today

    internal bool Complete(IKafkaFutureImpl<T> owner, T value)
    {
        lock (owner)                                  // the engine object, as today's lock (this)
        {
            // … as today
            Monitor.PulseAll(owner);
        }
        // … run the listeners, passing owner
    }
}
```

**The rules.** A copy of the struct splits a future in two: the copy completes, the field stays Pending, and
every `Get()` waits forever. So:

1. The struct is `internal`, in its own file, and `[StructLayout(LayoutKind.Auto)]`.
2. Each engine has exactly one field of it: `private FutureState<T> _state;`. It is **never `readonly`**: C#
   copies a `readonly` struct field before each call of a method that is not `readonly`, so every
   `Complete` would land on a throwaway copy.
3. The field is assigned once, in the engine's constructor. After that it is **never assigned, passed by value
   or returned**. Every call is `_state.Method(this, …)` on the field itself.

   ```csharp
   _state.Complete(this, value);           // right: works on the field

   FutureState<T> state = _state;          // WRONG: a copy (compiles, no warning)
   state.Complete(this, value);            //   completes the copy; _state stays Pending; Get() waits forever

   Helper(_state);                         // WRONG: passes a copy
   ```
4. Inside the struct, `this` is never used as an object. `lock (this)` does not compile (CS0185), which
   helps. But `Monitor.Wait(this)`, `Monitor.PulseAll(this)` and `token.Register(…, this)` do compile: each
   boxes a copy. The first two then throw `SynchronizationLockException` (nobody holds the copy's lock); the
   third pulses the copy, so a `Get(token)` whose token fires never wakes. The struct always uses the `owner`
   it is given.
5. No `async` method and no iterator (`yield`) in the struct: both copy `this` into their state machine.
   Static helpers that take what they need as arguments are fine (today's `CompletesWithin`, `AwaitUntil`).
   The compiler already refuses a lambda inside the struct that uses `this` (CS1673).
6. The struct is handed its engine (`owner`) on every call. It locks on it, waits on it, and passes it to the
   listeners (`ThenApply`'s and `WhenComplete`'s links, `AllOf`'s counter). Locking on the engine is today's
   design; D6 covers what changes now that users can reach it.
7. Users can never reach the struct: it is internal, and a future reference a user copies is the same object.

**How a mistake is caught in this phase.**
- Review: the two engine files and `FutureState.cs` are the only places that touch `_state`. The Critic reads
  every line that names it.
- Tests: a copy means a future that never completes, so a test fails loudly at its deadline (every future test
  waits with a timeout). Java's `KafkaFutureTest` runs on both flavors, and the engine tests run on both engines
  (§7).
- An analyzer rule for rules 2–5 (CKRDA0017) is a follow-up (D5, F2): it guards later edits, and it can come
  at any time without touching users.

### 3.6 What shared code holds: `IKafkaFutureImpl<T>`

Some code does not know the flavor: the delivery state of both producers, the admin client's native source,
`AllOf` over futures of different value types, the listeners. Today it holds the engine class, or the internal
non-generic base class `KafkaFutureImpl`. After the change the two engines have no common base class (each
uses its one base class for its public type), so shared code holds an internal interface both implement.

```csharp
internal interface IKafkaFutureImpl                 // a future of either flavor, its value type unknown
{
    Exception? Failure { get; }
    void AddListener(Action<IKafkaFutureImpl> listener);
}

internal interface IKafkaFutureImpl<T> : IKafkaFutureImpl
{
    bool Complete(T value);
    bool CompleteWithError(Exception error);
    bool Propagate(Exception error);
    // … the binding's own reads (the unwrapped Get / GetNow, IsSettled), under names of their own (S2)
}
```

- An interface holds no data, so it cannot be copied the wrong way. Calling through it allocates nothing.
- The statics `AllOf`, `GatherAll` and `CompletedFuture` of today's base class move to a static class
  `FutureState` beside the struct. Each takes the result future its caller made, so it serves both flavors.
- **The binding's own reads get names of their own.** Today, inside the engine, `Get()` and `GetNow()` rethrow
  the failure itself, while the public struct's `Get()` wraps it in `ExecutionException`. Once the engine is the
  public future, an internal call meant for the plain read could bind to the public, wrapping one and still
  compile. S2 renames the internal reads, so the compiler points at every call site.
- This is a .NET-only internal type (DoD §7). Java needs none: it has one engine class.

### 3.7 What goes away

- The four `readonly struct`s, with `_impl`, `Impl`, `Erase()` and `DefaultValueMessage`
  ("This KafkaFuture is a default value and is bound to no operation.").
- The two implicit conversions, `KafkaFuture<T>` → `KafkaFuture` and the async one (D2). C# does not allow a
  conversion to a base class (CS0553); `KafkaFuture plain = typed;` still compiles, by inheritance.
- The internal non-generic class `KafkaFutureImpl` (S3).

Kept: `AllOf<T>` (D7) and the equality members (D8).

## 4. Every place that makes a future

Found with `grep` over `N/` for `new KafkaFutureImpl`, `ForSend`, the `KafkaFutureImpl` statics, `new
KafkaFuture(`, `new AsyncKafkaFuture(`, `.Impl` and `.Erase()`. S5 also lets the compiler find them: once the
async Results take only `AsyncKafkaFuture<T>`, any site that passes a sync engine stops compiling.

| Site | Today | After | Flavor comes from |
|---|---|---|---|
| `KafkaFuture.CompletedFuture`, `AllOf` (`N/Common/KafkaFuture.cs`) | `KafkaFutureImpl.CompletedFuture` / `.AllOf`, wrapped in a struct | `new KafkaFutureImpl<TU>` / `<object?>`, returned as it is | the class |
| `AsyncKafkaFuture.CompletedFuture`, `AllOf` | the same engine, wrapped in the async struct | `AsyncKafkaFutureImpl` | the class |
| `ThenApply`, `WhenComplete` (both engines) | `new KafkaFutureImpl<TR>()` | the engine makes its own flavor (3.4) | the engine |
| `N/Internal/DeliveryState.cs:74` | `Future = KafkaFutureImpl<RecordMetadata>.ForSend();` in shared code | the constructor takes the future its caller made, held as `IKafkaFutureImpl<RecordMetadata>` (no new field) | the caller: `NativeProducer.cs:355` (sync) or `AsyncDeliveryState` (async) |
| `N/Internal/NativeProducer.cs:384` | `return new KafkaFuture<RecordMetadata>(state.Future);` | returns the sync engine it made | the sync send |
| `N/Internal/AsyncDeliveryState.cs:97`, `:160` | `new AsyncKafkaFuture<RecordMetadata>(Future)` | the async engine itself (a cast of the one field; no second field) | the async class |
| `N/Internal/FutureFailure.cs:41`, `:58`, `:62`, `:81`, `:250` | `Failed(error)` and `ForSend()`, shared | `Send` makes the sync engine, `SendAsync` and `EndAsyncCallback` the async one | the method |
| `N/Internal/NativeFutureSource.cs:46` | `new KafkaFutureImpl<T>(mayCancel: true, this)` | `Start` / `Create` take the flavor | the admin client |
| `N/Internal/NativeAdminClient.cs` and its 14 partial files (`NativeFutureSource<T>.Start` in `.Topics`, `.Partitions`, `.Scram`, `.Transactions`; `new KafkaFutureImpl` in `.GroupOffsets`, `.Scram`, `.Transactions`) | the one engine | the client keeps its `AdminFlavour` in a field (today it only names the drain thread, `NativeAdminClient.cs:69-75`), and every helper makes that flavor | `_flavour` |
| `N/Internal/CreateTopicsRefinements.cs:74` | `NativeFutureSource<T>.Start` + `ThenApply` | takes its Result's flavor | the Result |
| `N/Admin/ListTransactionsResult.cs:94` (`ByBroker`, shared by both Results) | `new KafkaFutureImpl<…>()` | takes the flavor | the Result |
| `N/Internal/SyncAdminFutures.cs`, `AsyncAdminFutures.cs` | `new KafkaFuture(KafkaFutureImpl.AllOf(…))` | the flavor's engine; the one cast from `IKafkaFutureImpl<T>` to the public type lives here | the helper |
| the 90 admin Results (45 sync, 45 `Async*`; all 90 name `KafkaFutureImpl`, 36 call its constructor or statics) | wrap each native engine (`new KafkaFuture<T>(impl)`) | keep the engine they get: it is the future | the Result |
| tests: 28 test files name `KafkaFutureImpl`: `NT/TestFutures.cs`, `NT/KafkaFutureTestBase.cs`, the `NT/Interop/` engine and admin tests, the future and admin `Public*` tests, two budget tests, and `D/tests/Confluent.Kafka.GrpcServer.UnitTests/Fixtures/ListGroupsOverride.cs` | `new KafkaFutureImpl<T>()` for both flavors | the flavor the test needs | the test |

The shared native admin code returns `IKafkaFutureImpl<T>`. Each Result turns it into its public type through
its flavor's helper. A wrong flavor there is an `InvalidCastException` in the first test that reads the
Result, not a silent bug.

## 5. Where a null future can now come in

Two public entry points take a future: `AllOf`, and the 14 public Result constructors (7 Java classes × 2
flavors: `AlterClientQuotasResult`, `AlterUserScramCredentialsResult`, `DeleteRecordsResult`,
`DescribeClassicGroupsResult`, `DescribeClientQuotasResult`, `DescribeConsumerGroupsResult`,
`ListOffsetsResult`). They follow M25/P2's five cases (`dotnet/CLAUDE.md` §3, **A null `TopicPartition` or
`Uuid`**). The "Now?" column applies the release rule (§1.1).

| # | Call | Java | Python | .NET today | .NET after | Now? |
|---|---|---|---|---|---|---|
| N1 | `AllOf(null)` (both flavors, both overloads) | NPE | `TypeError("futures must not be None")` | `ArgumentNullException("futures")` | unchanged | nothing to do |
| N2 | `AllOf(a, null)` | NPE (`kafkaFuture.toCompletionStage()`) | `TypeError("futures must not contain None")` | impossible; a `default` throws `InvalidOperationException` | `ArgumentNullException("futures", "futures must not contain null")` (case 3) | **yes**: else a `NullReferenceException` with no message |
| N3 | a Result constructor given a map with a null value | stored | stored | a `default` stored | stored (case 5) | no code needed |
| N4 | that Result's `All()` | NPE from `allOf` | `TypeError` from `all_of` | `InvalidOperationException` | N2's `ArgumentNullException`, at the call | **yes**: the same check, in the two admin helpers |
| N5 | `ListOffsetsResult.PartitionResult(tp)` (both flavors) when `tp` maps to null | IAE "List Offsets for partition \"…\" was not attempted", as for a missing key | `IllegalArgumentError`, the same text | returns the `default` | Java's IAE (case 2) | **yes**: returning `null` now and throwing later would change behaviour; one line per flavor |
| N6 | the getters that return the map (`Values()`, `LowWatermarks()`, `DescribedGroups()`) | return it, null included | same | return it | return it, null included | no code needed |
| N7 | `DescribeClientQuotasResult(null)` (both flavors), then `Entities()` | stored; returns null | `TypeError("entities must not be None")` at construction, because Python reads the Result's flavor from that one future (root `CLAUDE.md`, Class family) | a `default` | stored; returns null, as Java (Python's reason does not apply: .NET has one class per flavor) | no code needed |

Sources: `J/common/KafkaFuture.java` (`allOf`), `J/clients/admin/ListOffsetsResult.java` (`partitionResult`),
`J/clients/admin/DescribeConsumerGroupsResult.java` (`all`), `P/common/kafka_future.py` (`_checked_futures`),
`P/admin/list_offsets_result.py`, `P/admin/delete_records_result.py`, `P/admin/describe_client_quotas_result.py`.

No client-built Result holds a null future: the core always gives one. Tests that pin N3, N6 and N7 can come
later (F5).

## 6. Cost per record: no new object

Option A adds no object. Each future is one engine, as today. So **every producer allocation budget keeps its
number**, and the measured bytes must not grow:

| Test (`NT/`) | Constant | Today |
|---|---|---|
| `PublicProducerSendAllocationBudgetTests.cs:54-57` | `FastPathPerSendBudgetBytes` (async fast path) | 480 B budget, 472 B measured |
| same file | `PerSendBudgetBytes`, `AwaitedPathPerSendBudgetBytes`, `AwaitedPathPerSendUnoptimizedBudgetBytes` | 512, 544, 1232 B |
| `PublicSyncProducerSendAllocationBudgetTests.cs:88-91` | `AbsolutePerSendBudgetBytes` (sync) | 288 B budget; about 280 B measured (272 B at M21/P2, plus the 8 B field M22/P1 added to every engine; S1 measures it) |
| same file | `PerSendBudgetBytes` | 512 B |
| `PublicProducerDeliveryAllocationBudgetTests.cs:52-55` | `PerRecordBudgetBytes` (completion thread) | 152 B budget, 136 B measured |
| `PublicProducerDeliveryCallbackAllocationBudgetTests.cs` | `PlainPerSendBudgetBytes`, `RegistrationDeltaBudgetBytes`, `CallbackFastPathPerSendBudgetBytes` | 512, 64, 480 B |
| `PublicProducerHeadersAllocationTests.cs` | `PerSendHeadersBudgetBytes` | 16 B |

**Acceptance check:** no constant above changes, every test is green, and the measured bytes S1 records are the
same after S2, S4 and S5.

**The engine's own size** (probe, net10.0): today's engine, with its ten fields in today's order, is **72 B**.
The same fields moved into a struct stored in a class are also 72 B, whether the struct is `Sequential` (C#'s
default) or `Auto`. One more reference field makes it 80 B; an empty `object` is 24 B. S1 adds a test that
measures one new engine (`ForSend()` and a plain one) on net8.0 and net10.0 and freezes the number; it must come
back unchanged for `KafkaFutureImpl<T>` after S2 and for `AsyncKafkaFutureImpl<T>` after S5.

**Why `LayoutKind.Auto` anyway.** On net10.0 the runtime already packs a struct that holds references (probe).
`Auto` says so explicitly, at no cost, for every runtime the library supports (net462 included).

## 7. Tests

### 7.1 Tests that change

- **The default-future tests** in the four public future test files become tests of `null`:
  - `NT/PublicKafkaFutureTests.cs`: `Default_Get_ThrowsInvalidOperationException_WithItsMessage`,
    `Default_IsDone_ThrowsInvalidOperationException_WithTheSameMessage`,
    `Default_EveryMemberButEqualityAndToString_ThrowsInvalidOperationException`,
    `Equality_EveryDefaultFuture_EqualsEveryOther_TypedOrUntyped_Transitively_WithHashCodeZero`.
  - `NT/PublicAsyncKafkaFutureTests.cs`: the same three `Default_*` tests,
    `DefaultValueTask_AwaitedFuture_GetThrows`, and the same `Equality_EveryDefaultFuture_…`.
  - `NT/PublicNonGenericKafkaFutureTests.cs` and `NT/PublicNonGenericAsyncKafkaFutureTests.cs`: their two
    `Default_*` tests each.
  - In all, 43 uses of a default future across these four files (15, 12, 9 and 7), some of them inside other
    tests (`Erase()` and equality checks).
- **The equality tests keep their answers.** Copies of one future become one object, so every "equal" stays
  equal. A test that compared `default` values now compares `null`s (`null == null` is true).
- **The four shape tests** (`Shape_IsAPublicReadonlyStruct_WithJavasKafkaFutureMembers` and its three twins,
  which assert `type.IsValueType` and `IsReadOnlyAttribute`) assert instead: a public abstract class, not
  sealed, the typed one deriving from the plain one, no public or protected constructor (D3), the kept
  `IEquatable` and `==`, and Java's members.
- **`Erase()` and the conversion** (`PublicKafkaFutureTests.cs:334`, `PublicNonGenericAsyncKafkaFutureTests.cs:130-145`):
  a plain assignment, and `Assert.Same`.
- **`typeof(KafkaFuture<…>?)`** in `NT/PublicAdminP2aShapeParityTests.cs:72`, `:75` does not compile for a class
  (CS8639). It becomes `typeof(KafkaFuture<…>)`; the return's nullability is read with `NullabilityInfoContext`
  if the test checks it.
- **`.Value` on a nullable future**: `AllTopicNames()!.Value.Get()` and `AllTopicIds()!.Value.Get()` become
  `AllTopicNames()!.Get()`: 23 sites in 7 files (`D/soak/SoakClient.Tests/TopicSetupTests.cs` 2,
  `NT/Interop/AdminP14SyncCompositionTests.cs` 4, `NT/PublicAdminDeleteDescribeTopicsTests.cs` 3,
  `NT/PublicAdminNullOptionsTests.cs` 4, `NT/PublicAdminP14DifferentialTests.cs` 6,
  `NT/PublicAdminTfmSmokeTests.cs` 1, `NT/PublicAdminTopicResultConstructionTests.cs` 3).
- `D/tests/Confluent.Kafka.IntegrationTests/KafkaProducerBrokerTests.cs:108` declares
  `KafkaFuture<RecordMetadata>? first = null;`. It is now a nullable reference; the Actor checks its uses.
- **Java's `KafkaFutureTest`** (`NT/KafkaFutureTestBase.cs`: Java's 25 tests and 4 more, run by
  `KafkaFutureTests` and `AsyncKafkaFutureTests`): the base asks its derived class for a new future, so the
  async run uses the async engine.
- **The engine tests** `NT/Interop/KafkaFutureImplTests.cs` (47), `KafkaFutureWrappedWaitTests.cs` (12) and
  `NativeFutureSourceTests.cs` (5) run against both engines: an abstract test class with a sync and an async
  subclass, as `KafkaFutureTestBase` does. Cases that only make sense for one flavor (the async waits) stay
  on that one. Test-only, and the main guard against a struct copy in the new engine, so it is done now.
- **Builders of Results in tests** (`NT/TestFutures.cs`, the `NT/Interop/Admin*` tests,
  `ListGroupsOverride.cs`) make the flavor the Result takes.

### 7.2 Tests that are added

- The engine-size test (S1, §6).
- `default(KafkaFuture<int>)` and the three others are `null` (documents the change).
- A typed future and its plain view are the same object (`Assert.Same`).
- `AllOf` with a null element (N2), a Result's `All()` with a null value (N4) and
  `ListOffsetsResult.PartitionResult` of a null value (N5), each flavor, asserting the class, the message and
  `ParamName`.
- `ThenApply` and `WhenComplete` on each engine return the same flavor (the async one's `Get()` is a `Task`).

### 7.3 Java's tests (definition-of-done §3)

`KafkaFutureTest` (25 tests) is already translated for both flavors. Every one keeps its name and passes on both
engines. No other Java test covers these classes.

## 8. Analyzer lists (`D/analyzers/`)

The lists only shrink, except where a ruling adds an allowlist line. Each step runs
`cargo xtask dotnet-java-name-baseline` (from `rust/`) and lists every changed line in its commit message.

- **Baseline, removed:** the four `kind` lines (`kind T:Confluent.Kafka.Common.KafkaFuture`,
  ``…KafkaFuture`1``, `…AsyncKafkaFuture`, ``…AsyncKafkaFuture`1``): the two sync ones in S4, the two async
  ones in S5, each in the commit that changes its type (a stale line fails the build as CKRDA0014).
- **Baseline, expected to go stale too:** ``missing T:…KafkaFuture`1 …#allOf(KafkaFuture[])`` and
  ``…#completedFuture(U)`` (lines 207–208) and their async twins (201–202). The analyzer looks at a type's base
  classes, so the typed futures now inherit both statics. The tool decides.
- **Allowlist, removed:** the two `op_Implicit` lines (group "M1"), since the conversion goes.
- **Allowlist, kept:** the two ``AllOf``1`` lines (D7 keeps the overloads).
- **Allowlist, added only by a ruling:** with D3 = internal, a group for the constructors (CKRDA0013 rule 8
  expects a Java abstract class's constructors to be `protected`); with D10, any finding for the plain forms a
  typed future inherits. Each group gets its reason as a comment.
- **CKRDA0016** names the futures by type name (`AsyncCancellationTokenAnalyzer.IsFuture`). It keeps working: the
  public classes keep their names, and it skips the engines because they are internal.

## 9. The other projects

Each must build and pass its tests after S4 and S5.

| Project | Uses | Expected change |
|---|---|---|
| `D/grpc-server` | send and admin futures, through the public API; no `lock` on a future (checked) | none (checked by reading); `make -C dotnet test-grpc-server-dotnet` |
| `D/tests/Confluent.Kafka.GrpcServer.UnitTests` | `ListGroupsOverride.cs` builds engines | the flavor per Result (§4) |
| `D/soak` | `SoakClient.cs`; `SoakClient.Tests/TopicSetupTests.cs:82`, `:133` use `.Value` | the two `.Value` sites (§7.1); `make -C dotnet test-soak-dotnet` |
| `D/examples` (6 files) | `.All().Get()` and the like | none expected; `make -C dotnet test-examples-dotnet` |
| `D/tools/AotSmoke` | `Program.cs:244-252` calls `ThenApply`, `WhenComplete`, `AllOf` and `Equals` on a `KafkaFuture<int>` | none expected; it now also checks Native AOT with the generic virtual `ThenApply<TR>`; `make -C dotnet test-aot-dotnet` |
| `D/tools/VerifiableClients` | `RecordingProducer.cs` forwards the mock's futures | none expected; `make -C dotnet test-verifiable-clients-dotnet` |
| `D/tests/Performance` (`PerfV3/V3Backends.cs`, `V3TransactionalBackends.cs`) | send futures | none expected; `make -C dotnet perf-unit-test-dotnet` |

## 10. What changes for users

The client is in preview, so a break is allowed. Under the release rule (§1.1) only the breaks the
struct-to-class change forces remain. They go in the PR description.

```csharp
// 1. default is null.
KafkaFuture<RecordMetadata> last = default;     // before: a "dead" future. After: null.
last.Get();                                     // before: InvalidOperationException. After: NullReferenceException,
                                                //   and a compiler warning before you get there.

// 2. A nullable future is a nullable reference: .HasValue and .Value stop compiling.
KafkaFuture<IReadOnlyDictionary<string, TopicDescription>>? all = result.AllTopicNames();
all.Value.Get();                                // before
all!.Get();                                     // after (or: if (all is not null) { … })

// 3. typeof(KafkaFuture<X>?) stops compiling (CS8639): write typeof(KafkaFuture<X>).

// 4. Code compiled against the old binding must be recompiled (a struct and a class are different at run
//    time, and the conversion method op_Implicit is gone). The source still compiles:
KafkaFuture plain = typedFuture;                // the same object now, by inheritance

// 5. A null future gets Java's answer (§5) where it could not exist before.
KafkaFuture.AllOf(f1, null!);                   // ArgumentNullException "futures must not contain null"

// 6. lock (future) now compiles. Do not do it (D6).
```

Nothing else changes: `Get`, `GetNow`, `ThenApply`, `WhenComplete`, `AllOf` (both overloads), `Cancel`,
`IsDone`, `ToCompletionStage`, `ToString`, `==`, `Equals`, `IEquatable<…>` and `GetHashCode` keep their answers.
You still cannot derive from the futures (D3).

## 11. Steps for the Actor

One commit per step. Persona: `dotnet-actor` and `dotnet-critic` for every step (no Rust).
The Critic reviews after each commit.

**Step gate** (every step). From `dotnet/`, build all four TFMs:
`dotnet build -c Release -p:CargoProfileDir=debug Confluent.Kafka.sln` (the native library under
`rust/target/debug`). Then `make -C dotnet test-dotnet-macos` (every test project in the solution but the
broker-backed integration tests, on net8.0 and net10.0, the analyzer tests included, then the soak tests),
`dotnet format Confluent.Kafka.sln --verify-no-changes`, and `cargo xtask dotnet-java-name-baseline` (from
`rust/`) showing only the expected lines. .NET-only commits: build, format and tests first, then
`--no-verify` (the hook is for Rust and C changes).

**S1 — measurements first, on today's code.** Test only.
- Add the engine-size test (§6).
- Record, in the commit message, the bytes each producer budget test of §6 measures (from its test output).
- Gate: the step gate; the new test passes three runs in a row on net8.0 and net10.0.

**S2 — `FutureState<T>`.** A pure refactor: one engine, the public structs unchanged.
- Move the fields and the logic of `KafkaFutureImpl<T>` into `N/Internal/FutureState.cs`, following §3.5's
  rules. `KafkaFutureImpl<T>` keeps its members as one-line calls on `_state`.
- Add `IKafkaFutureImpl<T>` (only what the struct needs to talk to its engine).
- Rename the binding's own unwrapped reads so no internal name equals a public one (§3.6).
- Gate: the step gate; every test unchanged and green; S1's engine size and every budget unchanged. The Critic
  reviews the diff as a move: same lines, same order, `this` replaced by `owner`.

**S3 — `IKafkaFutureImpl` replaces the internal base class.** A pure refactor.
- Delete the internal non-generic class `KafkaFutureImpl`. Its statics move to the static class `FutureState`
  (§3.6); listeners take `IKafkaFutureImpl`.
- Every file that names the base class names the interface: the 90 Results, the helpers, the tests.
- Gate: the step gate; every test unchanged and green.

**S4 — the sync pair become abstract classes.**
- `KafkaFuture` and `KafkaFuture<T>` as §3.1; `KafkaFutureImpl<T> : KafkaFuture<T>, IKafkaFutureImpl<T>`.
- The sync creation sites of §4 return the engine itself; the sync Results keep the engines they get.
- Remove the conversion, `Erase`, `Impl` and `DefaultValueMessage` from the sync pair. Keep `AllOf<T>` and the
  equality members, rewritten as plain identity (D7, D8).
- The null checks N2, N4 and N5 for the sync pair (§5).
- The remark of D6 in the two classes' docs.
- The async structs keep wrapping `KafkaFutureImpl<T>` for one more step, through their private field, so
  nothing public changes for them.
- The tests of §7.1 and §7.2 for the sync pair. Delete the two sync `kind` lines; apply the allowlist rulings.
- Gate: the step gate, plus `make -C dotnet test-grpc-server-dotnet test-soak-dotnet test-examples-dotnet
  test-verifiable-clients-dotnet test-aot-dotnet perf-unit-test-dotnet`; §6's acceptance check.

**S5 — the async pair become abstract classes.**
- `AsyncKafkaFuture` and `AsyncKafkaFuture<T>` as §3.2; the new `N/Internal/AsyncKafkaFutureImpl.cs`.
- Every shared site of §4 takes the flavor: `DeliveryState`, `FutureFailure`, `NativeFutureSource`,
  `NativeAdminClient` (`_flavour`), `CreateTopicsRefinements`, `ListTransactionsResult.ByBroker`,
  `AsyncAdminFutures`, the 45 async Results.
- The null checks N2, N4 and N5 for the async pair; the remark of D6 in the two classes' docs.
- The engine tests run on both engines; `KafkaFutureTestBase` makes the async run use the async engine (§7.1).
- Delete the two async `kind` lines.
- Gate: as S4, and the engine size of §6 for `AsyncKafkaFutureImpl<T>`.

**S6 — docs and records.** §13's list, then the phase's comment-only commit (`dotnet/CLAUDE.md` §4 Comments,
rule 8).

**S7 — close-out.** The close gate (§12), then archive `COMMENTS.DONE.135.md` in this folder and reset
`COMMENTS.135.md`.

## 12. The close gate

- `make -C dotnet check-dotnet`.
- `make -C dotnet test-dotnet`: unit, broker-backed integration and soak tests, net8.0 and net10.0. The
  integration tests start containers: check Docker's free memory first.
- `make -C dotnet test-grpc-server-dotnet test-soak-dotnet test-examples-dotnet test-verifiable-clients-dotnet
  test-aot-dotnet perf-unit-test-dotnet`.
- The gRPC arms: `make -C dotnet test-integration-native` (the gRPC server as a native process), with
  `RUST_TEST_THREADS=6`, only after checking Docker's free memory: a run with many containers up once killed
  the user's `kafka-perf` container. `kafka-perf` (port 9092) must never be stopped or removed.
- The xtask tests: `cargo test` in `rust/xtask`, and `cargo xtask java-api ../dotnet/analyzers/java-api.txt
  --check` (this phase does not change `java-api.txt`).
- Known local failures, not caused by this phase: the four `PublicAdminNonNumericBrokerNameTests.RealClient_*`
  tests fail locally while `kafka-perf` holds port 9092, as in M25/P2.
- CI runs the Linux gate (`verify-except-grpc-arm`, the gRPC arms) on the PR.

## 13. Rules and records to update (S6)

Only the text that would be wrong after the change.

- `D/CLAUDE.md`:
  - §4 **Futures**, the paragraph "*(Decision 32 = yes, Agreed 2026-10-17.)* … are `readonly struct`s"
    (around lines 1199–1214): abstract classes; "converts implicitly to `KafkaFuture`" becomes "derives from
    `KafkaFuture`"; no default value; the lock remark of D6. It names the new ledger rows. The member list,
    `AllOf<T>` and every other bullet stay.
  - §3: the sketch comment "a readonly struct whose Get() is the delivery Task (M11/P3.6)" (line 288) and the
    `public readonly struct AsyncKafkaFuture<T>` sketch with "default(...) → InvalidOperationException"
    (lines 314–315); the idiom-map row for `Future<RecordMetadata>` ("a struct over `KafkaFutureImpl`", line
    816).
  - §4 sync-vs-async table, "a struct over the `Task<T>`" (line 1173).
  - §1's diagram (line 40) and §2's `Internal/` list (line 202): the record's engine, both engine names.
- `D/.claude/rules/ffi-marshalling.md`:
  - §A7: "`KafkaFuture<RecordMetadata>` (sync) and `AsyncKafkaFuture<RecordMetadata>` (async) are views over
    it" becomes "the record's future is its flavor's engine".
  - §C1: "only the public struct differs" becomes "each flavor has its own engine, sharing `FutureState<T>`";
    "built over `KafkaFutureImpl`" and "Compose over `KafkaFutureImpl` instead" name the engines.
  - §A1's diagram line "user callback → KafkaFutureImpl.Complete": the record's future.
- `D/design/current/STATUS.md`: the M25/P3 entry at the top, with the follow-ups of §17.
- The ledger rows of §15, which the PM adds after Pranav's rulings.

## 14. Open branches

Scanned read-only on 2026-11-23: the 150 most recent local and `origin/` refs, each diffed against its merge base
with HEAD, for changes to `KafkaFuture.cs`, `AsyncKafkaFuture.cs`, `KafkaFutureImpl.cs`, `DeliveryState.cs` or
`NativeFutureSource.cs`.

- **No branch newer than 2026-11-08 changes these files**, except `prashah_dev_dotnet_additional_rules-m25p2-presquash`,
  M25/P2's own backup, already in HEAD.
- Older ones that do (text conflicts on rebase): `origin/prashah_dev_dotnet_java_name_fixes` (2026-11-08,
  `DeliveryState.cs`), `origin/dev_dotnet-getters-as-methods` (2026-11-07, the three future files),
  `origin/dev_dotnet-error-categorization` (2026-11-05, `KafkaFutureImpl.cs`), `prashah_dev_dotnet_cffi_port`
  and `prashah_dev_dotnet_namespace_alignment` (2026-11-04 / 11-01; squash-merged into master as #259).

## 15. Decisions for Pranav

Each has a recommendation and its release-rule group (§1.1). Python is the tie-breaker where Java and .NET
differ.

1. **Option A, Way 3.** Already agreed by Pranav and Emanuele on the design page. *Recommended: record it* as a
   ledger row that supersedes the "readonly struct" part of row 32 (and `D/CLAUDE.md`'s decision-32 paragraph)
   and the M11/P3.6 / M11/P4.2 struct choice. Row 32's member list stays.
2. **`KafkaFuture<T> : KafkaFuture`** and `AsyncKafkaFuture<T> : AsyncKafkaFuture`, the typed `Get` (and the
   async `ToCompletionStage`) hiding the plain one with `new` (3.3). Java: one type, `KafkaFuture<Void>` for no
   value. Python: one class, `KafkaFuture[None]`, and `all_of` takes any `KafkaFuture[Any]`. .NET:
   `Task<TResult> : Task`. Group (iii): it is the smallest change that keeps `KafkaFuture plain = typed;` and a
   mixed `AllOf(a, s)` compiling without making an object. *Recommended: yes.* It supersedes the conversion of
   row 51 and of row 56 item 4: a typed future simply is a plain one.
3. **Constructor visibility.** Java declares a public constructor, but its own `allOf` assumes the only
   subclass is `KafkaFutureImpl`. Python's constructor raises `TypeError`, so no user future exists there either.
   - (a) internal, written `private protected`: users cannot derive (Rust: a sealed trait). It keeps today's
     surface. CKRDA0013 rule 8 expects `protected`, so it needs an allowlist group, with this reason.
   - (b) `protected`, Java's declared API: Java's protected `complete` / `completeExceptionally` become .NET API
     too; `AllOf`, `GatherAll`, the refinements and the 14 public Result constructors must then work through
     public members only (a `WhenComplete` per source, one more object each), never through the engine.

   Group (iii). *Recommended: (a).* Moving to (b) later is additive for users, since the members are already
   `public abstract` (D10); only the binding's internal casts would need rework then.
4. **Internal names.** The struct `FutureState<T>`, after Python's `_FutureState`, which plays the same role (the
   design page called it `FutureLogic<T>`). The interface `IKafkaFutureImpl<T>` / `IKafkaFutureImpl` ("Java's
   `KafkaFutureImpl`, of either flavor"). The engines `KafkaFutureImpl<T>` (as today) and
   `AsyncKafkaFutureImpl<T>`. Group (ii): internal, renamable later. *Recommended: these names.*
5. **A "do not copy" analyzer check** for §3.5's rules 2–5.
   - (a) Our own rule, CKRDA0017, in `D/analyzers/`.
   - (b) Roslyn's RS0042 on a `[NonCopyable]` struct: a new dev dependency (`Roslyn.Diagnostics.Analyzers`,
     made for Roslyn's own code), which also turns on Roslyn's other RS rules.
   - (c) Later.

   Group (ii): an internal check, which never touches users. *Recommended now: (c) later, as (a)* (follow-up F2).
   This phase's review and tests catch a copy made in this phase. **Changed:** the first draft recommended (a)
   now.
6. **The future becomes the lock users can reach.** Evaluated in full:

   **Today.** The engine locks on itself at ten sites in `N/Internal/KafkaFutureImpl.cs`: `lock (this)` at 295,
   418, 494, 647, 724 and 762; `Monitor.PulseAll(this)` at 308 and 661; `Monitor.Wait(this, wait)` at 789; and
   `cancellationToken.Register(s_pulseWaiters, this)` at 826, whose callback (224–230) locks and pulses the
   engine too. That is safe only because no user holds the engine: users hold a struct, and `lock` on a struct
   does not compile (CS0185). After the change the engine *is* the public future, so `lock (future)` compiles.

   **What a user's `lock (future)` can do** (probe: a toy engine that locks exactly as ours does):
   - `lock (f) { f.Get(); }` on one thread **works**. `Monitor.Wait` inside our `Get()` lets go of the lock
     completely (every nesting level) while it waits, so the completing thread can take it, and takes it back
     before it returns.
   - **A delay.** A thread that holds `lock (f1)` for 1 s makes f1's completion wait 1 s. One thread completes
     every future of a client, in order: the producer's completion thread, the admin client's drain thread.
     So every later completion of that client waits too (probe: f2 completed after 1001 ms). On the producer
     that also holds up every later delivery callback, and `Flush` and `Close`, which wait for them.
   - **A deadlock.** Thread A runs `lock (f1) { f2.Get(); }`. The completion thread must complete f1 before f2;
     it blocks on f1's lock, which A holds. A waits for f2, which nobody completes:

     ```csharp
     lock (f1)                // thread A takes f1's lock
     {
         f2.Get();            // A waits for f2, but lets go of f2's lock only, not f1's
     }
     // completion thread: f1.Complete(…) waits for f1's lock forever, so f2 is never completed
     ```

     Probe: `f2.Get(2 s)` returned "not done"; without the timeout it would wait forever.
   - `Monitor.Pulse(future)` from a user only wakes our waiters early; they check and wait again. Harmless.
   - **Our own code never locks on a future**: no `lock (…)` in `src`, the tests, the gRPC server, soak, the
     tools or the examples has a future as its target (checked). Today none could; this phase adds none.

   **Java and Python.** Java's `KafkaFutureImpl` and `KafkaCompletableFuture` have no `synchronized`, and
   `CompletableFuture` is lock-free; a send's `FutureRecordMetadata` waits on `ProduceRequestResult`'s latch and
   synchronizes only on its private `dependentResults` list. So a Java user's `synchronized (future)` blocks
   nothing. Python's `_KafkaFutureImpl` makes its own `threading.Lock()`, and the asyncio record uses the
   producer's `_fire_lock` "so no lock is made per record"; a Python user has no lock on the future object to
   take. Both never lock on the user-visible object.

   **.NET guidance.** The C# `lock` statement reference tells a type's author to lock on a dedicated private
   object and to avoid `lock (this)`, because callers may lock on it too. Option (a) knowingly goes against
   that guideline. (The binding already locks on one object users can reach: `RecordHeader` locks on itself for
   its lazy decode, `RecordHeader.cs:73`, `:93`; that lock is held only for the decode.)

   **The options**, cost per future and against the budgets (async fast path 472 B measured / 480 B budget;
   sync about 280 B / 288 B; completion thread 136 B / 152 B):

   | Option | Cost per future | Async send | Sync send | Completion thread | Risk |
   |---|---|---|---|---|---|
   | (a) keep locking on the future; document "do not lock on a future" | 0 B | 472 B | ~280 B | 136 B | the delay and the deadlock above, only for code that locks on a future |
   | (b) a private `object` lock per future | +32 B (a 24 B object and an 8 B field, probe) | 504 B: **over** | ~312 B: **over** | 136 B | two budgets break |
   | (c) a lock-free rewrite (`Interlocked`, as `Task` does) | 0 B on send; a sync `Get()` that blocks needs a wait handle, as `Task`'s does | ~472 B | ~280 B | 136 B | rewriting 848 lines of threading code just before a release |
   | (d) lock on an internal object the future already has: the `DeliveryState` / `AsyncDeliveryState` for a send; a `new object()` for an admin future, a dependant, `CompletedFuture` and `AllOf` | +8 B per send future (the engine 72 → 80 B, probe); +32 B for the others, which are not per record | 480 B: **at** the budget, no headroom | ~288 B: **at** the budget, no headroom | 136 B | nothing else locks on a `DeliveryState` (checked); but both send budgets end with no room, and a user's `ThenApply` on every send pays +32 B |
   | (e) a lock shared by all futures of a client, as Python's `_AsyncRecord` does | +8 B, as (d) | 480 B | ~288 B | 136 B | every completion wakes every thread waiting on any future of that client (a sync `Get()` waits on the lock), and all records of a client contend on one lock |

   **A fix later is internal.** Moving from (a) to (c), (d) or (e) changes no public member, and changes nothing
   for code that does not lock on a future; for code that does, it only removes the delay and the deadlock.
   So it is group (ii). F1 records it; it fits best together with F3, which frees 16 B on every sync future and
   so gives (d) room on the sync budget.

   *Recommended: (a) now, with this remark on the four classes:* "The binding synchronizes on the future object
   itself. Do not lock on a future: a `lock (future)` can delay its completion and every later one of the same
   client, and a `Get()` inside it can wait forever." *Then F1 later.* Unchanged from the first draft, now
   evaluated.
7. **`AllOf<T>(params KafkaFuture<T>[])`** and its async twin (row 51 added them). Keeping them next to the plain
   `AllOf` stays unambiguous once `KafkaFuture<T> : KafkaFuture` (probe, compiled with no warning): futures of
   one type pick the generic overload (`AllOf(a, b)`, `AllOf(array)`, `AllOf(a)`); mixed types (`AllOf(a, s)`,
   `AllOf(a, plain)`), `AllOf()` and `AllOf(null)` pick the plain one; `AllOf<int>()` still compiles. Both do
   the same thing. Java has one `allOf`, Python one `all_of`. Group (iii): removing them would break callers.
   *Recommended: keep them.* **Changed:** the first draft recommended dropping them.
8. **Equality.** Keep `IEquatable<…>`, `Equals`, `GetHashCode`, `==` and `!=`, rewritten as plain identity and
   null-safe (3.1). On a class they give the same answers as .NET's default identity, and as today (probe:
   `a == a` true, `a == b` false, a typed future equals its plain view both ways, `null == null` true, `a == null`
   false, a `HashSet` keeps one entry per future). Java and Python compare futures by identity too. Removing
   `IEquatable<…>` would break code that names it. Group (iii). *Recommended: keep them.* **Changed:** the first
   draft recommended removing them. Row 56 item 4's symmetric equality stays true, now because the two views are
   one object.
9. **Null futures**, §5: N2 and N4 (`ArgumentNullException("futures", "futures must not contain null")`) and N5
   (Java's IAE) now; N3, N6 and N7 store and return the null as given, which needs no code. Java throws NPE or
   IAE; Python raises `TypeError` or `IllegalArgumentError`. One row differs from Python: N7, a null `entities`
   future, which Python refuses only because it reads the Result's flavor from it. Group (i) for N2 and N4,
   (iii) for N5. *Recommended: yes.* **Narrowed:** the first draft had a step of its own (S7) for all seven
   rows; the checks now ride in S4 and S5, and the tests for N3, N6 and N7 wait (F5).
10. **Member shape.** Java's members are `public abstract` on the classes, and each engine overrides them; only
    the plain `Get`s and the async plain `ToCompletionStage` are normal methods over a hook (3.3). Whether a
    member is `abstract` or not is invisible to users while they cannot derive (D3), so it is group (ii).
    Nothing here is avoidable now: the hooks are forced by C# (3.3), and the inherited plain forms by D2. A
    typed future also shows the plain forms it inherits, `ThenApply(Func<TR>)`, `WhenComplete(Action<Exception?>)`
    and `GetNow()`, as `Task<TResult>` shows `Task`'s members; they do what they say (the future seen as one of
    no value), and today they are already reachable after the conversion. Python's typed future has no such
    forms; C# cannot remove an inherited member. *Recommended: accept*, with an allowlist group if CKRDA0013
    reports them.
11. **Base and PR.** This branch, on top of `prashah_dev_dotnet_additional_rules` at `20c9c27db` (PR #356's
    branch). *Recommended: a PR of its own, opened by you*, after #356, or rebased onto master once #356 is
    merged.

Ledger rows (Agreed 2026-11-23):

| # | ID in the evaluation doc | Decision | My pick | Status | Ruling and notes | Date |
|---|---|---|---|---|---|---|
| 626 | new (PM 135, M25/P3 PLAN D1; design page `kafkafuture-class-design.html`, agreed by Pranav and Emanuele) | The futures' kind: Option A, Way 3 | four public abstract classes; internal sealed engines `KafkaFutureImpl<T>` and `AsyncKafkaFutureImpl<T>` that are the futures; shared logic in the internal struct of row 629; supersedes the "readonly struct" part of row 32 and the M11/P3.6 / M11/P4.2 struct choice | Agreed | Pranav: as recommended | 2026-11-23 |
| 627 | new (PM 135, D2) | The typed and the plain future | `KafkaFuture<T> : KafkaFuture`, `AsyncKafkaFuture<T> : AsyncKafkaFuture`, hiding with `new`; supersedes the conversion of row 51 and of row 56 item 4 | Agreed | Pranav: as recommended | 2026-11-23 |
| 628 | new (PM 135, D3) | Constructor visibility | `private protected`; allowlisted against CKRDA0013 rule 8 | Agreed | Pranav: as recommended | 2026-11-23 |
| 629 | new (PM 135, D4) | Internal names | `FutureState<T>`, `IKafkaFutureImpl<T>` / `IKafkaFutureImpl`, `AsyncKafkaFutureImpl<T>` | Agreed | Pranav: as recommended | 2026-11-23 |
| 630 | new (PM 135, D5) | A "do not copy" check | later, as our own rule CKRDA0017 (F2) | Agreed | Pranav: as recommended | 2026-11-23 |
| 631 | new (PM 135, D6) | The future as its own lock | keep, with the remark; fix later (F1) | Agreed | Pranav: as recommended | 2026-11-23 |
| 632 | new (PM 135, D7) | `AllOf<T>` overloads | kept (row 51 stands) | Agreed | Pranav: as recommended | 2026-11-23 |
| 633 | new (PM 135, D8) | Equality | members kept, as plain identity, null-safe | Agreed | Pranav: as recommended | 2026-11-23 |
| 634 | new (PM 135, D9) | Null futures given to `AllOf` and the Results | N2, N4, N5 now; N3, N6, N7 stored as given | Agreed | Pranav: as recommended | 2026-11-23 |
| 635 | new (PM 135, D10) | Member shape | Java's `public abstract` members; hooks behind the plain `Get`s; inherited plain forms on typed futures accepted | Agreed | Pranav: as recommended | 2026-11-23 |
| 636 | new (PM 135, D11) | Base and PR | this branch on `20c9c27db`; a PR of its own | Agreed | Pranav: as recommended | 2026-11-23 |
| 637 | new (Pranav, 2026-11-23, relayed by the coordinator) | The release rule for M25/P3 | no ground-breaking change; what can be corrected later without breaking users waits (§1.1, §17) | Agreed | Pranav: as recommended | 2026-11-23 |

## 16. Risks

1. **Moving 848 lines of threading code.** Mitigation: S2 is a pure move with one engine and no public change,
   reviewed line by line; every future test, the KafkaFutureTest translation on both flavors and the budgets
   run before anything public changes.
2. **A silent struct copy** makes a future that never completes. Mitigation: §3.5's rules; every future test
   waits with a deadline and fails loudly; the engine tests run on both engines. The analyzer rule is F2.
3. **The engine grows.** Mitigation: the probe shows no growth; S1's size test and §6's budgets check it.
4. **A wrong flavor at a creation site.** Mitigation: in S5 the async Results accept only `AsyncKafkaFuture<T>`,
   so the compiler finds sync engines passed to them; the one cast per flavor fails at once in tests; both
   flavors are tested.
5. **An internal read binding to a public, wrapping member** (3.6). Mitigation: S2's renames; the Critic checks
   each internal read.
6. **A user locking on a future** (D6): a delayed completion, or a deadlock. Mitigation: the remark; F1.
7. **Analyzer findings not known in advance** (inherited members, constructors). Mitigation: the tool runs every
   step; a new allowlist line only with a ruling (D3, D10).
8. **Generic virtual methods and Native AOT.** `ThenApply<TR>` becomes an abstract generic method. Native AOT
   supports these; AotSmoke already calls it with an `int` (§9).
9. **Docker memory** at the close gate (§12).
10. **A breaking change** for users, limited to what the change forces (§10).

## 17. What cannot be checked before the work starts, and the follow-ups

**Cannot be checked yet.**
- **The measured bytes of the real engines after the change.** Why: the probe measured copies of today's fields,
  not the binding's code; S1 measures today's code, and the later gates measure what S2, S4 and S5 write.
- **The exact analyzer keys** that change (inherited statics, constructors, inherited plain forms). Why: they come
  from running CKRDA0013 on code that does not exist yet.

**Follow-ups** (group (ii): each can be done later without breaking users).
- **F1. A lock users cannot reach** (D6): option (d), (c) or (e), chosen then, with a budget review; best done
  with F3.
- **F2. CKRDA0017**, the do-not-copy analyzer rule for `FutureState<T>` (D5): it flags a `readonly` field of the
  struct, a local, parameter or return of its type, an assignment from the field outside the constructor, `this`
  used as an object inside the struct, and an `async` or iterator method on it, with unit tests in
  `Confluent.Kafka.Analyzers.UnitTests`.
- **F3. The sync engine without the async-only fields** `_task` and `_wrapped`: 16 B less per sync future.
- **F4. `protected` constructors** (Java's declared API, D3), if a need for user subclasses appears: additive.
- **F5. Tests pinning N3, N6 and N7** (§5), a null future stored and returned as given.

**Not in this phase and not a follow-up:** a sync `ToCompletionStage()` (row 45 stands); any change to the
futures' behaviour, timeouts or messages; Python. Removing `AllOf<T>` or the equality members would break users,
so it waits for a release that may break (D7, D8).
