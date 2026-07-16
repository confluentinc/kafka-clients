# Python binding conventions

Binding-specific rules for the Python client (`bindings/python/`). Supplements
`CLAUDE.md` — notably §3 (C FFI conventions) and §10 (error handling). Where a
rule here conflicts with the general guidance, the binding-specific rule wins
inside `bindings/python/`.

Scope note: this file is *organized* under `bindings/python/`, but it is loaded
because `CLAUDE.md` links it — so it is always on, not directory-scoped. Keep it
to enforceable invariants that are true of the binding *as built*; no aspirational
or persona content. Each rule states the rule, why it exists, how to apply it, and
the anti-patterns a reviewer should flag. Section refs: `§3` / `§9.5` / `§10` are
`CLAUDE.md` rules; `§27` / `§31` are `.claude/rules/consumer-threading.md`.

## 1. Callbacks & completion delivery

**1.1 — Errors and observability are not callbacks; don't invent background
listeners.**
Errors ride back on the failing call as `KafkaError` (raise / try-except). Stats &
throttling are pull-only via `metrics()` (deferred). Logs go to standard logging.
The ONLY push-callback families are OAuth token-refresh and the rebalance/commit
listeners — and they are the only callbacks that ever run *user* code.
- Why: mirrors Java; a librdkafka-style background `error_cb` / `stats_cb`
  diverges from the Java contract the whole client tracks.
- Anti-patterns: a background error stream; pushing stats via a callback.

**1.2 — Every completion callback resolves its waiter exactly once and frees its
handles on every path — sync `Event` and async `Future` alike.**
- Why: an unresolved waiter hangs `poll()` / `commit()` forever; a dropped payload
  leaks the C handles. Java guarantees the call always returns or throws (§9.5).
- How: on success AND on exception, the `cb` must (a) `free(payload)` and
  (b) resolve the waiter (deliver the result, or fail it). The invariant reads the
  same for both paths; the sync `cb` meets it trivially, the async `cb` needs 1.3.
- Anti-patterns: a `cb` that can raise before resolving; freeing handles but not
  failing the waiter (or vice versa); treating the sync path as exempt.

**1.3 — At the dispatcher→event-loop boundary, wrap delivery in
`try/except BaseException`.**
- Why: `call_soon_threadsafe` can raise (the loop may close between the
  `is_closed()` check and the call). A miss here = hung Future + leaked handle,
  worse than the usual "never catch `BaseException`" concern, so it must be total.
- How: on failure, `free(payload)` then schedule `_fail(fut, err)`; if that also
  fails, the loop is gone → the awaiter is gone → safe to drop.
- Anti-patterns: catching only `Exception`; `PyErr_Print()`-and-continue as the
  sole handling on a delivery path.

**1.4 — Bridged user callbacks (rebalance, commit, OAuth) run guarded, and their
exceptions surface at the driving call — never stderr. (Deferred; this rule gates
it.)**
- Why: user code raises far more readily than the internal shim; §31 requires the
  rebalance state machine not to advance on a swallowed error, and the user must
  see the failure.
- How: run the user callback inside the guarded path (1.2/1.3); on exception,
  resolve the driving op with that error so it is raised at `poll()` / `commit()`
  (Java behavior) and release the guard. OAuth also needs background token-refresh
  scheduling when added.
- Anti-patterns: fire-and-forget of a user callback; swallowing its error to
  stderr; advancing rebalance state before the callback result is known (§31).

## 2. Error model

**2.1 — Errors surface as `KafkaError` at the failing call, carrying the *core's*
error code + `is_retriable`/`is_fatal`. Identity comes from the Rust core, not the
binding.**
- Why: the core already models errors on Java (`CLAUDE.md` §10.3 — codes that
  correspond to Java exceptions); the binding must *preserve* that identity, not
  invent or flatten it. Errors are raised, never pushed via a background listener
  (see 1.1).
- How: build every `KafkaError` from the core handle (`_from_c`), consuming the C
  error once; keep `.code` / `.is_retriable` / `.is_fatal` sourced from the core.
- Anti-patterns: synthesizing error codes Python-side; dropping the retriable/fatal
  flags; a second error taxonomy that isn't the core's.

**2.2 — (Deferred; this rule gates it.) The Python exception hierarchy mirrors
Java's consumer-surface `KafkaException` subtypes, projected from a single
core/FFI-owned error-code catalog; every failing op raises the mapped type, not a
bare `KafkaError`.**
- Why: convergence on the Java API means `except SpecificError` must work as it does
  in Java. The catalog is the single source of truth — Java ⇄ code ⇄ Rust kind ⇄
  Python class — so C and future-language bindings share it; a Python-only catalog
  would strand them.
- How: define the subclasses under `KafkaError`; map code→class in `_from_c`; own
  the catalog at the core/FFI level and *project* it in `bindings/python/`. A parity
  check (xtask + a DoD item) flags drift when Java's consumer-surface exceptions
  change, so an author can update the catalog + projection.
- Anti-patterns: a Python-owned catalog; leaving ops raising bare `KafkaError` once
  the hierarchy lands; diffing by class *name* across languages instead of by the
  shared code; scoping the mirror to non-consumer-surface Java exceptions.
