# Python binding design

Design notes for the Python client (`bindings/python/`) — the narrative "how and
why". The enforceable invariants live in
`.claude/rules/bindings/python/binding-conventions.md`; this doc explains the
reasoning behind them and is meant to be read once before implementing.

## Callbacks

**Philosophy — mirror Java, not librdkafka.** Most librdkafka "callbacks" are not
callbacks here:
- `error_cb` → errors thrown at the call site as `KafkaError` (try/except).
- `stats_cb` / `throttle_cb` → read on demand via `metrics()` (pull). `metrics()`
  is deferred, so these are simply unavailable today.
- `log_cb` → routed to standard logging (Rust `log` + `init_default_logger`).
- `oauth_cb` → the one genuine "Kafka calls your code" callback (token refresh),
  deferred. Rebalance/commit listeners are the other push callbacks, also deferred.

**Two failure modes.**
- (A) The op fails for a Kafka reason (broker down, timeout, auth, bad message):
  the error rides back as the result and is raised at `poll()` / `await poll()`,
  where your try/except catches it. ✅
- (B) A completion callback's own code raises on the dispatcher thread: today that
  callback is only the binding's one-line shim, so the only realistic crash is the
  event loop being torn down mid-poll. Once user callbacks (rebalance/commit) run
  there, user code raises readily — so the delivery path must be exception-safe
  first.

**Delivery path.** submit → Rust runs the op on tokio → completion fires the C
trampoline on the dispatcher thread → it calls the Python `cb` → `cb` wakes the
waiter (flips the sync `Event`, or schedules `_deliver` to resolve the async
`Future`).

**Invariant (rule 1.2).** Every completion resolves its waiter exactly once and
frees the C handles on every path. The async path guards `call_soon_threadsafe`
against a closed loop and, on failure, fails the `Future` via `_fail` (rule 1.3).
The sync path already satisfies this (flipping an `Event` can't raise).

**Forward plan (deferred).** When rebalance/commit/OAuth are bridged, they run
user code inside the guarded path; a user exception surfaces at the driving call
(`poll`/`commit`), matching Java (§31), and releases the guard — never
printed-and-dropped. OAuth additionally needs background token-refresh scheduling.
The exception-safe trampoline is landed now precisely so this future work is safe.

## Error model

**Current state.** Errors surface as a single flat `KafkaError(Exception)`
(shared with the producer) carrying `.code` (int), `.message`, `.is_retriable`,
`.is_fatal`; every failing op raises it (see conventions §1.1). This is coarse:
an app can't `except` a specific error, and `.code` is an opaque integer with no
names.

**Target — mirror Java, typed (decided).** The Python error surface becomes a
typed exception hierarchy mirroring Java's `KafkaException` tree, so apps
`except`-by-type the way both Java and Python developers expect. Rationale: the
whole client's north star is convergence on the Java API, and Java's error model
*is* a typed hierarchy. The boolean `.is_retriable`/`.is_fatal` flags stay —
they're more Pythonic than Java's `instanceof RetriableException`; we just make
them accurate for every kind.

**Single source of truth — the error-code catalog.** Java, Rust, and Python are
three *projections* of one catalog:

    Java exception  ⇄  error code  ⇄  Rust KafkaError kind  ⇄  Python class
                         └── single source of truth ──┘

"Drift" then has a precise, computable meaning: a code with no Python class, a
Java exception with no code, or a changed parent. **The catalog is core/FFI-owned**
— shared by C and every future-language binding — so `bindings/python/` *projects*
from it and never defines its own. Scope is *consumer-surface* exceptions, not
every internal Java exception.

**Two halves, landing separately.**
- (A) *Exhaustive delivery* — "every error reaches the app." Already landed in §1
  (rules 1.2/1.3: the waiter is always resolved, never swallowed/hung). The one
  remainder is `commit_async`, fire-and-forget today, which must gain a result
  channel — a §1 (completion-delivery) extension, not part of this section.
- (B) *Classification / identity* — "the error is the right typed thing, faithful
  to Java." This section's subject: the typed classes + the Rust-code→Python-class
  translation (`_from_c`). Deferred to the error-model milestone; conventions §2
  gates it.

**Forward plan (deferred).** A milestone builds the typed classes + the translation
layer over the core-owned catalog; a parity check (xtask + a DoD item) keeps the
Python projection in sync with the catalog, and the catalog in sync with Java's
consumer-surface exceptions — the same "promote-to-rule + detect-drift" loop the
rest of the infra uses.

## Packaging & layout

**Decision (Option B): ship as an installable package `confluent_kafka4`.** Today
the binding is flat scripts — `producer.py` / `consumer.py` imported by bare name,
C extension `_confluentkafka`, `py-modules` in `pyproject.toml`. That works for the
test harness but is **not** installable as a real package. The target is a proper
src-layout package:

    bindings/python/
    ├── pyproject.toml            # distribution: confluent-kafka4
    ├── setup.py                  # ext: confluent_kafka4._confluentkafka
    ├── src/confluent_kafka4/     # import confluent_kafka4
    │   ├── __init__.py           # public API re-exports
    │   ├── consumer.py  producer.py  _confluentkafka.c  py.typed
    ├── test/                     # imports the installed package
    └── (grpc_*, Dockerfiles)     # harness — NOT shipped

**Why `confluent_kafka4` (the `4` in the name).** From the org packaging plan
(Confluence "Rust based clients repository and packages"): the new major fully
converges on the Java API, which is breaking vs the librdkafka-era client. Many
Python users pin loosely (`>=` or unpinned), so reusing `confluent-kafka` would
break them silently on upgrade. A distinct name (dist `confluent-kafka4`, import
`confluent_kafka4`) makes the upgrade opt-in and lets both live side by side — the
established Python pattern (`urllib2`, `psycopg2`, `bs4`, `jinja2`).

**Why src-layout.** Matches the reference `confluent-kafka-python` and forces tests
to run against the *installed* package, not the source tree — important for a
C-extension package where "works in the repo" ≠ "works installed".

**Rejected — namespace package `confluent.kafka`.** Cleaner folder (no `4`), keeps
the `4` only in the distribution name. Rejected: diverges from the packaging doc's
fixed import name and the reference precedent; the trailing-major convention is
idiomatic. Revisit only via the Confluence doc, not a local rename.

**Monorepo note.** At the repo split (`kafka-clients/` with per-language top-level
folders) `bindings/python/` becomes `python/` unchanged; its `.claude/rules/…` and
`design/…` colocate then.

Execution is tracked in `milestones/package-layout.md` — a pure restructure, no
behavior change.
