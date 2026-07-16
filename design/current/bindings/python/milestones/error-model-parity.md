# Milestone: Python consumer error-model parity

**Track:** Python binding (`bindings/python/`) · **Driver:** error-handling parity
with Java (**Option A — typed exception hierarchy**).
**Named per-track milestone** — no global integer; its presence in this
`milestones/` dir *is* the registry entry (avoids the shared-`MILESTONES.md`
conflict/collision problem). **Cross-track note:** Phase 1 (the error-code
catalog) is a **core/FFI deliverable**, shared with the C and future-language
bindings — this milestone drives it, but it isn't Python-owned.

## Goal

Make the errors a Python consumer app sees line up with Java's: a **typed
exception hierarchy** (not one flat `KafkaError`), each failing op raising the
mapped type, with `KafkaError` identity sourced from — and kept in sync with —
the Rust core. Satisfies conventions **§2** (specifically the deferred gate 2.2).

## References

- **Reasoning:** `design/current/bindings/python/binding-design.md` → `## Error model`.
- **Invariants:** `.claude/rules/bindings/python/binding-conventions.md` → §2.
- **Comparison behind the decision:** ours (flat `KafkaError`) vs confluent-kafka-python
  (data `KafkaError` + `KafkaException` + constants + `msg.error()`) vs Java KIP-932
  (typed unchecked hierarchy).

## Scope

**In:** consumer-surface errors — everything reachable through `poll` / `commit` /
`position` / `committed` / `seek*` / offsets queries / `subscribe` / `close`, plus
`WakeupError`.
**Out:** share-consumer errors (no share consumer in the core — KIP-932 out of
scope); serialization errors (bytes-in/out binding — the user's concern); the
flat-vs-typed question (settled: typed).

## Architectural anchor

Java, Rust, and Python are three projections of a **single error-code catalog**;
the catalog is **core/FFI-owned**. Everything below hangs off it. "Drift" = a code
with no Python class, a Java exception with no code, or a changed parent.

## Phases (dependency-ordered)

### Phase 1 — Error-code catalog  *(core/FFI; shared)*
Audit the core's existing `KafkaError` kinds against Java's consumer-surface
`KafkaException` subtypes; produce a **canonical catalog** (code ⇄ Java exception
⇄ Rust kind) and expose stable code values across the C ABI. The core already
carries Java-corresponding codes (`CLAUDE.md` §10.3) — this is mostly
*canonicalize + fill gaps + expose*, not invent.
- **Deliverable:** the catalog (Rust enum + a documented table) + stable,
  FFI-exposed codes.
- **DoD:** every in-scope Java exception maps to a code; codes stable + exposed;
  table checked in.
- **Note:** benefits C too — could be split into its own core-track milestone this
  one *depends on*. Kept here as Phase 1 for now.

### Phase 2 — Python typed hierarchy  *(Python)*
Define exception subclasses under `KafkaError` mirroring the in-scope Java tree
(`WakeupError`, `TimeoutError`, `AuthenticationError`, `AuthorizationError`,
`CommitFailedError`, `OffsetOutOfRangeError`, `IllegalStateError`, …). Keep
`.code` / `.is_retriable` / `.is_fatal`.
- **Deliverable:** the class hierarchy.
- **DoD:** mirrors the in-scope Java tree; flags accurate per kind; per-class unit
  tests. Satisfies rule 2.2.

### Phase 3 — Translation layer  *(Python)*
Wire `_from_c` to instantiate the mapped subclass from the catalog code (instead
of a bare `KafkaError`); every failing op raises the typed error.
- **Deliverable:** code→class mapping in `_from_c`.
- **DoD:** each op raises the correct type (parity tests); no op raises bare
  `KafkaError` afterward; the code round-trips (identity preserved).

### Phase 4 — Drift check + DoD hook  *(xtask + DoD)*
`cargo xtask check-error-parity`: (a) Java↔catalog, (b) catalog↔Python-classes.
Warns (author decides). Add a DoD item.
- **Deliverable:** the xtask + a `definition-of-done.md` entry + wiring into
  `make verify`.
- **DoD:** flags a synthetic drift; runs in `make verify`; reports rather than
  hard-fails.

## Sequencing

1 → 2 → 3 → 4. Phase 1 gates everything (source of truth); Phase 4 is last (needs
something to check). Each phase: Actor builds, Critic reviews, `make verify` green.

## Adjacent (NOT this milestone)

`commit_async` result channel (Priority 1b) is **§1 completion-delivery** work, not
classification — but once Phases 2–3 land it should deliver a *typed* exception.
Sequence it after this milestone (or fold into §1 work that references this
hierarchy).

## Open decisions (defaults set — implementer confirms; don't re-litigate)

- **Consumer-surface scope:** *default —* anchor to AK 4.2 `KafkaConsumer` `@throws`
  + errors reachable through the async consumer; exclude broker-internal exceptions.
- **Where the classes live:** *default —* a new `bindings/python/error.py`; re-export
  from where `KafkaError` lives today.
- **Hierarchy depth:** *default —* mirror only the reachable consumer-surface
  subtypes, not the entire Java tree.
- **Drift-check severity:** *default —* warn / report a finding, not hard-fail
  ("flag it, author decides").
- **Catalog form:** *default —* hand-maintained Rust enum + table now; generate from
  Java source later if the drift check makes it worthwhile.

## Definition of done (milestone)

Each in-scope op raises the Java-aligned typed exception; `KafkaError` identity
round-trips through the catalog; the drift check is wired into `make verify` and
passing; all phases reviewed and `make verify` green.
