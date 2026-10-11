# Milestone 16 — Python client interface design: producer and consumer

The `confluent_kafka` Python client for the producer and consumer families
(`bindings/python/`), generated from rules rather than written by hand. PR #187.

## 1. What this milestone delivers

- **The rules:** CLAUDE.md's `## Python Binding Conventions`, a section of its own
  after `## Translation Rules`. They are self-contained and leave no free choices:
  the same rules, Java source and FFI header give the same interfaces on every
  run. They also state the stability policy (development → preview → GA).
- **The package:** `confluent_kafka.producer`, `.consumer`, `.common` and the error
  modules, generated from the rules, the Java 4.3.1 client and the C FFI header.
  It replaces master's `producer.py`, `consumer.py` and `_error_code.py`; the soak
  client, the perf tests and the gRPC servers use it.
- **The FFI and core fixes** the package needed, among them: headers reaching the
  broker on the FFI's send path, `ProducerConfig` applying Java's validators, and
  rebalance-listener and commit callbacks running on the caller's thread through
  a callback queue the FFI exposes (`consumer-threading.md` §31).

## 2. Scope decisions (owner, 2026-09-25)

- **Inputs:** the rules, the Java source and the FFI header only. Where an
  interface already existed, it was compared with the rules' output: a wrong rule
  was fixed in the rules, a wrong interface in the package.
- **No new FFI entry points for Python.** Each Python form maps to the entry point
  CLAUDE.md §2/§3 derive for it, and is generated if that entry point exists on
  master or among the ones this work had already added. Otherwise the form is not
  generated yet; the share-consumer family is one such case. PR #187's
  description lists these forms ("Not generated yet"). Minor FFI changes are
  allowed: renames to the derived name with the old name kept as a deprecated
  alias, the `commit_nowait()` callback thread, and bug fixes.
- **Out of scope:** the Admin client. `admin.py` stays paused
  (`.claude/rules/admin-client.md` §11).
- Manager / Actor / Critic loops, with `make verify` at the end of each phase
  rather than on every commit.

The branch `dev_python-interface-on-master` is built on master at `dfade0be`
(after the repository rename, #198). It replaced PR #187's earlier head, which
carried the same work on top of `dev_interface_consolidation` (#191); P2 ported
that work.

## 3. Phases

| Phase | What | Actor / Critic |
|---|---|---|
| P1 | CLAUDE.md `## Python Binding Conventions`: every generation rule, in the §2/§3 bullet format, with no free choices, and the stability policy. The owner reviewed the draft before it was committed. | 72 |
| P2 | Port the earlier work onto master: the package, its tests, the gRPC servers, the C extension, the removal of the legacy modules, and the FFI entry points it had added, renamed to master's core and FFI (#191). | 73 |
| P3 | Common value types, records, errors, serialization and configuration: regenerated from the rules and compared with the package. | 74 |
| P4 | Producer family (sync, async, mock): regenerated, with every form mapped to its FFI entry point. | 75 |
| P5 | Consumer family (sync, async, mock) and its callbacks: as P4, plus the `commit_nowait()` callback on the caller's thread. | 76 |
| P6 | Full `make verify`, then PR #187 updated with the new branch. | Manager |

What is left — the owner's review of the choices made while generating, the FFI
and Rust-core gaps found along the way, and interface improvements — goes to a
later phase the owner runs.

## 4. Items carried between phases

Found while reviewing P2, and fixed in a later phase:

| Item | Fixed in |
|---|---|
| The Python perf tests imported the retired modules, and their librdkafka baseline (PyPI `confluent-kafka`) has the same top-level name, `confluent_kafka`. The tests now use the package, and the baseline runs in its own venv. | P2 |
| The soak comments on client-side timeout codes were wrong, and an existing venv kept PyPI `confluent-kafka`. | P2, P3 |
| Package docstrings cited the spec and the old rules file; they now cite the conventions. | P2–P5 |
| The base `KafkaError` gets `_ffi_id = -1` (`UNKNOWN_SERVER_ERROR`); the soak test that asserted otherwise changed with it. | P3 |
| `KafkaProducer.close()` cancelled in-flight sends; it now waits for them, as Java does. | P4 |
| `Producer_close_with_timeout` rejected a negative timeout for a `MockProducer` too, which Java's mock does not check. | P4 |
| `kafka_common_Error_new` accepted only wire codes, so a local error injected into a mock came back as `UnknownServerError`. | P5 |
| The `MockConsumer` error setters could not clear a pending error, as Java's `null` does. | P5 |
| `Consumer_close_with_timeout` clamped a negative timeout to 0 where Java throws. | P5 |
| The consumer constructors defaulted the deserializers to `bytes_*()` instances, so the config route never ran. | P5 |
