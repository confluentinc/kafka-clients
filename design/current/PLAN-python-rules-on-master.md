# Plan: Python client on master, generated from CLAUDE.md §4

Owner direction (2026-09-25): move PR #187 onto master; write the Python binding rules into
CLAUDE.md as section 4 (self-contained — the spec, decisions doc and other design files will not
exist in future); use those rules to (re)generate the Python interfaces; where an interface already
exists, verify it against the spec: if the rule's output differs from the spec, fix the rule; if the
earlier interface was wrong, fix the interface. FFI (owner, 2026-09-25, confirming item 33): **no new FFI entry
points.** Map each Python form to the entry point CLAUDE.md §2/§3 names; use it if it exists on master
or among the 23 entry points this branch already added (ported in P2). If it exists in neither, do not
generate that Python method or overload form now; record it in `design/current/ffi-overload-gaps.md`
for the owner. Minor FFI changes, updates and fixes are fine (renames to the rule's name with the old
name kept as a deprecated alias, the `commit_nowait()` callback thread, bug fixes). This supersedes
D7's silently ignored `timeout=`. Manager / Actor / Critic loops. No full
verification per commit — `make verify` at the end of each phase.

Branch `dev_python-interface-on-master`, built on `master` at `dfade0be` (after the repository rename,
#198). It replaces PR #187's earlier head, which carried the same work on top of `dev_interface_consolidation`
(#191); P2 ported that work, including the D29 parameter renames.

| Phase | What | Actor / Critic |
|---|---|---|
| P1 | CLAUDE.md `## Python Binding Conventions` (own section after `## Translation Rules`, ruling 32): all generation rules from the spec + D1–D29 + the old rules file + the owner rulings 1–33, in the §2/§3 bullet format; deterministic (no free choices); stability policy (dev → preview → GA). Owner reviews the draft before it is committed (CLAUDE.md is owner-controlled). | 72 |
| P2 | Bring the Python work onto the branch: `bindings/python/confluent_kafka/`, tests, gRPC servers, C extension, legacy-module removal, and this PR's FFI additions ported to master's renamed core/FFI (PR #191). Build + unit tests green. The old `.claude/rules/python-binding-interface.md` is NOT carried over (§4 replaces it). | 73 |
| P3 | Common value types, records, errors, serialization, configuration: regenerate from CLAUDE.md `## Python Binding Conventions` + Java + the FFI header only (the spec is not an input), compare with the package, make the package match the rules; CLAUDE.md is not edited — anything a rule can't settle goes to the owner's post-phase review. | 74 |
| P4 | Producer family (sync, async, mock): regenerate + compare; map every form to its FFI entry point by the §2/§3 rules; no new FFI; skip + log forms without an entry point; minor FFI fixes only. | 75 |
| P5 | Consumer family (sync, async, mock) incl. callbacks: same as P4, plus the `commit_nowait()` callback on the caller's thread (C47). | 76 |
| P6 | Full `make verify`; update PR #187 with the new branch (owner, 2026-09-25: "Run phase 6 as well" — run it without stopping; the old PR head is kept as a local backup ref first); `ffi-overload-gaps.md` and the post-phase review list reported to the owner at the end. | Manager |
| P7 | Created later by the owner for the remaining items: the nine round-5 choices (owner file, "Post-phase review"), the FFI and core gaps, and the improvements list. Not started by the Manager. | — |

## Carried into later phases (from P2, Actor 73)

- **P2 follow-up (before P3):** the Python perf tests (`bindings/python/test/performance/`) still import
  the retired `producer` / `consumer` modules, and the librdkafka baseline they compare against is the
  PyPI `confluent-kafka` — same top-level name `confluent_kafka`, so both cannot share one venv. Port the
  tests to the package and run the baseline in its own venv (CI `verify-python` runs
  `test-integration-perf-python`).
- **P4:** `KafkaProducer.close()` cancels in-flight sends (future raises `CancelledError`, returns in
  0.00 s); Java's `close()` waits for them up to the timeout. Inherited from master's legacy
  `producer.py` (`self._cancel()`).
- **P5:** `kafka_common_Error_new` accepts only wire codes, so a local error with a negative id (e.g.
  `IllegalStateError`) injected into a mock comes back as `UnknownServerError`. Minor FFI fix.
- **P3:** the base `KafkaError` gets `_ffi_id = -1` (rules, Errors). The soak test
  `test_error_code_is_none_for_the_package_base_error` (from P2) asserts the opposite and must change
  with it (Critic 73 N4).
- **P3–P5:** package docstrings and comments cite the spec and the old rules file (167 lines in 66
  files), which are not on this branch; replace them with CLAUDE.md `## Python Binding Conventions`
  as each file is regenerated (Critic 73 N5).
- **P5:** Java's `setPollException(null)` / `setOffsetsException(null)` clear a pending error
  (`MockConsumer.java:344-350`); the core setters, the `(code, message)` FFI and Python cannot. Minor
  FFI fix, following master's MockProducer `clear` flag (`src/ffi/producer.rs:4299-4336`) (Critic 73 N2).
- **P3 (first step):** Critic 73 R2-N1 — the soak comment at `soakclient.py:191-195` is wrong about
  timeouts (`TimeoutError._ffi_id` is 7, REQUEST_TIMED_OUT; only Wakeup -18 and LocalTimeout -5 are
  negative). R2-N3 — taking PyPI `confluent-kafka` out of `[dev]` doesn't remove it from an existing
  `venv/`; add a one-time uninstall step or comment.
- **P4:** Critic 73 R2-N2 — `Producer_close_with_timeout` (+ `_async`) reject a negative timeout for a
  MockProducer handle too; Java's `MockProducer.close(Duration)` doesn't check (`MockProducer.java:436-442`).
  Check only for the real producer.
- **P5 (note):** master's `Consumer_close_with_timeout` clamps a negative timeout to 0
  (`src/ffi/consumer.rs:4837`) where Java throws; the producer and consumer FFIs now disagree.
  Minor FFI fix.
- **P5:** the consumer constructors default `key_deserializer` / `value_deserializer` to `bytes_*()`
  instances rather than `None`, so the config route never runs (Critic 74 R2-N1; confirmed ruling 33).
  Default to `None` and add a client-level config-route test. (Producer side: Actor 75 in P4.)

