---
name: m11-bindings-critic-round5-patterns
description: Critic round-5 (M11 bindings B2) reusable patterns — mock-vs-production doc attribution, Py_BuildValue 'N' steal-on-failure, and closing FFI coverage gaps by unit-testing pure flatteners
metadata:
  type: feedback
---

Four patterns from Critic round 5 on the admin bindings, all likely to recur in
B3–B6 and in any future FFI slice.

**1. A doc comment that says "Java does X" must say *which* Java.**
`MockAdminClient` and `KafkaAdminClient` diverge, sometimes sharply. Attributing
a mock behaviour to "Java's `<method>`" is a false statement of the production
contract, and it is worst in FFI rustdoc because cbindgen ships that sentence
into the public header, where the reader is using the real client.

**Why:** `DescribeReplicaLogDirsResult_count` documented the mock's omit-unknown-
topics behaviour as general. Production seeds one future per requested replica
and completes all of them, so an unknown topic comes back *present* with a null
current log dir. A C caller writing `if (!found) { /* unknown topic */ }` gets
the mock right and the broker wrong.

**How to apply:** name the class (`MockAdminClient.describeReplicaLogDirs`), and
if the two differ state both, production first. Verify the corrected text in the
**generated header**, not just the source. When fixing one such site, grep for
the same claim in test docstrings — the same wrong sentence tends to be copied
there.

**2. `Py_BuildValue`'s `'N'` unit steals its argument even when the build fails.**
CPython's `do_mktuple` routes a `PyTuple_New` failure through `do_ignore`, which
re-runs `do_mkvalue` over the remaining format units and releases each result.
So `if (val == NULL) Py_XDECREF(err);` after a `'N'` build is a *second* release.

**How to apply:** only release on the branch where `Py_BuildValue` never ran.
The uniform `(error, value)` case is now `error_value_pair()` in
`_confluentkafka.c`; reuse it for new `*_drain` functions. Sites with a mixed
format (`"(NL)"`) or a guard that includes other variables (e.g. `key`) need
restructuring by hand, not the helper. OOM-only, so no test will catch it —
this must be caught by grep. When sweeping, grep `Py_BuildValue("[^"]*N` and
check each for a following `XDECREF`; unconditional returns are fine.

**3. Close FFI coverage gaps by unit-testing the pure flatteners, not only
end-to-end through the mock.**

**Why:** the mock ignores every `*Options` argument and builds config entries
with the two-arg `ConfigEntry(name, value)` ctor, so end-to-end tests cannot
observe an option flag, a non-`UNKNOWN` enum constant, documentation, synonyms,
a `LogDirDescription` error, or a volume size. Concretely: transposing two
boolean option flags anywhere along Python → C extension → FFI → `*Options`
passed all 66 C and all 63 Python tests. Only review guarded them.

**How to apply:** add a `#[cfg(test)] mod tests` to the FFI module (precedent:
`src/ffi/producer.rs`, 57 tests). Call every option builder **twice with
asymmetric flag values** so a transposition cannot satisfy both cases — this is
the assertion with teeth. Then assert every enum-constant-name mapping
exhaustively, and each flattener over a hand-built fixture exercising the
nullable and sentinel arms. Mirror the cheap half in Python over the `_to_*`
tuple converters, which pins tuple field order and arity.

**Always verify teeth by actually transposing one flag and confirming a test
fails, then restore.** (Per [[workflow-teeth-check-mtime]], `touch` the file
after restoring a `cp`-based backup.)

**4. When a suggested test does not fail as predicted, check whether the premise
is wrong before "fixing" the code.** The round-5 suggestion was an async
call-failure test; written against `describe_configs` it did not raise. That is
correct behaviour, not a bug — but the *reason* I first gave was wrong, and the
Critic corrected it in round 6. Both mocks fail **every** future they own on the
seeded-timeout branch (`MockAdminClient.java:822-831` for describeConfigs,
`:346-351` for describeCluster), so "individually vs all together" is not the
discriminator.

The real discriminator is the **shape of the binding's return**, which follows
from `admin-client.md` §5. A multi-key RPC has a per-key slot a `KafkaError` can
occupy, so the failure becomes data. A single-future RPC collapses into one
object (or list) with nowhere to put an error, so it must raise — and Java
agrees: `describeCluster().nodes().get()` throws. So: **per-key futures → the
failure lands in the dict; one future for the whole call → it raises.** Decide
by reading the Java `*Result`'s future shape, not by counting keys.
