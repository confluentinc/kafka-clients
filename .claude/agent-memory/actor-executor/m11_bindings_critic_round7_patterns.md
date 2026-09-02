---
name: m11-bindings-critic-round7-patterns
description: Critic round-7 (M11 bindings B3) reusable patterns — namespace decides handle reuse (not allocation cost), Java-dead-but-Rust-live branches, and pinning an escalation's claim at the layer it was made about
metadata:
  type: feedback
---

Round 7 found no defects but reframed three things in ways that generalise.
See also [[m11_bindings_b3_notes]] and [[m11_bindings_b4_notes]].

**1. In the Admin module, allocation cost is never a valid argument.**
`admin-client.md` §10 rules the hot-path allocation audit N/A for Admin. So an
"it costs one allocation per key" justification is not merely weak there, it is
*inadmissible*, and offering it weakens the arguments beside it. Reach for the
structural reason instead.

**Why:** I used allocation cost as one of three reasons for declining to reuse
`kafka_consumer_TopicPartition_t`, in a commit that invoked §10 twice.

**How to apply:** the reason that actually settles cross-module handle reuse is
**CLAUDE.md §3 namespacing**. `TopicPartition` is `org.apache.kafka.common`, so
the correct FFI spelling is `kafka_common_TopicPartition_t`;
`kafka_consumer_TopicPartition_t` is a pre-existing namespace error and reusing
it would propagate it into a second public C API, making the correction a
breaking change across two surfaces instead of one.

The rule cuts both ways, and B4 hit the mirror image: `OffsetAndMetadata`
really *is* `org.apache.kafka.clients.consumer`, so an admin-prefixed copy would
be the wrong name and the right move is to reuse the consumer type (Python) or
flatten the fields onto the parent handle (C).

**2. A branch that is dead in Java can be live in Rust, and it will be
untested.** `ArrayList.get(i)` throws where `Vec::get(i)` returns `None`;
`Optional.orElseThrow` vs `?`; a Java `switch` with an unreachable `default`.
The Rust translation is usually *better* there, but the branch is real, has an
error path, and nothing exercises it.

**How to apply:** when translating a guard whose Java form cannot fire, say so
in the rustdoc **and** write the test anyway. Grep for `.get(` on a container
Java indexes directly.

**3. An escalation must be pinned at the layer its justification names.**
The mock-panic escalation was justified as "a process abort reachable from
ordinary C code", but the regression test was Rust-only. The C test is the
direct evidence, and it is the artifact a future reader looks for when they
read the escalation commit. Its assertion is unusual and worth stating in the
comment: the *suite surviving* is the result, since the regression would kill
the binary rather than fail an assertion.

**4. A default argument is a behavioural claim about Java.** Adding
`partitions=None` to a Python method asserts that Java has a no-argument
overload. Check `Admin.java` before adding one: `listPartitionReassignments`
really does, `electLeaders` does not — and the one that did not was the
destructive call, where an omitted argument plus `UNCLEAN` meant a cluster-wide
unclean election. Symmetry between neighbouring methods is not evidence.
