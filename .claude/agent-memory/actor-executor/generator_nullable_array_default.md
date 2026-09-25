---
name: generator-nullable-array-default
description: A new validation guard's first job is to expose the wrong defaults feeding it — nullable arrays defaulted to None instead of empty
metadata:
  type: feedback
---

When you add a guard that inspects a field's value, expect its first failures to be
**wrong defaults**, not wrong guard logic. Check the default before "fixing" the guard.

**Why:** the §9.1 version-gate guard produced exactly one new test failure, and it was
not about version gating at all: `OffsetFetchRequest.Topics` was `None` where Java's
`FieldSpec.fieldDefault` gives `new <List>(0)` (`FieldSpec.java:465-475` — the `"null"`
return is reached *only* via an explicit `"default": "null"`; `validateNullDefault()`
sits on that path alone). Java's predicate treats null as **non**-default
(`field == null || !field.isEmpty()`), so a null default trips a guard that an empty
default does not. The tempting fix — loosening the guard for nullable arrays — would
have been unfaithful and would have hidden a real wire divergence: a default-constructed
message encoded a null array where Java encodes an empty one.

**How to apply:**

  - CLAUDE.md §2 states this rule for nullable *string/bytes*. It applies identically to
    nullable **arrays** (14 fields in `generator/messages/`), and the arm had never been
    written. If a future type gains nullability, check `FieldSpec.fieldDefault` for it.
  - Before changing a default, grep production for sites that relied on the old one.
    Here every "I want null" site already spelled it out, mirroring Java
    (`KafkaAdminClient.java:3014` `setTopics(null)`, `MetadataRequest.java:39`), so
    nothing broke. That check is what makes the change safe to assert, not hope.
  - Tests that pinned the old default must be re-derived from the **Java contract**, not
    edited until green. Two here had pinned our own output: a "known byte vector" whose
    vector was ours, and an `isAllTopicPartitions()` assertion that contradicted
    `DescribeLogDirsRequest.java:66-68`.

See also [[generator_version_gate_guard]].
