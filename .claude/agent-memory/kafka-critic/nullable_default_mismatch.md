---
name: Nullable string/bytes default value mismatch
description: Generator defaults nullable string/bytes without explicit "default":"null" to None but Java defaults to empty — 66 production fields affected, wire incompatible
type: project
---

Nullable string/bytes fields without explicit `"default": "null"` in their JSON spec get `None` in Rust but `""` / `Bytes.EMPTY` in Java. This is a wire protocol bug affecting 66 non-tagged production fields across 42 message specs.

**Why:** The `get_default_value_for_field` function in `generator/src/lib.rs` has a catch-all `return "None"` for nullable fields with no explicit default, but only handles the `Struct` special case. Java's `FieldSpec.fieldDefault()` returns `Bytes.EMPTY` for bytes and `""` for strings when `fieldDefault` is empty (not "null").

**How to apply:** When reviewing generator changes to nullable field handling, verify that the default value for nullable string/bytes fields matches Java: `Some(String::new())` for strings, `Some(Vec::new())` for bytes. Only fields with explicit `"default": "null"` should get Rust `None`. Also verify the `SimpleExampleMessageTest.testMyBytes` default assertion matches (should be `Some(vec![])` not `None`).
