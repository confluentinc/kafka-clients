---
name: Generator crate layout
description: Where codegen actually lives in the generator/ crate — most logic is in lib.rs, not the message/ submodules
type: project
---

The `generator/` crate has two parallel codegen paths and it's easy to edit the wrong one:

1. **`generator/src/lib.rs`** (~5000 lines) is the **active** codegen entry point invoked from `build.rs` via `generator::generate_messages()`. All the per-field write/read/size emitters live here, including the `schema()` method emitter. The helper `field_flexible_versions(field, msg_flex)` is in this file and already correct on the wire path.

2. **`generator/src/message/schema_generator.rs`** has its own `SchemaGenerator` struct that emits `Schema` declarations as code-buffers. **It is not invoked from build.rs** today, but it is `pub mod`-exported via `lib.rs`, so it could be picked up later. Keep it in sync with lib.rs's behavior.

**Why:** The 5000-line `lib.rs` is where bugs in field encoding actually surface in generated code. The `message/schema_generator.rs` looks more "structured" but doesn't drive the build.

**How to apply:** Before fixing a codegen bug, confirm which file the relevant emitter is in by searching `lib.rs` first (it has `generate_field_write`, `generate_field_read`, `generate_field_add_size`, `generate_schema_method`, `schema_type_expr_for_field`, `field_has_version_dependent_schema_type`, `field_type_to_rust`, `get_default_value_for_field`). Generated code lands at `target/debug/build/confluent-kafka-rust-*/out/generated/`.
