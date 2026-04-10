---
name: Generator per-field flexibleVersions fix
description: Code generator must respect per-field flexibleVersions overrides (e.g. "none") to match Java behavior
type: feedback
---

The code generator must check each field's `flexibleVersions` override, not just the message-level `flexibleVersions`. This is critical for wire-protocol compatibility.

**Why:** The `RequestHeader.json` defines `ClientId` with `"flexibleVersions": "none"`, meaning it always uses the old i16-prefixed encoding even in flexible message versions. Without this fix, the generator would incorrectly use varint encoding for ClientId in header v2, producing 10 bytes instead of the correct 11 bytes.

**How to apply:** The `field_flexible_versions()` helper function in `generator/src/lib.rs` mirrors Java's `MessageDataGenerator.fieldFlexibleVersions()`. It's called at all three code generation sites: `generate_field_add_size`, `generate_field_read`, and `generate_field_write`. When adding new generated code paths for field serialization, always use `field_flexible_versions(field, message_flexible_versions)` instead of the raw message-level flexible versions.
