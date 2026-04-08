---
name: Review patterns for generator tagged field handling
description: Tagged field default-value checks were fixed in commits 7feede4/1a01807 — all types now properly compared against defaults
type: project
---

Tagged field handling in generator/src/lib.rs was a high-defect-density area, now fixed.

Previous issues (all resolved as of 2026-04-04):
- `get_default_check()` now covers ALL field types (Bool, Int, Uint, Float64, Uuid, Struct, Bytes, Records) not just String/Array
- UUID default values from JSON specs are now properly parsed via `Uuid::from_string()`
- Both `generate_tagged_field_add_size` and `generate_tagged_field_write` use `get_default_check()` consistently

**Why:** The generator was built incrementally and the default-value-check logic was initially only implemented for String/Array. Issues 2 and 3 from Critic review caught this.

**How to apply:** When reviewing future generator changes to tagged fields, verify that `get_default_check` remains consistent across write, size, and counting paths. Also watch for edge cases with nullable fields that have non-null defaults (currently none exist in specs, but could be added).
