## Definition of Done

Every change must at least pass all of the following before it is considered complete:

1. Be consistent with CLAUDE.md and all Claude rules linked or not linked in it.
2. `cargo build` succeeds
3. `cargo test` passes
4. `cargo xtask format-check` passes (run `cargo xtask format` to fix)
5. `cargo xtask lint` passes (run `cargo xtask lint-fix` to auto-fix, then fix remaining issues manually)