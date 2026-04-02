## Definition of Done

Every change must at least pass all of the following before it is considered complete from the Actor or reviewed by the Critic:

1. Be consistent with CLAUDE.md and all Claude rules linked or not linked in it.
2. Are all methods from the transalated classes implemented?
3. Are all test using those classes translated?
4. Are there blockers for doing that?
5. `cargo build` succeeds
6. `cargo test` passes
7. `cargo xtask format-check` passes (run `cargo xtask format` to fix)
8. `cargo xtask lint` passes (run `cargo xtask lint-fix` to auto-fix, then fix remaining issues manually)