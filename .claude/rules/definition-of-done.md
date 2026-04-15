## Definition of Done

Every change must at least pass all of the following before it is considered complete from the Actor or reviewed by the Critic:

1. Be consistent with CLAUDE.md and all Claude rules linked or not linked in it.

2. Are all methods from the translated classes implemented?

3. Are all test using those classes translated? Never skip a test that is present in the Java codebase except if there are tests that are present in the Java codebase but not translated and they are not relevant to the Rust codebase, explain why they are not relevant and why they can be skipped.

4. Are there blockers for doing that? In case implement the needed classes as well.

5. Are all unit and integration tests passing? If there are any failing test fix them before considering the change done.

6. Are there duplicated classes that were translated twice? In case remove the duplicated code and keep only one implementation.

7. Are there structs or traits that aren't present in Java codebase? Avoid adding new structs or traits that aren't present in Java codebase. In case they are needed to implement the Rust client, explain why they are needed and how they are used.

8. Are there any TODO or FIXME left in the code? In case finish everything that should be done before considering the change done.

9. `cargo build` succeeds
10. `cargo test --features integration-tests,ffi` passes
11. `cargo xtask format-check` passes (run `cargo xtask format` to fix)
12. `cargo xtask lint` passes (run `cargo xtask lint-fix` to auto-fix, then fix remaining issues manually)