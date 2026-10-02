---
name: review-lint-java-rules-repro
description: How to run lint-custom's three Java-dependent rules (check-java-name, check-no-deprecated-translation, check-public-audience) faithfully when kafka/ is not checked out — throwaway tagged git repo symlinked into git-archive copies, with a master control run
metadata:
  type: project
---

Without `kafka/`, `cargo xtask lint` cannot run three of the six `lint-custom` rules. It reports them as "cannot run" and **stops before** doc hygiene, module-path hygiene and clippy. So a branch can look lint-clean locally while CI's Verify Rust job, which runs `cargo xtask fetch-java-refs` first, fails. This happened in M15 round 4 (see [[review-m15-password-redaction]]).

**What the rules read.**
- The working tree's `../kafka/clients/src/{main,test}/java` (the `JavaIndex`).
- `git -C ../kafka ls-tree -r <ref>` plus blobs under `clients/src/main/java/org/apache/kafka` at `DEPRECATION_REFS` (`4.3.1`, `4.4.0-rc3`) and `AUDIENCE_REF` (`4.4.0-rc3`). These feed the package-info disclaimers and the deprecated-list staleness check.

**Recipe** (about 5 minutes; the repository and the real submodule are never touched).
1. Resolve each tag to its commit with the GitHub API: `api.github.com/repos/apache/kafka/git/refs/tags/<t>` returns an annotated tag object; `.../git/tags/<sha>` then gives the commit. `4.3.1` resolves to the pinned `26b251a4`; `4.4.0-rc3` resolves to `a6e87dfe`.
2. Download `codeload.github.com/apache/kafka/tar.gz/<commit>` (about 15 MB each). Extract only `kafka-<sha>/clients/src/main/java` and `.../test/java`; bsdtar accepts the paths as arguments.
3. Build the stand-in repo. `git init` a throwaway repo, then:
   - commit the 4.4.0-rc3 tree and tag it;
   - replace the tree with 4.3.1, commit and tag it. HEAD and the working tree are then at 4.3.1, as in CI.
4. Make the copies. Run `git archive HEAD | tar -x` and `git archive <master> | tar -x` into scratch dirs. In each, replace the empty `kafka` dir with a symlink to the throwaway repo.
5. Build and run.
   - Build once: `CARGO_TARGET_DIR=<scratch>/target cargo build --offline -p xtask`.
   - Run `<scratch>/target/debug/xtask lint-custom` from each copy's `rust/`.
   - The **master control run** is what turns "fails" into "this branch introduced it".
6. Trial a fix. Edit a third copy, then run the full `xtask lint` there, now reachable past lint-custom, plus `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features`. This is CI's separate doc-check job.

**Shell traps hit while doing this.**
- `g` is a zsh alias in this user's shell; a function named `g` fails to parse.
- zsh does not word-split `$CMD` strings; use a function.
- `git add` has no `-q`.
- `rm` or `rmdir` with `$VAR` paths is refused by the safety check. Use literal absolute paths or `"${P:?}"`.
- A word starting with `=` (e.g. `echo =====`) triggers zsh `=cmd` expansion and errors; use dashes for separators.
- `grep --include=*.rs` is globbed by zsh ("no matches found"); quote it: `--include='*.rs'`.

**Re-running in a later round (M15 round 5).**
- The stand-in is an ordinary git repo: within a session, symlink it into fresh `git archive` copies instead of
  rebuilding it. Across sessions the scratchpad may be gone; rebuild by the recipe.
- Cost once the stand-in exists: xtask build about 3 s with its own `CARGO_TARGET_DIR`, `lint-custom` about 8 s per copy.
- Always pair HEAD with the master control run, and attribute any count delta by deleting the new module in a copy
  (see [[review-m15-password-redaction]] Lesson 13).
- If the brief scopes the re-run to `lint-custom` only, say so in the report and argue module-path hygiene from its
  inputs: it is textual (parent `use` re-exports vs `::<file_module>::<Name>` paths), so unchanged `use` lines and no
  `::<file_module>::` path mean an unchanged result.
