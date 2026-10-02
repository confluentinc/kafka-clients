---
name: m15-merge-review-followup
description: Merging master's rust/ restructure into a long-lived branch (stranded new dirs, local exclude for the stale root target/, rename list, auto-merged files still break); cargo xtask lint without kafka/ decomposed; fmt::from_fn for nested redacted Debug; sweep a defect class by type containment
metadata:
  type: project
---

Round 4 of Milestone 15 (2026-10-02, Actor 85): `origin/master` `4eb87db1` merged into
`fix/password-type-redaction` (merge `091f6f9d`), then the pull-request review answered
(fixup `21be3984`, docs `3455a5c0`). See [[m15-password-redaction]].

**Merging across the `rust/` restructure (master #210).**
- git pairs every file the branch MODIFIED with its new `rust/...` path, but a directory the
  branch ADDED after the merge base (here `src/common/config/types/`) stays under the old
  top-level `src/`, which master no longer has. The crate never compiles it, and nothing fails
  until something `use`s it. `git mv` it beside its siblings under `rust/src/`, then prove no
  top-level `src/` remains and that
  `git ls-files | grep -vE '^(rust|python|c|design|rfc|tools|\.claude|\.github|\.githooks|\.semaphore)/' | grep /`
  prints nothing.
- Master's `.gitignore` ignores `/rust/target` but not the root `target/` (a ~61 GB stale
  pre-restructure cache) or `.r2-rules/`. Put `/target` and `/.r2-rules` in `.git/info/exclude`
  (local-only) BEFORE merging, or they show as untracked. Never delete the root `target/`.
  Builds start cold in `rust/target`, so run the long arms in the background.
- Every `cargo` / `cargo xtask` command runs from `rust/`; `make` runs from the repo root.

**Master's renames at `4eb87db1` (#209, public-surface restriction).**
- `SslConfig` / `SaslConfig` → `SslConfigs` / `SaslConfigs`, with no aliases left.
- `ConcreteRequest` → `AbstractRequest`.
- Every `XxxRequestBuilder` → a nested `Builder`, reached as `xxx_request::Builder`. The
  `RequestBuilder` trait keeps its name.
- `ConfigResourceType` → `config_resource::Type`.
- Config fields are `pub(crate)`.
- Public items carry `#[non_exhaustive]` and `#[doc(alias = "org.apache.kafka...")]`, on
  structs, constructors and methods. Master marks no constant, so `Password::HIDDEN` has none.
- `ProducerConfig` lost `Clone`: its `partitioner` field is a `Box<dyn Any + Send + Sync>`.

**Merged files still break after the renames.**
- Cleanly auto-merged files break too. The branch's new code in a file git merged without a
  conflict still uses the old names. Here that was `network_client.rs` tests,
  `metadata_request.rs`'s `Display` on the old builder, and `mock_admin_client.rs` tests.
- Compile EVERY target and feature: `cargo test --workspace`, `--features ffi`, and
  `--all-features`. Plain `cargo build` skips `#[cfg(test)]` code, which is where most of these
  were.
- A "used but not defined" grep is only a pre-check. Mine counted any `use` line as a
  definition, so a stale import hid the removed name, and it saw only the naming pattern I
  guessed. The compiler is the check.
- Survival check after resolving:
  - every line the branch added (`git diff <merge-base> <branch-tip>`) must appear verbatim in
    the merge result, and every miss must be explained by a master rename;
  - every branch fn name must still exist.

**`cargo xtask lint` without the `kafka/` submodule.**
- `lint-custom` runs all six rules and fails only at the end. Three read
  `../kafka/clients/src/main/java/...` and report "cannot run": `check-java-name`,
  `check-no-deprecated-translation` and `check-public-audience`. So `lint` exits 1 BEFORE
  `doc-hygiene`, `module_path_hygiene` and clippy.
- Decompose it:
  - `cargo xtask lint-custom`, and read the three ✅ lines (no-data-carrying-enum-variants,
    no-public-field, dyn-compatible);
  - `cargo xtask doc-hygiene`;
  - module-path hygiene, which has no subcommand. A script adds a temporary dispatch arm
    `Some("module-path-hygiene-tmp") => module_path_hygiene()?,` after the `doc-hygiene` arm in
    `rust/xtask/src/main.rs`. Its `trap ... EXIT` restores the backup, `touch`es the file and
    checks `git diff --quiet`. Never run it concurrently with other cargo commands;
  - the three clippy passes exactly as `lint()` runs them: workspace, workspace
    `--all-features`, and `-p xtask`, all with `--all-targets -- -D warnings`.
- CI's Verify Rust job runs `cargo xtask fetch-java-refs`, then `make verify-rust`, whose `lint`
  covers all six rules. So report the three as left to CI.

**`std::fmt::from_fn` is stable on the pinned 1.95 toolchain.**
- Use it to build a nested redacted `Debug` (a list of structs of structs) with no helper type
  (DoD #7) and no allocation:
  `fmt::from_fn(|f| f.debug_list().entries(xs.iter().map(|x| fmt::from_fn(move |f| f.debug_struct("X")/*...*/.finish()))).finish())`.
- Read fields through accessors and never read the secret, so a field added later stays hidden
  until it is listed.

**Sweep a defect class by type containment, not by the plan's list.**
- Phase 2's list (plan §11.2) named `CreateDelegationTokenResponseOptions` and missed its sibling
  `DescribeDelegationTokenResponseOptions`; the pull-request review caught it. After fixing one
  instance, enumerate every struct whose fields hold the secret-bearing type, and check each one's
  derive or impl.
- Facts from that sweep:
  - `KafkaFuture`'s `Debug` prints only `is_done`, so `*Result` derives are safe.
  - FFI `*Inner` holders and the `*OptionsBuilder`s have no `Debug`.
  - Generated `*Data` types live in `OUT_DIR`, not `rust/src`.
- Grep trap: 4-space-indented fn parameters look like struct fields.

**Shell gotchas (zsh and the BSD userland on this Mac).**
- `echo ======` fails ("= not found") because of zsh's `=word` expansion.
- `grep -r --include=*.rs` fails with "no matches found", because zsh globs it. Quote it.
- BSD `sed` has no `\s` and fails silently. Use `perl -pi`.
- `git grep -E` needs `[[:space:]]`.
- macOS `uniq` has no `-w`.
- `grep` is ugrep here, and rejects `.{0,400}`-style patterns as too complex. Search
  transcripts with Python instead.

**Attribution.** The Manager's brief asked for a `Claude Fable 5.1` co-author line, while the
harness reminder named the running model, `Claude Opus 5.5`. I used the harness line and
flagged the mismatch in the report. The reminder yields only to the user's own instructions,
not to an agent's.
