# APPSEC-7665 / NONJAVACLI-4521 — FFI panic guard and CPython extension fixes

Jira: https://confluentinc.atlassian.net/browse/NONJAVACLI-4521
Branch: `fix/appsec-7665-4521-ffi-panic-guard` (cut from `origin/master`
`dfade0be`, 2026-09-25). Review loop: 79 (Actor 79, Critic 79). This file is
the plan and the deviation record; the Actor appends §6, the Manager keeps §5.

## 0. The problem in one paragraph

A Rust panic that reaches an `extern "C"` frame aborts the whole process.
The release profile deliberately keeps `panic = "unwind"` (see the comment
above `[profile.release]` in `Cargo.toml`: std `Mutex` poisoning relies on
unwinding), so the abort comes from the shim the compiler inserts at the
boundary, and nothing in `src/ffi/` catches the panic first. All 811 exported
functions (`#[unsafe(no_mangle)]`: `common.rs` 60, `consumer_handle.rs` 23,
`consumer.rs` 158, `admin.rs` 508, `producer.rs` 62) are exposed: every
`lock().unwrap()`, `expect`, `block_on` panic, and the seven deliberate
`assert!(count >= 0, "count must not be negative")` in `producer.rs`, kill the
C or Python host instead of returning an error. Separately the hand-written
CPython extension `bindings/python/_confluentkafka.c` has three memory-safety
defects (Jira findings 2–4): a reference leak in two `drain` functions, an
unchecked `PyMem_RawMalloc`, and 64-bit byte lengths truncated to `int32_t`.

## 1. Decisions (Manager, 2026-09-25; approach approved by the user)

- **D1. Mechanism: attribute proc-macro `#[ffi_guard]` in a new workspace
  crate `ffi-macros`** (`ffi-macros/`, `proc-macro = true`; dependencies
  `syn` 2 with `full`, `quote`, `proc-macro2` — all three already in
  `Cargo.lock` through `async-trait` / `tokio-macros` / `serde_derive`, so no
  new external code enters the tree). It is an optional dependency of the root
  crate, enabled by the `ffi` feature. Why a macro: bodies stay untouched and
  each function gains one line. Why a proc-macro and not `macro_rules!`:
  cbindgen (`build.rs`, `cbindgen.toml`, no `parse.expand`) reads the source
  without expanding macros, so a function *generated* by a macro vanishes
  from the header, while an attribute on a hand-written function is ignored.
  `parse.expand` needs a nightly toolchain and is not an option.
- **D2. The fallback value is derived from the return type by the macro**,
  with the function name consulted for counts:

  | return type | on panic |
  |---|---|
  | `()` | return; log only |
  | `bool` | `false` |
  | `i32`, `i64`, `i16` | `-1`; **`0` when the function name ends in `_count`** — `src/ffi/mod.rs` docs: counts are never negative because the C side does `malloc(count * n)` and `for (i = 0; i < count; i++)` |
  | `f64` | `f64::NAN` |
  | `kafka_common_ErrorCode_t` | the enumerator `error_code_of` produces for `Errors::UnknownServerError` |
  | `*mut kafka_common_Error_t` | `box_error(err)` — null would mean success (125 functions) |
  | `*const kafka_common_Error_t` | `box_error(err) as *const _` — a documented leak on the panic path; the caller must not free a borrowed pointer (37 admin accessors) |
  | any other `*mut T` / `*const T` | null |
  | anything else | compile error naming the function and asking for `fallback = ...` |

  Overrides: `#[ffi_guard(fallback = <expr>)]` replaces the value only;
  `#[ffi_guard(on_panic = |err| <expr>)]` takes full control (the closure gets
  `err: crate::common::Error` and must evaluate to the return type).
- **D3. `out_error`.** When the function has a parameter named exactly
  `out_error` of type `*mut *mut kafka_common_Error_t`, the generated
  on-panic path also writes `*out_error = box_error(err)` when the pointer is
  non-null, then returns the D2 fallback. `out_errors` (per-record arrays)
  are never written — the count is unknown on the panic path.
- **D4. Callback-style functions** (86 with a `callback:` parameter) each get
  an explicit `on_panic` that fires the callback exactly once with
  `box_error(err)` (plus a null result pointer where the typedef carries one)
  and evaluates to the return-type fallback. This mirrors the existing
  synchronous-failure convention (`admin_async_void_op`'s null-handle branch,
  the consumer `async_void_op`'s `acquire` failure). Firing on panic is only
  correct when the spawn / enqueue hand-off is the **last** thing the body
  does; where it is not, the Actor restructures the body so it is. The Critic
  checks every one of the 86 sites against its callback typedef.
- **D5. Error kind and message.** The caught panic becomes
  `Error::local_illegal_state(format!("Rust panic caught at the FFI boundary in {fn}: {msg}"))`,
  where `msg` is the `&str` / `String` payload or `"non-string panic payload"`.
  It is also logged with `log::error!` (`log` is already a dependency;
  `env_logger` prints it when `RUST_LOG` is set). The default panic hook is
  left alone, so the usual stderr line still appears first.
- **D6. Poisoned handle locks stay poisoned.** After a caught panic while a
  handle's `Mutex` was held, later calls on that handle fail through the guard
  with the poison message. This is intentional — the state behind the lock may
  be inconsistent — and the header text tells the caller to destroy and
  recreate the handle. No `into_inner()` recovery.
- **D7. The seven `assert!(count >= 0, ...)` stay.** The guard turns them
  into error returns. The tests that reached them through the `_inner`
  functions to avoid the abort now call the real `extern "C"` entry points and
  assert the error (message contains `count must not be negative`). The
  `_inner` helpers stay (they are shared code paths); the comments at the
  assert sites and on the tests change from "must panic" to "must fail rather
  than be clamped".
- **D8. Presence is enforced by a unit test** in `src/ffi/mod.rs` that reads
  the five source files with `include_str!` and requires every
  `#[unsafe(no_mangle)]` to be immediately preceded by the last line of an
  `#[ffi_guard...]` attribute. Placement rule everywhere: doc comments, then
  `#[ffi_guard...]`, then `#[unsafe(no_mangle)]`.
- **D9. Header text.** `cbindgen.toml` gains a `header = """..."""` block
  stating the contract: a Rust panic never unwinds into the caller; the
  function returns its failure value (NULL, false, -1, 0 for counts, or an
  error handle) and reports the panic through `out_error` where one exists;
  after such a failure the handle should be destroyed. The same paragraph goes
  into the `src/ffi/mod.rs` module docs. Apart from that preamble the
  generated `target/include/confluent_kafka.h` must be byte-identical before
  and after the change.
- **D10. CPython fixes** follow the Jira line references and the correct
  pattern already present at `_confluentkafka.c:5588-5597` (compute the
  epoch object, NULL-check it, then `Py_BuildValue` with `N`). Byte lengths go
  through one shared checked conversion that raises `OverflowError` above
  `INT32_MAX`.
- **D11. Out of scope for loop 79, user decision pending:** the Jira's two
  further remediation bullets — an inventory of every `unsafe` block with a
  soundness note (today 1,893 `unsafe {` blocks and 187 `unsafe fn` in
  `src/ffi/`) and an evaluation of replacing the hand-written extension with
  PyO3 (no such evaluation exists in the repository). Recorded in §4.

## 2. Work items (one commit each, `git commit --no-verify`)

### 2.1 Crate, runtime helper, header text

1. `ffi-macros/Cargo.toml` and `ffi-macros/src/lib.rs` (Apache 2.0 header,
   copyright Confluent Inc.). Add `"ffi-macros"` to `[workspace] members`.
   Root `Cargo.toml`: `ffi-macros = { path = "ffi-macros", optional = true }`
   under `[dependencies]`, and
   `ffi = ["dep:cbindgen", "dep:env_logger", "dep:ffi-macros"]`.
2. The macro: `#[proc_macro_attribute] pub fn ffi_guard(attr, item)`. Parse
   the item as `syn::ItemFn` (syn 2.0.117 parses `#[unsafe(no_mangle)]`), parse
   the optional `fallback = <Expr>` / `on_panic = <ExprClosure>` arguments,
   pick the on-panic closure per D2–D4, and emit
   `#(#attrs)* #vis #sig { crate::ffi::common::ffi_guard_or(<name>, <on_panic>, move || #block) }`
   (inside an `unsafe { }` block only where the generated closure needs it —
   the `out_error` write and `on_panic` bodies are the caller's
   responsibility). All other attributes are re-emitted unchanged. The
   hardcoded paths `crate::ffi::common::{ffi_guard_or, box_error}` are
   acceptable because the macro is only used inside this crate's `ffi` module;
   say so in the crate docs. An unsupported return type is a `compile_error!`
   that names the function.
3. `src/ffi/common.rs`:
   `pub(crate) fn ffi_guard_or<R>(fn_name: &'static str, on_panic: impl FnOnce(Error) -> R, body: impl FnOnce() -> R) -> R`
   built on `std::panic::catch_unwind(AssertUnwindSafe(body))`, and
   `pub(crate) fn panic_error(fn_name: &str, payload: &(dyn Any + Send)) -> Error`
   (D5). No fallback trait — the macro emits the fallback expression.
4. `src/ffi/mod.rs`: `pub(crate) use ffi_macros::ffi_guard;` plus the D9
   paragraph. `cbindgen.toml`: the D9 `header`.
5. Tests, in the `common.rs` tests module, on `#[ffi_guard] unsafe extern "C" fn`
   test functions **without** `no_mangle`, each body panicking: one per D2 row
   (including a `_count`-named `i32` → `0`, `*mut kafka_common_Error_t` →
   non-null handle whose message contains the function name and the panic
   text and whose code is `LocalIllegalState`, and the `*const` variant), the
   `out_error` write (D3) and the "null `out_error` is tolerated" case, an
   `on_panic` closure, a `fallback` override, a `String` and a `&str` payload,
   and a non-panicking pass-through that returns the body's value and leaves
   `out_error` null. The unknown-return-type `compile_error!` is checked by a
   `compile_fail` doctest on the macro if it is cheap; otherwise state in §6
   that it was checked by hand.

### 2.2 Apply to all 811 entry points; flip the panic tests; presence test

1. Insert `#[ffi_guard]` directly above every `#[unsafe(no_mangle)]` in the
   five files (a mechanical edit), add `use crate::ffi::ffi_guard;` (or
   `super::ffi_guard`) per file, then hand-edit: the 86 callback sites (D4);
   any `i32`/`i64` accessor that is a count but whose name does not end in
   `_count` (grep the `-> i32` / `-> i64` functions, decide, record the
   exceptions in §6); whatever the compile error flags.
2. Header check (D9): copy `target/include/confluent_kafka.h` before this
   step, rebuild with `--features ffi`, diff — only the new preamble may
   differ.
3. Flip the tests (D7) in `src/ffi/producer.rs`:
   `test_send_offsets_to_transaction_negative_count_panics` → calls
   `kafka_producer_Producer_send_offsets_to_transaction` with `count = -1`,
   asserts a non-null error whose message contains
   `count must not be negative`, frees it with the error destroy function;
   `..._async_negative_count_panics` → calls the `_async` entry point and
   asserts `capture_op_result` received exactly one error with that message;
   `assert_send_batch_panics` → `assert_send_batch_fails`, calling
   `kafka_producer_Producer_send_batch` and asserting `-1`, used by both
   `test_send_batch_null_producer_panics` and
   `test_send_batch_negative_count_panics`. Rename `*_panics` →
   `*_returns_error`. Update the comments at the seven assert sites.
4. Presence test (D8) in `src/ffi/mod.rs`.

### 2.3 C boundary test

`bindings/c/tests/test_mock_producer.c` (Unity): add
`test_send_batch_negative_count_returns_error` — `kafka_producer_Producer_send_batch(producer, records, -1, futures, errors)`
returns `-1` and the test process keeps running; register it in the runner's
`main`. This is the Jira's "a test that panics across the boundary", from
real C.

### 2.4 CPython extension (`bindings/python/_confluentkafka.c`)

1. **Finding 2** (lines 2826–2831 and 2853–2858): build the epoch object
   first, NULL-check it, then `Py_BuildValue("(LsN)", ...)` /
   `Py_BuildValue("(LLN)", ...)`, exactly like lines 5588–5597. Unit test if
   `MockConsumer` can produce an epoch-bearing `committed()` /
   `offsets_for_times()` entry: with an epoch above 256 (not a cached small
   int), `sys.getrefcount` of the epoch object must equal the leak-free
   expectation. If the mock cannot produce such an entry, say so in §6.
2. **Finding 3** (lines 807–808): NULL-check the `PyMem_RawMalloc`, set
   `PyErr_NoMemory()` and fail cleanly without leaving
   `last_accumulating_batch` (or any other producer state) pointing at a
   half-built node — read the surrounding state machine first.
3. **Finding 4** (lines 153, 160, 3282, 3289, 6432, 6434, plus the `hmac_len`
   casts at 6476 and 6488, which are the same class): one shared checked
   conversion raising `OverflowError("<what> exceeds 2 GiB")`. Unit test:
   `bytes(2**31)` as a record value (zero-filled, lazily mapped, cheap) raises
   `OverflowError`; guard with `sys.maxsize > 2**32` and skip on
   `MemoryError`. The list-count `(int32_t)n` casts are not byte buffers and
   are out of scope; note in §6 if any were touched anyway.

### 2.5 This file

Actor 79 appends §6 as it goes (one item per premise of this plan the code
proved wrong or incomplete). The Manager keeps §5.

## 3. Gates and constraints

- Toolchain 1.95.0, edition 2024, workspace members today: `.`, `generator`,
  `xtask`, `consumer-perf`, `multilanguage-test-server`.
- Rust: `cargo build --features ffi`; `cargo test --features ffi --lib` (the
  `src/ffi` tests do not exist without the feature — a `0 passed` on an `ffi::`
  filter means "not run"); `cargo test --workspace`; `cargo xtask lint`
  (workspace, `--all-features`, so it covers the new crate and the ffi module);
  `cargo xtask format-check`; `cargo test -p xtask && cargo xtask check-bindings`
  (the binding scanner does not parse Rust source, so the attribute cannot
  confuse it — run it anyway).
- Header: `cargo build --features ffi` regenerates
  `target/include/confluent_kafka.h`; diff against the copy from before §2.2.
- C: `make build-c`, then `cd bindings/c/build && ctest --output-on-failure`.
  Known environmental failure: the case at `test_kafka_admin.c:481` assumes no
  broker on 9092, and the user's `kafka-perf-local` container is up on 9092.
  Do NOT stop that container; report that single failure as environmental.
- Python: `. venv/bin/activate` (the venv is at the repository root),
  `cargo build --all-features --release`,
  `make -C bindings/python build RUST_PROJECT_ROOT=$PWD PROFILE=release`, then
  `cd bindings/python && ../../venv/bin/python -m pytest test/unit -q`. Run
  pytest through `python -m` from `bindings/python` so the freshly built `.so`
  wins over a stale site-packages copy.
- Long commands run in the background with a log file and are polled; every
  tool call stays under ten minutes (600 s watchdog). One commit per work
  item, `git commit --no-verify` (the pre-commit hook runs a multi-minute
  Docker build). Never `git add -A`; stage named paths only. Never commit:
  `APPSEC-7665-CHANGES.md`, `INVESTIGATION-*.md`, `OPEN-BUGS.md`, `issues.md`,
  `examples/eos_app.rs`, `bindings/python/examples/`,
  `bindings/python/macos_compat/`, anything under `.claude/`. Do not edit
  `CLAUDE.md` or `.claude/rules/*`. No `rm`, `rmdir`, `git push`,
  `git reset --hard`, `git rebase`, branch deletion.
- Every commit ends with a `Co-Authored-By:` trailer naming the model that
  wrote it.

## 4. Justified deviations from Java (definition-of-done §7) and scope notes

1. **New crate `ffi-macros`, helpers `ffi_guard_or` / `panic_error`.** No Java
   counterpart: Java has no C boundary, and nothing in `src/ffi/` has one
   either. Needed because Rust cannot unwind into C.
2. **`*const kafka_common_Error_t` fallback leaks one boxed error per caught
   panic** (D2). The alternative, null, would report success.
3. **The seven `assert!(count >= 0)` remain** although CLAUDE.md's FFI rules
   say not to check programming preconditions; they were kept deliberately for
   exactly-once safety. Their observable behaviour changes from abort to error
   return (D7).
4. **Out of scope, user decision pending:** the `unsafe` inventory with
   soundness notes and the PyO3 evaluation (D11).
5. **Panics inside tokio tasks spawned by the FFI** are contained by tokio
   and surface as a dropped completion; this plan does not change that path.
   It is pre-existing and separate from the boundary guard.

## 5. Status

- 2026-09-25: branch cut from `origin/master` `dfade0be`; the user approved
  the proc-macro approach; plan written; Actor 79 spawned.

## 6. Implementation notes (Actor 79)

Where the code proves a premise of this plan incomplete or wrong, follow the
code and record the difference here.
