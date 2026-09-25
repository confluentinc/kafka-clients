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
- 2026-09-25: Actor 79 finished: `931b5259` (§2.1), `7294d8d0` (§2.2),
  `4fb04f1f` (§2.3), `d5fcdeb2` (§2.4); 18 files, +3153/−149; 21 notes in §6.
  Gates on `d5fcdeb2`: `cargo test --features ffi --lib` 4273 passed / 0
  failed / 2 ignored; workspace tests by named target 4291 / 0 / 3 plus 7
  doctests; format-check clean; clippy (default and `--all-features`,
  `-D warnings`) clean when the untracked `examples/eos_app.rs` is left out —
  `cargo xtask lint` itself exits 1 on that file (§6 note 7, environmental);
  `check-bindings` clean; C ctest 7/7 and Unity 312 / 0; Python unit tests 371
  passed, 2 pre-existing skips; header identical to master's apart from the
  19-line preamble. Manager accepts §6 note 12 as an amendment to D6:
  `kafka_producer_Producer_destroy` may recover poisoned locks because it tears
  the state down rather than using it. §6 note 18 retires the "known
  environmental ctest failure" in §3: that case bootstraps from `127.0.0.1:1`
  since #191 (`5fc83544`). Critic 79 spawned over `be795fa3..d5fcdeb2`.

## 6. Implementation notes (Actor 79)

Where the code proves a premise of this plan incomplete or wrong, follow the
code and record the difference here.

1. **§2.1 — `#![deny(warnings)]` forces two transitional allows.** The crate
   root denies warnings, so until §2.2 applies the attribute the non-test build
   rejects the unused `pub(crate) use ffi_macros::ffi_guard;` and the uncalled
   `ffi_guard_or` / `panic_error`. §2.1 puts `#[allow(unused_imports)]` /
   `#[allow(dead_code)]` on exactly those three items, each with a comment
   naming §2.2, which removes them.
2. **§2.1 — D2's counts.** Of the 811 `#[unsafe(no_mangle)]` signatures, 108
   return `*mut kafka_common_Error_t` (plan: 125) and 35 return
   `*const kafka_common_Error_t` (plan: 37). Nothing depends on the numbers —
   the fallback is derived from the type — and D2 covers every return type in
   the five files (195 `*const T`, 169 `()`, 156 `i32`, 61 `bool`, 44 `*mut T`,
   33 `i64`, 5 `i16`, 4 `f64`, 1 `kafka_common_ErrorCode_t`, plus the 108 and
   35), so no entry point needs a `fallback` override.
3. **§2.1 — inner attributes.** syn files a body's inner attributes
   (`#![...]`) under `ItemFn::attrs`, so emitting `#(#attrs)*` before the
   signature, as the step-2 template reads, would hoist them out of the body,
   where they do not parse. The macro splits them off and re-emits them first
   inside the new body (macro unit test; no FFI function has one today).
4. **§2.1 — the `compile_fail` doctest was cheap, so it exists** (unsupported
   return type), next to a passing doctest of the expansion; both carry
   scaffold `crate::common` / `crate::ffi::common` modules because the macro
   emits crate paths. Also checked by hand in a scratch crate: an
   `extern "C" fn answer() -> u8` without an override fails with
   "`#[ffi_guard]` cannot derive a panic fallback for `answer`, which returns
   `u8`: add `#[ffi_guard(fallback = <expr>)]` or
   `#[ffi_guard(on_panic = |err| <expr>)]`", spanned on the return type.
5. **§2.1 — argument checking beyond the plan.** An unknown argument, a
   duplicate, or `fallback` together with `on_panic` is a compile error with a
   message, and so is a parameter named `out_error` whose type is not
   `*mut *mut kafka_common_Error_t` (D3 keys on name *and* type; ignoring a
   mistyped one would silently drop the error report). `fallback` replaces
   only the value, so D3's `out_error` store still happens with it; `on_panic`
   gets nothing generated around it — the literal reading of D2's overrides.
6. **§2.1 — a panic payload whose destructor panics.** Dropping the payload
   after reading its message can panic in turn (a `panic_any` value with a
   panicking `Drop`), and that second panic would unwind out of
   `ffi_guard_or` into C. The payload is dropped inside a second
   `catch_unwind`, and a nested payload is forgotten instead of dropped. Test:
   `ffi_guard_survives_a_payload_whose_destructor_panics`.
7. **§3 gates — the working tree holds an uncompilable untracked example.**
   `examples/eos_app.rs` (untracked, on the do-not-touch list) uses APIs this
   tree does not have (`consumer::new_consumer`,
   `ConsumerConfig::from_properties`, …), so `cargo test --workspace` and the
   `--all-targets` clippy passes of `cargo xtask lint` fail on it before they
   reach anything in this change. The gates were therefore run with every
   other target named: `cargo test --workspace --lib --bins --tests`,
   `cargo test --workspace --doc`, `cargo build` of the 15 tracked examples,
   and both clippy passes of `cargo xtask lint` (default and
   `--all-features`, `-D warnings`) with `--lib --bins --tests --benches` plus
   the 15 examples, and the xtask clippy pass; `cargo xtask lint` itself was
   still run for its doc-hygiene and module-path-hygiene steps, which pass
   before its clippy step reaches the file.
8. **§2.2 — D4 applies to 80 of the 86 callback sites; six keep the plain
   guard.** Their synchronous failures are documented as *not* invoking the
   callback, so firing it on a panic would break that contract:
   `kafka_consumer_Consumer_commit_async_with_callback` and
   `..._commit_async_offsets_with_callback` report through the returned error;
   `kafka_producer_Producer_send_with_callback` and
   `kafka_producer_Producer_send_async` through `out_error` ("`callback` is
   **not** invoked"); `kafka_producer_Producer_send_batch_async` through
   `out_errors[i]` and its return value; and
   `kafka_producer_RecordMetadata_copy`'s callback is a field sink with no
   error parameter. D2/D3 give them the error handle, `out_error` plus
   null/unit, `-1`, and log-only respectively. Of the other 80, 55 fire
   `(null, error, user_data)`, 23 `(error, user_data)`,
   `kafka_consumer_Consumer_position_async` fires `(0, error, user_data)` (its
   typedef leads with the `i64` position), and
   `kafka_producer_FutureRecordMetadata_get_all_async` fires in its own shape
   (note 9).
9. **§2.2 — `get_all_async` reports a panic in its callback's shape.** The
   typedef delivers parallel arrays, so the on-panic path
   (`fire_get_all_callback_with_error`) passes `count` null metadata entries
   and `count` copies of the panic error. A negative `count` — the precondition
   whose assert panics — has no length to report at: the callback still fires
   once, with empty arrays, and the panic is visible only in the guard's log
   line. Tests: `test_get_all_async_null_futures_reports_panic_through_callback`,
   `test_get_all_async_negative_count_fires_callback_once`.
10. **§2.2 — D4's "spawn is the last fallible step" did not hold in the
    producer.** `flush_or_close_async`, `kafka_producer_Producer_partitions_for_async`
    and `with_txn_control_async` spawned their task and only then called
    `register_pending_task`, whose `pending_tasks.lock().unwrap()` panics once
    that lock is poisoned: the guard would fire the callback while the spawned
    task delivered a second completion, and the unregistered task would escape
    `destroy`'s join. Now a two-phase registration: `reserve_pending_task`
    locks, prunes finished handles and reserves room *before* the spawn, and
    the caller `push`es the `JoinHandle` afterwards, which cannot fail
    (`build_producer_handle` uses it too, for uniformity). Test:
    `test_flush_async_with_poisoned_task_list_fires_callback_once`. The
    consumer and admin async paths needed no change: between their guard
    `acquire` (or null check) and the spawn only clones and pointer casts run,
    and admin's `submit` — which can panic — runs before it, with `complete`
    never invoked on that path.
11. **§2.2 — the transaction-control flag leaked on a panic before the
    spawn.** `with_txn_control_async` took `txn_control_busy` by CAS but created
    its `TxnControlAsyncGuard` inside the spawned task, so a panic in `prepare`
    or on a poisoned `kind` lock left the flag set, and every later control call
    on the handle was rejected as concurrent. The guard is now created right
    after the CAS and moved into the task; on the calling thread it is released
    by unwinding before `#[ffi_guard]` fires the callback. Test:
    `test_commit_transaction_async_panic_before_spawn_releases_txn_flag`.
12. **§2.2 — D6 is incomplete for `kafka_producer_Producer_destroy`.** The
    header tells the caller to destroy a handle after a caught panic, but
    destroy did `kind.lock().unwrap()` (a panic on a poisoned handle, unwinding
    through the teardown and dropping the producer before its runtime while a
    task could still be using it) and read a poisoned `pending_tasks` as empty
    (skipping the join, the same use-after-free window). Destroy now takes both
    with `into_inner().unwrap_or_else(PoisonError::into_inner)`: the one
    recovery D6's "no `into_inner()`" rule has to allow, since it tears the
    state down rather than using it. The consumer and admin destroys take no
    lock. Test: `test_destroy_joins_pending_tasks_on_a_poisoned_handle`.
13. **§2.2 — count audit: no exceptions.** 194 exported functions return an
    integer; 91 end in `_count`. The other 103 are ids, partitions, offsets,
    timestamps, epochs, sizes and enum codes, whose failure value is already
    -1 where they have one (`kafka_admin_TopicMetadataAndConfig_num_partitions`
    documents -1 when the metadata is unavailable;
    `kafka_consumer_ConsumerRecord_serialized_key_size` is -1 for a null key),
    plus `kafka_producer_Producer_send_batch` / `..._send_batch_async`, which
    return how many of the caller's own `count` records were accepted, where -1
    is a distinct failure. So no `fallback` override was needed anywhere.
14. **§2.2 — `# Panics` docs on exported functions stay verbatim.** Five
    producer entry points (`send_batch`, `send_batch_async`,
    `FutureRecordMetadata_get_all_async`, `send_offsets_to_transaction`,
    `..._async`) document "Panics if ...". Rewording them would change the
    header, which D9 requires byte-identical apart from the preamble. Kept
    verbatim they still name the violated precondition, and the preamble says
    what the caller observes (a failure value, not an abort). The docs of the
    three non-exported `_inner` helpers were updated.
15. **§2.2 — D7 named two `send_batch` panic tests; there were five.**
    `null_producer`, `null_records`, `null_out_futures`, `null_out_errors` and
    `negative_count` all became `*_returns_error` through
    `assert_send_batch_fails`, which calls the real entry point (asserting `-1`)
    and, because `send_batch` has no `out_error`, checks the message by running
    `send_batch_inner` through `ffi_guard_or` with an `on_panic` that keeps the
    error.
16. **§2.2 — `send_batch` / `send_batch_async` after a panic mid-loop.** The
    `-1` carries no index, so entries already written to `out_futures` /
    `out_errors` go unreported (a caller that does not zero the arrays and free
    what is non-null leaks them), and records `send_batch_async` had already
    accepted still deliver their callbacks. Reachable only through a bug — the
    asserts run before the first write — and not documented in the header
    (D9); the preamble's "destroy that handle" is the guidance.
17. **§2.2 — §4.5 extends to the dispatcher thread.** It runs completion jobs
    without catching, so a panicking job ends that thread; `enqueue_or_run_inline`
    then runs later jobs inline on the task that produced them. Nothing crosses
    into C, so this change leaves it alone.
18. **§3 gates — the known environmental ctest failure no longer occurs.** The
    B3 case of `bindings/c/tests/test_kafka_admin.c` (now
    `test_kafka_admin_b3_empty_batches_need_no_broker`, line 488) connects to
    `NO_BROKER_BOOTSTRAP` (`127.0.0.1:1`) rather than `localhost:9092` since
    `5fc83544` (#191), as its comment at 477–487 explains. With
    `kafka-perf-local` accepting connections on 9092, all 7 ctest executables
    pass, so there is no environmental failure to report.
19. **§2.4 finding 2 — only `committed()` can produce an epoch-bearing
    entry.** `MockConsumer.offsets_for_times` fails with "Not implemented yet."
    exactly as Java's does (`MockConsumer.java:535-536`), so
    `py_OffsetAndTimestampMap_drain` cannot be reached from a unit test. Both
    drains got the same fix (the pattern at 5588–5597, `(long long)` casts
    included). `test_committed_does_not_leak_the_leader_epoch` covers
    `py_OffsetMap_drain` with epoch 1000 and failed (`3 == 2`) against the
    unfixed extension; the timestamp drain is covered by inspection and by
    `check-bindings`.
20. **§2.4 finding 3 — the same unchecked allocation in
    `py_Producer_on_space_available`.** Its `PyMem_RawRealloc` of the list of
    senders waiting for space (which starts from empty after every drain,
    because the send task takes the list) was assigned over the list and then
    written through, so a failure crashed and lost the callbacks already
    waiting. It now reallocates into a temporary and, on failure, keeps the
    list and tells the caller not to wait (returns `True`) instead of raising:
    `Producer_send` has already queued the record, so a `MemoryError` there
    would report as failed a send that is still delivered and still fires
    `on_delivery`. Skipping the wait only lets the C queue pass its soft bound;
    the next batch node that cannot be allocated fails cleanly. At line 807 the
    node is checked before it is linked, so on failure nothing is linked,
    stored or counted, both queue references are given back, and `MemoryError`
    is raised. Both paths are tested under `_testcapi.set_nomemory` in a child
    interpreter (skipped where `_testcapi` is missing); both crashed with
    SIGSEGV against the unfixed extension.
21. **§2.4 finding 4 — the eight listed casts are all the byte-length casts,
    and all eight are tested.** The file's other `(int32_t)` casts are element
    counts, ids, partitions, epochs, enum codes and one stack-buffer size, and
    were left alone. The limit is `INT32_MAX`, so a buffer of exactly 2 GiB,
    the tests' `bytes(2**31)`, is already rejected; the plan's message "exceeds
    2 GiB" is kept, read as "is 2 GiB or more". Beyond the record value the
    plan asked for, each of the eight sites (`ProducerRecord` key and value,
    `MockConsumer.add_record` key and value, SCRAM password and salt, renew and
    expire hmac) has an assertion, sharing a `two_gib_bytes` fixture in the new
    `bindings/python/test/unit/conftest.py` that skips on a 32-bit Python and
    on `MemoryError`. Each assertion matches a message only the new check
    produces, and each of the three tests failed against the unfixed extension.
22. **Critic 79 issue 1 (round 2) — the preamble promised a panic callback
    that five functions never fire.** It said a function that reports
    ordinary failures through a completion callback "reports the panic
    through that callback instead, exactly once". That holds for the 80 D4
    sites and not for five of note 8's six plain-guard sites
    (`kafka_producer_Producer_send_async`, `..._send_with_callback`,
    `..._send_batch_async`,
    `kafka_consumer_Consumer_commit_async_with_callback`,
    `..._commit_async_offsets_with_callback`); the sixth,
    `kafka_producer_RecordMetadata_copy`, reports no failure through its
    callback, so the sentence never covered it. The behaviour stays; the text
    changed.
    - **Wording.** Both copies (`cbindgen.toml` and the `src/ffi/mod.rs`
      module docs) now add: "…exactly once, unless its documentation says that
      a synchronous failure does not invoke the callback. Such a function
      reports the panic as a synchronous failure, through its return value and
      out_error where it has one, and does not invoke the callback." The rest
      of the preamble is unchanged.
    - **Per-function docs.** Each of the five gained one sentence saying what a
      caught panic produces there: `send_with_callback` returns null with the
      error in `*out_error`; `send_async` writes `*out_error` only, so with a
      null `out_error` the panic is only logged; `send_batch_async` returns
      `-1` and stores nothing for the panic in `out_errors`; the two commits
      return the error handle, and `user_data_destroy` still fires. None fires
      the callback. "Does not invoke the callback" is what the guard does. A
      panic raised after the callback was handed on can still see it delivered:
      - note 16's mid-loop `send_batch_async` case (hence "not invoked *for
        it*" in that sentence);
      - the Critic's not-filed panic after registration in
        `commit_async_with_callback`;
      - in `send_with_callback`, the `transaction_manager.lock().unwrap()` at
        `src/producer/kafka_producer.rs:1936`, which runs after `append`
        (`:1897`) has moved the callback into a batch: on a poisoned
        transaction-manager lock the function returns null with the panic in
        `*out_error`, and the batch still fires the callback when it
        completes;
      - in `send_async`, the wake of the submission receiver after the request
        is queued (the `send_async` bullet below): the submission task still
        sends the record and fires its callback once.

      Only a bug reaches the first three, and only a failure in tokio or the
      OS reaches the fourth. The preamble's "destroy that handle" covers all
      four. `send_async`, whose only unwinding panic is the fourth, now also
      says in its own docs that the callback still fires (note 23).
    - **Header diff against `a0b5065b`:** 21 lines added and 3 removed, in six
      hunks (15744 → 15762 lines). The preamble's last three lines become seven
      (header lines 11–17), and the five sentences add 14 lines: 3 each for
      `commit_async_with_callback`, `commit_async_offsets_with_callback`,
      `send_with_callback` and `send_async`, and 2 for `send_batch_async`. No
      other line changed. This amends note 14's "byte-identical apart from the
      preamble" by exactly those five sentences; the `# Panics` docs stay
      verbatim.
    - **Tests (four of the five).**
      `test_send_with_callback_panic_is_a_synchronous_failure` poisons the
      `kind` lock and asserts null, a LOCAL_ILLEGAL_STATE error in `out_error`
      whose message starts with the function name and contains `PoisonError`,
      and a callback counter (in `user_data`) still at 0 after `destroy` plus
      200 ms. `test_send_batch_async_panic_is_a_synchronous_failure` uses the
      negative-`count` precondition and asserts `-1`, an `out_errors` slot
      still holding its sentinel, and the counter at 0. The two consumer tests
      share `assert_commit_panic_is_a_synchronous_failure`. It makes the call
      inside another tokio runtime, which the synchronous consumer API does not
      support: `sync_void_op`'s `block_on` panics ("Cannot start a runtime from
      within a runtime") before it polls the commit. It then asserts a
      LOCAL_ILLEGAL_STATE error handle naming the function, no callback, and
      exactly one `user_data_destroy` (the unwind drops the adapter with the
      unpolled future). The offsets test marshals valid offsets, so the panic
      comes after the one synchronous failure its docs name. Poisoning was not
      an option there: neither entry point takes a lock before the commit
      starts, and the access guard is an atomic CAS that fails with an
      ordinary error.
    - **`send_async`: its one unwinding panic comes after the hand-off.**
      (Round 2 wrote here that `send_async` could not be pinned, because
      tokio's `UnboundedSender::send` could only abort. Critic 79 issue 2
      showed that was wrong; note 23 records the fix.) It takes no lock, so
      poisoning does not reach it, and it has no `block_on`, so the nested
      runtime does not either. It builds its record through
      `Result`-returning code, and allocation failure, the
      `slice::from_raw_parts` precondition checks and the message-counter
      overflow in `UnboundedSender::send` (`sync/mpsc/unbounded.rs:570`)
      abort rather than unwind. But `send` can unwind after it has queued the
      request. In tokio 1.52.0 (the version in `Cargo.lock`) it calls
      `Chan::send`, which pushes the request and then wakes the receiver
      (`sync/mpsc/chan.rs:528-534`), and `AtomicWaker::wake` lets a panicking
      waker unwind: "If wake panics, we've consumed the waker which is a
      legitimate outcome" (`sync/task/atomic_waker.rs:303-308`). A test that
      owns the receiver can register such a waker, so the panic can be
      pinned without a hook in production code. In production the waker is
      the submission task's tokio waker. Short of a bug in tokio (the
      `panic!("inconsistent state in unpark")` at
      `runtime/scheduler/multi_thread/park.rs:288`), its one panic is
      `expect("failed to wake I/O driver")` (`runtime/io/driver.rs:260`): the
      OS failing to signal the I/O driver, raised after `push_remote_task`
      has already put the task on the scheduler's inject queue
      (`runtime/scheduler/multi_thread/worker.rs:1333-1334`). The task still
      runs once a worker next takes it from that queue, so the record is sent
      and its callback fires exactly once. That firing is not ordered against
      the panic report in `*out_error`: as on the success path, the callback
      can run on the dispatcher thread before `send_async` returns. The same
      firing is why the plain guard is the only correct guard here: the
      submission task makes its own at-most-once `fired` flag for each
      request (`src/ffi/producer.rs:797`), which a firing from the guard would
      not share, so a D4-style `on_panic` would fire the callback twice and
      free `user_data` twice.
    - **Teeth.** With each test's callback assertion inverted (`0` → `1`), all
      four fail on it. With the plain guard swapped for a D4-style `on_panic`
      that fires the callback (the producer ones still writing `out_error`, so
      only the callback assertion can differ), all four fail on the callback
      assertion (`left: 1, right: 0`) after their other assertions pass. With
      `#[ffi_guard]` removed from the four functions, each test's process
      aborts (exit 134, "panic in a function that cannot unwind"), which is
      how these functions behaved before `7294d8d0`.
    - `COMMENTS.DONE.79.md` and `COMMENTS.79.md` match `.gitignore`'s
      `/COMMENTS*\.md`, so the move of issue 1 exists in the working tree only.
23. **Critic 79 issue 2 (round 3) — `send_async`'s post-queue panic is
    pinned, and its docs say the callback still fires.** Round 2 left
    `send_async` unpinned on a false premise; note 22's `send_async` bullet
    now gives the correct account. The guard is unchanged: `send_async`
    keeps the plain `#[ffi_guard]` (note 8).
    - **Test.** `test_send_async_panic_after_queueing_is_reported_in_out_error`
      (`src/ffi/producer.rs`, after the round-2
      `test_send_batch_async_panic_is_a_synchronous_failure`) builds its
      handle as `dead_submission_handle` does, but keeps both receivers, so
      no submission task or dispatcher thread exists. Polling the empty
      submission channel registers a waker whose `wake` panics; the test
      then calls `send_async` with a valid record and `count_send_callback`.
      It asserts:
      - a non-null `out_error`, whose code is
        `kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE` and whose message starts
        "Rust panic caught at the FFI boundary in
        kafka_producer_Producer_send_async:" and contains the waker's panic
        text;
      - `queued_sends` at 1;
      - exactly one request on the channel, a `SubmitRequest::Send`
        carrying this call's `user_data`;
      - the callback counter at 0, once the request and the receiver are
        dropped, the handle is reclaimed and the completion queue has been
        run.

      Three of these checks go beyond the brief. `queued_sends` and
      `user_data` show the request is in the state a successful call leaves
      it in. Running the completion queue, as the dispatcher would, makes a
      callback fired through the dispatcher count too.
    - **What the counter at 0 shows.** Only that the guard did not invoke the
      callback. The test holds the receiver, so nothing delivers the queued
      record. In production the submission task delivers it and fires its
      callback exactly once, so the test's doc comment names "one callback,
      from the delivery" as the contract, not "no callback".
    - **Docs.** Only `send_async`'s panic sentence changed
      (`src/ffi/producer.rs:1718-1722`). It now reads: "A caught panic is
      reported the same way: `*out_error` receives a
      `kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE` error (with a null
      `out_error` it is only logged), and it is never reported through
      `callback`. The only panic reachable in this function happens after the
      record was queued, so that record is still sent and `callback` still
      fires for it, exactly once." Both preamble copies are unchanged.
      - *"The only panic reachable".* Checked against the body before
        writing it.
        - Cannot panic: the null checks, the `CStr` conversion, the
          `queued_sends` updates, `producer_handle` (a pointer cast),
          `box_error` (whose one `unwrap` is on `CString::new("")`) and the
          record builder (`build()` and `with_options` return `Result`).
        - Abort instead of unwinding: allocation failure, the
          `slice::from_raw_parts` precondition checks (`panic_nounwind`,
          which the null checks and `i32` lengths also make unreachable) and
          tokio's message-counter overflow.

        That leaves the wake after the push. The claim sets aside tokio's own
        invariant checks, which fire only on a bug in tokio:
        - the `debug_assert!`s that `find_block` reaches before the write
          (`sync/mpsc/block.rs:130, 139, 301`, compiled out of a release
          build);
        - the `park.rs:288` panic above, which comes after the push anyway.
      - *"Still fires".* This holds even if the caller destroys the handle
        straight after the call:
        - `destroy` drops `submit_tx` first;
        - the submission task, which is in `pending_tasks`
          (`src/ffi/producer.rs:928-930`), still receives the requests
          already queued before `recv()` returns `None`;
        - `destroy` joins it, then drops the producer (whose close fires any
          callback still in flight), and only then detaches the dispatcher.

        The callback then reports the close's error if the record had not
        completed, as it would for any send queued just before `destroy`.
      - *Telling this error apart.* `send_async` reports one other
        `LOCAL_ILLEGAL_STATE` error, "producer is closed", and that one does
        not fire the callback. A C caller tells the two apart only by the
        message prefix the preamble documents. The sentence before the new
        one, "`out_error` reports only synchronous validation errors (null
        topic / bad key/value length), in which case `callback` is **not**
        invoked", does not mention the closed-producer case either. That
        sentence is already on `master` (`dfade0be`). It stays as it is,
        because the brief allowed no other doc change; it is left for the
        Manager.
    - **Header diff against `de90b540`:** one hunk,
      `14539,14540c14539,14542`: 2 lines removed and 4 added
      (15762 → 15764 lines), all in the sentence above. No other line
      changed.
    - **Teeth.** Each mutation ran the new test on its own.
      - With the final assertion inverted (`0` → `1`), the test fails on that
        assertion (`left: 0, right: 1`).
      - With the plain guard swapped for a D4-style `on_panic` that writes
        `*out_error` and fires the callback, it fails on the callback
        assertion (`left: 1, right: 0`) after every other assertion passes.
      - With `#[ffi_guard]` removed, the process aborts ("panic in a
        function that cannot unwind", SIGABRT).

      After each mutation `src/ffi/producer.rs` was restored from a saved
      copy and confirmed byte-identical (sha256).
    - `COMMENTS.DONE.79.md` now holds issue 2, plus the corrected
      `send_async` clause of issue 1's round-2 resolution. `COMMENTS.79.md`
      keeps a pointer where issue 2 was, and Note A, which is the Manager's
      to settle. Both files are gitignored (note 22).
