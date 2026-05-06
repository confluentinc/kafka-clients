---
name: Phase 1 lint gotchas
description: Recurring clippy patterns and fixes encountered while translating common/utils, common/config, common/header for the Rust Kafka producer
type: feedback
---

Each time I forgot one of these the lint pass kicked it back. `#![deny(warnings)]` is on, so every clippy lint is a hard error.

**Empty line after a `///` doc comment.** `clippy::empty_line_after_doc_comments` fires when there's a blank line between a doc-comment block and the item it documents. Fix: convert the trailing comment to `//` (regular comment) or move the blank line up.

**`new()` returning anything other than `Self`.** `clippy::new_ret_no_self` fires for singleton-style accessors like `SystemTime::new() -> Arc<dyn Time>`. Fix: rename to `instance()` (we did this for `SystemTime`).

**Same-name child module.** `clippy::module_inception` fires when `mod foo { mod foo; }`. CLAUDE.md rule 2 mandates each Java class gets its own file, so `header/header.rs` and `utils/utils.rs` are unavoidable. Fix: `#[allow(clippy::module_inception)]` on the inner `mod` declaration with a CLAUDE.md reference comment.

**`& 0xff` masking on a `u8`.** `clippy::identity_op` fires because Rust `u8` is already 8-bit unsigned. Java's `byte` is signed (`i8`) so the mask is necessary there. When literal-translating Java code, drop the `& 0xff` for `u8` operands but keep it for `i8`.

**`vec![0u8; n]` instead of `Vec::with_capacity(n) + resize(n, 0)`.** `clippy::slow_vector_initialization` fires on the longer form.

**`iter().any(|x| x == y)` on a fixed slice.** `clippy::manual_contains` prefers `.contains(&y)` for slice/array literals.

**Duplicate `from_iter` method.** `clippy::should_implement_trait` fires when a `pub fn from_iter<I: IntoIterator>(...)` exists alongside no `impl FromIterator`. Fix: rename inherent method (e.g. `from_headers`) and add `impl FromIterator`.

**`approx_constant`.** `3.14` triggers it. Use `2.5` or `std::f64::consts::PI` in test fixtures.

**`collapsible_if` with `&&`.** When you have `if let Some(x) = ... { if let Some(y) = x.foo() { ... } }`, edition 2024 clippy wants the merged form: `if let Some(x) = ... && let Some(y) = x.foo() { ... }`.

**Dead code in `#![deny(warnings)]` mode.** Unused private helper functions or fields will fail the build, not just warn. Either use them or annotate `#[allow(dead_code)]` on the field/method (with a comment explaining why it's preserved for parity with Java).

**`#[derive(Debug)]` on traits used inside `Result::unwrap_err`.** When a Rust function returns `Result<T, E>` and tests call `.unwrap_err()`, the surrounding `T` must be `Debug`. If `T` contains an `Arc<dyn SomeTrait>`, the trait needs `Debug` as a supertrait — make it `pub trait Foo: Debug + ...`.

## Why
Every one of these cost me one extra lint cycle (and a re-format) during Phase 1. None are documented in CLAUDE.md but all of them are enforced by the project's `#![deny(warnings)]` + clippy stance.

## How to apply
Before committing, run `cargo xtask lint` and `cargo xtask format-check` together. If either fails, fix using the patterns above before re-running tests.
