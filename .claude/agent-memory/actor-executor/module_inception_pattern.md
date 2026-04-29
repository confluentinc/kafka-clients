---
name: Module-inception in this project
description: When a Java class shares a name with its parent package, the Rust translation needs an explicit clippy allow to satisfy CLAUDE.md rule 2.
type: feedback
---

CLAUDE.md rule 2 mandates each Java class lives in its own file, with imports going through the parent module re-export rather than the file path. This means files like:
- `common/header/header.rs` (defines `Header` trait, parent module is `header`)
- `common/utils/utils.rs` (defines static helpers from Java's `Utils.java`, parent module is `utils`)

trigger `clippy::module_inception`. Add `#[allow(clippy::module_inception)]` on the child `mod` declaration with a comment pointing back to CLAUDE.md.

## Why
The lint exists because most code that shadows the parent name is unintentional. In our case it's deliberate and load-bearing — the parent module re-exports the type via `pub use header::Header;` so external imports read `use crate::common::header::Header;`.

## How to apply
Whenever I create a Rust file whose stem matches its parent directory name, immediately add the `#[allow]` annotation. Don't let it sit until lint runs.
