// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Data-integrity testing: every record a writer sends must be read back
//! byte for byte, exactly as often as its delivery outcome allows, in order,
//! and never when it was aborted.
//!
//! - [`record`]: the self-checking record format (identity header + SHA-256).
//! - [`checker`]: the [`Ledger`](checker::Ledger) of outcomes and the verdict.
//! - [`workload`]: the writers, the live group, the copiers, the final
//!   read-back, and the [`run`](workload::run) that drives them.
//!
//! Used by the `integrity_test` integration tests (seconds each, in CI) and by
//! the long-running `integrity` target (`cargo xtask integrity-test`), against
//! a local cluster or any external one. See `tests/integrity/README.md`.
//!
//! Not part of `common/mod.rs`: that module is compiled into the `chaos` and
//! `performance` targets too, which do not use this one, and an unused module
//! there would need an `#[expect(dead_code)]` the integration target would
//! leave unfulfilled. Each target that uses it includes it by path.

pub mod checker;
pub mod record;
pub mod workload;
