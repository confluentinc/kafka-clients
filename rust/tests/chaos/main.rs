// Copyright 2025 Confluent Inc.
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

//! Chaos / fault-injection tests for the producer and KIP-848 consumer.
//!
//! Stands up a dedicated (non-pooled) multi-broker Docker cluster, runs
//! in-process produce/consume workloads through the Rust client, injects
//! broker faults (clean/unclean stop, restart, leader migration, …), and
//! verifies no acknowledged/committed record is lost.
//!
//! Design: `design/current/chaos-fault-injection-harness.md`.
//!
//! These tests are **slow, Docker-heavy, and destructive to their own
//! cluster**, so they are excluded from the default sweep two ways:
//!   - the target is `test = false` in `Cargo.toml` (never built by a plain
//!     `cargo test`), and
//!   - every scenario is `#[ignore]`.
//!
//! Run explicitly:
//!
//! ```text
//! cargo test --features integration-tests --test chaos -- --ignored --nocapture
//! ```
//!
//! or via `cargo xtask chaos`.

#[path = "../common/mod.rs"]
mod common;

mod actions;
mod config;
mod harness;
mod verifier;
mod workload;
mod workload_config;

mod run_test;
mod simple_flow_test;
