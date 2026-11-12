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

//! The test selection tasks of xtask on their own: `selected-run`,
//! `selected-cargo-test`, `show-test-selection` and `select-tests`, taking the
//! same arguments as `cargo xtask <task>`.
//!
//! This binary builds on Windows, where xtask itself does not: its chaos and
//! perf-cloud tasks use Unix-only APIs. The Windows CI jobs
//! (.semaphore/verify-dotnet-windows.ps1) run their tests through it, so they
//! apply the test selection as the Linux and macOS jobs do. Its tests are
//! xtask's: the module is the same file.

use std::env;

// The same file as xtask's module; this binary's root is in src/, so it is found by name.
mod test_selection;

/// xtask's working directory is rust/, as for `cargo xtask`.
const REPO_ROOT: &str = "..";

fn main() -> anyhow::Result<()> {
    let task = env::args().nth(1);
    let args: Vec<String> = env::args().skip(2).collect();
    match task.as_deref() {
        Some("select-tests") => test_selection::select(&args),
        Some("selected-cargo-test") => test_selection::cargo_test(&args),
        Some("selected-run") => test_selection::selected_run(&args),
        Some("show-test-selection") => test_selection::show(),
        other => anyhow::bail!(
            "unknown task {other:?}; it runs select-tests, selected-cargo-test, selected-run or show-test-selection"
        ),
    }
}
