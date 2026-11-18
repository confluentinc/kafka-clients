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

//! Per-arm timing line of the multilanguage test macros.
//!
//! Every arm the three macros generate holds an [`ArmTimer`], which prints one
//! line to stderr when the arm ends, passed or panicked:
//!
//! ```text
//! [multilanguage] test_commit_and_committed backend=grpc_python setup=0.84s elapsed=12.34s result=ok
//! ```
//!
//! `setup` is the time to get the cluster and the backend container, `elapsed`
//! the time of the scenario body. The line is written through
//! [`std::io::stderr`] rather than `eprintln!`: libtest captures the print
//! macros of a passing test, not a direct write to the stream, so the line
//! shows in CI logs for every test without `--nocapture`.

use std::io::Write;
use std::time::Instant;

/// Times one macro-generated arm and reports it when dropped.
pub struct ArmTimer {
    scenario: &'static str,
    backend: &'static str,
    start: Instant,
    body_start: Option<Instant>,
}

impl ArmTimer {
    /// Start timing the arm `scenario__backend`.
    pub fn start(scenario: &'static str, backend: &'static str) -> Self {
        Self { scenario, backend, start: Instant::now(), body_start: None }
    }

    /// Mark the end of the setup (cluster and backend) and the start of the body.
    pub fn body_started(&mut self) {
        self.body_start = Some(Instant::now());
    }
}

impl Drop for ArmTimer {
    fn drop(&mut self) {
        let body_start = self.body_start.unwrap_or_else(Instant::now);
        let setup = body_start.duration_since(self.start).as_secs_f64();
        let elapsed = body_start.elapsed().as_secs_f64();
        let result = if std::thread::panicking() { "panicked" } else { "ok" };
        // Best effort: a closed stderr must not turn into a second panic.
        let _ = writeln!(
            std::io::stderr(),
            "[multilanguage] {} backend={} setup={setup:.2}s elapsed={elapsed:.2}s result={result}",
            self.scenario,
            self.backend,
        );
    }
}
