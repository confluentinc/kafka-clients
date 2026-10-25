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

//! Test-only log capture.
//!
//! The `log` facade takes one logger per process, and the unit tests share
//! one process, so this module installs the test binary's only logger: it
//! records the lines logged on a thread inside [`capture_logs`] and hands
//! every line, captured or not, to an `env_logger` honouring `RUST_LOG`, as
//! the C clients' `init_default_logger` (`ffi::common`) does outside tests;
//! in the unit tests that function installs this logger instead. Recording
//! is per thread, so lines other tests log concurrently are not captured.
//!
//! Used to assert that a configuration copied for one more client logs none
//! of the post-processing lines its original logged (`ConsumerConfig` and
//! `ProducerConfig` `copy_for_client`).

use std::cell::RefCell;
use std::sync::OnceLock;

use log::{Level, LevelFilter, Log, Metadata, Record};

thread_local! {
    /// The lines logged on this thread while [`capture_logs`] runs, `None`
    /// outside it.
    static CAPTURED: RefCell<Option<Vec<(Level, String)>>> = const { RefCell::new(None) };
}

/// The logger [`install`] sets: records a capturing thread's lines and
/// forwards every line to `inner`.
struct CaptureLogger {
    inner: env_logger::Logger,
}

impl Log for CaptureLogger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        is_capturing() || self.inner.enabled(metadata)
    }

    fn log(&self, record: &Record<'_>) {
        CAPTURED.with(|captured| {
            if let Ok(mut captured) = captured.try_borrow_mut()
                && let Some(lines) = captured.as_mut()
            {
                lines.push((record.level(), record.args().to_string()));
            }
        });
        if self.inner.matches(record) {
            self.inner.log(record);
        }
    }

    fn flush(&self) {
        self.inner.flush();
    }
}

fn is_capturing() -> bool {
    CAPTURED.with(|captured| captured.try_borrow().is_ok_and(|captured| captured.is_some()))
}

static LOGGER: OnceLock<CaptureLogger> = OnceLock::new();
/// Whether [`install`] set [`LOGGER`]; set once, concurrent callers waiting.
static INSTALLED: OnceLock<bool> = OnceLock::new();

/// Sets the capturing logger as the process's logger, once; later calls do
/// nothing. The level is raised to at least `Info`, so a capture sees the
/// INFO and WARN lines whatever `RUST_LOG` says.
///
/// # Panics
///
/// When another logger was set first: nothing else in the unit tests sets
/// one.
pub(crate) fn install() {
    let installed = *INSTALLED.get_or_init(|| {
        let logger = LOGGER
            .get_or_init(|| CaptureLogger { inner: env_logger::Builder::from_default_env().is_test(true).build() });
        let installed = log::set_logger(logger).is_ok();
        if installed {
            log::set_max_level(logger.inner.filter().max(LevelFilter::Info));
        }
        installed
    });
    assert!(installed, "another logger was set before the test log capture");
}

/// Runs `f` and returns its result with the lines it logged on this thread,
/// each with its level, in order.
pub(crate) fn capture_logs<R>(f: impl FnOnce() -> R) -> (R, Vec<(Level, String)>) {
    install();
    CAPTURED.with(|captured| *captured.borrow_mut() = Some(Vec::new()));
    // Restores "not capturing" even when `f` panics.
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            CAPTURED.with(|captured| *captured.borrow_mut() = None);
        }
    }
    let reset = Reset;
    let result = f();
    let lines = CAPTURED.with(|captured| captured.borrow_mut().take().unwrap_or_default());
    drop(reset);
    (result, lines)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capture_logs_records_this_threads_lines_only() {
        let ((), lines) = capture_logs(|| {
            log::warn!("captured {}", 1);
            log::info!("captured {}", 2);
            std::thread::spawn(|| log::warn!("another thread")).join().unwrap();
        });
        assert_eq!(
            lines,
            vec![
                (Level::Warn, "captured 1".to_string()),
                (Level::Info, "captured 2".to_string())
            ]
        );
        let ((), lines) = capture_logs(|| ());
        assert!(lines.is_empty());
    }
}
