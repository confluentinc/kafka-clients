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

//! Translation of `org.apache.kafka.common.utils.Exit`.
//!
//! In Java this class indirects `System.exit()` and `Runtime.getRuntime().halt()`
//! through swappable `Procedure`s so tests can intercept them.
//!
//! In the Rust client we never actually want to exit the process from
//! library code — the producer is a library, not a daemon. We keep the
//! function names so translated call sites read naturally, but every
//! function panics. Tests that previously installed an `Exit` shim should
//! catch the panic via `std::panic::catch_unwind` if they need to.

/// Always panics. Translation of `Exit.exit(int)` — see module docs.
pub fn exit(status_code: i32) -> ! {
    panic!("Exit.exit({status_code}) called from library code");
}

/// Always panics. Translation of `Exit.exit(int, String)`.
pub fn exit_with_message(status_code: i32, message: &str) -> ! {
    panic!("Exit.exit({status_code}, {message:?}) called from library code");
}

/// Always panics. Translation of `Exit.halt(int)`.
pub fn halt(status_code: i32) -> ! {
    panic!("Exit.halt({status_code}) called from library code");
}

/// Always panics. Translation of `Exit.halt(int, String)`.
pub fn halt_with_message(status_code: i32, message: &str) -> ! {
    panic!("Exit.halt({status_code}, {message:?}) called from library code");
}
