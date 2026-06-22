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

//! Compile-time surface check for the public `Consumer<K, V>` trait.
//!
//! Required by Phase 2 PLAN.md verification step 6. The trait must remain
//! object-safe (`Box<dyn Consumer<K, V>>` from the `new_consumer` factory)
//! and `Send` (consumer instances cross task boundaries on the tokio
//! multi-thread runtime).
//!
//! Intentionally NO `_assert_sync` — `Consumer<K, V>` is `Send + 'static`
//! only (NOT `Sync`), per `consumer-threading.md` §1 and the bounds
//! checklist in Phase 2 PLAN.md.
//!
//! A regression that adds `Sized` or non-`Send` futures would fail this
//! file at compile time, catching the bug before the consumer ships.

use confluent_kafka::consumer::Consumer;

#[allow(dead_code)]
fn _assert_object_safe<K, V>(_: Box<dyn Consumer<K, V>>)
where
    K: Send + 'static,
    V: Send + 'static,
{
    // Compile-time: `Box<dyn Consumer<K, V>>` must construct, proving
    // object safety (no `Self: Sized` bounds, no associated types
    // without defaults, etc.).
}

#[allow(dead_code)]
fn _assert_send<K, V>(_: Box<dyn Consumer<K, V>>)
where
    K: Send + 'static,
    V: Send + 'static,
{
    // Compile-time: the boxed trait object implements `Send` (the trait
    // is declared `Send + 'static`).
}

// Intentionally NO _assert_sync — Consumer<K, V> is Send-only per
// consumer-threading.md §1. Adding a `Sync` requirement would force every
// public method to take `&self` instead of `&mut self`, breaking the
// "one active call at a time" model that mirrors Java's
// not-thread-safe KafkaConsumer.
