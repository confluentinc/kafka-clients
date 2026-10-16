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
//! Required by Phase 2 PLAN.md verification step 6, as amended by the
//! `Consumer` / `DynConsumer` split (`consumer-threading.md` §2):
//!
//! - `KafkaConsumer<K, V>` and `MockConsumer<K, V>` implement `Consumer<K, V>`,
//!   so generic code taking `C: Consumer<K, V>` accepts either.
//! - The futures returned by `Consumer` methods are `Send`, so generic code can
//!   move them across tokio tasks.
//! - `DynConsumer<K, V>` is dyn-compatible and `Send`: a `KafkaConsumer` and a
//!   `MockConsumer` both box into `Box<dyn DynConsumer<K, V>>`, and
//!   `dyn DynConsumer<K, V>` is itself a `Consumer<K, V>`.
//!
//! Intentionally NO `_assert_sync` — `Consumer<K, V>` is `Send + 'static`
//! only (NOT `Sync`), per `consumer-threading.md` §1 and the bounds
//! checklist in Phase 2 PLAN.md.
//!
//! A regression that makes a returned future non-`Send`, or `DynConsumer`
//! not dyn-compatible, fails this file at compile time.

use std::time::Duration;

use confluent_kafka::consumer::{Consumer, DynConsumer, KafkaConsumer, MockConsumer};

fn _assert_consumer<K, V, C>()
where
    K: Send + 'static,
    V: Send + 'static,
    C: Consumer<K, V> + ?Sized,
{
}

fn _assert_send<T: Send>(_: T) {}

fn _assert_implementations<K, V>()
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    _assert_consumer::<K, V, KafkaConsumer<K, V>>();
    _assert_consumer::<K, V, MockConsumer<K, V>>();
    // `dyn DynConsumer<K, V>` is a `Consumer<K, V>` in turn, so a boxed
    // consumer can be passed as `&mut *boxed` to generic code.
    _assert_consumer::<K, V, dyn DynConsumer<K, V>>();
}

fn _assert_futures_are_send<K, V, C>(consumer: &mut C)
where
    K: Send + 'static,
    V: Send + 'static,
    C: Consumer<K, V> + ?Sized,
{
    // Compile-time: the trait declares every asynchronous method as returning
    // `impl Future + Send`, so the future of a generic call is `Send`.
    _assert_send(consumer.poll(Duration::ZERO));
}

fn _assert_object_safe<K, V>(kafka: KafkaConsumer<K, V>, mock: MockConsumer<K, V>) -> Vec<Box<dyn DynConsumer<K, V>>>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    // Compile-time: `Box<dyn DynConsumer<K, V>>` must construct from both
    // implementations, proving object safety (no `Self: Sized` bounds, no
    // associated types without defaults, etc.).
    vec![Box::new(kafka), Box::new(mock)]
}

fn _assert_boxed_send<K, V>(consumer: Box<dyn DynConsumer<K, V>>)
where
    K: Send + 'static,
    V: Send + 'static,
{
    // Compile-time: the boxed trait object implements `Send` (the trait
    // is declared `Send + 'static`).
    _assert_send(consumer);
}

// Intentionally NO _assert_sync — Consumer<K, V> is Send-only per
// consumer-threading.md §1. Adding a `Sync` requirement would force every
// public method to take `&self` instead of `&mut self`, breaking the
// "one active call at a time" model that mirrors Java's
// not-thread-safe KafkaConsumer.
