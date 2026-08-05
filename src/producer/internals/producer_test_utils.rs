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

//! Shared driver for the producer's synchronously-driven `Sender` tests.
//!
//! Translated from `org.apache.kafka.clients.producer.internals.ProducerTestUtils`
//! (`clients/src/test/java/`), whose whole content is `runUntil`.

use crate::mock_client::MockClient;
use crate::producer::internals::Sender;

/// `ProducerTestUtils.MAX_TRIES` (Java 24).
const MAX_TRIES: u32 = 10;

/// Runs `sender` until `condition` holds, then asserts that it does.
///
/// Translated from `ProducerTestUtils.runUntil(Sender, Supplier<Boolean>)`
/// (Java 26-31), which delegates to the `maxTries` overload (Java 33-43).
///
/// `condition` receives the `Sender` so a predicate over the `MockClient` — Java's
/// `() -> !client.hasPendingResponses()`, by far the commonest one — can reach it
/// through [`Sender::client`]. Predicates over the `TransactionManager` or over a
/// record future capture their own handle, exactly as Java's closures do.
///
/// # Panics
///
/// If `condition` still does not hold after [`MAX_TRIES`] iterations, mirroring
/// Java's `assertTrue(condition.get(), ..)` (Java 42).
pub(crate) async fn run_until<F>(sender: &mut Sender<MockClient>, condition: F)
where
    F: Fn(&Sender<MockClient>) -> bool,
{
    run_until_with_tries(sender, condition, MAX_TRIES).await;
}

/// Runs `sender` up to `max_tries` times, waiting for `condition`.
///
/// Translated from `ProducerTestUtils.runUntil(Sender, Supplier<Boolean>, int)`
/// (Java 33-43).
///
/// # Panics
///
/// If `condition` still does not hold after `max_tries` iterations.
pub(crate) async fn run_until_with_tries<F>(sender: &mut Sender<MockClient>, condition: F, max_tries: u32)
where
    F: Fn(&Sender<MockClient>) -> bool,
{
    let mut tries = 0;
    while !condition(sender) && tries < max_tries {
        tries += 1;
        // Java's `runOnce` throws nothing the tests catch; a `run_once` error here is a
        // harness failure, so it is surfaced rather than swallowed.
        sender.run_once().await.expect("run_once");
    }
    assert!(condition(sender), "Condition not satisfied after {max_tries} tries");
}
