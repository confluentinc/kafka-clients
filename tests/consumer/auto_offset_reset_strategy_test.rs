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

//! Translated from
//! `org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategyTest`.
//!
//! Skipped tests:
//! - `testValidator` — exercises `ConfigDef.Validator`, the Java
//!   reflection-based config-validation framework. Per PLAN.md
//!   §AutoOffsetResetStrategy, the `Validator` inner class is not
//!   translated; runtime validation flows through the `from_string`
//!   `Result` return type directly. The behavioral assertion ("the same
//!   set of inputs accepted/rejected") is covered by `test_from_string`.
//! - `fromString(null)` — Java accepts `String` and throws on null. In
//!   Rust the parameter is `&str`, which is non-null by the type system.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use confluent_kafka::consumer::AutoOffsetResetStrategy;

// `ListOffsetsRequest.EARLIEST_TIMESTAMP` and `LATEST_TIMESTAMP` are
// `pub(crate)` inside the `internals` module; we duplicate the protocol
// constants here so the external-crate test compiles.
const EARLIEST_TIMESTAMP: i64 = -2;
const LATEST_TIMESTAMP: i64 = -1;

fn hash_of<T: Hash>(v: &T) -> u64 {
    let mut h = DefaultHasher::new();
    v.hash(&mut h);
    h.finish()
}

/// Translated from `AutoOffsetResetStrategyTest.testFromString`.
#[test]
fn test_from_string() {
    assert_eq!(
        AutoOffsetResetStrategy::from_string("earliest").unwrap(),
        AutoOffsetResetStrategy::EARLIEST
    );
    assert_eq!(
        AutoOffsetResetStrategy::from_string("latest").unwrap(),
        AutoOffsetResetStrategy::LATEST
    );
    assert_eq!(
        AutoOffsetResetStrategy::from_string("none").unwrap(),
        AutoOffsetResetStrategy::NONE
    );
    assert!(AutoOffsetResetStrategy::from_string("invalid").is_err());
    assert!(AutoOffsetResetStrategy::from_string("by_duration:invalid").is_err());
    assert!(AutoOffsetResetStrategy::from_string("by_duration:-PT1H").is_err());
    assert!(AutoOffsetResetStrategy::from_string("by_duration:").is_err());
    assert!(AutoOffsetResetStrategy::from_string("by_duration").is_err());
    assert!(AutoOffsetResetStrategy::from_string("LATEST").is_err());
    assert!(AutoOffsetResetStrategy::from_string("EARLIEST").is_err());
    assert!(AutoOffsetResetStrategy::from_string("NONE").is_err());
    assert!(AutoOffsetResetStrategy::from_string("").is_err());

    let strategy = AutoOffsetResetStrategy::from_string("by_duration:PT1H").unwrap();
    assert_eq!(strategy.name(), "by_duration");
}

/// Translated from `AutoOffsetResetStrategyTest.testEqualsAndHashCode`.
#[test]
fn test_equals_and_hash_code() {
    let earliest1 = AutoOffsetResetStrategy::from_string("earliest").unwrap();
    let earliest2 = AutoOffsetResetStrategy::from_string("earliest").unwrap();
    let latest1 = AutoOffsetResetStrategy::from_string("latest").unwrap();

    let duration1 = AutoOffsetResetStrategy::from_string("by_duration:P2D").unwrap();
    let duration2 = AutoOffsetResetStrategy::from_string("by_duration:P2D").unwrap();

    assert_eq!(earliest1, earliest2);
    assert_ne!(earliest1, latest1);
    assert_eq!(hash_of(&earliest1), hash_of(&earliest2));
    assert_ne!(hash_of(&earliest1), hash_of(&latest1));

    assert_ne!(latest1, duration2);
    assert_eq!(duration1, duration2);
}

/// Translated from `AutoOffsetResetStrategyTest.testTimestamp`.
#[test]
fn test_timestamp() {
    let earliest1 = AutoOffsetResetStrategy::from_string("earliest").unwrap();
    let earliest2 = AutoOffsetResetStrategy::from_string("earliest").unwrap();
    assert_eq!(earliest1.timestamp(), Some(EARLIEST_TIMESTAMP));
    assert_eq!(earliest1, earliest2);

    let latest1 = AutoOffsetResetStrategy::from_string("latest").unwrap();
    let latest2 = AutoOffsetResetStrategy::from_string("latest").unwrap();
    assert_eq!(latest1.timestamp(), Some(LATEST_TIMESTAMP));
    assert_eq!(latest1, latest2);

    let none1 = AutoOffsetResetStrategy::from_string("none").unwrap();
    let none2 = AutoOffsetResetStrategy::from_string("none").unwrap();
    assert!(none1.timestamp().is_none());
    assert_eq!(none1, none2);

    let by_duration1 = AutoOffsetResetStrategy::from_string("by_duration:PT1H").unwrap();
    let timestamp = by_duration1.timestamp().unwrap();
    let now_millis = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    let expected = now_millis - Duration::from_secs(3600).as_millis() as i64;
    assert!(
        timestamp <= expected + 5_000,
        "timestamp {timestamp} not within expected window of now-1h ({expected})"
    );

    let by_duration2 = AutoOffsetResetStrategy::from_string("by_duration:PT1H").unwrap();
    let by_duration3 = AutoOffsetResetStrategy::from_string("by_duration:PT2H").unwrap();
    assert_eq!(by_duration1, by_duration2);
    assert_ne!(by_duration1, by_duration3);
}
