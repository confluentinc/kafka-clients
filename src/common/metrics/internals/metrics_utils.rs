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

//! Internal helpers shared by the metrics framework.
//!
//! Translated from `org.apache.kafka.common.metrics.internals.MetricsUtils`.

use indexmap::IndexMap;

use crate::common::KafkaError;
use crate::common::metrics::TimeUnit;

/// Converts the provided time from milliseconds into the requested unit.
pub(crate) fn convert(time_ms: i64, unit: TimeUnit) -> f64 {
    let time_ms = time_ms as f64;
    match unit {
        TimeUnit::Nanoseconds => time_ms * 1000.0 * 1000.0,
        TimeUnit::Microseconds => time_ms * 1000.0,
        TimeUnit::Milliseconds => time_ms,
        TimeUnit::Seconds => time_ms / 1000.0,
        TimeUnit::Minutes => time_ms / (60.0 * 1000.0),
        TimeUnit::Hours => time_ms / (60.0 * 60.0 * 1000.0),
        TimeUnit::Days => time_ms / (24.0 * 60.0 * 60.0 * 1000.0),
    }
}

/// Builds an ordered set of tags from key/value pairs.
///
/// Returns [`KafkaError::IllegalArgument`] if an odd number of arguments is
/// supplied.
// Consumed by the `Metrics` registry, which lands in a later phase.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn get_tags(key_value: &[&str]) -> Result<IndexMap<String, String>, KafkaError> {
    if !key_value.len().is_multiple_of(2) {
        return Err(KafkaError::illegal_argument("keyValue needs to be specified in pairs"));
    }
    let mut tags = IndexMap::with_capacity(key_value.len() / 2);
    for pair in key_value.chunks_exact(2) {
        tags.insert(pair[0].to_string(), pair[1].to_string());
    }
    Ok(tags)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert() {
        assert_eq!(convert(1000, TimeUnit::Seconds), 1.0);
        assert_eq!(convert(1, TimeUnit::Milliseconds), 1.0);
        assert_eq!(convert(1, TimeUnit::Microseconds), 1000.0);
        assert_eq!(convert(60_000, TimeUnit::Minutes), 1.0);
    }

    #[test]
    fn test_creating_tags() {
        let tags = get_tags(&["k1", "v1", "k2", "v2"]).unwrap();
        assert_eq!(tags.get("k1").map(String::as_str), Some("v1"));
        assert_eq!(tags.get("k2").map(String::as_str), Some("v2"));
        assert_eq!(tags.len(), 2);
    }

    #[test]
    fn test_creating_tags_with_odd_number_of_tags() {
        let err = get_tags(&["k1", "v1", "k2", "v2", "extra"]).unwrap_err();
        assert!(
            err.message().contains("keyValue needs to be specified in pairs"),
            "unexpected message: {}",
            err.message()
        );
    }
}
