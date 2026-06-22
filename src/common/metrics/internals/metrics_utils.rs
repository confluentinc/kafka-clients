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

//! Metrics internal helpers
//! (`org.apache.kafka.common.metrics.internals.MetricsUtils`).

use std::collections::BTreeMap;

use crate::common::KafkaError;

/// Convert a sequence of `key, value` pairs to a tags map.
///
/// Returns an error (Java throws `IllegalArgumentException`) if the number of
/// elements is odd.
pub fn get_tags(key_value: &[&str]) -> Result<BTreeMap<String, String>, KafkaError> {
    if !key_value.len().is_multiple_of(2) {
        return Err(KafkaError::illegal_argument("keyValue needs to be specified in pairs"));
    }
    let mut tags = BTreeMap::new();
    let mut i = 0;
    while i < key_value.len() {
        tags.insert(key_value[i].to_string(), key_value[i + 1].to_string());
        i += 2;
    }
    Ok(tags)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pairs_to_map() {
        let tags = get_tags(&["k1", "v1", "k2", "v2"]).unwrap();
        assert_eq!(tags.get("k1").map(String::as_str), Some("v1"));
        assert_eq!(tags.get("k2").map(String::as_str), Some("v2"));
        assert_eq!(tags.len(), 2);
    }

    #[test]
    fn odd_count_is_error() {
        let err = get_tags(&["k1"]).unwrap_err();
        assert!(err.to_string().contains("keyValue needs to be specified in pairs"));
    }

    #[test]
    fn empty_is_empty_map() {
        assert!(get_tags(&[]).unwrap().is_empty());
    }
}
