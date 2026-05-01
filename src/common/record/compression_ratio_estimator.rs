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

//! Translation of `org.apache.kafka.common.record.CompressionRatioEstimator`.
//!
//! Tracks running compression-ratio estimates per `(topic, codec)`. Used by
//! the producer to budget how many records fit into a given batch size
//! given the codec's expected compression rate.

use std::sync::Mutex;
use std::sync::OnceLock;

use dashmap::DashMap;

use crate::common::record::CompressionType;

/// Constant speed at which the estimate increases when a batch compresses
/// better than expected. Mirrors Java's `COMPRESSION_RATIO_IMPROVING_STEP`.
pub const COMPRESSION_RATIO_IMPROVING_STEP: f32 = 0.005;

/// Constant speed at which the estimate decreases when a batch compresses
/// worse than expected. Mirrors Java's `COMPRESSION_RATIO_DETERIORATE_STEP`.
pub const COMPRESSION_RATIO_DETERIORATE_STEP: f32 = 0.05;

/// Number of compression-type slots (matches the Java `CompressionType.values().length`).
const NUM_TYPES: usize = 5;

/// Map from topic name to `[ratio; NUM_TYPES]`. Wrapped in a `Mutex` per
/// entry to mirror Java's `synchronized (compressionRatioForTopic)` block.
struct PerTopicRatios(Mutex<[f32; NUM_TYPES]>);

fn map() -> &'static DashMap<String, PerTopicRatios> {
    static M: OnceLock<DashMap<String, PerTopicRatios>> = OnceLock::new();
    M.get_or_init(DashMap::new)
}

fn initial_ratios() -> [f32; NUM_TYPES] {
    let mut a = [0.0f32; NUM_TYPES];
    for t in [
        CompressionType::None,
        CompressionType::Gzip,
        CompressionType::Snappy,
        CompressionType::Lz4,
        CompressionType::Zstd,
    ] {
        a[t.id() as usize] = t.rate();
    }
    a
}

fn get_or_create(topic: &str) -> dashmap::mapref::one::RefMut<'_, String, PerTopicRatios> {
    let m = map();
    if let Some(r) = m.get_mut(topic) {
        return r;
    }
    m.entry(topic.to_owned())
        .or_insert_with(|| PerTopicRatios(Mutex::new(initial_ratios())))
}

/// Update the compression-ratio estimate for a topic and compression type
/// based on `observed_ratio`. Returns the new estimate.
///
/// Mirrors Java's `CompressionRatioEstimator#updateEstimation`.
pub fn update_estimation(topic: &str, codec: CompressionType, observed_ratio: f32) -> f32 {
    let entry = get_or_create(topic);
    let mut ratios = entry.0.lock().expect("compression-ratio lock poisoned");
    let idx = codec.id() as usize;
    let current = ratios[idx];
    if observed_ratio > current {
        ratios[idx] = (current + COMPRESSION_RATIO_DETERIORATE_STEP).max(observed_ratio);
    } else if observed_ratio < current {
        ratios[idx] = (current - COMPRESSION_RATIO_IMPROVING_STEP).max(observed_ratio);
    }
    ratios[idx]
}

/// Get the current compression-ratio estimate for the given `(topic, codec)`.
/// Mirrors Java's `CompressionRatioEstimator#estimation`.
pub fn estimation(topic: &str, codec: CompressionType) -> f32 {
    let entry = get_or_create(topic);
    let ratios = entry.0.lock().expect("compression-ratio lock poisoned");
    ratios[codec.id() as usize]
}

/// Reset the per-codec estimates for a topic to the initial values.
/// Mirrors Java's `CompressionRatioEstimator#resetEstimation`.
pub fn reset_estimation(topic: &str) {
    let entry = get_or_create(topic);
    let mut ratios = entry.0.lock().expect("compression-ratio lock poisoned");
    *ratios = initial_ratios();
}

/// Set the estimate for a `(topic, codec)`. Visible for testing —
/// mirrors Java's `setEstimation`.
pub fn set_estimation(topic: &str, codec: CompressionType, ratio: f32) {
    let entry = get_or_create(topic);
    let mut ratios = entry.0.lock().expect("compression-ratio lock poisoned");
    ratios[codec.id() as usize] = ratio;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Translation of `CompressionRatioEstimatorTest.testUpdateEstimation`.
    #[test]
    fn update_estimation_test() {
        // The Java test uses topic "tp" — but the static map persists
        // across tests, so we use a unique topic per test invocation to
        // avoid cross-test interference.
        let topic = "compression_ratio_test_update_estimation";

        let cases = [
            (0.8f32, 0.84f32),
            (0.6f32, 0.7f32),
            (0.6f32, 0.4f32),
            (0.004f32, 0.001f32),
        ];
        for (current, observed) in cases {
            set_estimation(topic, CompressionType::Zstd, current);
            let updated = update_estimation(topic, CompressionType::Zstd, observed);
            assert!(updated >= observed, "updated {updated} should be >= observed {observed}");
        }
    }

    #[test]
    fn estimation_returns_initial_rate_for_new_topic() {
        let topic = "compression_ratio_test_initial_rate";
        // Initial = CompressionType::Zstd.rate() = 1.0
        assert_eq!(estimation(topic, CompressionType::Zstd), 1.0);
        assert_eq!(estimation(topic, CompressionType::None), 1.0);
    }

    #[test]
    fn reset_estimation_restores_initial_values() {
        let topic = "compression_ratio_test_reset";
        set_estimation(topic, CompressionType::Gzip, 0.123);
        assert_eq!(estimation(topic, CompressionType::Gzip), 0.123);
        reset_estimation(topic);
        assert_eq!(estimation(topic, CompressionType::Gzip), 1.0);
    }
}
