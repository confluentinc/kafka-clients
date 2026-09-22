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

//! Per-topic compression ratio estimation.
//!
//! This class helps estimate the compression ratio for each topic and
//! compression type combination.
//!
//! Corresponds to Java's `org.apache.kafka.common.record.CompressionRatioEstimator`.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use crate::common::record::internal::CompressionType;

/// Process-wide global compression ratio estimator instance.
///
/// Matches the Java behavior where `CompressionRatioEstimator` has static
/// methods backed by a static `ConcurrentHashMap`, making it a process-wide
/// singleton that shares compression ratio estimates across all producers
/// and consumers in the same process.
static INSTANCE: LazyLock<CompressionRatioEstimator> = LazyLock::new(CompressionRatioEstimator::new_instance);

/// Per-topic compression ratio estimation.
///
/// Thread-safe singleton that tracks compression ratio estimates per topic
/// and compression type. Use the static methods ([`update_estimation`](Self::update_estimation),
/// [`estimation`](Self::estimation), etc.) which operate on the process-wide
/// global instance, matching Java's static `CompressionRatioEstimator` methods.
///
/// Corresponds to Java's `org.apache.kafka.common.record.CompressionRatioEstimator`.
pub struct CompressionRatioEstimator {
    compression_ratio: Mutex<HashMap<String, [f32; CompressionType::COUNT]>>,
}

impl CompressionRatioEstimator {
    /// The constant speed to increase compression ratio when a batch compresses
    /// better than expected.
    pub const COMPRESSION_RATIO_IMPROVING_STEP: f32 = 0.005;

    /// The minimum speed to decrease compression ratio when a batch compresses
    /// worse than expected.
    pub const COMPRESSION_RATIO_DETERIORATE_STEP: f32 = 0.05;

    /// Create a new empty estimator instance (private).
    fn new_instance() -> Self {
        Self { compression_ratio: Mutex::new(HashMap::new()) }
    }

    /// Update the compression ratio estimation for a topic and compression type.
    ///
    /// Returns the compression ratio estimation after the update.
    /// Operates on the process-wide global instance.
    pub fn update_estimation(topic: &str, compression_type: CompressionType, observed_ratio: f32) -> f32 {
        let mut map = INSTANCE.compression_ratio.lock().unwrap();
        let ratios = map.entry(topic.to_string()).or_insert_with(Self::initial_compression_ratio);
        let idx = compression_type.id() as usize;
        let current_estimation = ratios[idx];

        if observed_ratio > current_estimation {
            ratios[idx] = f32::max(
                current_estimation + CompressionRatioEstimator::COMPRESSION_RATIO_DETERIORATE_STEP,
                observed_ratio,
            );
        } else if observed_ratio < current_estimation {
            ratios[idx] = f32::max(
                current_estimation - CompressionRatioEstimator::COMPRESSION_RATIO_IMPROVING_STEP,
                observed_ratio,
            );
        }

        ratios[idx]
    }

    /// Get the compression ratio estimation for a topic and compression type.
    ///
    /// Operates on the process-wide global instance.
    pub fn estimation(topic: &str, compression_type: CompressionType) -> f32 {
        let mut map = INSTANCE.compression_ratio.lock().unwrap();
        let ratios = map.entry(topic.to_string()).or_insert_with(Self::initial_compression_ratio);
        ratios[compression_type.id() as usize]
    }

    /// Reset the compression ratio estimation to the initial values for a topic.
    ///
    /// Operates on the process-wide global instance.
    #[allow(dead_code)]
    pub fn reset_estimation(topic: &str) {
        let mut map = INSTANCE.compression_ratio.lock().unwrap();
        let ratios = map.entry(topic.to_string()).or_insert_with(Self::initial_compression_ratio);
        for ct in CompressionType::values() {
            ratios[ct.id() as usize] = ct.rate();
        }
    }

    /// Set the compression estimation for a topic compression type combination.
    ///
    /// This method is for unit test purpose.
    /// Operates on the process-wide global instance.
    pub fn set_estimation(topic: &str, compression_type: CompressionType, ratio: f32) {
        let mut map = INSTANCE.compression_ratio.lock().unwrap();
        let ratios = map.entry(topic.to_string()).or_insert_with(Self::initial_compression_ratio);
        ratios[compression_type.id() as usize] = ratio;
    }

    fn initial_compression_ratio() -> [f32; CompressionType::COUNT] {
        let mut ratios = [0.0f32; CompressionType::COUNT];
        for ct in CompressionType::values() {
            ratios[ct.id() as usize] = ct.rate();
        }
        ratios
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Corresponds to Java's testUpdateEstimation.
    ///
    /// Each test case uses a unique topic name to avoid interference from
    /// the shared global state and parallel test execution.
    #[test]
    fn test_update_estimation() {
        struct EstimationsObservedRatios {
            current_estimation: f32,
            observed_ratio: f32,
        }

        // If currentEstimation is smaller than observedRatio, the updatedCompressionRatio
        // is currentEstimation plus COMPRESSION_RATIO_DETERIORATE_STEP 0.05, otherwise
        // currentEstimation minus COMPRESSION_RATIO_IMPROVING_STEP 0.005.
        // There are four cases, and updatedCompressionRatio should not be smaller than
        // observedRatio in all of cases.
        let test_cases = [
            EstimationsObservedRatios { current_estimation: 0.8, observed_ratio: 0.84 },
            EstimationsObservedRatios { current_estimation: 0.6, observed_ratio: 0.7 },
            EstimationsObservedRatios { current_estimation: 0.6, observed_ratio: 0.4 },
            EstimationsObservedRatios { current_estimation: 0.004, observed_ratio: 0.001 },
        ];

        for (i, case) in test_cases.iter().enumerate() {
            let topic = &format!("test_update_estimation_{}", i);
            CompressionRatioEstimator::set_estimation(topic, CompressionType::Zstd, case.current_estimation);
            let updated =
                CompressionRatioEstimator::update_estimation(topic, CompressionType::Zstd, case.observed_ratio);
            assert!(
                updated >= case.observed_ratio,
                "Updated ratio {} should be >= observed ratio {} (current estimation: {})",
                updated,
                case.observed_ratio,
                case.current_estimation
            );
        }
    }

    #[test]
    fn test_estimation_returns_initial_rate() {
        let topic = "test_estimation_returns_initial_rate";
        for ct in CompressionType::values() {
            assert_eq!(CompressionRatioEstimator::estimation(topic, *ct), ct.rate());
        }
    }

    #[test]
    fn test_reset_estimation() {
        let topic = "test_reset_estimation";
        CompressionRatioEstimator::set_estimation(topic, CompressionType::Gzip, 0.5);
        assert_eq!(CompressionRatioEstimator::estimation(topic, CompressionType::Gzip), 0.5);

        CompressionRatioEstimator::reset_estimation(topic);
        assert_eq!(
            CompressionRatioEstimator::estimation(topic, CompressionType::Gzip),
            CompressionType::Gzip.rate()
        );
    }
}
