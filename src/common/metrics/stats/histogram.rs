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

//! A bucketed histogram used to compute percentiles and frequencies.
//!
//! Translated from `org.apache.kafka.common.metrics.stats.Histogram`.

use std::fmt;

use crate::common::utils::double_to_string;

/// A histogram over a fixed set of bins determined by a [`BinScheme`].
#[derive(Debug)]
pub struct Histogram {
    bin_scheme: std::sync::Arc<dyn BinScheme>,
    hist: Vec<f32>,
    count: f64,
}

impl Histogram {
    /// Creates an empty histogram over the given bin scheme.
    pub fn new(bin_scheme: std::sync::Arc<dyn BinScheme>) -> Self {
        let hist = vec![0.0f32; bin_scheme.bins()];
        Self { bin_scheme, hist, count: 0.0 }
    }

    /// Records a value into its bin.
    pub fn record(&mut self, value: f64) {
        self.hist[self.bin_scheme.to_bin(value)] += 1.0;
        self.count += 1.0;
    }

    /// The value at the given quantile.
    pub fn value(&self, quantile: f64) -> f64 {
        if self.count == 0.0 {
            return f64::NAN;
        }
        if quantile > 1.0 {
            return f64::INFINITY;
        }
        if quantile < 0.0 {
            return f64::NEG_INFINITY;
        }
        let mut sum = 0.0f32;
        let quant = quantile as f32;
        for i in 0..self.hist.len() - 1 {
            sum += self.hist[i];
            if sum as f64 / self.count > quant as f64 {
                return self.bin_scheme.from_bin(i as i32);
            }
        }
        self.bin_scheme.from_bin((self.hist.len() - 1) as i32)
    }

    /// The raw per-bin counts.
    pub fn counts(&self) -> &[f32] {
        &self.hist
    }

    /// Clears all counts.
    pub fn clear(&mut self) {
        self.hist.iter_mut().for_each(|c| *c = 0.0);
        self.count = 0.0;
    }
}

impl fmt::Display for Histogram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{")?;
        for i in 0..self.hist.len() - 1 {
            write!(f, "{:.10}:{:.0},", self.bin_scheme.from_bin(i as i32), self.hist[i])?;
        }
        write!(f, "{}:{:.0}}}", double_to_string(f64::INFINITY), self.hist[self.hist.len() - 1])
    }
}

/// Determines the bin a value belongs to and the upper bound of each bin.
pub trait BinScheme: fmt::Debug + Send + Sync {
    /// The number of bins.
    fn bins(&self) -> usize;

    /// The 0-based bin index for the given value.
    fn to_bin(&self, value: f64) -> usize;

    /// The value at the upper end of the given bin, or an infinity for
    /// out-of-range bin numbers.
    //
    // Reads the scheme's own bounds, so it is an instance method despite the
    // `from_` name.
    #[allow(clippy::wrong_self_convention)]
    fn from_bin(&self, bin: i32) -> f64;
}

/// A scheme whose bins all have the same constant width across a value range.
#[derive(Debug)]
pub struct ConstantBinScheme {
    min: f64,
    bins: usize,
    bucket_width: f64,
    max_bin_number: i32,
}

impl ConstantBinScheme {
    const MIN_BIN_NUMBER: i32 = 0;

    /// Creates a constant-width bin scheme.
    ///
    /// Returns [`KafkaError::IllegalArgument`](crate::common::KafkaError) if
    /// fewer than two bins are requested.
    pub fn new(bins: i32, min: f64, max: f64) -> Result<Self, crate::common::KafkaError> {
        if bins < 2 {
            return Err(crate::common::KafkaError::illegal_argument("Must have at least 2 bins."));
        }
        Ok(Self {
            min,
            bins: bins as usize,
            bucket_width: (max - min) / bins as f64,
            max_bin_number: bins - 1,
        })
    }
}

impl BinScheme for ConstantBinScheme {
    fn bins(&self) -> usize {
        self.bins
    }

    fn from_bin(&self, b: i32) -> f64 {
        if b < Self::MIN_BIN_NUMBER {
            return f64::NEG_INFINITY;
        }
        if b > self.max_bin_number {
            return f64::INFINITY;
        }
        self.min + b as f64 * self.bucket_width
    }

    fn to_bin(&self, x: f64) -> usize {
        let bin_number = ((x - self.min) / self.bucket_width) as i32;
        if bin_number < Self::MIN_BIN_NUMBER {
            return Self::MIN_BIN_NUMBER as usize;
        }
        bin_number.min(self.max_bin_number) as usize
    }
}

/// A scheme where each bin is one unit wider than the previous, scaled so the
/// value range fits within the bins.
#[derive(Debug)]
pub struct LinearBinScheme {
    bins: usize,
    max: f64,
    scale: f64,
}

impl LinearBinScheme {
    /// Creates a linear bin scheme.
    ///
    /// Returns [`KafkaError::IllegalArgument`](crate::common::KafkaError) if
    /// fewer than two bins are requested.
    pub fn new(num_bins: i32, max: f64) -> Result<Self, crate::common::KafkaError> {
        if num_bins < 2 {
            return Err(crate::common::KafkaError::illegal_argument("Must have at least 2 bins."));
        }
        let denom = num_bins as f64 * (num_bins as f64 - 1.0) / 2.0;
        Ok(Self { bins: num_bins as usize, max, scale: max / denom })
    }
}

impl BinScheme for LinearBinScheme {
    fn bins(&self) -> usize {
        self.bins
    }

    fn from_bin(&self, b: i32) -> f64 {
        if b > self.bins as i32 - 1 {
            f64::INFINITY
        } else if b < 0 {
            f64::NEG_INFINITY
        } else {
            self.scale * (b as f64 * (b as f64 + 1.0)) / 2.0
        }
    }

    fn to_bin(&self, x: f64) -> usize {
        // Linear bins are defined over non-negative values; a negative value
        // maps to the first bin rather than being rejected.
        if x < 0.0 {
            0
        } else if x > self.max {
            self.bins - 1
        } else {
            (-0.5 + 0.5 * (1.0 + 8.0 * x / self.scale).sqrt()) as usize
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    const EPS: f64 = 0.0000001;

    #[test]
    fn test_histogram() {
        let scheme = Arc::new(ConstantBinScheme::new(10, -5.0, 5.0).unwrap());
        let mut hist = Histogram::new(scheme.clone());
        for i in -5..5 {
            hist.record(i as f64);
        }
        for i in 0..10 {
            assert!((scheme.from_bin(i) - hist.value(i as f64 / 10.0 + EPS)).abs() < EPS);
        }
    }

    #[test]
    fn test_constant_bin_scheme() {
        let scheme = ConstantBinScheme::new(5, -5.0, 5.0).unwrap();
        assert_eq!(
            scheme.to_bin(-5.01),
            0,
            "A value below the lower bound should map to the first bin"
        );
        assert_eq!(
            scheme.to_bin(5.01),
            4,
            "A value above the upper bound should map to the last bin"
        );
        assert_eq!(scheme.to_bin(-5.0001), 0);
        assert_eq!(scheme.to_bin(-5.0000), 0);
        assert_eq!(scheme.to_bin(-4.99999), 0);
        assert_eq!(scheme.to_bin(-3.00001), 0);
        assert_eq!(scheme.to_bin(-3.0), 1);
        assert_eq!(scheme.to_bin(-1.00001), 1);
        assert_eq!(scheme.to_bin(-1.0), 2);
        assert_eq!(scheme.to_bin(0.99999), 2);
        assert_eq!(scheme.to_bin(1.0), 3);
        assert_eq!(scheme.to_bin(2.99999), 3);
        assert_eq!(scheme.to_bin(3.0), 4);
        assert_eq!(scheme.to_bin(4.9999), 4);
        assert_eq!(scheme.to_bin(5.0), 4);
        assert_eq!(scheme.to_bin(5.001), 4);
        assert_eq!(scheme.from_bin(-1), f64::NEG_INFINITY);
        assert_eq!(scheme.from_bin(5), f64::INFINITY);
        assert!((scheme.from_bin(0) - -5.0).abs() < 0.001);
        assert!((scheme.from_bin(1) - -3.0).abs() < 0.001);
        assert!((scheme.from_bin(2) - -1.0).abs() < 0.001);
        assert!((scheme.from_bin(3) - 1.0).abs() < 0.001);
        assert!((scheme.from_bin(4) - 3.0).abs() < 0.001);
        check_binning_consistency(&scheme);
    }

    #[test]
    fn test_constant_bin_scheme_with_positive_range() {
        let scheme = ConstantBinScheme::new(5, 0.0, 5.0).unwrap();
        assert_eq!(
            scheme.to_bin(-1.0),
            0,
            "A value below the lower bound should map to the first bin"
        );
        assert_eq!(
            scheme.to_bin(5.01),
            4,
            "A value above the upper bound should map to the last bin"
        );
        assert_eq!(scheme.to_bin(-0.0001), 0);
        assert_eq!(scheme.to_bin(0.0000), 0);
        assert_eq!(scheme.to_bin(0.0001), 0);
        assert_eq!(scheme.to_bin(0.9999), 0);
        assert_eq!(scheme.to_bin(1.0000), 1);
        assert_eq!(scheme.to_bin(1.0001), 1);
        assert_eq!(scheme.to_bin(1.9999), 1);
        assert_eq!(scheme.to_bin(2.0000), 2);
        assert_eq!(scheme.to_bin(2.0001), 2);
        assert_eq!(scheme.to_bin(2.9999), 2);
        assert_eq!(scheme.to_bin(3.0000), 3);
        assert_eq!(scheme.to_bin(3.0001), 3);
        assert_eq!(scheme.to_bin(3.9999), 3);
        assert_eq!(scheme.to_bin(4.0000), 4);
        assert_eq!(scheme.to_bin(4.9999), 4);
        assert_eq!(scheme.to_bin(5.0000), 4);
        assert_eq!(scheme.to_bin(5.0001), 4);
        assert_eq!(scheme.from_bin(-1), f64::NEG_INFINITY);
        assert_eq!(scheme.from_bin(5), f64::INFINITY);
        assert!((scheme.from_bin(0) - 0.0).abs() < 0.001);
        assert!((scheme.from_bin(1) - 1.0).abs() < 0.001);
        assert!((scheme.from_bin(2) - 2.0).abs() < 0.001);
        assert!((scheme.from_bin(3) - 3.0).abs() < 0.001);
        assert!((scheme.from_bin(4) - 4.0).abs() < 0.001);
        check_binning_consistency(&scheme);
    }

    #[test]
    fn test_linear_bin_scheme() {
        let scheme = LinearBinScheme::new(10, 10.0).unwrap();
        assert_eq!(scheme.from_bin(-1), f64::NEG_INFINITY);
        assert_eq!(scheme.from_bin(11), f64::INFINITY);
        assert!((scheme.from_bin(0) - 0.0).abs() < 0.001);
        assert!((scheme.from_bin(1) - 0.2222).abs() < 0.001);
        assert!((scheme.from_bin(2) - 0.6666).abs() < 0.001);
        assert!((scheme.from_bin(3) - 1.3333).abs() < 0.001);
        assert!((scheme.from_bin(4) - 2.2222).abs() < 0.001);
        assert!((scheme.from_bin(5) - 3.3333).abs() < 0.001);
        assert!((scheme.from_bin(6) - 4.6667).abs() < 0.001);
        assert!((scheme.from_bin(7) - 6.2222).abs() < 0.001);
        assert!((scheme.from_bin(8) - 8.0000).abs() < 0.001);
        assert!((scheme.from_bin(9) - 10.000).abs() < 0.001);
        assert_eq!(scheme.to_bin(0.0000), 0);
        assert_eq!(scheme.to_bin(0.2221), 0);
        assert_eq!(scheme.to_bin(0.2223), 1);
        assert_eq!(scheme.to_bin(0.6667), 2);
        assert_eq!(scheme.to_bin(1.3334), 3);
        assert_eq!(scheme.to_bin(2.2223), 4);
        assert_eq!(scheme.to_bin(3.3334), 5);
        assert_eq!(scheme.to_bin(4.6667), 6);
        assert_eq!(scheme.to_bin(6.2223), 7);
        assert_eq!(scheme.to_bin(8.0000), 8);
        assert_eq!(scheme.to_bin(10.000), 9);
        assert_eq!(scheme.to_bin(10.001), 9);
        assert_eq!(scheme.from_bin(10), f64::INFINITY);
        check_binning_consistency(&scheme);
    }

    fn check_binning_consistency(scheme: &dyn BinScheme) {
        for bin in 0..scheme.bins() {
            let from_bin = scheme.from_bin(bin as i32);
            let bin_again = scheme.to_bin(from_bin + EPS);
            assert_eq!(
                bin, bin_again,
                "unbinning and rebinning bin {bin} gave a different result ({from_bin})"
            );
        }
    }
}
