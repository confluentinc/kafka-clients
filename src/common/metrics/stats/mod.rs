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

//! Concrete statistics (`org.apache.kafka.common.metrics.stats`).

mod avg;
mod cumulative_count;
mod cumulative_sum;
mod max;
mod meter;
mod min;
mod rate;
pub mod sampled_stat;
mod simple_rate;
mod value;
mod windowed_count;
mod windowed_sum;

pub use avg::Avg;
pub use cumulative_count::CumulativeCount;
pub use cumulative_sum::CumulativeSum;
pub use max::Max;
pub use meter::Meter;
pub use min::Min;
pub use rate::Rate;
pub use sampled_stat::{Sample, SampledStat, SampledStatKind};
pub use simple_rate::SimpleRate;
pub use value::Value;
pub use windowed_count::WindowedCount;
pub use windowed_sum::WindowedSum;
pub(crate) use windowed_sum::WindowedSumKind;
