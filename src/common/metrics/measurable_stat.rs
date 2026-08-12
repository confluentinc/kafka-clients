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

//! A `MeasurableStat` is a `Stat` that is also `Measurable`
//! (`org.apache.kafka.common.metrics.MeasurableStat`).

use crate::common::metrics::{Measurable, Stat};

/// A `MeasurableStat` is a [`Stat`] that is also [`Measurable`] (i.e. can produce
/// a single floating point value). This is the interface used for most of the
/// simple statistics such as `Avg`, `Max`, `CumulativeCount`, etc.
pub trait MeasurableStat: Stat + Measurable {}

// Blanket impl so any type implementing both `Stat` and `Measurable` is a
// `MeasurableStat`, mirroring Java's empty extending interface.
impl<T: Stat + Measurable> MeasurableStat for T {}
