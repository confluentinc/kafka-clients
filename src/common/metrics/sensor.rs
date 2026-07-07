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

//! Sensors and their recording levels.
//!
//! A sensor applies a continuous sequence of numerical values to a set of
//! associated metrics. Its [`RecordingLevel`] controls the verbosity at which
//! measurements are recorded.

use crate::common::KafkaError;

/// The recording level configured for a sensor, controlling the verbosity at
/// which its measurements are kept.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RecordingLevel {
    /// Always recorded.
    Info,
    /// Recorded at `DEBUG` verbosity and above.
    Debug,
    /// Recorded only at `TRACE` verbosity.
    Trace,
}

/// The lowest valid recording-level id.
pub const MIN_RECORDING_LEVEL_KEY: i32 = 0;

/// The highest valid recording-level id.
pub const MAX_RECORDING_LEVEL_KEY: i32 = 2;

impl RecordingLevel {
    /// The permanent, immutable id of the recording level.
    pub fn id(&self) -> i16 {
        match self {
            Self::Info => 0,
            Self::Debug => 1,
            Self::Trace => 2,
        }
    }

    /// The upper-case name of the recording level.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Info => "INFO",
            Self::Debug => "DEBUG",
            Self::Trace => "TRACE",
        }
    }

    /// Returns the recording level for the given id.
    ///
    /// Returns [`KafkaError::IllegalArgument`] if the id is outside the valid
    /// range.
    pub fn for_id(id: i32) -> Result<Self, KafkaError> {
        match id {
            0 => Ok(Self::Info),
            1 => Ok(Self::Debug),
            2 => Ok(Self::Trace),
            _ => Err(KafkaError::illegal_argument(format!(
                "Unexpected RecordLevel id `{id}`, it should be between `{MIN_RECORDING_LEVEL_KEY}` and \
                 `{MAX_RECORDING_LEVEL_KEY}` (inclusive)"
            ))),
        }
    }

    /// Case-insensitive lookup by name.
    ///
    /// Returns [`KafkaError::IllegalArgument`] if the name does not match a
    /// known level.
    pub fn for_name(name: &str) -> Result<Self, KafkaError> {
        match name.to_uppercase().as_str() {
            "INFO" => Ok(Self::Info),
            "DEBUG" => Ok(Self::Debug),
            "TRACE" => Ok(Self::Trace),
            other => Err(KafkaError::illegal_argument(format!("No enum constant RecordingLevel.{other}"))),
        }
    }

    /// Whether a sensor at this recording level should record when the metrics
    /// repository is configured at `config_id`.
    ///
    /// Returns [`KafkaError::IllegalState`] if `config_id` is not a recognized
    /// recording level.
    pub fn should_record(&self, config_id: i32) -> Result<bool, KafkaError> {
        let this = self.id() as i32;
        if config_id == Self::Info.id() as i32 {
            Ok(this == Self::Info.id() as i32)
        } else if config_id == Self::Debug.id() as i32 {
            Ok(this == Self::Info.id() as i32 || this == Self::Debug.id() as i32)
        } else if config_id == Self::Trace.id() as i32 {
            Ok(true)
        } else {
            Err(KafkaError::illegal_state(format!(
                "Did not recognize recording level {config_id}"
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id_and_name() {
        assert_eq!(RecordingLevel::Info.id(), 0);
        assert_eq!(RecordingLevel::Debug.id(), 1);
        assert_eq!(RecordingLevel::Trace.id(), 2);
        assert_eq!(RecordingLevel::Info.name(), "INFO");
        assert_eq!(RecordingLevel::Debug.name(), "DEBUG");
        assert_eq!(RecordingLevel::Trace.name(), "TRACE");
    }

    #[test]
    fn test_for_id() {
        assert_eq!(RecordingLevel::for_id(0).unwrap(), RecordingLevel::Info);
        assert_eq!(RecordingLevel::for_id(2).unwrap(), RecordingLevel::Trace);
        let err = RecordingLevel::for_id(3).unwrap_err();
        assert!(
            err.message().contains("Unexpected RecordLevel id `3`"),
            "unexpected message: {}",
            err.message()
        );
        assert!(RecordingLevel::for_id(-1).is_err());
    }

    #[test]
    fn test_for_name_case_insensitive() {
        assert_eq!(RecordingLevel::for_name("info").unwrap(), RecordingLevel::Info);
        assert_eq!(RecordingLevel::for_name("Debug").unwrap(), RecordingLevel::Debug);
        assert_eq!(RecordingLevel::for_name("TRACE").unwrap(), RecordingLevel::Trace);
        assert!(RecordingLevel::for_name("bogus").is_err());
    }

    #[test]
    fn test_should_record() {
        // Configured at INFO: only INFO sensors record.
        assert!(RecordingLevel::Info.should_record(0).unwrap());
        assert!(!RecordingLevel::Debug.should_record(0).unwrap());
        assert!(!RecordingLevel::Trace.should_record(0).unwrap());
        // Configured at DEBUG: INFO and DEBUG sensors record.
        assert!(RecordingLevel::Info.should_record(1).unwrap());
        assert!(RecordingLevel::Debug.should_record(1).unwrap());
        assert!(!RecordingLevel::Trace.should_record(1).unwrap());
        // Configured at TRACE: everything records.
        assert!(RecordingLevel::Info.should_record(2).unwrap());
        assert!(RecordingLevel::Trace.should_record(2).unwrap());

        let err = RecordingLevel::Info.should_record(9).unwrap_err();
        assert!(
            err.message().contains("Did not recognize recording level 9"),
            "unexpected message: {}",
            err.message()
        );
    }
}
