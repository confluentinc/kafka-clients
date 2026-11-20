// Copyright 2026 Confluent Inc.
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

//! The parameters of
//! [`ShareConsumer::acknowledge_with_options`](crate::consumer::ShareConsumer::acknowledge_with_options).
//!
//! Has no Java class (`definition-of-done.md` §7): Java's
//! `ShareConsumer#acknowledge(String, int, long, AcknowledgeType)` takes four
//! parameters, and CLAUDE.md §2 gives a method with more than three
//! discriminating parameters one `Options` parameter built with a builder.

use crate::common::Error;
use crate::consumer::AcknowledgeType;

/// The parameters of
/// [`ShareConsumer::acknowledge_with_options`](crate::consumer::ShareConsumer::acknowledge_with_options),
/// built with [`AcknowledgeOptionsBuilder`]: the topic, partition and offset of
/// the record to acknowledge, and the acknowledge type. All four are mandatory.
// Java's `acknowledge(String topic, int partition, long offset,
// AcknowledgeType type)`. Read only by the consumers, so the fields are
// crate-private, as `ConsumerRecordOptions`'s are.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
// the options of ShareConsumer's four-parameter acknowledge overload (CLAUDE.md §2)
#[doc(alias = "rust-only")]
pub struct AcknowledgeOptions {
    // The topic of the record to acknowledge. Java's `topic`.
    pub(crate) topic: String,
    // The partition of the record to acknowledge. Java's `partition`.
    pub(crate) partition: i32,
    // The offset of the record to acknowledge. Java's `offset`.
    pub(crate) offset: i64,
    // The acknowledge type which indicates whether it was processed
    // successfully. Java's `type`.
    pub(crate) acknowledge_type: AcknowledgeType,
}

/// Fluent builder for [`AcknowledgeOptions`]: every parameter has a setter,
/// and [`Self::build`] checks that the mandatory ones were set.
#[derive(Clone, Debug, Default)]
// builds AcknowledgeOptions (CLAUDE.md §2)
#[doc(alias = "rust-only")]
pub struct AcknowledgeOptionsBuilder {
    topic: Option<String>,
    partition: Option<i32>,
    offset: Option<i64>,
    acknowledge_type: Option<AcknowledgeType>,
}

impl AcknowledgeOptionsBuilder {
    /// Creates a builder with every mandatory parameter unset.
    ///
    /// # Returns
    ///
    /// The new builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the topic, a mandatory parameter: [`Self::build`] returns an error
    /// if it was not set.
    ///
    /// # Arguments
    ///
    /// - `topic` - The topic of the record to acknowledge
    ///
    /// # Returns
    ///
    /// This builder.
    pub fn set_topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = Some(topic.into());
        self
    }

    /// Sets the partition, a mandatory parameter: [`Self::build`] returns an
    /// error if it was not set.
    ///
    /// # Arguments
    ///
    /// - `partition` - The partition of the record to acknowledge
    ///
    /// # Returns
    ///
    /// This builder.
    pub fn set_partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }

    /// Sets the offset, a mandatory parameter: [`Self::build`] returns an
    /// error if it was not set.
    ///
    /// # Arguments
    ///
    /// - `offset` - The offset of the record to acknowledge
    ///
    /// # Returns
    ///
    /// This builder.
    pub fn set_offset(mut self, offset: i64) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Sets the acknowledge type, a mandatory parameter: [`Self::build`]
    /// returns an error if it was not set.
    ///
    /// # Arguments
    ///
    /// - `r#type` - The acknowledge type which indicates whether it was processed successfully
    ///
    /// # Returns
    ///
    /// This builder.
    pub fn set_type(mut self, r#type: AcknowledgeType) -> Self {
        self.acknowledge_type = Some(r#type);
        self
    }

    /// Returns the built options.
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` naming the first of `topic`,
    ///   `partition`, `offset` and `type` that was not set.
    // The mandatory parameters are validated here rather than being named in a
    // constructor, so a later Java version that makes one of them optional
    // changes the set this accepts instead of adding a second method.
    pub fn build(self) -> Result<AcknowledgeOptions, Error> {
        Ok(AcknowledgeOptions {
            topic: self.topic.ok_or_else(|| Self::missing("topic"))?,
            partition: self.partition.ok_or_else(|| Self::missing("partition"))?,
            offset: self.offset.ok_or_else(|| Self::missing("offset"))?,
            acknowledge_type: self.acknowledge_type.ok_or_else(|| Self::missing("type"))?,
        })
    }

    /// Builds the error naming a mandatory parameter [`Self::build`] found
    /// unset.
    fn missing(parameter: &str) -> Error {
        Error::local_illegal_argument(format!(
            "AcknowledgeOptionsBuilder::build: mandatory parameter `{parameter}` was not set"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn complete() -> AcknowledgeOptionsBuilder {
        AcknowledgeOptionsBuilder::new()
            .set_topic("topic")
            .set_partition(3)
            .set_offset(42)
            .set_type(AcknowledgeType::Reject)
    }

    /// R8 O1: the built options carry every parameter.
    #[test]
    fn build_with_every_parameter() {
        let options = complete().build().expect("every parameter is set");
        assert_eq!("topic", options.topic);
        assert_eq!(3, options.partition);
        assert_eq!(42, options.offset);
        assert_eq!(AcknowledgeType::Reject, options.acknowledge_type);
    }

    /// R8 O2: `build` names the first mandatory parameter left unset.
    #[test]
    fn build_names_the_first_unset_parameter() {
        let cases: [(AcknowledgeOptionsBuilder, &str); 5] = [
            (AcknowledgeOptionsBuilder::new(), "topic"),
            (AcknowledgeOptionsBuilder::new().set_topic("topic"), "partition"),
            (AcknowledgeOptionsBuilder::new().set_topic("topic").set_partition(0), "offset"),
            (
                AcknowledgeOptionsBuilder::new()
                    .set_topic("topic")
                    .set_partition(0)
                    .set_offset(0),
                "type",
            ),
            (
                AcknowledgeOptionsBuilder::new()
                    .set_partition(0)
                    .set_offset(0)
                    .set_type(AcknowledgeType::Accept),
                "topic",
            ),
        ];
        for (builder, missing) in cases {
            let error = builder.build().expect_err("a mandatory parameter is unset");
            assert!(error.is_local_illegal_argument_error(), "{error:?}");
            assert_eq!(
                format!("AcknowledgeOptionsBuilder::build: mandatory parameter `{missing}` was not set"),
                error.message()
            );
        }
    }
}
