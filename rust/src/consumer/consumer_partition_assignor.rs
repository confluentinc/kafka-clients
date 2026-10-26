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

//! Data holders for the consumer partition assignor protocol.
//!
//! Translated from the `Subscription` and `Assignment` nested data classes of
//! `org.apache.kafka.clients.consumer.ConsumerPartitionAssignor`.
//!
//! Only the two data holders are in scope here: they are consumed by
//! `ConsumerProtocol`
//! for (de)serializing member subscriptions/assignments, which the admin
//! group-describe path relies on. The `ConsumerPartitionAssignor` trait itself
//! and the client-side assignors (`RangeAssignor`, `StickyAssignor`, ...) are
//! out of scope for Milestone 8/11 (KIP-848 uses server-side assignment) — see
//! `consumer-threading.md` §20.

use crate::common::Error;
use crate::common::TopicPartition;

/// The default generation id used when a subscription carries no generation.
///
/// Corresponds to `AbstractStickyAssignor.DEFAULT_GENERATION`, which
/// `org.apache.kafka.clients.consumer.ConsumerPartitionAssignor` static-imports.
///
/// Stays a module-level constant rather than becoming an associated const:
/// the Java class that *declares* it, `AbstractStickyAssignor`, is a
/// client-side assignor and is out of scope per `consumer-threading.md` §20,
/// so there is no translated type to host it on. It is referenced only within
/// this file, and is not public: its Java home is in `consumer.internals`.
pub(crate) const DEFAULT_GENERATION: i32 = -1;

/// The parameters of [`Subscription::with_options`], built with
/// [`SubscriptionOptionsBuilder`]: `topics` is mandatory.
// The parameters of Java's widest `Subscription` constructor
// (`Subscription(List, ByteBuffer, List, int, Optional<String>)`). That
// constructor carries four parameters beyond the overload group's `{topics}`
// intersection, so the naming rule caps its derived name and makes this struct
// the method's only parameter, `topics` included (DoD #7).
//
// It deliberately has no `Default`: `topics` is what even Java's narrowest
// `Subscription(List)` constructor takes from its caller, so it has no
// Java-derived default, and a synthesised empty topic list would silently
// produce a subscription to nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
// the options of Subscription's constructor overloads (CLAUDE.md §2)
#[doc(alias = "rust-only")]
pub struct SubscriptionOptions {
    // Java's `topics`.
    pub(crate) topics: Vec<String>,
    // Java's `userData`. Starts as `None`, as in `Subscription(List)`.
    pub(crate) user_data: Option<Vec<u8>>,
    // Java's `ownedPartitions`. Starts empty, as in `Subscription(List)`
    // (`Collections.emptyList()`).
    pub(crate) owned_partitions: Vec<TopicPartition>,
    // Java's `generationId`. Starts as `DEFAULT_GENERATION`, as in
    // `Subscription(List)`.
    pub(crate) generation_id: i32,
    // Java's `rackId`. Starts as `None`, as `Subscription(List)`'s
    // `Optional.empty()`.
    pub(crate) rack_id: Option<String>,
}

/// Fluent builder for [`SubscriptionOptions`]: every parameter has a setter,
/// and [`Self::build`] checks that the mandatory ones were set.
#[derive(Debug)]
// builds SubscriptionOptions (CLAUDE.md §2)
#[doc(alias = "rust-only")]
pub struct SubscriptionOptionsBuilder {
    topics: Option<Vec<String>>,
    user_data: Option<Vec<u8>>,
    owned_partitions: Vec<TopicPartition>,
    generation_id: i32,
    rack_id: Option<String>,
}

impl Default for SubscriptionOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SubscriptionOptionsBuilder {
    /// Creates a builder with every mandatory parameter unset and every other
    /// parameter at the value Java passes on the caller's behalf.
    ///
    /// # Returns
    ///
    /// The new builder.
    pub fn new() -> Self {
        Self {
            topics: None,
            user_data: None,
            owned_partitions: Vec::new(),
            generation_id: DEFAULT_GENERATION,
            rack_id: None,
        }
    }

    /// Sets `SubscriptionOptions::topics`, a mandatory parameter: [`Self::build`]
    /// returns an error if it was not set.
    ///
    /// # Arguments
    ///
    /// - `topics` - The list of topics to subscribe to
    ///
    /// # Returns
    ///
    /// This builder.
    pub fn set_topics(mut self, topics: Vec<String>) -> Self {
        self.topics = Some(topics);
        self
    }
    /// Sets `SubscriptionOptions::user_data`.
    ///
    /// # Arguments
    ///
    /// - `user_data` - Nullable user data to include in the subscription
    ///
    /// # Returns
    ///
    /// This builder.
    pub fn set_user_data(mut self, user_data: Option<Vec<u8>>) -> Self {
        self.user_data = user_data;
        self
    }
    /// Sets `SubscriptionOptions::owned_partitions`.
    ///
    /// # Arguments
    ///
    /// - `owned_partitions` - The partitions currently owned by this consumer
    ///
    /// # Returns
    ///
    /// This builder.
    pub fn set_owned_partitions(mut self, owned_partitions: Vec<TopicPartition>) -> Self {
        self.owned_partitions = owned_partitions;
        self
    }
    /// Sets `SubscriptionOptions::generation_id`.
    ///
    /// # Arguments
    ///
    /// - `generation_id` - The generation ID of the consumer group
    ///
    /// # Returns
    ///
    /// This builder.
    pub fn set_generation_id(mut self, generation_id: i32) -> Self {
        self.generation_id = generation_id;
        self
    }
    /// Sets `SubscriptionOptions::rack_id`.
    ///
    /// # Arguments
    ///
    /// - `rack_id` - Optional rack ID for rack-aware assignment
    ///
    /// # Returns
    ///
    /// This builder.
    pub fn set_rack_id(mut self, rack_id: Option<String>) -> Self {
        self.rack_id = rack_id;
        self
    }

    /// Returns the built options.
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` naming the first mandatory
    ///   parameter (today only `topics`) that was not set.
    // The mandatory parameters are validated here rather than being named in
    // the constructor, so a later Java version that makes one of them optional
    // changes the set this accepts instead of adding a second constructor.
    // Only presence is checked here; semantic validation belongs to the method
    // the options are passed to.
    pub fn build(self) -> Result<SubscriptionOptions, Error> {
        Ok(SubscriptionOptions {
            topics: self.topics.ok_or_else(|| Self::missing("topics"))?,
            user_data: self.user_data,
            owned_partitions: self.owned_partitions,
            generation_id: self.generation_id,
            rack_id: self.rack_id,
        })
    }

    /// Builds the error naming a mandatory parameter [`Self::build`] found
    /// unset.
    fn missing(parameter: &str) -> Error {
        Error::local_illegal_argument(format!(
            "SubscriptionOptionsBuilder::build: mandatory parameter `{parameter}` was not set"
        ))
    }
}

/// Represents a consumer's subscription information including topics, user
/// data, and owned partitions.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$Subscription")]
pub struct Subscription {
    topics: Vec<String>,
    user_data: Option<Vec<u8>>,
    owned_partitions: Vec<TopicPartition>,
    group_instance_id: Option<String>,
    generation_id: Option<i32>,
    rack_id: Option<String>,
}

impl Subscription {
    // Java's four `Subscription` constructors intersect on `{topics}`, and
    // `Subscription(List topics)` is exactly that, so it keeps the plain name
    // `new` and the others carry their Rust parameters beyond it (CLAUDE.md
    // §2). Only the five-parameter one would need more than three parameters
    // in its name, so it alone takes the `Options` shape, where the struct is
    // the method's only parameter.

    /// Constructs a basic subscription with only topics.
    ///
    /// # Arguments
    ///
    /// - `topics` - The list of topics to subscribe to
    ///
    /// # Returns
    ///
    /// The new `Subscription`.
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$Subscription#Subscription")]
    pub fn new(topics: Vec<String>) -> Self {
        Self::with_options(
            SubscriptionOptionsBuilder::new()
                .set_topics(topics)
                .build()
                .expect("SubscriptionOptionsBuilder::build: every mandatory parameter is set above"),
        )
    }

    /// Constructs a subscription without owned partitions.
    ///
    /// # Arguments
    ///
    /// - `topics` - The list of topics to subscribe to
    /// - `user_data` - Nullable user data to include in the subscription
    ///
    /// # Returns
    ///
    /// The new `Subscription`.
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$Subscription#Subscription")]
    pub fn with_user_data(topics: Vec<String>, user_data: Option<Vec<u8>>) -> Self {
        Self::with_options(
            SubscriptionOptionsBuilder::new()
                .set_topics(topics)
                .set_user_data(user_data)
                .build()
                .expect("SubscriptionOptionsBuilder::build: every mandatory parameter is set above"),
        )
    }

    /// Constructs a subscription without generation ID and rack ID.
    ///
    /// # Arguments
    ///
    /// - `topics` - The list of topics to subscribe to
    /// - `user_data` - Nullable user data to include in the subscription
    /// - `owned_partitions` - The partitions currently owned by this consumer
    ///
    /// # Returns
    ///
    /// The new `Subscription`.
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$Subscription#Subscription")]
    pub fn with_user_data_owned_partitions(
        topics: Vec<String>,
        user_data: Option<Vec<u8>>,
        owned_partitions: Vec<TopicPartition>,
    ) -> Self {
        Self::with_options(
            SubscriptionOptionsBuilder::new()
                .set_topics(topics)
                .set_user_data(user_data)
                .set_owned_partitions(owned_partitions)
                .build()
                .expect("SubscriptionOptionsBuilder::build: every mandatory parameter is set above"),
        )
    }

    /// Constructs a subscription with full details.
    ///
    /// # Arguments
    ///
    /// - `options` - The topics, user data, owned partitions, generation ID and
    ///   rack ID
    ///
    /// # Returns
    ///
    /// The new `Subscription`.
    // A `generation_id` less than zero is mapped to `None`, matching Java's
    // `generationId < 0 ? Optional.empty() : Optional.of(generationId)`.
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$Subscription#Subscription")]
    pub fn with_options(options: SubscriptionOptions) -> Self {
        let SubscriptionOptions { topics, user_data, owned_partitions, generation_id, rack_id } = options;
        Self {
            topics,
            user_data,
            owned_partitions,
            group_instance_id: None,
            generation_id: if generation_id < 0 { None } else { Some(generation_id) },
            rack_id,
        }
    }

    /// The subscribed topics.
    ///
    /// # Returns
    ///
    /// The list of topics
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$Subscription#topics")]
    pub fn topics(&self) -> &[String] {
        &self.topics
    }

    /// The opaque user data attached to the subscription.
    ///
    /// # Returns
    ///
    /// The user data, or None if none was provided
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$Subscription#userData")]
    pub fn user_data(&self) -> Option<&[u8]> {
        self.user_data.as_deref()
    }

    /// The partitions currently owned by the member.
    ///
    /// # Returns
    ///
    /// The list of owned partitions
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$Subscription#ownedPartitions")]
    pub fn owned_partitions(&self) -> &[TopicPartition] {
        &self.owned_partitions
    }

    /// The rack id of the member, if any.
    ///
    /// # Returns
    ///
    /// The rack ID, or empty if not provided
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$Subscription#rackId")]
    pub fn rack_id(&self) -> Option<&str> {
        self.rack_id.as_deref()
    }

    /// Sets the group instance id.
    ///
    /// # Arguments
    ///
    /// - `group_instance_id` - The group instance ID
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$Subscription#setGroupInstanceId")]
    pub fn set_group_instance_id(&mut self, group_instance_id: Option<String>) {
        self.group_instance_id = group_instance_id;
    }

    /// The group instance id of the member, if any.
    ///
    /// # Returns
    ///
    /// The group instance ID, or empty if this is a dynamic member
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$Subscription#groupInstanceId")]
    pub fn group_instance_id(&self) -> Option<&str> {
        self.group_instance_id.as_deref()
    }

    /// The generation id of the subscription, if any.
    ///
    /// # Returns
    ///
    /// The generation ID, or empty if not provided
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$Subscription#generationId")]
    pub fn generation_id(&self) -> Option<i32> {
        self.generation_id
    }
}

/// Represents the partition assignment for a consumer.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$Assignment")]
pub struct Assignment {
    partitions: Vec<TopicPartition>,
    user_data: Option<Vec<u8>>,
}

impl Assignment {
    // Java's two `Assignment` constructors intersect on `{partitions}`, and
    // `Assignment(List partitions)` is exactly that, so it keeps the plain
    // name `new` (CLAUDE.md §2).

    /// Constructs an assignment with only partitions.
    ///
    /// # Arguments
    ///
    /// - `partitions` - The list of partitions assigned to the consumer
    ///
    /// # Returns
    ///
    /// The new `Assignment`.
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$Assignment#Assignment")]
    pub fn new(partitions: Vec<TopicPartition>) -> Self {
        Self::with_user_data(partitions, None)
    }

    /// Constructs an assignment with partitions and user data.
    ///
    /// # Arguments
    ///
    /// - `partitions` - The list of partitions assigned to the consumer
    /// - `user_data` - Nullable user data to include in the assignment
    ///
    /// # Returns
    ///
    /// The new `Assignment`.
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$Assignment#Assignment")]
    pub fn with_user_data(partitions: Vec<TopicPartition>, user_data: Option<Vec<u8>>) -> Self {
        Self { partitions, user_data }
    }

    /// The assigned partitions.
    ///
    /// # Returns
    ///
    /// The list of partitions
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$Assignment#partitions")]
    pub fn partitions(&self) -> &[TopicPartition] {
        &self.partitions
    }

    /// The opaque user data attached to the assignment.
    ///
    /// # Returns
    ///
    /// The user data, or None if none was provided
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$Assignment#userData")]
    pub fn user_data(&self) -> Option<&[u8]> {
        self.user_data.as_deref()
    }
}

/// Java's `List.toString()`: `[a, b]`.
fn java_list<T: std::fmt::Display>(items: &[T]) -> String {
    format!("[{}]", items.iter().map(ToString::to_string).collect::<Vec<_>>().join(", "))
}

impl std::fmt::Display for Subscription {
    // Java's `toString()`: the user data prints as its size and only when
    // present, an absent group instance or rack id as `null`, an absent
    // generation as `-1`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Subscription(topics={}", java_list(&self.topics))?;
        if let Some(user_data) = &self.user_data {
            write!(f, ", userDataSize={}", user_data.len())?;
        }
        write!(
            f,
            ", ownedPartitions={}, groupInstanceId={}, generationId={}, rackId={})",
            java_list(&self.owned_partitions),
            self.group_instance_id.as_deref().unwrap_or("null"),
            self.generation_id.unwrap_or(-1),
            self.rack_id.as_deref().unwrap_or("null")
        )
    }
}

impl std::fmt::Display for Assignment {
    // Java's `toString()`: the user data prints as its size and only when
    // present.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Assignment(partitions={}", java_list(&self.partitions))?;
        if let Some(user_data) = &self.user_data {
            write!(f, ", userDataSize={}", user_data.len())?;
        }
        f.write_str(")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_string_follows_java() {
        let tp = |p| TopicPartition::new("t", p);
        assert_eq!(
            Subscription::new(vec!["a".to_string(), "b".to_string()]).to_string(),
            "Subscription(topics=[a, b], ownedPartitions=[], groupInstanceId=null, generationId=-1, rackId=null)"
        );
        let mut s = Subscription::with_options(
            SubscriptionOptionsBuilder::new()
                .set_topics(vec!["t".to_string()])
                .set_user_data(Some(vec![1, 2, 3]))
                .set_owned_partitions(vec![tp(0), tp(1)])
                .set_generation_id(5)
                .set_rack_id(Some("r".to_string()))
                .build()
                .unwrap(),
        );
        s.set_group_instance_id(Some("i".to_string()));
        assert_eq!(
            s.to_string(),
            "Subscription(topics=[t], userDataSize=3, ownedPartitions=[t-0, t-1], groupInstanceId=i, generationId=5, \
             rackId=r)"
        );
        assert_eq!(Assignment::new(vec![tp(0)]).to_string(), "Assignment(partitions=[t-0])");
        assert_eq!(
            Assignment::with_user_data(Vec::new(), Some(Vec::new())).to_string(),
            "Assignment(partitions=[], userDataSize=0)"
        );
    }

    #[test]
    fn subscription_negative_generation_is_none() {
        let s = Subscription::with_options(
            SubscriptionOptionsBuilder::new()
                .set_topics(vec!["t".to_string()])
                .set_generation_id(-1)
                .build()
                .unwrap(),
        );
        assert_eq!(s.generation_id(), None);
    }

    #[test]
    fn subscription_non_negative_generation_is_some() {
        let s = Subscription::with_options(
            SubscriptionOptionsBuilder::new()
                .set_topics(vec!["t".to_string()])
                .set_generation_id(5)
                .set_rack_id(Some("r".to_string()))
                .build()
                .unwrap(),
        );
        assert_eq!(s.generation_id(), Some(5));
        assert_eq!(s.rack_id(), Some("r"));
    }

    #[test]
    fn subscription_defaults() {
        let s = Subscription::new(vec!["a".to_string(), "b".to_string()]);
        assert_eq!(s.topics(), &["a".to_string(), "b".to_string()]);
        assert!(s.user_data().is_none());
        assert!(s.owned_partitions().is_empty());
        assert_eq!(s.generation_id(), None);
    }

    #[test]
    fn assignment_accessors() {
        let a = Assignment::new(vec![TopicPartition::new("t", 0)]);
        assert_eq!(a.partitions().len(), 1);
        assert!(a.user_data().is_none());
    }

    /// CLAUDE.md §2: the mandatory parameters are validated in
    /// [`SubscriptionOptionsBuilder::build`], not named in the constructor, so a
    /// builder left untouched panics naming the first one it finds unset.
    #[test]
    fn subscription_options_builder_build_errors_when_no_mandatory_parameter_is_set() {
        let Err(error) = SubscriptionOptionsBuilder::new().build() else {
            panic!("build must reject the unset mandatory parameter");
        };
        assert!(matches!(error, Error::LocalIllegalArgument(_)), "{error:?}");
        assert_eq!(
            error.message(),
            "SubscriptionOptionsBuilder::build: mandatory parameter `topics` was not set"
        );
    }
}
