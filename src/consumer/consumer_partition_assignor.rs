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
//! [`ConsumerProtocol`](crate::consumer::internals::ConsumerProtocol)
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
/// `ConsumerPartitionAssignor.java:34` static-imports.
///
/// Stays a module-level constant rather than becoming an associated const:
/// the Java class that *declares* it, `AbstractStickyAssignor`, is a
/// client-side assignor and is out of scope per `consumer-threading.md` §20,
/// so there is no translated type to host it on. It is referenced only within
/// this file.
pub const DEFAULT_GENERATION: i32 = -1;

/// The parameters of Java's widest `Subscription` constructor
/// (`Subscription(List, ByteBuffer, List, int, Optional<String>)`,
/// `ConsumerPartitionAssignor.java:113`).
///
/// That constructor carries four parameters beyond the overload group's
/// `{topics}` intersection, so CLAUDE.md §2 caps its derived name and makes
/// this struct the method's *only* parameter — every Java parameter lives
/// here, `topics` included. This struct has no Java counterpart: it exists
/// solely to satisfy that naming rule (DoD #7).
///
/// It deliberately has **no** `Default`. `topics` is what even Java's
/// narrowest `Subscription` constructor (`:130`) takes from its caller, so it
/// has no Java-derived default, and a synthesised empty topic list would
/// silently produce a subscription to nothing. Construct it with
/// [`SubscriptionOptionsBuilder::new`] and set it: [`SubscriptionOptionsBuilder::build`] returns an error if `topics` was not set.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct SubscriptionOptions {
    /// Java's `topics`.
    pub topics: Vec<String>,
    /// Java's `userData`. Starts as `None`, as in `:130`.
    pub user_data: Option<Vec<u8>>,
    /// Java's `ownedPartitions`. Starts empty, as in `:130`
    /// (`Collections.emptyList()`).
    pub owned_partitions: Vec<TopicPartition>,
    /// Java's `generationId`. Starts as [`DEFAULT_GENERATION`], as in `:130`.
    pub generation_id: i32,
    /// Java's `rackId`. Starts as `None`, as in `:130`'s `Optional.empty()`.
    pub rack_id: Option<String>,
}

/// Fluent builder for [`SubscriptionOptions`].
///
/// Per CLAUDE.md §2 [`Self::new`] takes no parameters, every parameter has a
/// fluent setter, and [`Self::build`] validates the mandatory ones — returning
/// [`Error::LocalIllegalArgument`] if they were not set. Like [`SubscriptionOptions`] it has no Java counterpart and
/// exists solely to satisfy that naming rule (DoD #7).
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
    pub fn new() -> Self {
        Self {
            topics: None,
            user_data: None,
            owned_partitions: Vec::new(),
            generation_id: DEFAULT_GENERATION,
            rack_id: None,
        }
    }

    /// Sets [`SubscriptionOptions::topics`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_topics(mut self, topics: Vec<String>) -> Self {
        self.topics = Some(topics);
        self
    }
    /// Sets [`SubscriptionOptions::user_data`].
    pub fn set_user_data(mut self, user_data: Option<Vec<u8>>) -> Self {
        self.user_data = user_data;
        self
    }
    /// Sets [`SubscriptionOptions::owned_partitions`].
    pub fn set_owned_partitions(mut self, owned_partitions: Vec<TopicPartition>) -> Self {
        self.owned_partitions = owned_partitions;
        self
    }
    /// Sets [`SubscriptionOptions::generation_id`].
    pub fn set_generation_id(mut self, generation_id: i32) -> Self {
        self.generation_id = generation_id;
        self
    }
    /// Sets [`SubscriptionOptions::rack_id`].
    pub fn set_rack_id(mut self, rack_id: Option<String>) -> Self {
        self.rack_id = rack_id;
        self
    }

    /// Returns the built options.
    ///
    /// Per CLAUDE.md §2 the mandatory parameters are validated here rather than
    /// being named in the constructor, so a later Java version that makes one of
    /// them optional changes the set this accepts instead of adding a second
    /// constructor. Today there is one mandatory set: `topics`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LocalIllegalArgument`] naming the first parameter of that
    /// set which was not given a setter call. Only presence is checked here;
    /// semantic validation belongs to the method the options are passed to
    /// (CLAUDE.md §2).
    pub fn build(self) -> Result<SubscriptionOptions, Error> {
        Ok(SubscriptionOptions {
            topics: self.topics.ok_or_else(|| Self::missing("topics"))?,
            user_data: self.user_data,
            owned_partitions: self.owned_partitions,
            generation_id: self.generation_id,
            rack_id: self.rack_id,
        })
    }

    /// Builds the [`Error::LocalIllegalArgument`] naming a mandatory parameter
    /// [`Self::build`] found unset.
    fn missing(parameter: &str) -> Error {
        Error::local_illegal_argument(format!(
            "SubscriptionOptionsBuilder::build: mandatory parameter `{parameter}` was not set"
        ))
    }
}

/// A consumer member's subscription.
///
/// Corresponds to `ConsumerPartitionAssignor.Subscription`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Subscription {
    topics: Vec<String>,
    user_data: Option<Vec<u8>>,
    owned_partitions: Vec<TopicPartition>,
    group_instance_id: Option<String>,
    generation_id: Option<i32>,
    rack_id: Option<String>,
}

impl Subscription {
    // Java's four `Subscription` constructors
    // (`ConsumerPartitionAssignor.java:113,122,126,130`) intersect on
    // `{topics}`, and `Subscription(List topics)` (`:130`) is exactly that — so
    // it keeps the plain name `new` and the others carry their Rust parameters
    // beyond it (CLAUDE.md §2). Only `:113` would need more than three
    // parameters in its name, so it alone takes the `Options` shape, where the
    // struct is the method's only parameter.

    /// Creates a subscription from topics only.
    ///
    /// Mirrors `Subscription(List)` (`:130`).
    pub fn new(topics: Vec<String>) -> Self {
        Self::with_options(
            SubscriptionOptionsBuilder::new()
                .set_topics(topics)
                .build()
                .expect("SubscriptionOptionsBuilder::build: every mandatory parameter is set above"),
        )
    }

    /// Creates a subscription from topics and optional user data.
    ///
    /// Mirrors `Subscription(List, ByteBuffer)` (`:126`).
    pub fn with_user_data(topics: Vec<String>, user_data: Option<Vec<u8>>) -> Self {
        Self::with_options(
            SubscriptionOptionsBuilder::new()
                .set_topics(topics)
                .set_user_data(user_data)
                .build()
                .expect("SubscriptionOptionsBuilder::build: every mandatory parameter is set above"),
        )
    }

    /// Creates a subscription from topics, optional user data and owned
    /// partitions (default generation, no rack).
    ///
    /// Mirrors `Subscription(List, ByteBuffer, List)` (`:122`).
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

    /// Creates a subscription with all fields.
    ///
    /// Mirrors `Subscription(List, ByteBuffer, List, int, Optional<String>)`
    /// (`:113`); all five of its parameters are carried by
    /// [`SubscriptionOptions`] — see the note above this overload group.
    ///
    /// A `generation_id` less than zero is mapped to `None`, matching Java's
    /// `generationId < 0 ? Optional.empty() : Optional.of(generationId)`.
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
    pub fn topics(&self) -> &[String] {
        &self.topics
    }

    /// The opaque user data attached to the subscription.
    pub fn user_data(&self) -> Option<&[u8]> {
        self.user_data.as_deref()
    }

    /// The partitions currently owned by the member.
    pub fn owned_partitions(&self) -> &[TopicPartition] {
        &self.owned_partitions
    }

    /// The rack id of the member, if any.
    pub fn rack_id(&self) -> Option<&str> {
        self.rack_id.as_deref()
    }

    /// Sets the group instance id.
    pub fn set_group_instance_id(&mut self, group_instance_id: Option<String>) {
        self.group_instance_id = group_instance_id;
    }

    /// The group instance id of the member, if any.
    pub fn group_instance_id(&self) -> Option<&str> {
        self.group_instance_id.as_deref()
    }

    /// The generation id of the subscription, if any.
    pub fn generation_id(&self) -> Option<i32> {
        self.generation_id
    }
}

/// A consumer member's assignment.
///
/// Corresponds to `ConsumerPartitionAssignor.Assignment`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Assignment {
    partitions: Vec<TopicPartition>,
    user_data: Option<Vec<u8>>,
}

impl Assignment {
    // Java's two `Assignment` constructors
    // (`ConsumerPartitionAssignor.java:179,184`) intersect on `{partitions}`,
    // and `Assignment(List partitions)` (`:184`) is exactly that — so it keeps
    // the plain name `new` (CLAUDE.md §2).

    /// Creates an assignment from partitions only.
    ///
    /// Mirrors `Assignment(List)` (`:184`).
    pub fn new(partitions: Vec<TopicPartition>) -> Self {
        Self::with_user_data(partitions, None)
    }

    /// Creates an assignment with partitions and optional user data.
    ///
    /// Mirrors `Assignment(List, ByteBuffer)` (`:179`).
    pub fn with_user_data(partitions: Vec<TopicPartition>, user_data: Option<Vec<u8>>) -> Self {
        Self { partitions, user_data }
    }

    /// The assigned partitions.
    pub fn partitions(&self) -> &[TopicPartition] {
        &self.partitions
    }

    /// The opaque user data attached to the assignment.
    pub fn user_data(&self) -> Option<&[u8]> {
        self.user_data.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
