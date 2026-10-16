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

//! The common APIs that are shared by the options of the `Admin` commands.
//!
//! Corresponds to `org.apache.kafka.clients.admin.AbstractOptions`.

use crate::admin::{
    AbortTransactionOptions, AlterClientQuotasOptions, AlterConfigsOptions, AlterConsumerGroupOffsetsOptions,
    AlterPartitionReassignmentsOptions, AlterReplicaLogDirsOptions, AlterUserScramCredentialsOptions,
    CreateAclsOptions, CreateDelegationTokenOptions, CreatePartitionsOptions, CreateTopicsOptions, DeleteAclsOptions,
    DeleteConsumerGroupOffsetsOptions, DeleteConsumerGroupsOptions, DeleteRecordsOptions, DeleteTopicsOptions,
    DescribeAclsOptions, DescribeClassicGroupsOptions, DescribeClientQuotasOptions, DescribeClusterOptions,
    DescribeConfigsOptions, DescribeConsumerGroupsOptions, DescribeDelegationTokenOptions, DescribeFeaturesOptions,
    DescribeLogDirsOptions, DescribeProducersOptions, DescribeReplicaLogDirsOptions, DescribeTopicsOptions,
    DescribeTransactionsOptions, DescribeUserScramCredentialsOptions, ElectLeadersOptions,
    ExpireDelegationTokenOptions, FenceProducersOptions, ListConfigResourcesOptions, ListConsumerGroupOffsetsOptions,
    ListGroupsOptions, ListOffsetsOptions, ListPartitionReassignmentsOptions, ListTopicsOptions,
    ListTransactionsOptions, RemoveMembersFromConsumerGroupOptions, RenewDelegationTokenOptions,
    TerminateTransactionOptions, UnregisterControllerOptions, UpdateFeaturesOptions,
};

/// The common APIs that are shared by the options of the various `Admin`
/// commands: the timeout of the operation.
///
/// Corresponds to `org.apache.kafka.clients.admin.AbstractOptions`, the
/// abstract class every Java `*Options` class extends. Rust has no class
/// inheritance, so the abstract class becomes a trait every options type
/// implements, and each type keeps the field Java inherits (`timeoutMs`). The
/// trait delegates to each type's own `set_timeout_ms` and `timeout_ms`, so
/// calling them needs no import; the trait is for code generic over the
/// options types, as Java code can be over `AbstractOptions<?>`.
#[doc(alias = "org.apache.kafka.clients.admin.AbstractOptions")]
pub trait AbstractOptions {
    /// Set the timeout in milliseconds for this operation, or `None` if the
    /// default API timeout for the `AdminClient` should be used.
    ///
    /// Java's `timeoutMs(Integer)` setter, which returns the options itself.
    /// It returns `Self`, so it is not callable through `dyn AbstractOptions`.
    #[must_use]
    fn set_timeout_ms(self, timeout_ms: Option<i32>) -> Self
    where
        Self: Sized;

    /// The timeout in milliseconds for this operation, or `None` if the default
    /// API timeout for the `AdminClient` should be used.
    #[doc(alias = "org.apache.kafka.clients.admin.AbstractOptions#timeoutMs")]
    fn timeout_ms(&self) -> Option<i32>;
}

/// Implements [`AbstractOptions`] for each options type by delegating to the
/// type's own `set_timeout_ms` and `timeout_ms`, which Java inherits from
/// `AbstractOptions`.
macro_rules! impl_abstract_options {
    ($($options:ty),* $(,)?) => {
        $(
            impl AbstractOptions for $options {
                fn set_timeout_ms(self, timeout_ms: Option<i32>) -> Self {
                    <$options>::set_timeout_ms(self, timeout_ms)
                }

                fn timeout_ms(&self) -> Option<i32> {
                    <$options>::timeout_ms(self)
                }
            }
        )*
    };
}

impl_abstract_options!(
    AbortTransactionOptions,
    AlterClientQuotasOptions,
    AlterConfigsOptions,
    AlterConsumerGroupOffsetsOptions,
    AlterPartitionReassignmentsOptions,
    AlterReplicaLogDirsOptions,
    AlterUserScramCredentialsOptions,
    CreateAclsOptions,
    CreateDelegationTokenOptions,
    CreatePartitionsOptions,
    CreateTopicsOptions,
    DeleteAclsOptions,
    DeleteConsumerGroupOffsetsOptions,
    DeleteConsumerGroupsOptions,
    DeleteRecordsOptions,
    DeleteTopicsOptions,
    DescribeAclsOptions,
    DescribeClassicGroupsOptions,
    DescribeClientQuotasOptions,
    DescribeClusterOptions,
    DescribeConfigsOptions,
    DescribeConsumerGroupsOptions,
    DescribeDelegationTokenOptions,
    DescribeFeaturesOptions,
    DescribeLogDirsOptions,
    DescribeProducersOptions,
    DescribeReplicaLogDirsOptions,
    DescribeTopicsOptions,
    DescribeTransactionsOptions,
    DescribeUserScramCredentialsOptions,
    ElectLeadersOptions,
    ExpireDelegationTokenOptions,
    FenceProducersOptions,
    ListConfigResourcesOptions,
    ListConsumerGroupOffsetsOptions,
    ListGroupsOptions,
    ListOffsetsOptions,
    ListPartitionReassignmentsOptions,
    ListTopicsOptions,
    ListTransactionsOptions,
    RemoveMembersFromConsumerGroupOptions,
    RenewDelegationTokenOptions,
    TerminateTransactionOptions,
    UnregisterControllerOptions,
    UpdateFeaturesOptions,
);

#[cfg(test)]
mod tests {
    use super::*;

    /// Sets a timeout through the trait alone, as Java code generic over
    /// `AbstractOptions` would.
    fn with_timeout<O: AbstractOptions>(options: O, timeout_ms: i32) -> O {
        options.set_timeout_ms(Some(timeout_ms))
    }

    /// New test, no Java original (Java has no `AbstractOptionsTest`): every
    /// options type implements `AbstractOptions`, and the trait sets and reads
    /// the same timeout the type's own methods do. The timeouts differ per
    /// type, so an implementation reading another type's field would show.
    #[test]
    fn every_options_type_sets_and_reads_its_timeout_through_the_trait() {
        let members = RemoveMembersFromConsumerGroupOptions::default();
        let all: Vec<(Box<dyn AbstractOptions>, Option<i32>)> = vec![
            (Box::new(with_timeout(AbortTransactionOptions::new(), 1)), Some(1)),
            (Box::new(with_timeout(AlterClientQuotasOptions::new(), 2)), Some(2)),
            (Box::new(with_timeout(AlterConfigsOptions::new(), 3)), Some(3)),
            (Box::new(with_timeout(AlterConsumerGroupOffsetsOptions::new(), 4)), Some(4)),
            (Box::new(with_timeout(AlterPartitionReassignmentsOptions::new(), 5)), Some(5)),
            (Box::new(with_timeout(AlterReplicaLogDirsOptions::new(), 6)), Some(6)),
            (Box::new(with_timeout(AlterUserScramCredentialsOptions::new(), 7)), Some(7)),
            (Box::new(with_timeout(CreateAclsOptions::new(), 8)), Some(8)),
            (Box::new(with_timeout(CreateDelegationTokenOptions::new(), 9)), Some(9)),
            (Box::new(with_timeout(CreatePartitionsOptions::new(), 10)), Some(10)),
            (Box::new(with_timeout(CreateTopicsOptions::new(), 11)), Some(11)),
            (Box::new(with_timeout(DeleteAclsOptions::new(), 12)), Some(12)),
            (Box::new(with_timeout(DeleteConsumerGroupOffsetsOptions::new(), 13)), Some(13)),
            (Box::new(with_timeout(DeleteConsumerGroupsOptions::new(), 14)), Some(14)),
            (Box::new(with_timeout(DeleteRecordsOptions::new(), 15)), Some(15)),
            (Box::new(with_timeout(DeleteTopicsOptions::new(), 16)), Some(16)),
            (Box::new(with_timeout(DescribeAclsOptions::new(), 17)), Some(17)),
            (Box::new(with_timeout(DescribeClassicGroupsOptions::new(), 18)), Some(18)),
            (Box::new(with_timeout(DescribeClientQuotasOptions::new(), 19)), Some(19)),
            (Box::new(with_timeout(DescribeClusterOptions::new(), 20)), Some(20)),
            (Box::new(with_timeout(DescribeConfigsOptions::new(), 21)), Some(21)),
            (Box::new(with_timeout(DescribeConsumerGroupsOptions::new(), 22)), Some(22)),
            (Box::new(with_timeout(DescribeDelegationTokenOptions::new(), 23)), Some(23)),
            (Box::new(with_timeout(DescribeFeaturesOptions::new(), 24)), Some(24)),
            (Box::new(with_timeout(DescribeLogDirsOptions::new(), 25)), Some(25)),
            (Box::new(with_timeout(DescribeProducersOptions::new(), 26)), Some(26)),
            (Box::new(with_timeout(DescribeReplicaLogDirsOptions::new(), 27)), Some(27)),
            (Box::new(with_timeout(DescribeTopicsOptions::new(), 28)), Some(28)),
            (Box::new(with_timeout(DescribeTransactionsOptions::new(), 29)), Some(29)),
            (Box::new(with_timeout(DescribeUserScramCredentialsOptions::new(), 30)), Some(30)),
            (Box::new(with_timeout(ElectLeadersOptions::new(), 31)), Some(31)),
            (Box::new(with_timeout(ExpireDelegationTokenOptions::new(), 32)), Some(32)),
            (Box::new(with_timeout(FenceProducersOptions::new(), 33)), Some(33)),
            (Box::new(with_timeout(ListConfigResourcesOptions::new(), 34)), Some(34)),
            (Box::new(with_timeout(ListConsumerGroupOffsetsOptions::new(), 35)), Some(35)),
            (Box::new(with_timeout(ListGroupsOptions::new(), 36)), Some(36)),
            (Box::new(with_timeout(ListOffsetsOptions::new(), 37)), Some(37)),
            (Box::new(with_timeout(ListPartitionReassignmentsOptions::new(), 38)), Some(38)),
            (Box::new(with_timeout(ListTopicsOptions::new(), 39)), Some(39)),
            (Box::new(with_timeout(ListTransactionsOptions::new(), 40)), Some(40)),
            (Box::new(with_timeout(members, 41)), Some(41)),
            (Box::new(with_timeout(RenewDelegationTokenOptions::new(), 42)), Some(42)),
            (Box::new(with_timeout(TerminateTransactionOptions::new(), 43)), Some(43)),
            (Box::new(with_timeout(UnregisterControllerOptions::new(), 44)), Some(44)),
            (Box::new(with_timeout(UpdateFeaturesOptions::new(), 45)), Some(45)),
            // `None` restores the default API timeout through the trait too.
            (
                Box::new(AbstractOptions::set_timeout_ms(
                    with_timeout(CreateTopicsOptions::new(), 46),
                    None,
                )),
                None,
            ),
        ];
        for (options, expected) in &all {
            assert_eq!(options.timeout_ms(), *expected);
        }
    }
}
