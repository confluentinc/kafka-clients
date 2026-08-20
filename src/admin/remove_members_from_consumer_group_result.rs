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

//! The result of `Admin::remove_members_from_consumer_group`.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupResult`.

use std::collections::{HashMap, HashSet};

use crate::admin::MemberToRemove;
use crate::common::protocol::Errors;
use crate::common::{Error, KafkaFuture};
use crate::leave_group_request_data::MemberIdentity;

/// The per-member removal errors carried by the underlying future.
type MemberErrors = HashMap<MemberIdentity, Errors>;

/// The result of `Admin::remove_members_from_consumer_group`.
///
/// Corresponds to
/// `org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupResult`.
#[derive(Clone, Debug)]
pub struct RemoveMembersFromConsumerGroupResult {
    future: KafkaFuture<MemberErrors>,
    member_infos: HashSet<MemberToRemove>,
}

impl RemoveMembersFromConsumerGroupResult {
    /// Creates a result wrapping the per-member-error future and the set of
    /// members from the original request (empty in `removeAll` mode).
    pub(crate) fn new(future: KafkaFuture<MemberErrors>, member_infos: HashSet<MemberToRemove>) -> Self {
        Self { future, member_infos }
    }

    /// Whether all members were removed (no specific members were provided).
    fn remove_all(&self) -> bool {
        self.member_infos.is_empty()
    }

    /// Returns a future which indicates whether the request was 100% successful
    /// (no top-level or member-level error); otherwise the first member error
    /// is returned.
    ///
    /// Mirrors `all()`.
    pub fn all(&self) -> KafkaFuture<()> {
        // Stable iteration order for a deterministic "first error" (Java relies
        // on unspecified set/map iteration order anyway).
        let mut member_infos: Vec<MemberToRemove> = self.member_infos.iter().cloned().collect();
        member_infos.sort_by(|a, b| a.group_instance_id().cmp(b.group_instance_id()));
        self.future.then_apply_try(move |member_errors| {
            if member_infos.is_empty() {
                // removeAll mode: fail on the first member-level error.
                let mut entries: Vec<(&MemberIdentity, &Errors)> = member_errors.iter().collect();
                entries.sort_by(|a, b| {
                    (a.0.member_id.as_str(), a.0.group_instance_id.as_deref())
                        .cmp(&(b.0.member_id.as_str(), b.0.group_instance_id.as_deref()))
                });
                for (identity, error) in entries {
                    if *error != Errors::None {
                        return Err(Error::with_message(
                            *error,
                            format!("Encounter exception when trying to remove: {}", describe_identity(identity)),
                        ));
                    }
                }
            } else {
                for member in &member_infos {
                    let identity = member.to_member_identity();
                    if let Some(error) = sub_level_error(&member_errors, &identity) {
                        return Err(error);
                    }
                }
            }
            Ok(())
        })
    }

    /// Returns the selected member's future.
    ///
    /// Mirrors `memberResult(MemberToRemove)`.
    ///
    /// # Errors
    ///
    /// Returns an error immediately (Java's `IllegalArgumentException`) when
    /// called in `removeAll` mode, or when `member` was not part of the original
    /// request. The returned future fails if the member's removal failed (or the
    /// member is missing from the response).
    pub fn member_result(&self, member: &MemberToRemove) -> Result<KafkaFuture<()>, Error> {
        if self.remove_all() {
            return Err(Error::illegal_argument(
                "The method: memberResult is not applicable in 'removeAll' mode",
            ));
        }
        if !self.member_infos.contains(member) {
            return Err(Error::illegal_argument(format!(
                "Member {} was not included in the original request",
                member.group_instance_id()
            )));
        }
        let identity = member.to_member_identity();
        Ok(self
            .future
            .then_apply_try(move |member_errors| match sub_level_error(&member_errors, &identity) {
                Some(error) => Err(error),
                None => Ok(()),
            }))
    }
}

/// Mirrors `KafkaAdminClient.getSubLevelError` specialised for member removal:
/// an absent member yields the "not included in the response"
/// `IllegalArgumentException`, a present member yields its error (or `None` when
/// the error is `NONE`).
fn sub_level_error(member_errors: &MemberErrors, member: &MemberIdentity) -> Option<Error> {
    match member_errors.get(member) {
        None => Some(Error::illegal_argument(format!(
            "Member \"{}\" was not included in the removal response",
            describe_identity(member)
        ))),
        Some(&Errors::None) => None,
        Some(&error) => Some(Error::new(error)),
    }
}

/// Renders a `MemberIdentity` for error messages.
fn describe_identity(identity: &MemberIdentity) -> String {
    format!(
        "MemberIdentity(memberId={}, groupInstanceId={})",
        identity.member_id,
        identity.group_instance_id.as_deref().unwrap_or("null")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::kafka_future::KafkaFutureImpl;

    fn instance_one() -> MemberToRemove {
        MemberToRemove::new("instance-1")
    }

    fn instance_two() -> MemberToRemove {
        MemberToRemove::new("instance-2")
    }

    fn members_to_remove() -> HashSet<MemberToRemove> {
        HashSet::from([instance_one(), instance_two()])
    }

    fn errors_map() -> MemberErrors {
        MemberErrors::from([
            (instance_one().to_member_identity(), Errors::None),
            (instance_two().to_member_identity(), Errors::FencedInstanceId),
        ])
    }

    /// Translated from `testTopLevelErrorConstructor`.
    #[tokio::test]
    async fn top_level_error_constructor() {
        let handle: KafkaFutureImpl<MemberErrors> = KafkaFutureImpl::new();
        handle.complete_exceptionally(Error::group_authorization("group"));
        let result = RemoveMembersFromConsumerGroupResult::new(handle.future(), members_to_remove());
        assert!(matches!(result.all().get().await.unwrap_err(), Error::GroupAuthorization(_)));
    }

    /// Translated from `testMemberLevelErrorConstructor` +
    /// `testMemberLevelErrorInResponseConstructor`.
    #[tokio::test]
    async fn member_level_error_constructor() {
        let handle: KafkaFutureImpl<MemberErrors> = KafkaFutureImpl::new();
        handle.complete(errors_map());
        let result = RemoveMembersFromConsumerGroupResult::new(handle.future(), members_to_remove());

        assert_eq!(result.all().get().await.unwrap_err().error(), Errors::FencedInstanceId);
        assert_eq!(result.member_result(&instance_one()).unwrap().get().await.unwrap(), ());
        assert_eq!(
            result.member_result(&instance_two()).unwrap().get().await.unwrap_err().error(),
            Errors::FencedInstanceId
        );

        // memberResult for a member not in the original request throws synchronously.
        assert!(matches!(
            result.member_result(&MemberToRemove::new("invalid-instance-id")),
            Err(Error::IllegalArgument(_))
        ));
    }

    /// Translated from `testMemberMissingErrorInRequestConstructor`.
    #[tokio::test]
    async fn member_missing_error_in_request_constructor() {
        let mut errors = errors_map();
        errors.remove(&instance_two().to_member_identity());
        let handle: KafkaFutureImpl<MemberErrors> = KafkaFutureImpl::new();
        handle.complete(errors);
        let result = RemoveMembersFromConsumerGroupResult::new(handle.future(), members_to_remove());

        assert!(matches!(result.all().get().await.unwrap_err(), Error::IllegalArgument(_)));
        assert_eq!(result.member_result(&instance_one()).unwrap().get().await.unwrap(), ());
        assert!(matches!(
            result.member_result(&instance_two()).unwrap().get().await.unwrap_err(),
            Error::IllegalArgument(_)
        ));
    }

    /// Translated from `testNoErrorConstructor`.
    #[tokio::test]
    async fn no_error_constructor() {
        let handle: KafkaFutureImpl<MemberErrors> = KafkaFutureImpl::new();
        handle.complete(MemberErrors::from([
            (instance_one().to_member_identity(), Errors::None),
            (instance_two().to_member_identity(), Errors::None),
        ]));
        let result = RemoveMembersFromConsumerGroupResult::new(handle.future(), members_to_remove());
        assert_eq!(result.all().get().await.unwrap(), ());
        assert_eq!(result.member_result(&instance_one()).unwrap().get().await.unwrap(), ());
        assert_eq!(result.member_result(&instance_two()).unwrap().get().await.unwrap(), ());
    }

    /// `removeAll` mode: `member_result` is not applicable.
    #[tokio::test]
    async fn remove_all_mode_member_result_errors() {
        let handle: KafkaFutureImpl<MemberErrors> = KafkaFutureImpl::new();
        handle.complete(MemberErrors::new());
        let result = RemoveMembersFromConsumerGroupResult::new(handle.future(), HashSet::new());
        assert!(matches!(result.member_result(&instance_one()), Err(Error::IllegalArgument(_))));
    }

    /// `removeAll` mode: `all` fails on the first member-level error.
    #[tokio::test]
    async fn remove_all_mode_all_reports_member_error() {
        let mut member = MemberIdentity::new();
        member
            .set_member_id("m1".to_string())
            .set_group_instance_id(Some("instance-1".to_string()));
        let handle: KafkaFutureImpl<MemberErrors> = KafkaFutureImpl::new();
        handle.complete(MemberErrors::from([(member, Errors::UnknownMemberId)]));
        let result = RemoveMembersFromConsumerGroupResult::new(handle.future(), HashSet::new());
        let err = result.all().get().await.unwrap_err();
        assert_eq!(err.error(), Errors::UnknownMemberId);
        assert!(err.message().contains("Encounter exception when trying to remove"));
    }
}
