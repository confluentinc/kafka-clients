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
use crate::common::Errors;
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
    /// Mirrors `all()`. In `removeAll` mode the first member-level error is
    /// wrapped, as Java's `new KafkaException("Encounter exception when trying
    /// to remove: " + identity, exception)` does: the returned error is a bare
    /// [`Error::KafkaError`] whose [`source`](Error::source) is the member's
    /// own error. With explicit members the member's error is returned as is.
    pub fn all(&self) -> KafkaFuture<()> {
        let member_infos = self.sorted_member_infos();
        self.future
            .then_apply_try(move |member_errors| all_of(&member_errors, &member_infos))
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
        self.ensure_member_result_applicable(member)?;
        let identity = member.to_member_identity();
        Ok(self
            .future
            .then_apply_try(move |member_errors| member_result_of(&member_errors, &identity)))
    }

    /// The single future every accessor derives from — Java's
    /// `KafkaFuture<Map<MemberIdentity, Errors>> future` field.
    ///
    /// Crate-internal so the C FFI can await the one outcome once and then
    /// evaluate [`member_result`](Self::member_result) / [`all`](Self::all)
    /// against it through [`resolved_member_result`](Self::resolved_member_result)
    /// / [`resolved_all`](Self::resolved_all).
    #[cfg_attr(not(feature = "ffi"), allow(dead_code))]
    pub(crate) fn future(&self) -> &KafkaFuture<MemberErrors> {
        &self.future
    }

    /// Evaluates [`member_result`](Self::member_result) against an
    /// already-resolved outcome of [`future`](Self::future), without awaiting.
    ///
    /// Same derivation as the future-based accessor: the `removeAll` and
    /// "not included in the original request" checks come first and do not
    /// depend on the outcome (Java throws them before touching the future);
    /// then a failed outcome propagates its error, as Java's `whenComplete`
    /// forwards the `throwable`.
    #[cfg_attr(not(feature = "ffi"), allow(dead_code))]
    pub(crate) fn resolved_member_result(
        &self,
        outcome: &Result<MemberErrors, Error>,
        member: &MemberToRemove,
    ) -> Result<(), Error> {
        self.ensure_member_result_applicable(member)?;
        member_result_of(outcome.as_ref().map_err(Error::clone)?, &member.to_member_identity())
    }

    /// Evaluates [`all`](Self::all) against an already-resolved outcome of
    /// [`future`](Self::future), without awaiting.
    #[cfg_attr(not(feature = "ffi"), allow(dead_code))]
    pub(crate) fn resolved_all(&self, outcome: &Result<MemberErrors, Error>) -> Result<(), Error> {
        all_of(outcome.as_ref().map_err(Error::clone)?, &self.sorted_member_infos())
    }

    /// Java's two synchronous `memberResult` guards, in Java's order.
    fn ensure_member_result_applicable(&self, member: &MemberToRemove) -> Result<(), Error> {
        if self.remove_all() {
            return Err(Error::local_illegal_argument(
                "The method: memberResult is not applicable in 'removeAll' mode",
            ));
        }
        if !self.member_infos.contains(member) {
            return Err(Error::local_illegal_argument(format!(
                "Member {} was not included in the original request",
                member.group_instance_id()
            )));
        }
        Ok(())
    }

    /// The requested members in a stable order, for a deterministic "first
    /// error" in `all()` (Java relies on unspecified set iteration order).
    fn sorted_member_infos(&self) -> Vec<MemberToRemove> {
        let mut member_infos: Vec<MemberToRemove> = self.member_infos.iter().cloned().collect();
        member_infos.sort_by(|a, b| a.group_instance_id().cmp(b.group_instance_id()));
        member_infos
    }
}

/// The body of Java's `memberResult` `whenComplete` lambda, for a
/// successfully resolved map.
fn member_result_of(member_errors: &MemberErrors, identity: &MemberIdentity) -> Result<(), Error> {
    match sub_level_error(member_errors, identity) {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// The body of Java's `all()` `whenComplete` lambda, for a successfully
/// resolved map. An empty `member_infos` is `removeAll` mode.
fn all_of(member_errors: &MemberErrors, member_infos: &[MemberToRemove]) -> Result<(), Error> {
    if member_infos.is_empty() {
        // removeAll mode: fail on the first member-level error, wrapped in a
        // bare `KafkaException` carrying the member's error as its cause.
        // Stable iteration order for a deterministic "first error" (Java
        // relies on unspecified map iteration order anyway).
        let mut entries: Vec<(&MemberIdentity, &Errors)> = member_errors.iter().collect();
        entries.sort_by(|a, b| {
            (a.0.member_id.as_str(), a.0.group_instance_id.as_deref())
                .cmp(&(b.0.member_id.as_str(), b.0.group_instance_id.as_deref()))
        });
        for (identity, error) in entries {
            if *error != Errors::None {
                // "exception" is reworded to "error" per CLAUDE.md §2; the rest
                // is Java's text verbatim.
                return Err(Error::kafka_message_source(
                    format!("Encounter error when trying to remove: {}", describe_identity(identity)),
                    Error::new(*error),
                ));
            }
        }
    } else {
        for member in member_infos {
            if let Some(error) = sub_level_error(member_errors, &member.to_member_identity()) {
                return Err(error);
            }
        }
    }
    Ok(())
}

/// Mirrors `KafkaAdminClient.getSubLevelError` specialised for member removal:
/// an absent member yields the "not included in the response"
/// `IllegalArgumentException`, a present member yields its error (or `None` when
/// the error is `NONE`).
fn sub_level_error(member_errors: &MemberErrors, member: &MemberIdentity) -> Option<Error> {
    match member_errors.get(member) {
        None => Some(Error::local_illegal_argument(format!(
            "Member \"{}\" was not included in the removal response",
            describe_identity(member)
        ))),
        Some(&Errors::None) => None,
        Some(&error) => Some(Error::new(error)),
    }
}

/// Renders a `MemberIdentity` exactly as its Java counterpart's generated
/// `toString()` does, because two user-visible error messages embed it.
///
/// The Kafka message generator emits, for every declared field of a struct,
/// `"<name>=" + ((<name> == null) ? "null" : "'" + <name> + "'")` when the
/// field is a string (`MessageDataGenerator.generateFieldToString`), joins the
/// fields with `", "` and wraps them in `<ClassName>(...)`. `MemberIdentity`
/// declares three fields — `MemberId`, `GroupInstanceId` and `Reason`
/// (`LeaveGroupRequest.json`) — so all three are printed, strings are quoted,
/// and a null renders as a bare `null`. Unknown tagged fields are *not*
/// printed: the generated `toString` iterates only over declared fields.
///
/// This crate's generated `Display` for message structs is `{:?}` (derived
/// `Debug`), which is a different rendering, hence this local helper.
fn describe_identity(identity: &MemberIdentity) -> String {
    fn quote(value: Option<&str>) -> String {
        match value {
            Some(value) => format!("'{value}'"),
            None => "null".to_string(),
        }
    }
    format!(
        "MemberIdentity(memberId={}, groupInstanceId={}, reason={})",
        quote(Some(&identity.member_id)),
        quote(identity.group_instance_id.as_deref()),
        quote(identity.reason.as_deref())
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::internals::KafkaFutureImpl;

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
        handle.complete_with_error(Error::group_authorization("group"));
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
            Err(Error::LocalIllegalArgument(_))
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

        assert!(matches!(result.all().get().await.unwrap_err(), Error::LocalIllegalArgument(_)));
        assert_eq!(result.member_result(&instance_one()).unwrap().get().await.unwrap(), ());
        let err = result.member_result(&instance_two()).unwrap().get().await.unwrap_err();
        assert!(matches!(err, Error::LocalIllegalArgument(_)));
        // The embedded identity must render as Java's generated `toString()`:
        // all three declared fields, strings quoted, absent fields as `null`.
        // `MemberToRemove.toMemberIdentity()` sets the member id to
        // `UNKNOWN_MEMBER_ID` (the empty string), which is *not* null, so it
        // is quoted as `''` and not printed as `null`.
        assert_eq!(
            err.message(),
            "Member \"MemberIdentity(memberId='', groupInstanceId='instance-2', reason=null)\" \
             was not included in the removal response"
        );
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
        assert!(matches!(
            result.member_result(&instance_one()),
            Err(Error::LocalIllegalArgument(_))
        ));
    }

    /// `removeAll` mode: `all` fails on the first member-level error.
    #[tokio::test]
    async fn remove_all_mode_all_reports_member_error() {
        let mut member = MemberIdentity::new();
        member
            .set_member_id("m1".to_string())
            .set_group_instance_id(Some("instance-1".to_string()))
            .set_reason(Some("left the group".to_string()));
        let handle: KafkaFutureImpl<MemberErrors> = KafkaFutureImpl::new();
        handle.complete(MemberErrors::from([(member, Errors::UnknownMemberId)]));
        let result = RemoveMembersFromConsumerGroupResult::new(handle.future(), HashSet::new());
        let err = result.all().get().await.unwrap_err();
        // Java wraps the member's exception in a bare `KafkaException`
        // (`RemoveMembersFromConsumerGroupResult.java`, `all()`), so the
        // outer error is not the member's own kind: it is a plain Kafka error
        // whose cause is the member's error.
        assert!(matches!(err, Error::KafkaError(_)), "{err:?}");
        assert!(err.is_kafka_error());
        assert!(!err.is_api_error());
        let cause = err.source().expect("the member's error is the cause");
        assert!(matches!(cause, Error::UnknownMemberId(_)), "{cause:?}");
        assert_eq!(cause.error(), Errors::UnknownMemberId);
        // Exact Java text (with `exception` spelled `error`, per CLAUDE.md §2),
        // including the generated `toString()` of the identity. `reason` is
        // populated here so the field a shorter rendering would omit is the
        // one carrying a distinctive value.
        assert_eq!(
            err.message(),
            "Encounter error when trying to remove: \
             MemberIdentity(memberId='m1', groupInstanceId='instance-1', reason='left the group')"
        );
    }

    /// `member_result` / `all` and their `resolved_*` twins share one
    /// derivation, so they must agree on every requested member, including
    /// the exact messages; the unrequested-member guard fires first, whatever
    /// the outcome.
    #[tokio::test]
    async fn resolved_accessors_agree_with_the_future_based_ones() {
        let mut errors = errors_map();
        errors.remove(&instance_two().to_member_identity());
        let handle: KafkaFutureImpl<MemberErrors> = KafkaFutureImpl::new();
        handle.complete(errors);
        let result = RemoveMembersFromConsumerGroupResult::new(handle.future(), members_to_remove());
        let outcome = result.future().get().await;

        for member in [instance_one(), instance_two()] {
            let via_future = result.member_result(&member).unwrap().get().await;
            let resolved = result.resolved_member_result(&outcome, &member);
            assert_eq!(
                format!("{via_future:?}"),
                format!("{resolved:?}"),
                "{}",
                member.group_instance_id()
            );
        }
        assert_eq!(
            format!("{:?}", result.all().get().await),
            format!("{:?}", result.resolved_all(&outcome))
        );

        let unrequested = MemberToRemove::new("invalid-instance-id");
        let via_future = result.member_result(&unrequested).unwrap_err();
        let failed: Result<MemberErrors, Error> = Err(Error::group_authorization("g"));
        for outcome in [&outcome, &failed] {
            let resolved = result.resolved_member_result(outcome, &unrequested).unwrap_err();
            assert_eq!(format!("{via_future:?}"), format!("{resolved:?}"));
            assert_eq!(
                resolved.message(),
                "Member invalid-instance-id was not included in the original request"
            );
        }

        // A failed outcome reaches the requested-member accessors unchanged.
        assert!(matches!(
            result.resolved_member_result(&failed, &instance_one()),
            Err(Error::GroupAuthorization(_))
        ));
        assert!(matches!(result.resolved_all(&failed), Err(Error::GroupAuthorization(_))));
    }

    /// `removeAll` mode through the resolved accessors: `member_result` is
    /// refused whatever the outcome, and `all` wraps the first member error.
    #[test]
    fn resolved_accessors_in_remove_all_mode() {
        let result = RemoveMembersFromConsumerGroupResult::new(KafkaFutureImpl::new().future(), HashSet::new());
        let mut member = MemberIdentity::new();
        member.set_member_id("m1".to_string());
        let ok: Result<MemberErrors, Error> = Ok(MemberErrors::from([(member.clone(), Errors::None)]));
        let failed_member: Result<MemberErrors, Error> = Ok(MemberErrors::from([(member, Errors::UnknownMemberId)]));
        let failed: Result<MemberErrors, Error> = Err(Error::group_authorization("g"));
        for outcome in [&ok, &failed_member, &failed] {
            assert_eq!(
                result.resolved_member_result(outcome, &instance_one()).unwrap_err().message(),
                "The method: memberResult is not applicable in 'removeAll' mode"
            );
        }
        assert!(result.resolved_all(&ok).is_ok());
        let err = result.resolved_all(&failed_member).unwrap_err();
        assert!(matches!(err, Error::KafkaError(_)));
        assert_eq!(
            err.message(),
            "Encounter error when trying to remove: MemberIdentity(memberId='m1', groupInstanceId=null, reason=null)"
        );
        assert!(matches!(err.source(), Some(Error::UnknownMemberId(_))));
        assert!(matches!(result.resolved_all(&failed), Err(Error::GroupAuthorization(_))));
    }

    /// The three-field, quoted rendering is what Java's generated
    /// `MemberIdentity.toString()` produces; both messages above embed it.
    #[test]
    fn describe_identity_matches_the_generated_java_to_string() {
        let mut identity = MemberIdentity::new();
        assert_eq!(
            describe_identity(&identity),
            "MemberIdentity(memberId='', groupInstanceId=null, reason=null)"
        );

        identity
            .set_member_id("m-9".to_string())
            .set_group_instance_id(Some("inst-1".to_string()))
            .set_reason(Some("shutting down".to_string()));
        assert_eq!(
            describe_identity(&identity),
            "MemberIdentity(memberId='m-9', groupInstanceId='inst-1', reason='shutting down')"
        );

        // An empty nullable string is quoted-empty, never `null`: the
        // generator branches on null, not on emptiness.
        identity
            .set_group_instance_id(Some(String::new()))
            .set_reason(Some(String::new()));
        assert_eq!(
            describe_identity(&identity),
            "MemberIdentity(memberId='m-9', groupInstanceId='', reason='')"
        );
    }
}
