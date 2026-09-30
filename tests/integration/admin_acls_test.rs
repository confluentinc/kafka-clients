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

//! Integration tests for the admin ACL RPCs (createAcls / describeAcls /
//! deleteAcls) against a real Kafka 4.2.0 broker with the KRaft
//! `StandardAuthorizer` enabled.
//!
//! Mirrors the ACL round-trip scenarios in Java's
//! `KafkaAdminClientIntegrationTest`, exercising the real network engine end to
//! end.
//!
//! Each scenario is a body generic over
//! [`AdminBackendFactory`](crate::common::backend_factory::AdminBackendFactory)
//! and registered with [`multilanguage_admin_test!`], so it runs against the
//! native Rust client, the Python sync binding, the Python asyncio binding and
//! the C FFI — a disagreement shows up as three backends agreeing and one not.
//! With only `integration-tests` enabled the `__rust` arm is the whole expansion
//! and drives the same production `Admin` trait against the same broker as the
//! single-backend tests these scenarios were converted from.
//!
//! # Two fixtures, because a denial and a round-trip need opposite privileges
//!
//! Most scenarios use [`authorizer_single_broker`], where the anonymous PLAINTEXT
//! principal the client authenticates as is a super user, so it may freely
//! manage ACLs and read every topic.
//!
//! [`explicit_deny_is_enforced_by_the_authorizer`] uses
//! [`authorizer_deny_reachable_single_broker`] instead, where `User:ANONYMOUS` is
//! *not* a super user. `PLAN-multilanguage-admin.md` §D3 recorded ACL denial as
//! unreachable on the assumption that it had to be one — it does not, and the
//! fixture doc comment records what it took (and what a naive attempt does
//! instead: the broker refuses to start).
//!
//! # What is exercised, and what is only carried
//!
//! `AclBindingFilter`'s three nullable strings are Java's match-any when absent.
//! The **resource name** is exercised in *both* directions by
//! [`describe_acls_filter_selectivity`]: a `Some(name)` filter matches one topic
//! while a `None` filter matches across topics, so a backend that collapsed
//! `None` into `Some("")` fails.
//!
//! **`principal` and `host` cross with real values**, and an earlier revision of
//! this note claiming they are "only ever `None`" was wrong: the `delete_acls`
//! calls all filter by `acl.to_filter()`, and `AclBinding::to_filter` delegates to
//! `AccessControlEntry::to_filter`, which carries the principal and host through
//! (`src/common/acl/acl_binding.rs:58-60`). What is missing is not plumbing but
//! *discrimination*: no scenario pairs a matching principal/host filter with a
//! non-matching one, so a backend that garbled either field into another non-empty
//! string would still delete the ACL it was asked to (the resource pattern alone
//! selects it) — while a backend that dropped it to `None` would also match. The
//! `None` side is exercised by `AccessControlEntryFilter::any()` in the describe
//! filters, which is what Java's own integration tests use.

use std::time::Duration;

use confluent_kafka::admin::{CreateAclsOptions, DeleteAclsOptions, DescribeAclsOptions, DescribeTopicsOptions};
use confluent_kafka::common::Errors;
use confluent_kafka::common::acl::{
    AccessControlEntry, AccessControlEntryFilter, AclBinding, AclBindingFilter, AclOperation, AclPermissionType,
};
use confluent_kafka::common::resource::{PatternType, ResourcePattern, ResourcePatternFilter, ResourceType};

use crate::common::admin_backend::{AdminBackend, admin_for, admin_for_plaintext, all_of_exactly, create_topic};
use crate::common::backend_factory::AdminBackendFactory;
use crate::common::cluster_config::{authorizer_deny_reachable_single_broker, authorizer_single_broker};
use crate::common::test_context::TestContext;
use crate::common::test_utils::wait_until_true_with_timeout;
use crate::multilanguage_admin_test;

/// How long to poll for an ACL change to become visible, and the pause between
/// attempts. The original single-backend test's 50 x 200 ms.
const ACL_PROPAGATION_TIMEOUT_MS: u64 = 10_000;
const ACL_PROPAGATION_PAUSE_MS: u64 = 200;

/// A literal-topic READ/ALLOW ACL for the given topic and principal.
fn topic_read_acl(topic: &str, principal: &str) -> AclBinding {
    AclBinding::new(
        ResourcePattern::new(ResourceType::Topic, topic, PatternType::Literal).expect("valid resource pattern"),
        AccessControlEntry::new(principal, "*", AclOperation::Read, AclPermissionType::Allow).expect("valid entry"),
    )
}

/// A filter matching every ACE on the given literal topic.
fn topic_filter(topic: &str) -> AclBindingFilter {
    AclBindingFilter::new(
        ResourcePatternFilter::new(ResourceType::Topic, Some(topic.to_string()), PatternType::Literal),
        AccessControlEntryFilter::any(),
    )
}

/// A filter with **no** resource name: Java's null, i.e. match every topic.
///
/// The state that separates a preserved `Option<String>` from one collapsed to
/// `""`; `Some("")` would match only a topic literally named the empty string.
fn any_topic_filter() -> AclBindingFilter {
    AclBindingFilter::new(
        ResourcePatternFilter::new(ResourceType::Topic, None, PatternType::Literal),
        AccessControlEntryFilter::any(),
    )
}

/// Reads the ACLs matching `filter`, panicking with the backend's name.
async fn acls_matching<B: AdminBackend>(admin: &B, filter: &AclBindingFilter) -> Vec<AclBinding> {
    admin
        .describe_acls(filter, DescribeAclsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{} backend: describe acls: {e}", admin.name()))
}

/// Polls `describe_acls(filter)` until it returns exactly `expected` bindings,
/// then returns them.
///
/// Fails with a message rather than a bare `false` on timeout, unlike the
/// original helper which fell through to one last unchecked read.
async fn wait_for_acls<B: AdminBackend>(admin: &B, filter: &AclBindingFilter, expected: usize) -> Vec<AclBinding> {
    wait_until_true_with_timeout(
        || async { acls_matching(admin, filter).await.len() == expected },
        &format!(
            "{} backend: describe_acls should report exactly {expected} binding(s) for {filter}",
            admin.name()
        ),
        ACL_PROPAGATION_TIMEOUT_MS,
        ACL_PROPAGATION_PAUSE_MS,
    )
    .await;
    acls_matching(admin, filter).await
}

/// Creates `acls` and asserts every binding was accepted.
///
/// `all_of_exactly` rather than `all_of`: the fold alone returns `Ok` for an
/// empty map, so a backend answering with no entries would pass silently — and
/// the key here is the whole [`AclBinding`], which is what the response is keyed
/// by, so this also pins that the per-key keying survived the round trip.
async fn create_acls<B: AdminBackend>(admin: &B, acls: &[AclBinding]) {
    let created = admin
        .create_acls(acls, CreateAclsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{} backend: create acls: {e}", admin.name()));
    all_of_exactly(admin, &created, acls, "createAcls");
}

// ---------------------------------------------------------------------------
// Test bodies — generic over AdminBackendFactory
// ---------------------------------------------------------------------------

/// The bindings `delete_acls` reported as removed, folded the way Java's
/// `DeleteAclsResult::all()` does.
///
/// # Why this exists rather than an inline `deleted[&filter]` read
///
/// `deleteAcls` is one of the three value-carries-its-own-error RPCs
/// (`admin_service.proto`'s envelope exception 3), so "the per-filter future
/// resolved" is *not* "every matched ACL was deleted": each
/// `FilterResult { binding, exception }` reports separately. Java's `all()` folds
/// exactly that — `AclBindingsFuture::collect`
/// (`src/admin/delete_acls_result.rs:119-133`) surfaces the first per-ACL
/// `exception()` before flattening the bindings — and the conversion of scenario
/// (a) replaced an `all()` call with a bare per-filter read, which silently
/// dropped the per-ACL check. This helper is that fold, so a caller cannot forget
/// the inner level again.
async fn deleted_bindings<B: AdminBackend>(admin: &B, filters: &[AclBindingFilter]) -> Vec<AclBinding> {
    let backend = admin.name();
    let deleted = admin
        .delete_acls(filters, DeleteAclsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: delete acls: {e}"));
    all_of_exactly(admin, &deleted, filters, "deleteAcls");
    let mut bindings = Vec::new();
    for filter in filters {
        let results = deleted[filter].as_ref().expect("checked by all_of_exactly");
        for result in results.values() {
            assert!(
                result.error().is_none(),
                "{backend} backend: deleting an ACL matched by {filter:?} failed inside the FilterResult: {:?}",
                result.error()
            );
            if let Some(binding) = result.binding() {
                bindings.push(binding.clone());
            }
        }
    }
    bindings
}

/// (a) `create_acls` -> `describe_acls` round-trip.
async fn create_then_describe_acls<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let topic = ctx.topic("acl_create_describe");
    let acl = topic_read_acl(&topic, "User:alice");
    create_acls(&admin, std::slice::from_ref(&acl)).await;

    let found = wait_for_acls(&admin, &topic_filter(&topic), 1).await;
    assert_eq!(
        found,
        vec![acl.clone()],
        "{backend} backend: describe_acls should return the created binding"
    );

    // Clean up, and assert the deletion reported the binding it removed rather
    // than only that it succeeded — through the same per-ACL fold the original's
    // `DeleteAclsResult::all()` performed, which the first conversion of this
    // scenario dropped.
    assert_eq!(
        deleted_bindings(&admin, &[acl.to_filter()]).await,
        vec![acl.clone()],
        "{backend} backend: the deletion should report exactly the binding it removed"
    );

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

/// (b) Filter selectivity, in both directions of the nullable resource name.
///
/// The original asserted only the negative half (an unrelated topic's filter
/// matches nothing). The positive half — a `None` resource name matching *across*
/// topics — is what makes the nullable field's two states distinguishable end to
/// end, so it is asserted too.
async fn describe_acls_filter_selectivity<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let topic = ctx.topic("acl_nonmatching");
    let acl = topic_read_acl(&topic, "User:bob");
    create_acls(&admin, std::slice::from_ref(&acl)).await;
    // Ensure the create has propagated before asserting either half.
    assert_eq!(wait_for_acls(&admin, &topic_filter(&topic), 1).await.len(), 1);

    // Negative: a filter naming a different, unrelated topic matches nothing.
    let other = ctx.topic("acl_nonmatching_other");
    let empty = acls_matching(&admin, &topic_filter(&other)).await;
    assert!(
        empty.is_empty(),
        "{backend} backend: a filter naming an unrelated topic should return no ACLs, got {empty:?}"
    );

    // Positive: a filter with **no** resource name is Java's match-any, so it
    // must find the binding the named filter found. A backend that encoded the
    // absent name as `Some("")` would match nothing here and fail.
    let any = acls_matching(&admin, &any_topic_filter()).await;
    assert!(
        any.contains(&acl),
        "{backend} backend: a filter with no resource name matches every topic, so it must contain {acl}; got {any:?}"
    );

    admin
        .delete_acls(&[acl.to_filter()], DeleteAclsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: delete acls: {e}"));
    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

/// (c) `delete_acls` reports the removed binding through its per-filter
/// [`FilterResults`], and `describe_acls` then shows it gone.
///
/// The original read only the flattened `all()` list, which is a fold over the
/// same data. This asserts the two-level shape Java actually has — the per-filter
/// value being a list of `FilterResult { binding, exception }` — because that is
/// `admin_service.proto`'s envelope exception 3 and the level a backend could
/// flatten away while `all()` still looked right.
async fn delete_acls_reports_the_removed_binding<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let topic = ctx.topic("acl_delete");
    let acl = topic_read_acl(&topic, "User:carol");
    create_acls(&admin, std::slice::from_ref(&acl)).await;
    assert_eq!(wait_for_acls(&admin, &topic_filter(&topic), 1).await.len(), 1);

    let filter = acl.to_filter();
    let deleted = admin
        .delete_acls(std::slice::from_ref(&filter), DeleteAclsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: delete acls: {e}"));
    all_of_exactly(&admin, &deleted, std::slice::from_ref(&filter), "deleteAcls");

    let results = deleted[&filter]
        .as_ref()
        .unwrap_or_else(|e| panic!("{backend} backend: delete acls for the filter: {e}"));
    assert_eq!(
        results.values().len(),
        1,
        "{backend} backend: the filter matched exactly one ACL, got {results:?}"
    );
    let result = &results.values()[0];
    assert_eq!(
        result.binding(),
        Some(&acl),
        "{backend} backend: the FilterResult should carry the deleted binding"
    );
    // Both halves of a FilterResult are independent optionals, so "the binding is
    // set" does not imply "the exception is not" — assert it.
    assert!(
        result.error().is_none(),
        "{backend} backend: a successfully deleted ACL carries no exception, got {:?}",
        result.error()
    );

    // describe now shows it gone.
    assert!(
        wait_for_acls(&admin, &topic_filter(&topic), 0).await.is_empty(),
        "{backend} backend: the ACL should be gone after delete"
    );

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

/// (d) A DENY binding round-trips through the live authorizer.
///
/// This is the authorizer-acceptance half: `create_acls` installs an enforceable
/// DENY rule for a *distinct* non-super principal, `describe_acls` confirms the
/// live `StandardAuthorizer` stored exactly that rule, and `delete_acls` removes
/// it. The enforcement half — a gated operation actually failing — is
/// [`explicit_deny_is_enforced_by_the_authorizer`], which needs the other fixture
/// because on this one the client is a super user and bypasses every check.
async fn deny_binding_round_trip<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let topic = ctx.topic("acl_enforce");
    let deny = AclBinding::new(
        ResourcePattern::new(ResourceType::Topic, &topic, PatternType::Literal).expect("valid resource pattern"),
        AccessControlEntry::new("User:blocked", "*", AclOperation::Read, AclPermissionType::Deny).expect("valid entry"),
    );

    // No ACL installed yet: the authorizer holds nothing for this resource.
    let before = acls_matching(&admin, &topic_filter(&topic)).await;
    assert!(
        before.is_empty(),
        "{backend} backend: no ACL should exist before create, got {before:?}"
    );

    create_acls(&admin, std::slice::from_ref(&deny)).await;
    let found = wait_for_acls(&admin, &topic_filter(&topic), 1).await;
    assert_eq!(
        found,
        vec![deny.clone()],
        "{backend} backend: the authorizer should return the installed DENY rule"
    );

    admin
        .delete_acls(&[deny.to_filter()], DeleteAclsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: delete acls: {e}"));
    assert!(
        wait_for_acls(&admin, &topic_filter(&topic), 0).await.is_empty(),
        "{backend} backend: the DENY rule should be gone after delete"
    );

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

/// (e) A real, live-authorizer **denial** — the state `PLAN-multilanguage-admin.md`
/// §D3 recorded as unreachable.
///
/// It is reachable, on a fixture where `User:ANONYMOUS` is not a super user (see
/// [`authorizer_deny_reachable_single_broker`] for why that also needs
/// `allow.everyone.if.no.acl.found=true`, and what happens without it). The
/// sequence proves the authorizer is acting on the binding this client sent rather
/// than merely storing it:
///
///   1. `describe_topics_with_topics` on a fresh topic succeeds — the implicit allow applies;
///   2. `create_acls` installs DENY DESCRIBE on that topic for `User:ANONYMOUS`;
///   3. the same `describe_topics_with_topics` now fails with `TOPIC_AUTHORIZATION_FAILED`;
///   4. `delete_acls` removes it and access returns.
///
/// So `AclBinding`, `AccessControlEntry`, `ResourcePattern` and the
/// `AclOperation` / `AclPermissionType` codes are checked against a broker that
/// *enforces* them, not just one that echoes them back — and step 4 rules out the
/// alternative explanation that the topic became permanently unreadable.
async fn explicit_deny_is_enforced_by_the_authorizer<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    // Pinned to PLAINTEXT: the DENY rule below targets `User:ANONYMOUS`, the
    // principal this client authenticates as over PLAINTEXT and over 1-way SSL
    // (no client certificate). Over SASL_SSL the client would authenticate as
    // `User:{SASL_USERNAME}` instead, which the rule does not name, so the
    // enforcement step would no longer exercise an explicit-DENY match. Pinning
    // keeps the assertion identical in every run.
    let admin = admin_for_plaintext(factory, ctx).await;
    let backend = factory.name();

    let topic = ctx.topic("acl_deny_enforced");
    create_topic(&admin, &topic, 1, 1).await;

    // Reads `describe_topics` for our topic, returning the per-topic outcome.
    let describe = || async {
        admin
            .describe_topics_with_topics(std::slice::from_ref(&topic), DescribeTopicsOptions::new())
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: describe topics: {e}"))
            .get(&topic)
            .cloned()
    };

    assert!(
        matches!(describe().await, Some(Ok(_))),
        "{backend} backend: before any ACL the implicit allow should let describe_topics succeed"
    );

    let deny = AclBinding::new(
        ResourcePattern::new(ResourceType::Topic, &topic, PatternType::Literal).expect("valid resource pattern"),
        // `User:ANONYMOUS` is exactly the principal this client authenticates as
        // over the PLAINTEXT listener, and it is not a super user on this fixture.
        AccessControlEntry::new("User:ANONYMOUS", "*", AclOperation::Describe, AclPermissionType::Deny)
            .expect("valid entry"),
    );
    create_acls(&admin, std::slice::from_ref(&deny)).await;

    // Poll: the authorizer observes the new binding asynchronously (measured to
    // take effect on the first attempt, but the bound is what makes it not flaky).
    wait_until_true_with_timeout(
        || async { matches!(describe().await, Some(Err(_))) },
        &format!("{backend} backend: describe_topics should be denied once the DENY rule is installed"),
        ACL_PROPAGATION_TIMEOUT_MS,
        ACL_PROPAGATION_PAUSE_MS,
    )
    .await;

    let error = describe()
        .await
        .expect("the topic has an outcome")
        .expect_err("describe_topics is denied");
    assert_eq!(
        error.error(),
        Errors::TopicAuthorizationFailed,
        "{backend} backend: a DENY DESCRIBE rule should surface as TOPIC_AUTHORIZATION_FAILED, got {error:?}"
    );

    // Removing the rule restores access, which is what rules out "the topic broke"
    // as an alternative explanation for step 3.
    admin
        .delete_acls(&[deny.to_filter()], DeleteAclsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: delete acls: {e}"));
    wait_until_true_with_timeout(
        || async { matches!(describe().await, Some(Ok(_))) },
        &format!("{backend} backend: describe_topics should succeed again once the DENY rule is deleted"),
        ACL_PROPAGATION_TIMEOUT_MS,
        ACL_PROPAGATION_PAUSE_MS,
    )
    .await;

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

multilanguage_admin_test!(
    test_create_then_describe_acls,
    create_then_describe_acls,
    authorizer_single_broker()
);
multilanguage_admin_test!(
    test_describe_acls_filter_selectivity,
    describe_acls_filter_selectivity,
    authorizer_single_broker()
);
multilanguage_admin_test!(
    test_delete_acls_reports_the_removed_binding,
    delete_acls_reports_the_removed_binding,
    authorizer_single_broker()
);
multilanguage_admin_test!(
    test_deny_binding_round_trip,
    deny_binding_round_trip,
    authorizer_single_broker()
);
multilanguage_admin_test!(
    test_explicit_deny_is_enforced_by_the_authorizer,
    explicit_deny_is_enforced_by_the_authorizer,
    authorizer_deny_reachable_single_broker()
);
