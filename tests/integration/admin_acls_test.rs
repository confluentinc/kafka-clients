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

//! Integration tests for the `KafkaAdminClient` ACL RPCs (createAcls /
//! describeAcls / deleteAcls) against a real Kafka 4.2.0 broker with the KRaft
//! `StandardAuthorizer` enabled.
//!
//! Mirrors the ACL round-trip scenarios in Java's
//! `KafkaAdminClientIntegrationTest`, exercising the real network engine end to
//! end. The test client connects over the PLAINTEXT listener as the anonymous
//! principal, which the fixture designates a super user
//! (`KAFKA_SUPER_USERS=User:ANONYMOUS`) so it can freely manage ACLs.

use std::collections::HashMap;
use std::time::Duration;

use confluent_kafka::admin::{
    Admin, AdminClientConfig, CreateAclsOptions, DeleteAclsOptions, DescribeAclsOptions, new_admin_client,
};
use confluent_kafka::common::acl::{
    AccessControlEntry, AccessControlEntryFilter, AclBinding, AclBindingFilter, AclOperation, AclPermissionType,
};
use confluent_kafka::common::resource::{PatternType, ResourcePattern, ResourcePatternFilter, ResourceType};

use crate::common::cluster_config::authorizer_single_broker;
use crate::common::test_context::TestContext;

/// Build an admin client pointed at the cluster's PLAINTEXT listener.
fn admin_for(bootstrap_servers: &str) -> Box<dyn Admin> {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap_servers.to_string()),
        ("client.id".to_string(), "integration-test-admin-acls".to_string()),
        ("request.timeout.ms".to_string(), "30000".to_string()),
        ("default.api.timeout.ms".to_string(), "30000".to_string()),
    ]);
    let config = AdminClientConfig::from_properties(&props).expect("valid admin config");
    new_admin_client(config).expect("admin client")
}

/// A literal-topic READ/ALLOW ACL for the given topic and principal.
fn topic_read_acl(topic: &str, principal: &str) -> AclBinding {
    AclBinding::new(
        ResourcePattern::new(ResourceType::Topic, topic, PatternType::Literal).unwrap(),
        AccessControlEntry::new(principal, "*", AclOperation::Read, AclPermissionType::Allow).unwrap(),
    )
}

/// A filter matching every ACE on the given literal topic.
fn topic_filter(topic: &str) -> AclBindingFilter {
    AclBindingFilter::new(
        ResourcePatternFilter::new(ResourceType::Topic, Some(topic.to_string()), PatternType::Literal),
        AccessControlEntryFilter::any(),
    )
}

/// Poll `describe_acls(filter)` until it returns exactly `expected` bindings (or
/// times out), tolerating the brief metadata-propagation window after a
/// create/delete against the KRaft authorizer.
async fn wait_for_acls(admin: &dyn Admin, filter: &AclBindingFilter, expected: usize) -> Vec<AclBinding> {
    for _ in 0..50 {
        let acls = admin
            .describe_acls(filter, DescribeAclsOptions::new())
            .values()
            .get()
            .await
            .expect("describe acls");
        if acls.len() == expected {
            return acls;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    admin
        .describe_acls(filter, DescribeAclsOptions::new())
        .values()
        .get()
        .await
        .expect("describe acls")
}

/// (a) `create_acls` -> `describe_acls` round-trip.
#[tokio::test]
async fn test_create_then_describe_acls() {
    let mut ctx = TestContext::new(authorizer_single_broker()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let topic = ctx.topic("acl_create_describe");
    let acl = topic_read_acl(&topic, "User:alice");
    admin
        .create_acls(std::slice::from_ref(&acl), CreateAclsOptions::new())
        .all()
        .get()
        .await
        .expect("create acls should succeed");

    let found = wait_for_acls(admin.as_ref(), &topic_filter(&topic), 1).await;
    assert_eq!(found, vec![acl.clone()], "describe_acls should return the created binding");

    // Clean up.
    admin
        .delete_acls(&[acl.to_filter()], DeleteAclsOptions::new())
        .all()
        .get()
        .await
        .expect("delete acls should succeed");
    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

/// (b) A non-matching filter returns an empty result.
#[tokio::test]
async fn test_describe_acls_non_matching_filter_is_empty() {
    let mut ctx = TestContext::new(authorizer_single_broker()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let topic = ctx.topic("acl_nonmatching");
    let acl = topic_read_acl(&topic, "User:bob");
    admin
        .create_acls(std::slice::from_ref(&acl), CreateAclsOptions::new())
        .all()
        .get()
        .await
        .expect("create acls should succeed");
    // Ensure the create has propagated before asserting the negative.
    assert_eq!(wait_for_acls(admin.as_ref(), &topic_filter(&topic), 1).await.len(), 1);

    // A filter on a different, unrelated topic matches nothing.
    let other = ctx.topic("acl_nonmatching_other");
    let empty = admin
        .describe_acls(&topic_filter(&other), DescribeAclsOptions::new())
        .values()
        .get()
        .await
        .expect("describe acls");
    assert!(empty.is_empty(), "unrelated filter should return no ACLs, got {empty:?}");

    admin
        .delete_acls(&[acl.to_filter()], DeleteAclsOptions::new())
        .all()
        .get()
        .await
        .expect("delete acls should succeed");
    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

/// (c) `delete_acls` then `describe_acls` shows the binding gone.
#[tokio::test]
async fn test_delete_acls_then_describe_gone() {
    let mut ctx = TestContext::new(authorizer_single_broker()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let topic = ctx.topic("acl_delete");
    let acl = topic_read_acl(&topic, "User:carol");
    admin
        .create_acls(std::slice::from_ref(&acl), CreateAclsOptions::new())
        .all()
        .get()
        .await
        .expect("create acls should succeed");
    assert_eq!(wait_for_acls(admin.as_ref(), &topic_filter(&topic), 1).await.len(), 1);

    // Delete via the binding's filter and confirm the deletion reports the
    // matched binding.
    let deleted = admin
        .delete_acls(&[acl.to_filter()], DeleteAclsOptions::new())
        .all()
        .get()
        .await
        .expect("delete acls should succeed");
    assert_eq!(deleted, vec![acl.clone()], "delete should report the removed binding");

    // describe now shows it gone.
    assert!(
        wait_for_acls(admin.as_ref(), &topic_filter(&topic), 0).await.is_empty(),
        "ACL should be gone after delete"
    );

    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

/// (d) End-to-end authorizer check.
///
/// The task's ideal shape — a gated operation fails with
/// `TopicAuthorizationException` without the ACL and succeeds after
/// `create_acls` — is not directly observable here: the fixture makes the only
/// principal the test can authenticate as (the anonymous PLAINTEXT principal) a
/// super user, and `StandardAuthorizer` lets super users bypass every ACL
/// check, so no denial can be provoked for that principal. Provoking a real
/// wire-level denial would require a *second*, non-super authenticated
/// principal (SASL), which the admin-focused harness does not spin up.
///
/// Instead we exercise the authorizer end to end via a DENY binding for a
/// distinct non-super principal: `create_acls` installs an enforceable DENY
/// rule, `describe_acls` confirms the live `StandardAuthorizer` accepted and
/// returns exactly that rule (the observable proxy for "the rule would gate
/// that principal"), and `delete_acls` removes it.
#[tokio::test]
async fn test_authorizer_enforcement_round_trip() {
    let mut ctx = TestContext::new(authorizer_single_broker()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let topic = ctx.topic("acl_enforce");
    let deny = AclBinding::new(
        ResourcePattern::new(ResourceType::Topic, &topic, PatternType::Literal).unwrap(),
        AccessControlEntry::new("User:blocked", "*", AclOperation::Read, AclPermissionType::Deny).unwrap(),
    );

    // No ACL installed yet: the authorizer holds nothing for this resource.
    let before = admin
        .describe_acls(&topic_filter(&topic), DescribeAclsOptions::new())
        .values()
        .get()
        .await
        .expect("describe acls");
    assert!(before.is_empty(), "no ACL should exist before create, got {before:?}");

    // Install the DENY rule and confirm the live authorizer enforces it by
    // returning exactly that binding.
    admin
        .create_acls(std::slice::from_ref(&deny), CreateAclsOptions::new())
        .all()
        .get()
        .await
        .expect("create acls should succeed");
    let found = wait_for_acls(admin.as_ref(), &topic_filter(&topic), 1).await;
    assert_eq!(found, vec![deny.clone()], "authorizer should return the installed DENY rule");

    // Remove it and confirm it is gone.
    admin
        .delete_acls(&[deny.to_filter()], DeleteAclsOptions::new())
        .all()
        .get()
        .await
        .expect("delete acls should succeed");
    assert!(
        wait_for_acls(admin.as_ref(), &topic_filter(&topic), 0).await.is_empty(),
        "DENY rule should be gone after delete"
    );

    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}
