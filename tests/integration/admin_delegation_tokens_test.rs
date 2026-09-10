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

//! Integration tests for the four admin delegation-token RPCs across all four
//! backends.
//!
//! Net-new in slice G5: no committed integration test covered these, and
//! `~/Desktop/ckr-apitest` recorded them as error-path only, so
//! `DelegationToken` / `TokenInformation` / `KafkaPrincipal` marshaling had never
//! been exercised with real data in *any* language. Both halves of that gap are
//! addressed here, one by measurement and one by finding the surface that can
//! reach it.
//!
//! # The real broker cannot mint a token, and no fixture changes that
//!
//! `KafkaApis.allowTokenRequests`
//! (`kafka/core/src/main/scala/kafka/server/KafkaApis.scala:2345-2354`) returns
//! false whenever `request.context.securityProtocol == PLAINTEXT`, and all four
//! handlers test it *before* anything else — `handleDescribeTokensRequest` checks
//! it before `tokenManager.isEnabled` (`:2320-2323`). So over a PLAINTEXT
//! connection every one of the four answers
//! `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED(64)` regardless of broker configuration.
//!
//! **Measured, not assumed.** A throwaway probe ran the four RPCs against a
//! fixture with `KAFKA_DELEGATION_TOKEN_SECRET_KEY` set and against the default
//! fixture without it. Both produced byte-identical answers: error code 64 with
//! Java's message "Delegation Token requests are not allowed on PLAINTEXT/1-way
//! SSL channels and on delegation token authenticated channels." Configuring the
//! secret key changes nothing, because the gate is the *client's* security
//! protocol.
//!
//! The blocker is therefore client-side and out of scope here
//! (`PLAN-multilanguage-admin.md` §0): `AdminClientConfig` recognises no
//! `security.protocol` / `sasl.*` key and `KafkaAdminClient::from_config`
//! (`src/admin/kafka_admin_client.rs:283-291`) passes a literal
//! `SecurityProtocol::Plaintext`, so the admin client cannot authenticate at all.
//! The fixture *does* already expose SASL_PLAINTEXT and SASL_SSL listeners with a
//! `PLAIN` user, so the moment the admin client can speak SASL this becomes
//! reachable with no fixture work — the gap is recorded in
//! `design/current/status.md:606-609`.
//!
//! [`delegation_token_rpcs_are_rejected_on_a_plaintext_connection`] pins that
//! error path on all four backends. It is not vacuous: it drives the *request*
//! encoders end to end to a real broker — renewer and owner principals, the
//! `max_lifetime_ms` sentinel, raw HMAC bytes — and any of them being malformed
//! would surface as a different error than 64.
//!
//! # `MockAdminClient` reaches the marshaling the broker will not
//!
//! Unlike the ACL, quota and SCRAM RPCs — where Java's `MockAdminClient` throws
//! `UnsupportedOperationException` and the Rust mock faithfully mirrors it — the
//! mock **implements all four token RPCs with real in-memory logic** (mirroring
//! `MockAdminClient.createDelegationToken` and friends). So
//! [`delegation_token_round_trip_on_the_mock_client`] does exercise the full
//! `TokenInformation` marshaling on every backend: owner and requester
//! `KafkaPrincipal`s, the renewer list, all three timestamps, the token id, the
//! raw HMAC and its base64 form.
//!
//! Two fields have to be asserted by their own getter rather than through object
//! equality, because Rust's hand-written `PartialEq` ignores them — faithfully,
//! since Java's does too: `TokenInformation.equals` ignores `expiryTimestamp` and
//! `KafkaPrincipal.equals` ignores `tokenAuthenticated`. An equality assertion
//! alone would be blind to both.

use std::time::Duration;

use confluent_kafka::admin::{
    CreateDelegationTokenOptions, DescribeDelegationTokenOptions, ExpireDelegationTokenOptions,
    RenewDelegationTokenOptions,
};
use confluent_kafka::common::protocol::Errors;
use confluent_kafka::common::security::auth::KafkaPrincipal;

use crate::common::admin_backend::{AdminBackend, admin_for};
use crate::common::backend_factory::AdminBackendFactory;
use crate::common::test_context::TestContext;
use crate::multilanguage_admin_test;

/// The number of brokers the mock is built with. Irrelevant to the token RPCs
/// (they read `all_tokens`, not the broker list), but the factory requires one.
const MOCK_BROKERS: i32 = 1;

/// An HMAC that matches no stored token, for the not-found paths.
const UNKNOWN_HMAC: &[u8] = b"not-a-real-hmac";

/// Asserts an error is Java's `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED`, with the
/// message the broker sends.
fn assert_not_allowed(backend: &str, what: &str, error: &confluent_kafka::common::Error) {
    assert_eq!(
        error.error(),
        Errors::DelegationTokenRequestNotAllowed,
        "{backend} backend: {what} over PLAINTEXT should be DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, got {error:?}"
    );
    assert!(
        error.message().contains("not allowed on PLAINTEXT"),
        "{backend} backend: {what} should carry the broker's message, got {:?}",
        error.message()
    );
}

// ---------------------------------------------------------------------------
// Test bodies — generic over AdminBackendFactory
// ---------------------------------------------------------------------------

/// All four RPCs are refused on a PLAINTEXT connection, with Java's error and
/// message.
///
/// See the module docs for why this is the reachable state and why no broker
/// fixture can move it. The value is in driving the four *request* encoders to a
/// real broker: a mangled principal, lifetime sentinel or HMAC would produce a
/// different failure than 64.
async fn delegation_token_rpcs_are_rejected_on_a_plaintext_connection<F: AdminBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    // createDelegationToken with a renewer and an explicit owner, so both
    // principal columns of the request are populated.
    let error = admin
        .create_delegation_token(
            CreateDelegationTokenOptions::new()
                .set_renewers(vec![KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, "renewer")])
                .set_owner(KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, "owner"))
                .set_max_lifetime_ms(3_600_000),
        )
        .await
        .expect_err("createDelegationToken is refused over PLAINTEXT");
    assert_not_allowed(backend, "createDelegationToken", &error);

    // describeDelegationToken with an owners filter, so the wrapper's present
    // branch is encoded (its absent branch is exercised on the mock below).
    let error = admin
        .describe_delegation_token(
            DescribeDelegationTokenOptions::new()
                .set_owners(Some(vec![KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, "owner")])),
        )
        .await
        .expect_err("describeDelegationToken is refused over PLAINTEXT");
    assert_not_allowed(backend, "describeDelegationToken", &error);

    let error = admin
        .renew_delegation_token(
            UNKNOWN_HMAC,
            RenewDelegationTokenOptions::new().set_renew_time_period_ms(3_600_000),
        )
        .await
        .expect_err("renewDelegationToken is refused over PLAINTEXT");
    assert_not_allowed(backend, "renewDelegationToken", &error);

    let error = admin
        .expire_delegation_token(UNKNOWN_HMAC, ExpireDelegationTokenOptions::new().set_expiry_time_period_ms(-1))
        .await
        .expect_err("expireDelegationToken is refused over PLAINTEXT");
    assert_not_allowed(backend, "expireDelegationToken", &error);

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

/// The full create -> describe -> renew -> expire round trip against
/// `MockAdminClient`, which is the only surface on which the token *response*
/// marshaling is reachable at all.
///
/// The mock's semantics, mirroring Java's: `createDelegationToken` makes
/// `renewers[0]` the owner, uses a random UUID as both the token id and (its UTF-8
/// bytes as) the HMAC, sets the issue timestamp to now, copies
/// `max_lifetime_ms` into `max_timestamp`, and leaves the expiry at -1;
/// `renewDelegationToken` sets the expiry to the requested period;
/// `expireDelegationToken` with the -1 sentinel removes the token.
async fn delegation_token_round_trip_on_the_mock_client<F: AdminBackendFactory>(_ctx: &mut TestContext, factory: &F) {
    let admin = factory
        .create_mock(MOCK_BROKERS)
        .await
        .unwrap_or_else(|e| panic!("{} backend: create mock admin client: {e}", factory.name()));
    let backend = factory.name();

    let alice = KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, "alice");
    let bob = KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, "bob");
    let max_lifetime_ms = 86_400_000;

    // 1. Create. Two renewers, so a dropped or truncated renewer list is visible
    //    and the owner (= renewers[0]) is distinguishable from the second entry.
    let token = admin
        .create_delegation_token(
            CreateDelegationTokenOptions::new()
                .set_renewers(vec![alice.clone(), bob.clone()])
                .set_max_lifetime_ms(max_lifetime_ms),
        )
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: create delegation token: {e}"));

    let info = token.token_info();
    assert_eq!(info.owner(), &alice, "{backend} backend: the mock makes renewers[0] the owner");
    // `TokenInformation::new` sets the requester equal to the owner, and the
    // harness rebuilds through `new_token_requester`, so this pins that the requester
    // crossed as its own field rather than being re-derived from the owner.
    assert_eq!(
        info.token_requester(),
        &alice,
        "{backend} backend: the requester should equal the owner for the six-argument TokenInformation::new"
    );
    assert_eq!(
        info.renewers(),
        &[alice.clone(), bob.clone()],
        "{backend} backend: both renewers should round-trip in order"
    );
    // `KafkaPrincipal::equals` ignores `token_authenticated`, so the assertions
    // above cannot see it — check it explicitly on every principal that crossed.
    for principal in std::iter::once(info.owner())
        .chain(std::iter::once(info.token_requester()))
        .chain(info.renewers())
    {
        assert!(
            !principal.token_authenticated(),
            "{backend} backend: a principal supplied by the caller is not token-authenticated, got {principal}"
        );
    }
    assert!(
        !info.token_id().is_empty(),
        "{backend} backend: the token id should be non-empty"
    );
    assert_eq!(
        info.max_timestamp(),
        max_lifetime_ms,
        "{backend} backend: the mock copies max_lifetime_ms into max_timestamp, so this pins that the two \
         timestamps did not transpose"
    );
    assert!(
        info.issue_timestamp() > 0,
        "{backend} backend: the issue timestamp should be a real wall-clock value, got {}",
        info.issue_timestamp()
    );
    // `TokenInformation::equals` ignores the expiry, so it is asserted by getter.
    // The mock leaves it at -1 until a renew.
    assert_eq!(
        info.expiry_timestamp(),
        -1,
        "{backend} backend: a freshly minted mock token has no expiry yet"
    );
    // The mock's HMAC is the token id's UTF-8 bytes, which is what makes the raw
    // bytes checkable against another field rather than only self-consistent.
    assert_eq!(
        token.hmac(),
        info.token_id().as_bytes(),
        "{backend} backend: the mock's HMAC is the UTF-8 token id"
    );
    // `hmac_as_base64` is derived, and `MultilanguageAdmin` already fails the call
    // if the wire's copy disagrees with the re-derivation; assert it is non-empty
    // so a backend that omitted the field entirely is caught here too.
    assert!(
        !token.hmac_as_base64_string().is_empty(),
        "{backend} backend: the base64 HMAC should be non-empty"
    );

    // 2. Describe with no owners filter — Java's unset filter, so every token.
    let all_tokens = admin
        .describe_delegation_token(DescribeDelegationTokenOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe delegation token: {e}"));
    assert_eq!(
        all_tokens.len(),
        1,
        "{backend} backend: an unset owners filter describes every token, got {all_tokens:?}"
    );
    assert_eq!(
        all_tokens[0].token_info().token_id(),
        info.token_id(),
        "{backend} backend: describe should report the token that was created"
    );

    // 3. Describe with an owners filter that *excludes* the owner: the mock filters
    //    by owner, so this must be empty. That is what proves the filter's
    //    contents crossed rather than being ignored — an ignored filter would
    //    return the token here and pass step 2 as well.
    let filtered = admin
        .describe_delegation_token(DescribeDelegationTokenOptions::new().set_owners(Some(vec![bob.clone()])))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe delegation token (filtered): {e}"));
    assert!(
        filtered.is_empty(),
        "{backend} backend: filtering by a non-owner should return no tokens, got {filtered:?}"
    );

    // 4. Renew. The mock sets the expiry to the requested period and returns it.
    let renew_period = 7_200_000;
    let expiry = admin
        .renew_delegation_token(
            token.hmac(),
            RenewDelegationTokenOptions::new().set_renew_time_period_ms(renew_period),
        )
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: renew delegation token: {e}"));
    assert_eq!(
        expiry, renew_period,
        "{backend} backend: the mock returns the requested renew period as the new expiry"
    );
    // Read it back to prove the *stored* expiry moved, not just the return value —
    // and this is the assertion that would be blind if it went through
    // `TokenInformation`'s equality instead of its getter.
    let renewed = admin
        .describe_delegation_token(DescribeDelegationTokenOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe after renew: {e}"));
    assert_eq!(
        renewed[0].token_info().expiry_timestamp(),
        renew_period,
        "{backend} backend: the renewed expiry should be visible through describe"
    );

    // 5. Renewing an unknown HMAC is DELEGATION_TOKEN_NOT_FOUND, which also pins
    //    that the HMAC bytes are compared rather than ignored.
    let error = admin
        .renew_delegation_token(UNKNOWN_HMAC, RenewDelegationTokenOptions::new())
        .await
        .expect_err("renewing an unknown HMAC fails");
    assert_eq!(
        error.error(),
        Errors::DelegationTokenNotFound,
        "{backend} backend: an unknown HMAC should be DELEGATION_TOKEN_NOT_FOUND, got {error:?}"
    );

    // 6. Expire with the -1 sentinel, which for *this* RPC means "immediately"
    //    rather than "use the broker default" — the mock removes the token.
    let expired = admin
        .expire_delegation_token(token.hmac(), ExpireDelegationTokenOptions::new().set_expiry_time_period_ms(-1))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: expire delegation token: {e}"));
    assert_eq!(expired, -1, "{backend} backend: the mock echoes the expiry period it was given");
    let remaining = admin
        .describe_delegation_token(DescribeDelegationTokenOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe after expire: {e}"));
    assert!(
        remaining.is_empty(),
        "{backend} backend: the token should be gone after expiring it, got {remaining:?}"
    );

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
}

multilanguage_admin_test!(
    test_delegation_token_rpcs_are_rejected_on_a_plaintext_connection,
    delegation_token_rpcs_are_rejected_on_a_plaintext_connection
);
multilanguage_admin_test!(
    test_delegation_token_round_trip_on_the_mock_client,
    delegation_token_round_trip_on_the_mock_client
);
