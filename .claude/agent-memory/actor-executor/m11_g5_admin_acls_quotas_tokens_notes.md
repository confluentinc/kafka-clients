---
name: m11-g5-admin-acls-quotas-scram-tokens-features
description: Milestone 11 G5 (admin multilanguage ACLs/quotas/SCRAM/delegation tokens/features) — how the two "unreachable" gaps actually resolve, why MockAdminClient is the surface for token marshaling, the broker rule that makes remove-vs-zero self-proving, and the grounded ILLEGAL_ARGUMENT arm
metadata:
  type: project
---

Slice G5 of `design/history/Milestone-11/PLAN-multilanguage-admin.md`: the
thirteen ACL / quota / SCRAM / delegation-token / feature RPCs on all four
backends. 14 scenarios → 56 entries; **260 admin entries, 40 of 46 RPCs**.
`__rust` 50 → 64, `__grpc` 0 without the feature.

## Do not declare a state unreachable until a *purpose-built* fixture has failed

Both of PLAN §D3's standing "unreachable" claims were re-tested. One was wrong,
one was right for a reason different from the one recorded.

**ACL denial is reachable.** The recorded blocker — `User:ANONYMOUS` is a super
user — is a property of the fixture, not the broker. But the naive fix does not
work, and the failure mode is the interesting part: with `super.users` not
containing `User:ANONYMOUS` and `allow.everyone.if.no.acl.found` at its default
`false`, **the broker never starts**. Its own `CONTROLLER_REGISTRATION` /
`BROKER_REGISTRATION` are denied because `kafka_cluster.rs` maps the CONTROLLER
and BROKER listeners to PLAINTEXT, so the broker authenticates *to itself* as
`User:ANONYMOUS`. Super-user status for the broker and for the test client is one
bit in this fixture.

Adding `allow.everyone.if.no.acl.found=true` fixes it: the broker boots, the
client can still manage ACLs, and an explicit DENY still binds because
`StandardAuthorizer` gives a matching DENY precedence over the implicit allow.
`authorizer_deny_reachable_single_broker()` is that fixture. A DENY DESCRIBE for
`User:ANONYMOUS` on a topic flips `describe_topics` to
`TOPIC_AUTHORIZATION_FAILED` on the *first* poll (no propagation flakiness), and
deleting it restores access — the round trip is what rules out "the topic broke".

**A real delegation token is unreachable, and no broker config changes it.**
Measured both ways: with `KAFKA_DELEGATION_TOKEN_SECRET_KEY` set and without, all
four RPCs answer `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED(64)` byte-identically.
`KafkaApis.allowTokenRequests` (`KafkaApis.scala:2345-2354`) returns false
whenever the security protocol is PLAINTEXT and every handler tests it *before*
`tokenManager.isEnabled` (`:2320-2323`) — the gate is the **client's** protocol.
The blocker is `KafkaAdminClient::from_config` passing a literal
`SecurityProtocol::Plaintext` with no `security.protocol`/`sasl.*` key in
`AdminClientConfig` (`status.md:606-609`). The fixture already exposes
SASL_PLAINTEXT/SASL_SSL listeners with a PLAIN user, so this needs *zero* fixture
work once the admin client can authenticate.

## `MockAdminClient` is not uniformly "unsupported" — check per RPC

The reflex from earlier slices is that the mock throws for the exotic RPCs. For
G5 that is true for ACLs, quotas and SCRAM (`MockAdminClient.java:806-818`,
`:1243-1259`), but **false for all four delegation-token RPCs and both feature
RPCs**, which the mock implements with real in-memory logic. That is what closes
the `DelegationToken`/`TokenInformation`/`KafkaPrincipal` marshaling gap the real
broker cannot: mint a token on the mock and the full object crosses all four
backends. General rule: grep `src/admin/mock_admin_client.rs` for the specific
method before concluding the mock cannot exercise a value type.

Mock token semantics worth knowing: `renewers[0]` becomes the owner, the token id
is a random UUID whose **UTF-8 bytes are the HMAC** (so the HMAC is checkable
against another field, not just self-consistent), `max_lifetime_ms` is copied
verbatim into `max_timestamp`, and the expiry stays -1 until a renew.

## Two Rust types whose equality ignores a field, faithfully

`TokenInformation`'s hand-written `PartialEq` **ignores `expiry_timestamp`** and
`KafkaPrincipal`'s ignores `token_authenticated` — both matching Java's `equals`.
So an `assert_eq!` on whole objects is *blind* to those two fields and a backend
dropping or mangling either would pass. Assert them by getter. Same class of trap
as G4's derived fields, different cause: there the harness re-derived the value,
here the comparison discards it.

## The broker's own validation can make a distinction self-proving

The `alterClientQuotas` remove-vs-set assertion was drafted as "set 0.0, observe
0.0, then remove, observe absent". **The first half is invalid**: a real 4.2
broker rejects it — "Quota producer_byte_rate must be greater than 0". That makes
the coverage *stronger*, not weaker: a layer collapsing `None` into `0.0` cannot
silently zero the quota, because the broker refuses the request. So assert the
rejection directly (per-entity slot — which is also the only exercise of
`alterClientQuotas`' per-key error arm), assert the rejected alteration left the
stored value untouched, then assert `None` makes the key vanish. Lesson: probe the
broker's validation before assuming a "neutral" value is settable.

## A grounded `guess_variant` arm, and when guessing is not an option

The C FFI drops the `KafkaError` discriminator: a core `IllegalArgument` and a
core `IllegalState` both report `Errors::UnknownServerError`, and
`KafkaError::message()` returns the bare text with **no** `IllegalArgumentError:`
prefix. So the message is the only signal. Adding an arm is still legitimate when
the phrases are *Java's own literal strings* rather than invented heuristics:
`"Feature updates can not be null or empty."` / `"Provided feature can not be
empty."` (`KafkaAdminClient.java:4578,4585` → `kafka_admin_client.rs:4586,4592`).
Without the arm `updateFeatures`' empty-map rejection crossed as GENERIC while
native reported `IllegalArgument` — a false 3-against-1. Kept byte-identical in
`grpc_translate.py` and `server.cc`. Where a phrase is *not* Java-literal, prefer
asserting the **message** in the scenario over the variant, which is
backend-independent by construction.

## Envelope shapes: G5 completes the value-carries-its-own-error set

`deleteAcls` is the third and last (with `createTopics` G1, `describeLogDirs` G2).
`DeletedAcl { optional binding, optional exception }` is deliberately **not** a
`oneof`: the per-filter future already resolved, so the exactly-one-of guarantee
does not apply at that level, and a `oneof` would normalise a backend that set
both or neither. Three new `ResultKey` variants (`acl_binding`,
`acl_binding_filter`, `client_quota_entity`) and no new response shape. Six
whole-value responses, four `VoidKeyedResponse`, one ordinary per-key `oneof`
(`describeUserScramCredentials`, whose per-user shape is copied from the FFI's
documented three-view composition, **not** the `Listings` valid/errors split —
every error there has a user to attach to).

`updateFeatures` is the only RPC in the harness whose *Rust submission* is
fallible (`Result<UpdateFeaturesResult, KafkaError>`), so it is the only scenario
exercising the top-level-error arm for a **synchronous** throw.

## C++ traps hit again

`using confluent::kafka::test::QuotaValue` was missing, so `QuotaValue* pair =
...` parsed as an expression and the error read `'pair' was not declared in this
scope; did you mean 'std::pair'?` — pointing at the *variable*, not the missing
type. The list does not glob; add every new message name first. Also
`std::vector<bool>` is a bit-proxy with no contiguous buffer, so a `const bool*`
column for an entry point needs a real `bool[]` per row.

## Teeth check

Transposed `AclBinding.resource_name ↔ .principal` and
`TokenInformation.issue_timestamp ↔ .max_timestamp` in the **sync** Python
encoder, rebuilt only that image: exactly the 6 predicted `__grpc_python` arms
went red (5 ACL + the token round trip; the token *rejection* arm correctly stayed
green since it never decodes a token) while `__grpc_python_async` stayed green on
the same shared file. Failure messages printed the swap literally
(`name: "User:blocked"`, and `left: 1786469540268` vs `right: 86400000`).
Restoring returned the image id to the exact pre-mutation sha `6e52e5807b0e` and
the file's sha256 to its pristine value.
