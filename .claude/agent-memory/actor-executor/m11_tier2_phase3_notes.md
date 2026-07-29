---
name: m11-tier2-phase3-notes
description: Milestone 11 Tier 2 Phase 3 (group/member deletion) — decisions, the removeAll chain, and the reason-truncation
metadata:
  type: project
---

Milestone 11 Tier 2 Phase 3 = Admin group/member deletion (deleteConsumerGroups,
removeMembersFromConsumerGroup). FINAL Tier 2 phase. Actor/Critic N=1. COMPLETE
2026-07-29. Lib tests 2561 -> 2621 (+60). 3 new integration tests pass vs real
4.2.0 broker. Commits: 9ed0648 (wire), 6bb701c (RPCs+handlers+POJOs+mock),
ced9e89 (client tests), 4bc378e (integration).

WIRE (both NET-NEW, verified no dup): DeleteGroups (api key 42) + LeaveGroup
(api key 13) request/response wrappers in src/common/requests/. Generated data
structs already existed (build.rs). LeaveGroup builder reproduces Java's
version-gated shape: v>=3 batched Members, v<3 single top-level member_id (err
if !=1 member); empty-members check moved to build_version (Java checks in ctor
but our RequestBuilder ctor is infallible). Also translated
JoinGroupRequest.maybeTruncateReason (255 chars) + UNKNOWN_MEMBER_ID into a
minimal common::requests::join_group_request (full JoinGroupRequest is classic,
out of scope §20). Reason-truncation max = 255 chars, JoinGroupRequest.java:79.

ABSTRACT-BASE + THIN-SUBCLASS split (DeleteGroupsHandler / DeleteConsumerGroupsHandler):
modeled by COMPOSITION not newtype — DeleteGroupsHandler struct holds api_name +
display_name fields; DeleteConsumerGroupsHandler is a factory
(new(log_context) -> DeleteGroupsHandler) supplying the two names. Needs
#[allow(clippy::new_ret_no_self)] (factory returns base, not Self). The Java
abstract DeleteGroupsHandlerTest's methods run against the concrete subclass, so
they live in delete_consumer_groups_handler.rs tests (mirrors Java's
`extends`). display_name() accessor is #[cfg(test)] (only tests read it; log
paths use the field directly) to satisfy #![deny(warnings)] dead_code.

removeMembersFromConsumerGroup removeAll CHAIN (the hard part): mirrors Java's
getMembersFromGroup -> whenComplete -> invokeDriver. Rust: build the describe
SimpleAdminApiFuture, grab its per-key completable handle via the NEW
SimpleAdminApiFuture::handle(&key) -> Option<KafkaFutureImpl<V>> accessor,
invoke_driver(describe), then describe_handle.when_complete(move |res| ...) which
on success builds MemberIdentity list (groupInstanceId present -> by instance,
else by consumerId; +reason) and invoke_driver(leaveGroup, admin_future) — all
via a captured DriverContext (Clone; tx+wakeup are thread-safe, callback fires on
bg task). On describe error -> admin_future.complete_exceptionally (needs
`use AdminApiFuture` trait in scope). Derived Clone on ExponentialBackoff to reuse
the backoff across the two chained drivers. when_complete lives only on
KafkaFutureImpl (not public KafkaFuture) — that's why we needed the internal
handle accessor rather than chaining on the public describe result.

MockClient RESPONSE MATCHING (bit me): future_responses are matched to a request
AT SEND TIME only. A response prepared AFTER a request is already in self.requests
(in-flight) is NEVER delivered to it -> the awaiting future hangs. So the
reason-verification test (Java uses a request-matcher predicate on the prepared
response; our mock has none) must inspect the emitted-but-unanswered LeaveGroup
request via requests_mut()...request_builder_mut().build() and NOT await the RPC
(don't prepare its response). num_retries timeout tests DO complete: after the
clock jumps past request_timeout_ms, poll() disconnects the in-flight request ->
driver retry exhausted (max_retries=0) -> handle_timeout_failure -> Timeout.

MOCK (§9): both methods throw UnsupportedOperationException in Java's
MockAdminClient (773-775 / 801-803) -> Rust returns unsupported_version("Not
implemented yet") per key / as an exceptional future.

RESULT fidelity: RemoveMembersFromConsumerGroupResult.all()/member_result via
then_apply_try (propagates source error); member_result returns
Result<KafkaFuture,_> (sync IllegalArgument for removeAll-mode or member-not-in-
request; future fails for member-in-request-but-missing-from-response). removeAll
all() iterates the response map; non-removeAll iterates member_infos. Errors map
key = MemberIdentity{member_id,group_instance_id} WITHOUT reason (matches
toMemberIdentity()).

FIX CYCLE 1 (COMMENTS.1, 2 issues, 2621->2625 lib tests): (a) removeAll deadline
divergence — describe must use the client DEFAULT api timeout
(calc_deadline_ms(now, None, default_api_timeout_ms)), NOT options.timeout()
(Java issues describe with default DescribeConsumerGroupsOptions,
KafkaAdminClient.java:4172); and LeaveGroup deadline must be RECOMPUTED fresh
INSIDE the describe-completion callback (leave_now = (ctx.time_provider)();
calc_deadline_ms(leave_now, options_timeout, default_api_timeout_ms)) — Java's
invokeDriver(..., options.timeoutMs()) computes calcDeadlineMs(time.ms(),..) at
whenComplete time (:4224-4230). Captured options_timeout+default_api_timeout_ms
(both Copy) into the move closure; used ctx.time_provider for fresh now.
TEETH TECHNIQUE for deadline tests: a Call's deadline_ms surfaces as the queued
ClientRequest.request_timeout_ms() == min(request.timeout.ms, deadline-now).
To observe it uncapped, keep options.timeout < request.timeout.ms (30000). Test
1: default.api.timeout.ms=20000 < request.timeout=30000, options.timeout=5000 →
describe lookup req timeout == 20000 (fixed) vs 5000 (buggy). Test 2: advance
mock clock past the pre-describe window (1000+5000=6000) BEFORE driving → fixed
recomputes fresh (leave lookup queued at 5000), buggy leaves window expired so
NO leave request queued (find→None→panic). Verified both fail pre-fix by
temporarily reverting the two deadline lines. (b) LeaveGroup flexible v5
byte-vector tests: request serialize + response parse, hand-derived from
LeaveGroup*.json (compact strings, nullable Reason "r"=0x02 0x72 v5+, member +
top-level tagged fields 0x00). Fixups: 798e68d (→6bb701c), 5665418 (→9ed0648).

INTEGRATION: LeaveGroup admin removal DOES work for KIP-848 STATIC members
(group.instance.id) on 4.2.0 — added static_consumer_config helper. NON_EMPTY_GROUP
for deleting a live group; empty (retained via committed offset) group deletes.
removeAll empties the group. Integration clippy NOT in `cargo xtask lint` — run
`cargo clippy --features integration-tests --test integration` separately (my
file was clean; pre-existing warnings in other integration files).
