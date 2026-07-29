---
name: m11-tier2-phase1-notes
description: Milestone 11 Tier 2 Phase 1 (group listing & describe) — progress, key decisions, and remaining work
metadata:
  type: project
---

Milestone 11 Tier 2 Phase 1 = Admin group listing & describe (listGroups,
listConsumerGroups, describeConsumerGroups, describeClassicGroups). Actor/Critic N=1.

**Why:** first real use of CoordinatorStrategy + the hand-rolled broker-enumeration
Call idiom for the list pair. Scope is Rust core + unit + integration tests ONLY
(no C FFI, no Python — deferred).

**How to apply:** work in dependency order, commit per layer. All 4 checks must pass
per commit (build/test; format via `rustfmt --edition 2024 <files>` NOT bulk cargo fmt
because it trips on metadata_response.rs let-chains and would touch user's untracked
tests/integration/admin_smoke_test_manual.rs + main.rs — LEAVE THOSE ALONE).

Key facts discovered:
- Generated wire data structs already exist (build.rs generates all 197 JSON specs):
  crate::list_groups_request_data::*, describe_groups_*, consumer_group_describe_*,
  consumer_protocol_{assignment,subscription}_data.
- Project edition = 2024; global `#![deny(warnings)]` in lib.rs. Not-yet-wired pub(crate)
  items need `#[allow(dead_code)]` (existing precedent) until their consumer lands.
- ConcreteRequest/ConcreteResponse are enums in src/common/requests/abstract_{request,response}.rs.
  Adding a wire type = add variant + ~9 match arms each file (used a python script anchored
  on the FindCoordinator arms).
- valid_acl_operations already exists in admin/internals/admin_utils.rs → BTreeSet<AclOperation>.
- CoordinatorKey.idValue String + CoordinatorType (from find_coordinator_request.rs).
- ApiRequestScope is a closed enum (SingleLookup, Fulfillment(i32)); needs a per-key
  CoordinatorLookup variant for CoordinatorStrategy non-batch mode.
- SimpleAdminApiFuture (Java AdminApiFuture.forKeys) not yet translated — needed for the
  describe handlers (keyed by CoordinatorKey). PartitionLeaderFuture is the existing precedent.
- Driver-backed RPC wiring pattern: kafka_admin_client.rs invoke_driver + AdminApiDriver::new
  (handler, future, deadline, retry_backoff, log_context). See list_offsets fn ~2705.
- List pair uses plain Call broker-enumeration: findAllBrokers (LeastLoadedNodeProvider,
  Metadata request, empty topics) then one Call per broker (ConstantNodeIdProvider,
  ListGroups). handleResponse of findAllBrokers must submit N calls — HandleResult::NewCall
  is single only; use a captured DriverContext-like submit channel OR extend HandleResult.
  ListGroupsResults accumulator = Arc<Mutex<..>> completing a KafkaFutureImpl<Vec<Result<L,KafkaError>>>.
- consumer-threading.md §20 amended (ConsumerProtocol + Assignment/Subscription carve-out for Admin).

PHASE COMPLETE (2026-07-29). All layers landed + all 4 checks green (2484 lib tests,
lint clean, format-check clean except the user's untracked admin_smoke_test_manual.rs
and its out-of-order mod line in main.rs — intentionally left; see below).

Final-pass findings (resumed a dead session with valuable uncommitted work):
- Unit-test coverage of KafkaAdminClientTest group slices is a REPRESENTATIVE subset at
  the client level (test_list_groups, _filters_protocol_type, _list_consumer_groups,
  _describe_consumer_groups, _group_id_not_found, _with_both_unsupported_apis). The deep
  behavior lives in handler tests: DescribeConsumerGroupsHandlerTest (all 10 Java methods
  + 2 fallback extras) and CoordinatorStrategyTest (14/14). No Java
  DescribeClassicGroupsHandlerTest exists → 0 Rust tests there is correct, not a gap.
- Added test_describe_groups_with_both_unsupported_apis (was the one plan-named slice
  missing): find_coordinator then two prepare_unsupported_version_response() (queue-order,
  not predicate); fallback is a driver retry gated on retry.backoff so the MockTime clock
  must advance (loop run_once + time.sleep(200)). Asserts err.error()==UnsupportedVersion.
  Handler both-unsupported logic: use_classic_group_api.insert() returns false the 2nd
  time → that key fails (faithful to Java).
- Integration test tests/integration/admin_groups_test.rs (3 tests, all PASS vs real
  broker via shared cluster pool): only KIP-848 consumer groups are creatable (§20 scopes
  classic consumer out), so classic-fallback is unit-only (documented in the file header).
  Bringing a group to Stable: subscribe + poll loop until assignment() non-empty; the bg
  task keeps heartbeating so the group stays Stable while admin queries run.
- NONEXISTENT-GROUP DUAL-PROTOCOL NUANCE: describing a never-created group is broker-
  version dependent — ConsumerGroupDescribe→GROUP_ID_NOT_FOUND then classic DescribeGroups
  fallback may surface GROUP_ID_NOT_FOUND OR a classic "Dead" placeholder group (NONE
  error). The integration test accepts EITHER (err GroupIdNotFound, or Ok with empty
  members and state != Stable) — a strict GROUP_ID_NOT_FOUND assert would be flaky. This
  is a deliberate deviation from the plan's literal "(c) → GROUP_ID_NOT_FOUND" wording.
- Integration-test clippy is NOT covered by `cargo xtask lint` — run
  `cargo clippy --features integration-tests --test integration` separately. My file
  tripped cloned_ref_to_slice_refs → use std::slice::from_ref(&x) not &[x.clone()].
- The prior session's uncommitted kafka_admin_client.rs + mock_admin_client.rs were left
  UNFORMATTED (edition-2024 import sort). `tail -N` on format-check output hides earlier
  diffs — always pipe to `grep '^Diff in' | sed 's/:.*//' | sort -u` to see all files.

FIX CYCLE (Critic round-1, 3 issues, all test/completeness gaps — landed 2026-07-29):
- Asserting emitted wire-request contents with MockClient: DON'T prepare a future
  response for the request under inspection → it stays queued in MockClient.requests.
  Then `runnable.client_mut().requests_mut()[i].request_builder_mut().build()` returns
  the ConcreteRequest (build_version clones self.data, so building is non-destructive —
  you can build() AND build_version(4) AND then respond_from() on the same queued req).
  Import ConcreteRequest locally in the test (`use super::*` only re-exports ConcreteResponse,
  not ConcreteRequest). RequestBuilder trait is already in scope via the file's top imports.
- MockClient does NOT negotiate API versions and NEVER calls build_version — so the
  ListGroups older-broker downgrade (omit CLASSIC-only / throw SHARE|CONSUMER-only) has
  no end-to-end code path through the mock. Model it two ways: omit path = build the queued
  req at build_version(4) and assert types_filter dropped; reject path = 
  prepare_unsupported_version_response() (== the version-mismatch ClientResponse the real
  NetworkClient makes when the builder throws) then assert future surfaces UnsupportedVersion.
  UnsupportedVersion is NOT is_retriable → per-broker ListGroups Call handle_unsupported_version_fn
  is `|| false` → fail_call hits `!is_retriable` → handle_failure → error into ListGroupsResults
  accumulator → result.all() throws. Documented deviation in COMMENTS.DONE.1.md.
- Classic-describe full retry chain (testDescribeClassicGroups) works with the FIFO queue-based
  mock: prepare responses in exact send order (FC-err, FC-err, FC-ok, DG-load, DG-notcoord, FC-ok,
  DG-notavail, FC-ok, DG-good), loop run_once + time.sleep(50). Needs default retries (i32::MAX)
  via env_with_props/env_nodes_with_props — env()'s test_config retries=2 is too few. Single
  in-flight per coordinator key keeps FIFO intact across re-lookups.
- metadata-failure slice: env_nodes_with_props(3, &[("retries","0")]) + metadata_resp(&[], _)
  (empty broker list) → findAllBrokers Call fails terminally → handle_failure wraps
  "Failed to find brokers to send list{Groups,ConsumerGroups}".
- ConsumerProtocol overloads that Java overloads by type: Rust can't overload, so
  serializeAssignment(ConsumerProtocolAssignment,short) → serialize_assignment_data (suffix).
- New helpers added to the kafka_admin_client test module: env_nodes_with_props(n, extra),
  pump_until_request_queued, listed_groups(&[(id,ptype,state,gtype)]), find_coordinator_error_resp,
  described_member, describe_groups_full_resp.
- COMMENTS.1.md is GITIGNORED; COMMENTS.DONE.1.md is untracked-by-convention (don't git add
  either — they're local workflow artifacts). Fixup commits reference 9f4eae7 (ConsumerProtocol)
  and 08885a3 (group RPC wiring). Lib test count 2484 → 2497 (+13).
