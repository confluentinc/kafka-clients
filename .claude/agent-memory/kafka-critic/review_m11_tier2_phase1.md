---
name: review-m11-tier2-phase1
description: M11 Tier2 Phase1 (group listing/describe) review findings + coverage-gap heuristics
metadata:
  type: project
---

# M11 Tier 2 Phase 1: Group listing & describe (range 5bfebe8..947042a)

RPCs: listGroups, listConsumerGroups(deprecated), describeConsumerGroups
(dual-protocol CGD→classic fallback), describeClassicGroups. See
[[review-m11-phase2-admin-driver]] for the driver engine.

Production code was faithful: DescribeConsumerGroupsHandler / classic handler /
CoordinatorStrategy / ConsumerProtocol.deserialize_assignment all match Java.
Byte-level wire tests exist+correct for all 6 new request/response types
(list_groups, describe_groups, consumer_group_describe × req/resp).

## Real findings (all test/completeness gaps, no functional bugs)
1. **describeClassicGroups has ZERO tests.** No Java DescribeClassicGroupsHandlerTest
   exists (only DescribeConsumerGroupsHandlerTest under internals/), so Java's
   only coverage is 3 KafkaAdminClientTest client slices (testDescribeClassicGroups
   ~7075, ...WithAuthorizedOperationsOmitted, testDescribeMultipleClassicGroups) —
   none translated. Rust classic handler file has no #[cfg(test)]; only a mock
   "unsupported" test exists (which is correct per §9 but exercises no real logic).
2. **list states/types filter wiring untested.** No Rust test passes a non-empty
   group_states/types filter through client list_groups/list_consumer_groups; the
   set_states_filter/set_types_filter wiring + older-broker end-to-end (Share/Consumer
   →UnsupportedVersion vs Classic-only→omit) uncovered. Builder version-gating IS
   unit-tested in list_groups_request.rs (that part fine). Java: testListGroupsWithTypes
   /WithTypesOlderBrokerVersion, testListConsumerGroupsWithStates/WithTypes + older +
   Deprecated variants + metadata-failure slices.
3. **ConsumerProtocol omits 3 public methods** (deserializeConsumerProtocol
   Assignment/Subscription ×2, serializeAssignment(data) overload) despite PLAN
   finding #3's "all methods per DoD #2". LOW — genuinely unused in-scope, no
   behavior change; flag as stated-requirement deviation needing add-or-document.

## Adjudications that were NOT defects (avoid FP)
- **Integration nonexistent-group dual-outcome** (admin_groups_test.rs:243): accepts
  EITHER GROUP_ID_NOT_FOUND OR a "Dead"/empty classic group. Defensible: both are
  faithful 4.2 broker responses; the meaningful invariants (empty members, not
  Stable) are still asserted; handler behavior for both paths is pinned by unit
  tests. Not masking a bug.
- **CoordinatorStrategy test rename** testBuildOldLookupRequestRequiresAtLeastOneKey
  → test_build_old_lookup_request_requires_matching_type: Java test name is
  MISLEADING — it actually passes ONE wrong-type key expecting IAE. Rust rename is
  accurate; covers the same intent. NOT a divergence.
- **CoordinatorStrategy unrepresentable-key no-op**: Java filters null-idValue keys
  into failedKeys(InvalidGroupId); Rust CoordinatorKey.id_value is non-null String
  so filter is unrepresentable-in-types. Correct adjudication (isRepresentableKey =
  groupId!=null; "" is representable in both).
- **Per-group deserialize-error → failed key** (both describe handlers): Java lets
  ConsumerProtocol SchemaException escape handleResponse (fails whole batch); Rust
  catches per-group and fails only that key. Slight divergence but documented,
  unreachable in practice (malformed broker bytes), and CLAUDE.md §5 prefers
  explicit per-op error over panic. Did not flag.
- MockAdminClient: Java mock implements listGroups/listConsumerGroups, THROWS
  unsupported for describeConsumerGroups/describeClassicGroups. Rust mirrors exactly
  → §9-compliant. Not a finding.
- exception_with_optional_message collapses Some("")→default; Java error.exception("")
  would keep empty msg. Divergence only on empty-string (vs null) broker message;
  never happens in practice. Did not flag.

## Fix-cycle re-review (342a0b5 / 3cda733 / cadd443): all 3 GENUINE + complete
- #3 methods: faithful one-liners over generated read/to_version_prefixed_byte_buffer,
  pub(crate) (internal pkg correct), 2 real round-trips. No behavior change.
- #1 3 client slices: assignment genuinely decoded from real serialize_assignment
  bytes (not hardcoded), ClassicGroupState::Stable asserted, FindCoord/DescribeGroups
  retriable+NOT_COORDINATOR+COORDINATOR_NOT_AVAILABLE re-lookup chain exercised.
- #2 deviation ACCEPTABLE: MockClient can't negotiate versions, so older-broker
  modeled as (a) manual build_version(4) asserting omit, (b)
  prepare_unsupported_version_response asserting UnsupportedVersion surfaces. The
  builder gating itself is fully unit-tested in list_groups_request.rs (classic-omit
  / consumer+share-reject / states-requires-v4). Filter wiring inspects the ACTUAL
  queued builder (requests_mut()[0].request_builder_mut().build()), not the options
  object. Generic "NetworkClient calls build_version(negotiated)" seam is shared
  across all RPCs (exercised by every byte-vector test) → risk contained. Not a gap.
- No new COMMENTS.1 entries. Note: authorized_operations()→&BTreeSet (empty for
  omitted) vs Java null is PRE-EXISTING convention shared by ConsumerGroupDescription
  /TopicDescription; not introduced by fixup, not re-litigated.
- Stray untracked tests/integration/admin_smoke_test_manual.rs in worktree (not in
  any fixup commit) — flagged to Manager, likely scratch.

## Heuristic: "handler tests cover it" is FALSE for RPCs with no Java HandlerTest
When an Actor argues client-level slices are redundant with handler tests, CHECK
whether a Java *HandlerTest even exists. describeClassicGroups had none → its ONLY
Java coverage was client slices → skipping them = total coverage loss. Also: list
RPCs use a hand-rolled Call (submit_list_groups), NOT a handler, so options→request
wiring lives outside any handler test by construction.
