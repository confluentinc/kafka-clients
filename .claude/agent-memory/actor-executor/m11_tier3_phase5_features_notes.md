---
name: m11-tier3-phase5-features
description: M11 Tier 3 Phase 5 describeFeatures/updateFeatures — reused ApiVersions wire, net-new UpdateFeatures wire, mock version-bounds validation, sync-throw→Result deviation
metadata:
  type: project
---

Milestone 11 Tier 3 Phase 5 (Features): `describeFeatures` + `updateFeatures`. Landed in 2 commits on `dev/adminclient_translation_and_bindings` (impl + integration test). Base 4ec63cf → lib tests 2808→2857.

**Wire types**
- `ApiVersionsRequest/Response` wrappers + `UpdateFeatures*RequestData/ResponseData` generated types ALL pre-existed (JSON specs already in `generator/messages/`). `describeFeatures` REUSES ApiVersions (zero new wire type). Only `UpdateFeatures{Request,Response}` wrappers were net-new (`src/common/requests/update_features_{request,response}.rs`).
- Wiring a new enum arm = 10 edits each in `abstract_request.rs`/`abstract_response.rs` (import x2, variant, version/api_key/to_send/serialize_with_header/serialize/get_error_response/parse arms, Display) + `requests/mod.rs` mod+re-export. Insert after the last existing arm (`DescribeDelegationToken`).
- `update_features_request.rs` imports `crate::admin::feature_update::UpgradeType` — common::requests depending on admin is fine (same crate).

**Node providers (verified against KafkaAdminClient.java ~4520/4575)**
- `describeFeatures`: `ConstantNodeId(nodeId)` if `options.nodeId()` set, else `LeastLoadedBrokerOrActiveKController`. (Java's `ConstantNodeIdProvider(id, true)` flag is irrelevant — bootstrap.controllers unsupported.)
- `updateFeatures`: `NodeProvider::Controller`. NOT_CONTROLLER top-level → clear_controller+request_update+`HandleResult::Retry` (mirrors the `handleNotControllerError(Errors)` overload, NOT the error-counts overload).

**Key deviation (flag for Critic): `update_features` returns `Result<UpdateFeaturesResult, KafkaError>`, not the bare `*Result`.** Java `updateFeatures` throws `IllegalArgumentException` synchronously for empty map ("Feature updates can not be null or empty.") / blank feature ("Provided feature can not be empty.") BEFORE enqueuing the Call. Per CLAUDE.md §10.2 (unchecked-but-recoverable throw → Result) + precedent `new_partition_reassignment::new`. `describe_features` doesn't validate → returns `DescribeFeaturesResult` directly. This asymmetry mirrors Java.

**Constructors returning Result** (Java IllegalArgumentException): `FeatureUpdate::new`, `FinalizedVersionRange::new`, `SupportedVersionRange::new`. The `downgradeFlagNotSetDuringDeletion` test is a `FeatureUpdate::new(0, Upgrade)` constructor test → lives in `feature_update.rs`, not the client test.

**MockAdminClient finding #9 (real validation, not stub):** seeded `feature_levels`/`min_supported`/`max_supported` maps (State fields + `set_feature_levels` inherent setter). `update_features` validates via free fn `validate_feature_update(cur,next,min,max,upgrade_type)->Result<(),String>` then wraps ALL failures as `KafkaError::with_message(InvalidRequest, "Invalid update version {next} for feature {feature}. {inner}")` (Java's `invalidUpdateVersion` always produces InvalidRequestException regardless of inner type). The UNSAFE_DOWNGRADE inner `SAFE_DOWNGRADE` guard is dead code in Java — translated faithfully as a no-op loop. `describe_features` returns epoch `Some(123)`.

**Parameterized test bounds (read from Java @ValueSource):** `testUpdateFeaturesDuringSuccess` and `testUpdateFeaturesHandleNotControllerException` are `@ValueSource(shorts = {1, 2})` → loop `[1i16, 2]`. v≤1 response carries per-feature results; v2 carries empty results (top-level NONE completes all). testUpdateFeatures NOT-a-@ParameterizedTest (`testUpdateFeaturesTopLevelError` is plain @Test).

**Test harness:** `env()` returns `(admin, runnable, MockTime, nodes)`; metadata is ready from t=0 (Controller/ConstantNodeId providers resolve immediately). `prepare_response_for_node(resp, &nodes[i])` targets a broker; a request to a different node is left unanswered → times out (used for `testDescribeFeaturesWithNodeFailure`). NOT_CONTROLLER retry needs a `time.sleep(200)` loop (retry-backoff gate) — mirror `test_create_topics_handle_not_controller_exception`.

**Integration:** `admin_features_test.rs` — (a) describe reports sane metadata.version range, (b) update beyond max rejected with `validate_only(true)`. Both PASS against Dockerized 4.2.0. Successful upgrade deliberately omitted (feature levels are cluster-wide/persistent/irreversible on shared cluster). `main.rs` staged via remove-smoke-line → add → restore-smoke-line so `git show HEAD:tests/integration/main.rs | grep admin_smoke_test_manual` is EMPTY.

**Gotcha:** `cargo xtask format-check` covers `tests/integration/*` even though they're feature-gated — format the integration file before committing (build.rs auto-format only touches generated + src, not integration tests).
