---
name: review-m11-tier3-phase5-features
description: M11 Tier3 Phase5 Features (describe/update) review — CLEAN; synchronous-throw→Result faithfulness, mock dead-guard, ApiVersions reuse verdicts
metadata:
  type: project
---

Tier 3 Phase 5 (Features: `describeFeatures`/`updateFeatures`, commits 21871d1+21d28de) reviewed CLEAN — zero issues. Adjudications worth reusing:

- **`update_features` returns `Result<UpdateFeaturesResult, KafkaError>` (only admin RPC with this shape) is FAITHFUL.** Verified `KafkaAdminClient.java:4575` throws `IllegalArgumentException` SYNCHRONOUSLY (empty map → "Feature updates can not be null or empty."; blank feature → "Provided feature can not be empty.") BEFORE `runnable.call()`. The third "downgrade-flag-during-deletion" validation is in the `FeatureUpdate` CONSTRUCTOR (`FeatureUpdate.java`: `maxVersionLevel==0 && upgradeType==UPGRADE` → "The upgradeType flag should be set to SAFE_DOWNGRADE or UNSAFE_DOWNGRADE when the provided maxVersionLevel:%d is < 1."), translated as `FeatureUpdate::new -> Result`. Both faithful per §10.2. When a critic sees a lone `Result`-returning RPC, check whether Java throws pre-`runnable.call` vs completes-future-exceptionally — here it's genuinely a synchronous throw.
- **MockAdminClient dead-guard: Java's `UNSAFE_DOWNGRADE` arm has `while(next!=cur){ if(cur%2==0){ if(upgradeType==SAFE_DOWNGRADE) throw...} cur--; }` — the inner SAFE_DOWNGRADE test is DEAD (we're in the UNSAFE arm).** Rust reduces to `while next!=cur { cur-=1; }` (a no-op loop). Faithful: both have zero observable effect. Actor correctly flagged and kept it.
- Mock error application: first validation failure sets `error` and `break`s, but ALL per-feature futures get `completeExceptionally(error)` and featureLevels are NOT written. Rust two-loop structure mirrors exactly; validation loop reads pre-update levels, results loop writes.
- `handleNotControllerError(Errors)` = clearController + requestUpdate + throw → Rust `clear_controller()+request_update()+HandleResult::Retry(NotController)`. Established pattern across all Tier admin RPCs.
- `@ValueSource(shorts = {1,2})` for DuringSuccess/HandleNotController = exactly [1,2]; `version<=1 ? features : empty`. v≤1 populates per-feature results; v2 empty results + top-level NONE completes all. Rust loops `[1i16,2]` and gates features identically.
- ApiVersions REUSED (no dup wire type); `create_feature_metadata` decodes finalized(min/maxVersionLevel), epoch (>=0 present else None), supported(min/maxVersion) — matches Java. FinalizedVersionRange/SupportedVersionRange toStrings use snake_case field names in Java too (`min_version_level`, `min_version`) so Rust Display matches verbatim.
- UpdateFeatures wire: all ConcreteRequest/Response arms wired; byte-vectors v1-flexible hand-computed & correct; get_feature v0(allow_downgrade)/v1+(upgrade_type code) paths both tested.

Non-defect deviations noted (correctly NOT reported): production `handle_response` drops Java's `log.warn("unknown feature")` (log-only, comment explains); mock `describe_features` uses `.unwrap_or(0)` where Java would NPE on inconsistent seeding (defensive, error-path-only).
