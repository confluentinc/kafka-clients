---
name: review-m11-phase4-log-dirs
description: M11 Phase 4 (describe/alterReplica/describeReplica LogDirs) review — clean; parity checkpoints for future admin phases
metadata:
  type: project
---

M11 Tier-1 Phase 4 (log-dir RPCs: describeLogDirs / alterReplicaLogDirs / describeReplicaLogDirs) reviewed clean — no blockers/should-fix. Commits 8755f06, 06b7114, 8196216.

**Why worth remembering:** parity checkpoints that recur across admin RPC phases.

**How to apply (checkpoints that held here):**
- `describeReplicaLogDirs` reshaping merge is **order-independent by design**: each branch (isFuture vs not) rebuilds ReplicaLogDirInfo preserving the OTHER field from the existing entry, so iterating `logDirDescriptions` (a HashMap, unordered) still merges current+future correctly. Verify any future reshaping keeps this invariant.
- `completeUnrealizedFutures` (Java `new ApiException(msg)`) is translated as `KafkaError::with_message(Errors::UnknownServerError, msg)` — tests assert `err.error() == UnknownServerError`. Consistent with the Phase-2 `||UnknownServerError` adjudication; not a defect.
- describeLogDirs empty-descriptions branch: error_code==NONE → ClusterAuthorizationFailed, else forCode(error_code). Both branches must be tested.
- MockAdminClient log-dir methods faithfully mirror Java (broker_log_dirs/partition_log_dirs/replica_moves state); Rust uses `.get()` (Option) where Java `List.get` would IndexOutOfBounds on bad brokerId/partition — defensive, not a behavior bug for tests.

**Two non-defect deviations (did NOT file — coverage preserved / Java parity):**
1. Response wire wrappers (DescribeLogDirsResponse, AlterReplicaLogDirsResponse) have NO byte-level known-vector test — but Java has none either, generated *ResponseData codec is separately validated, and responses are exercised serialize→parse in admin unit tests. Request wrappers DO have byte vectors.
2. `test_describe_log_dirs_with_volume_bytes` omits the two empty-error subtests that Java's `testDescribeLogDirsWithVolumeBytes` duplicates from `testDescribeLogDirs` — identical code path already fully covered by `test_describe_log_dirs`, so no coverage loss.
