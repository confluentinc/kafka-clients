# Admin client Java-parity fixes

Status: **APPROVED 2026-09-29** (decisions recorded at the end of this file).

- Branch: `fix/admin-java-parity`, worktree `.claude/worktrees/admin-java-parity`
- Base: `origin/master` @ `010d0620` (2026-09-29), fetched fresh at approval time
- PR target: `master`
- Java reference: `kafka/` (Apache Kafka 4.3.1)
- Actor / Critic number: **N = 66** (highest number in `design/history` is 65; no `COMMENTS.*.md` exists on the base)

Every finding below was re-checked on `origin/master` and, where marked, reproduced in a throwaway test during planning. None of that throwaway code is on this branch.

Fixing the `Error::unsupported_version` constructor (it builds a generic code-35 error rather than the typed `UnsupportedVersion` variant) is **out of scope** for this PR by decision; see "Decisions". Nothing in this PR may change that constructor.

## Process (agent-roles.md, Manager role)

1. **Actor 66** implements one phase, commits it (more than one commit if the phase has natural steps), after running build, tests, format, lint and a self-review against the DoD.
2. **Critic 66** reviews the commits since its last review and writes findings to `COMMENTS.66.md` (with an exclusive lock). Afterwards it reads `COMMENTS.FP.md` / `COMMENTS.FN.md` and may propose rule updates in `COMMENTS.66.md`.
3. **Actor 66** fixes every comment with a `fixup!` commit referencing the commit it corrects, and moves the resolved item to `COMMENTS.DONE.66.md`.
4. Steps 2–3 repeat until the Critic reports nothing new for that phase; then the next phase starts at step 1. The Manager keeps `design/current/admin-java-parity/RUN-LOG.md`.
5. After the last phase: the Manager runs `make verify`, `make verify-c`, `make verify-python`, pushes, and opens the PR against `master`.

Agents must not edit `CLAUDE.md` or `.claude/rules/*` ("Any change to this prompt is to be avoided by automatic agents"). Rule changes are proposed in `COMMENTS.66.md` only.

## Environment notes (verified during planning)

- The `cargo` on `PATH` (Nix) has **no clippy**. Lint with the pinned toolchain:
  `PATH="$HOME/.rustup/toolchains/1.95.0-aarch64-apple-darwin/bin:$PATH" cargo xtask lint`
- Integration tests: `cargo test --features integration-tests --test integration <filter>`. Docker 29.6.1 is running; the harness starts brokers itself.
- The shell is zsh: quote globs (`--include='*.java'`) and brace refs in `git show "${REF}:path"` (zsh reads `$R:s…` as a modifier).
- A test that spawns the real admin loop over `MockClient` must make the loop park (see Phase 1); otherwise the loop never yields and starves the single-threaded test runtime.
- **Commit messages and the PR description carry no AI attribution** — no `Co-Authored-By` trailer and no "Generated with Claude Code" line. Repo style: descriptive subject, body explaining what and why.
- Final gate: `make verify` (build, format-check, lint, test, check-bindings), plus `make verify-c` and `make verify-python`, because the mock's default cluster id and the `describeTopics` ELR lists are visible through the bindings.

## Phase 1 — A call submitted just before `close()` is dropped

**Symptom.** `create_topics(..)` then `close(30s)` fails the call with `"The AdminClient thread has exited. Call: createTopics"`. Java completes the create.

**Root cause (reproduced).** The queues match Java (`admin_rx` ≙ `newCalls`, `pending_calls` ≙ `pendingCalls`); the **exit-check position** does not. Java drains `newCalls` and checks `threadShouldExit` back to back at the top of each iteration (`KafkaAdminClient.java:1497-1504`). Rust's `process_requests` (`src/admin/internals/admin_client_runnable.rs`) runs all of `run_once()` — drain, timeouts, assign, send, **network poll**, responses — and only then calls `should_exit()`. A call that arrives while the task is parked in the poll is still in `admin_rx` at the exit check, `has_active_external_calls()` cannot see it, and the loop exits; `fail_all_remaining` then fails it.

Planning reproduction: a unit test driving the real spawned `run()` with the loop parked in its poll failed with exactly the audit's error; adding one `drain_new_calls()` immediately before `should_exit()` made it pass.

**Change.** Restore Java's order: the exit check happens immediately after the drain and before timeouts / assignment / send / poll. The Actor chooses the shape (for example drain + exit check at the top of each `process_requests` iteration, keeping `run_once` usable by tests that pump single iterations), but the Java phase order is the contract.

**Tests.**
- Regression test that spawns the real loop, parks it in its network poll, submits `create_topics`, calls `close(30s)`, and asserts the create **succeeds**. The park hook must wait on `client.wakeup_notify()` — the `Notify` that production `submit()` and `close()` poke — so the fixture uses the production primitive (DoD #12). (Planning used a park-once flag on the existing `WaitingClient` test client.)
- Existing close/shutdown tests stay green, in particular `close_waits_for_an_active_external_call_until_the_hard_deadline` and `close_exits_the_io_task_while_only_the_internal_metadata_call_is_active`. Note that the former pumps once after submitting, which is why it never caught this bug.

## Phase 2 — A driver RPC key never resolves after `close()`

**Symptom.** `fence_producers(["a","b"])` then `close(0)`: one key's future never resolves.

**Root cause (reproduced on a real 4.3.1 broker).** The driver issues at most one fulfillment request per broker at a time (`AdminApiDriver.java:381-383`), so `b`'s `InitProducerId` call is created only after `a`'s completes or fails. During shutdown `fail_all_remaining` fails `a`'s call; its failure hook runs `maybe_send_requests`, which sends `b`'s new call straight into `admin_rx` after the final drain. The runnable is then dropped with that call still in the channel, so `b`'s key is never completed.

Java routes driver follow-ups through `runnable.call(...)` (`KafkaAdminClient.java:5108-5111`), which rejects any new call once `close()` has set the shutdown deadline, with `IllegalStateException("Cannot accept new calls when AdminClient is closing.")` (`:1598-1600`). Rust's `submit()` already has that gate; `maybe_send_requests` (`src/admin/kafka_admin_client.rs`) bypasses it.

Planning reproduction: 24 real-broker runs sweeping the delay before `close(0)` — 3 hung without the gate, 0 with it; the previously hanging keys failed with Java's message.

**Change.** Give driver-issued calls the same closing gate as `submit()` (Rust's `closing` flag is set together with the hard-shutdown deadline, which is what Java's `call()` checks). Prefer one shared submission path over a second copy of the check.

**Also expected to fix** two related symptoms: driver RPCs issued after `close()` get a retriable Timeout instead of Java's `IllegalStateException`, and a driver RPC submitted during the shutdown tail hangs. They share the code path; the Actor verifies both with tests rather than assuming it.

**Tests.** Deterministic unit test (no timing sweeps): one broker, two transactional ids, prepared FindCoordinator + InitProducerId responses, close while the second key's fulfillment has not been issued yet; assert **every** key resolves and the unissued one fails with Java's message. Plus a test for the after-`close()` error case.

## Phase 3 — A zero topic id panics the whole admin task

**Symptom.** `errors_by_topic_id` (`src/common/requests/metadata_response.rs:132`) `assert!`s on a zero topic id. A 4.x broker returns one when a topic is deleted during a by-id `describeTopics` (`KafkaApis.scala:869`). The panic ends the I/O task, that call hangs, and every later call is rejected.

**Java.** `MetadataResponse.errorsByTopicId()` throws `IllegalStateException("Use errors() when managing topic using topic name")`; `handleResponses` catches it and calls `call.fail(now, t)` (`KafkaAdminClient.java:1394-1403`), so only that call fails.

**Change.** Return a `Result` (CLAUDE.md §10.2) with Java's message as `Error::local_illegal_state(..)`; the only caller (the by-id describe handler in `kafka_admin_client.rs`) returns `HandleResult::Retry(err)`, which routes through `fail_call` exactly like Java's `call.fail(now, t)` — non-retriable, so only that call's futures fail. No existing test pins the panic.

Planning reproduction: today the panic leaves the by-id call **never resolved** and rejects the next unrelated call with "Cannot accept new calls when AdminClient is closing.". With the fix (about 14 lines), the call fails with `LocalIllegalStateError: Use errors() when managing topic using topic name`, the next `listTopics` succeeds, and the full lib suite passes (3919 / 0). Origin: `5e1db7cf` (2026-04-09) translated Java's `throw` as `assert!`; the admin by-id handler is its only caller.

**Tests.** A metadata response containing a zero topic id fails only that call, with the exact message; a second call on the same client succeeds afterwards. Drive it with `pump` (`run_once` stepped by hand), **not** a spawned loop: once the panic is gone a spawned `MockClient` loop never yields and the test hangs — the planning probe hit exactly this.

Out of scope: the sibling `errors()` has the same `expect`-where-Java-throws shape. The Actor notes whether an admin path can reach it; it is not changed in this PR unless it can.

## Phase 4 — Six `Result` constructors are crate-private

Make `new` public (`pub fn new`) on:
- `DeleteRecordsResult`, `DescribeConsumerGroupsResult`, `DescribeClassicGroupsResult`, `ListOffsetsResult` — Java `public`;
- `CreateTopicsResult`, `DescribeConfigsResult` — Java `protected`. Rust has no `protected`; Java code outside the package constructs these through an anonymous subclass, so `pub` is the Rust equivalent that lets user code (for example a hand-written `Admin` fake) build them. Record this mapping in the rustdoc of both constructors, since no rule in the repo states it.

`DeleteTopicsResult` and `DescribeTopicsResult` need **no change**: they are `pub enum`s with public `ByTopicId` / `ByTopicName` variants, already constructible outside the crate (verified with an external test crate during planning), and the enum makes Java's "both maps / neither map" `IllegalArgumentException` unrepresentable. Their `of_topic_*` factories stay `pub(crate)`, matching Java's package-private `ofTopicIds` / `ofTopicNames`.

Planning check: flipping the six to `pub` passes clippy with `-D warnings` and needs no other change; all parameter types are already public.

Out of scope, noted for a later pass: 8 admin results whose Java constructor is package-private are `pub` in Rust (`CreateAcls`, `DeleteAcls`, `DescribeAcls`, the four delegation-token results, `DescribeUserScramCredentials`).

**Tests.** One external test under `tests/` (a separate crate, so it genuinely exercises visibility) that constructs all six from `KafkaFuture::completed(..)` values and reads a value back.

## Phase 5 — `MockAdminClient` has no `Builder`

**Rust today.** Only `create(num_brokers)` plus post-construction `set_feature_levels` / `set_broker_log_dirs`; cluster id, controller (broker 0), default partitions (1), replication factor (`min(n, 3)`) and default group configs are hard-coded. The state struct already has 10 of Java's 11 constructor fields.

**Requirement: match Java's `Builder` exactly** — same methods, parameters, defaults, evaluation order and failure points. Only the names change, and only as CLAUDE.md §2 dictates.

**Java shape → Rust** (`kafka/clients/src/test/java/org/apache/kafka/clients/admin/MockAdminClient.java`, `Builder` class and `build()`):

| Java | Rust | Notes |
|---|---|---|
| `public static class Builder` (nested) | `pub struct Builder` in `mock_admin_client.rs`, reached as `crate::admin::mock_admin_client::Builder` | CLAUDE.md §2: nested classes stay in the parent's file and are reached through the submodule |
| `public Builder()` → `numBrokers(1)` | `Builder::new()` → runs the same `num_brokers(1)` logic | CLAUDE.md §2: constructors are `new`. A fresh builder has 1 broker, `Node(0, "localhost", 1000)`, and `DEFAULT_LOG_DIRS` |
| `clusterId(String)` | `set_cluster_id(..) -> Self` | Setter names use `set_` per CLAUDE.md §2 and the existing precedent (`AlterConfigsOptions.timeoutMs` / `validateOnly` → `set_timeout_ms` / `set_validate_only`) |
| `brokers(List<Node>)` | `set_brokers(Vec<Node>) -> Self` | Same order as Java: first resize `broker_log_dirs` to the new length (keep existing entries, pad with `DEFAULT_LOG_DIRS`), **then** replace the broker list |
| `numBrokers(int)` | `set_num_brokers(i32) -> Result<Self, Error>` | Shrink truncates `brokers` and `broker_log_dirs`; grow appends `Node(id, "localhost", 1000 + id)` and `DEFAULT_LOG_DIRS`. Java throws `IndexOutOfBoundsException` for a negative count (`subList(0, n)`), so Rust returns `Err` at the same call |
| `controller(int index)` | `set_controller(i32) -> Result<Self, Error>` | Java throws `IndexOutOfBoundsException` from `brokers.get(index)` **at this call**, so Rust returns `Err` here too, not deferred to `build()` |
| `brokerLogDirs(List<List<String>>)` | `set_broker_log_dirs(Vec<Vec<String>>) -> Self` | No length check, as in Java |
| `defaultReplicationFactor(int)` | `set_default_replication_factor(i32) -> Self` | `build()` narrows with a wrapping `as i16`, matching Java's `shortValue()` |
| `usingRaftController(boolean)` | `set_using_raft_controller(bool) -> Self` | Included, stored as the 11th state field. Its only Java reader is `unregisterBroker`, which the Rust client does not implement yet, so it has no effect until that RPC is translated. Document this at the setter |
| `defaultPartitions(short)` | `set_default_partitions(i16) -> Self` | |
| `featureLevels` / `minSupportedFeatureLevels` / `maxSupportedFeatureLevels(Map<String, Short>)` | `set_feature_levels` / `set_min_supported_feature_levels` / `set_max_supported_feature_levels(HashMap<String, i16>) -> Self` | Distinct type from the existing post-construction `MockAdminClient::set_feature_levels`, so no name clash |
| `defaultGroupConfigs(Map<String, String>)` | `set_default_group_configs(HashMap<String, String>) -> Self` | |
| `build()` | `build(self) -> Result<MockAdminClient, Error>` | Defaults exactly as Java: controller = `brokers[0]`, partitions = 1, replication factor = `min(brokers.len(), 3)`. Errors where Java throws: no brokers (`brokers.get(0)`), and a controller no longer in the broker list (`IllegalArgumentException("The controller node must be in the list of brokers")`, for example `set_controller(2)` followed by `set_num_brokers(1)`) |

Error mapping: Java's `IndexOutOfBoundsException` becomes `Error::local_illegal_argument(..)`, following the mock's existing precedent (`create(0)` returns "num_brokers must be at least 1"). The crate has no `IndexOutOfBounds` variant, and adding one is out of scope. Every error message is asserted exactly.

**Constants the Builder depends on.** Java's builder defaults to `DEFAULT_CLUSTER_ID` and `DEFAULT_LOG_DIRS`, both `public static final`. So this phase also:
- makes them public associated constants, `MockAdminClient::DEFAULT_CLUSTER_ID` and `MockAdminClient::DEFAULT_LOG_DIRS` (CLAUDE.md §2: a constant is exported by the struct that defines it);
- fixes `DEFAULT_CLUSTER_ID`'s value to Java's `I4ZmrWqfT2e-upky_4fdPA`. The current `4A5xz_QZTB2CtL4wc0X0Jw` is also pinned by `bindings/python/test/unit/test_admin.py` and `bindings/c/tests/test_mock_admin.c`; those assertions change with it.

**`create(num_brokers)` is removed** (no Java counterpart). Its 28 callers on `origin/master` (`kafka_admin_client.rs` ×19, `mock_admin_client.rs` ×6, `mod.rs` ×1, `ffi/admin.rs` ×1, `tests/common/admin_backend.rs` ×1) move to the Builder; `create(n)` behaved exactly like `Builder::new().set_num_brokers(n)?.build()`, so the migration is behaviour-preserving. Keep every existing assertion those callers make.

**C API.** `kafka_admin_MockAdminClient_new(num_brokers)` keeps its signature and builds through the Builder internally. Exposing the Builder in C or Python is out of scope.

**Not included:** Java's public `MockAdminClient()` / `MockAdminClient(List<Node>, Node)` constructors, which were not requested.

**Tests.** No clients-module Java test uses the Builder (its 6 users are in connect/streams/tools), so there is nothing to translate. Write tests for: each setter; the defaults of `Builder::new().build()`; `set_brokers`' log-dir resizing order; `set_num_brokers` shrink and grow; the three error points with exact messages; and the `shortValue()` narrowing.

## Phase 6 — `describeTopics` by name never uses `DescribeTopicPartitions`

**Why it exists.** A deliberate Phase-1 deferral (`src/admin/kafka_admin_client.rs` module header), contrary to `admin-client.md` §7, which lists `DescribeTopicPartitions` and its `DescribeCluster` prerequisite as required Phase-1 wire types. Java's 5 tests for the path were never translated nor listed as skipped. Nothing was ever implemented and removed, so this is a gap, not a regression.

**Observable today.** `elr()` / `last_known_elr()` are always `None`; Java returns the broker's list (`[]` on a healthy cluster — the 4.3.1 broker always sends a list, `KRaftMetadataCache.java:221-222`, converted by `DescribeTopicPartitionsResponse.java:82-83`). `DescribeTopicsOptions::partition_size_limit_per_response` is public (and in the C API) but never read. Timeout messages name the wrong call.

**Change.**
1. `DescribeTopicPartitionsRequest` / `DescribeTopicPartitionsResponse` wrappers + request builder in `src/common/requests/`, wired into every `ConcreteRequest` / `ConcreteResponse` match arm (`admin-client.md` §7), including `DescribeTopicPartitionsResponse.partitionToTopicPartitionInfo`. The generated message types already exist; the `DescribeCluster` wrapper already exists. Follow `admin-client.md` / `producer-transactions.md` §12 for the builder's `super(...)` version bound.
2. The by-name flow translated from `KafkaAdminClient.java:2152-2450`: `describeCluster` first for the node map, then paginated `DescribeTopicPartitions` calls following `nextCursor`, honouring `partitionSizeLimitPerResponse`, and falling back to the existing Metadata path on `UnsupportedVersionException` (older brokers) exactly as Java does. By-id stays on the Metadata API (Java does too). On `origin/master` the entry point is `describe_topics_with_topics_options` → `get_describe_topics_by_names_call`.
   Because the constructor fix is out of scope, `Error::unsupported_version(..)` still builds the generic code-35 variant; detect the fallback condition by error **code** (`error.error() == Errors::UnsupportedVersion`, as `fail_call` already does), not by matching the typed `Error::UnsupportedVersion(_)` variant.
3. Remove the module-header deviation and correct `design/current/status.md` / `structure.md`, which currently describe the Metadata path as "behavior-faithful".

**Tests.**
- Translate `testDescribeTopicsWithDescribeTopicPartitionsApiBasic`, `testDescribeTopicPartitionsApiWithAuthorizedOps`, `testDescribeTopicPartitionsApiWithoutAuthorizedOps`, `testDescribeTopicsWithDescribeTopicPartitionsApiEdgeCase`, `testDescribeTopicsWithDescribeTopicPartitionsApiErrorHandling` (`KafkaAdminClientTest.java:1508-1745`), plus a fallback-to-Metadata test.
- Byte-level encode/decode tests for the new request/response against known vectors (DoD #3).
- Integration test on a real broker: `elr()` is `Some(vec![])` for a healthy topic, and pagination works with a small partition limit.
- C / Python: `elr` changes from none to an empty list; update binding tests to the Java semantics so `make verify-c` / `make verify-python` stay green.

## Definition of Done (whole PR)

All items of `definition-of-done.md`, with these specifics:

- #3: every behaviour change has a regression test that fails before the fix; error messages asserted exactly.
- #7: any new struct/trait not in Java is justified in the commit message.
- #9: `make verify`, `make verify-c`, `make verify-python` green.
- #10 (hot path): N/A — admin calls are not per-record (`admin-client.md` §10).
- #12: every fixture that stands in for production wiring uses the production primitive.
- No TODO/FIXME; no `panic!` on a recoverable path.

## Decisions (approved 2026-09-29)

1. **Base:** latest `origin/master` (`010d0620` at approval).
2. **`DescribeTopicPartitions` for `describeTopics`:** in this PR, as the last phase.
3. **`Error::unsupported_version` constructor fix:** skipped — not part of this PR.
4. **`MockAdminClient` Builder:** Java's `Builder` shape exactly (Phase 5 table). `create(num_brokers)` is removed and its callers migrated (the plan's recommended option; no counter-instruction was given). Java's two public `MockAdminClient` constructors are not included.
5. **`Result` constructors:** `pub` for all six, including Java's two `protected` constructors.
6. **Attribution:** no "Generated with Claude Code" in the PR description and no AI co-author trailer in commits.
