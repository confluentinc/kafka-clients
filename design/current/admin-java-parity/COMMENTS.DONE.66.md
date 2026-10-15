# Critic 66 — resolved comments

## Review of `36dc7704` (Phase 1: a call submitted just before `close()`)

### Issue 1: Stale 4.2-era Java line citations and `run_once` references remain beside the lines this commit corrected

- **Severity**: Low (documentation accuracy; no behaviour impact)
- **Files**:
  - `src/admin/internals/admin_client_runnable.rs:190`: cites
    `KafkaAdminClient.java:1459-1476` for the `run()` `try/finally`. This is inside
    the comment block the commit edited (line 193 changed).
  - `src/admin/internals/admin_client_runnable.rs:322`: the hard-shutdown clamp,
    now inside the new `process_pending_calls`, says "Mirrors
    `KafkaAdminClient.java:1500-1502`".
  - `src/admin/kafka_admin_client.rs:6884`: the same `1459-1476` citation for the
    `finally`.
  - `src/admin/kafka_admin_client.rs:13359`: the same `1500-1502` citation, sitting
    directly above a quote of the clamp code.
  - `src/admin/kafka_admin_client.rs:13233` and `:13571`: these still describe
    `run_once` as the production iteration ("any `await` inside a `run_once`
    phase", "the poll budget that `run_once` reads on every iteration"). The commit
    fixed the identical wording at `:4932` but missed these two.
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/admin/KafkaAdminClient.java`
  (4.3.1, gitlink `26b251a451`). `run()` with its `try/finally` is at `1469-1493`,
  and the `finally` is at `1473-1492`. The hard-shutdown poll clamp is at
  `1512-1515`. In 4.3.1, lines `1500-1502` are the blank line, the "Check if the
  AdminClient thread should shut down" comment and the `hardShutdownTimeMs.get()`
  read, and `1459-1476` runs from inside `threadShouldExit` to the `finally`.
- **What is wrong**: The commit message says the two Java line citations "in the
  touched docs are corrected for 4.3.1" (`1461→1472`, and the new
  `1498-1504` / `1506-1549`, all of which are correct). The citations in the same
  Java region next to them were not corrected. A reader who checks the `322`
  and `13359` citations against `kafka/` lands on the exit check, not the clamp.
  This is the phase this commit moved the clamp away from, which makes the
  citation actively misleading.
- **Failure scenario**: A later reviewer checks the clamp citation against
  4.3.1. It seems to say that Java clamps where it checks for exit. The reviewer
  then either reports a false phase-order defect or wrongly accepts a
  reordering.
- **Expected fix**: `1459-1476` → `1469-1493` (or `1471-1492` for the
  `try/finally` alone) at `admin_client_runnable.rs:190` and
  `kafka_admin_client.rs:6884`. `1500-1502` → `1512-1515` at
  `admin_client_runnable.rs:322` and `kafka_admin_client.rs:13359`. `run_once` →
  `process_pending_calls` in the two production-describing comments at
  `kafka_admin_client.rs:13233` and `:13571`. Comments only; no code change.

- **Resolution**: Fixed in `fixup! 36dc7704`. Each citation was checked against 4.3.1 `kafka/`: the try/finally is `1469-1493` (runnable) and the `finally` alone is `1473-1492` (test doc); the clamp is `1512-1515` at both sites. `run_once` → `process_pending_calls` at the two named sites and at three more production-describing comments found by grep (`:6890`, `:6899`, `:6998`), plus the injected-panic text at `:6915`.

## Review of `7a2f7ab2` (Phase 2: the closing gate for every submitted call)

### Issue 2: Quota-retry follow-ups (`HandleResult::NewCall`) still bypass the `runnable.call` gate

- **Severity**: Behaviour Mismatch (Medium). This is pre-existing, not introduced
  by `7a2f7ab2`. I am reporting it because the commit and its rustdoc say
  `runnable_call` is "the single submission path", and PLAN Phase 2 asks for one
  shared path.
- **File**: `src/admin/internals/admin_client_runnable.rs:688-690`
  (`HandleResult::NewCall(new_call) => self.pending_calls.push(*new_call)`). The
  follow-ups come from `get_create_topics_call`, `get_create_partitions_call`,
  `get_delete_topics_call` and `get_delete_topics_with_ids_call`
  (`src/admin/kafka_admin_client.rs:2664`, `:2786`, `:2899`, `:3022`).
- **Java Reference**: `KafkaAdminClient.java:1880` (createTopics), `:3257`
  (createPartitions), `:2025` (deleteTopics) and `:2098` (deleteTopicsWithIds).
  Each quota-exceeded retry goes through `runnable.call(call, now)`, which
  rejects it once the deadline is set (`:1599-1601`). The retry call's
  `handleFailure` (for example `:1897-1903`) then runs
  `maybeCompleteQuotaExceededException`. That is a no-op, because the throwable
  is not a `TimeoutException`, so `completeAllExceptionally(futures, IllegalStateException)`
  runs.
- **What is wrong**: This is the fourth follow-up path that `runnable.call` covers
  in Java, alongside driver follow-ups, listGroups per-broker calls and user
  calls. It is the only one that still skips the deadline check. The follow-up
  is pushed straight into `pending_calls`, so it counts as an active external
  call and the loop keeps retrying it until the hard deadline.
- **Failure scenario**: `create_topics(["t"])` with the default
  `retry_on_quota_violation = true`, then `close(Duration::from_secs(30))` while
  the request is in flight. The controller answers `THROTTLING_QUOTA_EXCEEDED`.
  - Java: the retry is rejected, and `t`'s future fails right away with
    `IllegalStateException("Cannot accept new calls when AdminClient is closing.")`.
  - Rust: the retry is queued and sent again after the throttle backoff. Either
    the topic gets created during `close()`, which Java never does, or at the
    deadline `fail_all_remaining` fails it with a Timeout. In that case
    `maybe_complete_quota_exceeded` (`kafka_admin_client.rs:1148`) turns the
    result into `ThrottlingQuotaExceeded`. Either way the outcome and the error
    class differ from Java.
- **Expected fix**: In the `NewCall` arm, apply the same deadline gate before
  pushing. If `hard_shutdown_deadline_ms != NO_HARD_SHUTDOWN`, call
  `new_call.handle_failure(&Error::local_illegal_state("Cannot accept new calls
  when AdminClient is closing."))`; otherwise push. The `enqueue` half is always
  "accepted" there, because the loop is still running. Best is to factor the gate
  out of `runnable_call` so all paths share one check. Add a regression test:
  createTopics with the quota error and retry enabled, `close(30s)` set before
  the response is handled, and assert the exact IllegalState message on the
  topic's future.

- **Resolution**: Fixed in `fixup! admin: route every call through Java's runnable.call gate` (7a2f7ab2). The rejection half of Java's `AdminClientRunnable.call` is now `ShutdownSignal::admit_new_call` (`admin_client_runnable.rs`): the hard-shutdown deadline check plus the bootstrap-controllers check, each written once. `runnable_call` (user, driver and listGroups calls) uses it, and so does the `HandleResult::NewCall` arm, so quota retries pass the same gate. The `enqueue` half always accepts in that arm, because response hooks only run inside the loop, before `admin_rx.close()`. New test `a_quota_retry_during_close_is_rejected_by_the_closing_gate` fails with the raw `pending_calls.push` and passes with the gate. All 7 quota-retry tests stay green.

### Issue 3: The post-panic shutdown test asserts the error messages by prefix, not exactly

- **Severity**: Low (test fidelity; plan DoD "error messages asserted exactly")
- **File**: `src/admin/kafka_admin_client.rs:13796`
  (`a_follow_up_issued_in_the_shutdown_tail_after_a_panic_resolves`)
- **Java Reference**: `KafkaAdminClient.java:1583-1586` (`enqueue`, "The AdminClient
  thread has exited.") and the `finally`'s `handleTimeouts(..., "The AdminClient
  thread has exited.")` (`:1480-1485`).
- **What is wrong**: The test checks both keys with
  `message().starts_with("The AdminClient thread has exited.")`. The two keys
  reach that text by different paths:
  - The in-flight key is failed by `fail_all_remaining`: "... Call:
    fenceProducer(api=INIT_PRODUCER_ID)".
  - The follow-up is rejected by the closed channel: exactly "The AdminClient
    thread has exited.".

  The prefix match cannot tell them apart. For example, it would still pass if the
  follow-up were queued and then failed by a later drain with the "Call: …"
  suffix, which is the very path this test is meant to rule out.
- **Failure scenario**: A regression that re-routes the tail follow-up through
  `pending_calls` and `fail_all_remaining` would still resolve both keys with the
  same prefix, so the test would stay green.
- **Expected fix**: Do what the unresolved-driver-key test does: collect both messages, sort them,
  and `assert_eq!` against `["The AdminClient thread has exited.", "The AdminClient
  thread has exited. Call: fenceProducer(api=INIT_PRODUCER_ID)"]`.

- **Resolution**: Fixed in the same fixup. The post-panic shutdown test now collects both messages, sorts them, and `assert_eq!`s the exact pair: `"The AdminClient thread has exited."` and `"The AdminClient thread has exited. Call: fenceProducer(api=INIT_PRODUCER_ID)"`.

## Review of `f67cd7e8` (Phase 5: `MockAdminClient` Builder)

### Issue 4: `MockAdminClient.create()` is not translated

- **Severity**: Missing Requirement (Medium). The plan requires matching Java's
  Builder **exactly**, and this is its public entry point. The Manager verified
  the gap.
- **File**: `src/admin/mock_admin_client.rs` (`impl MockAdminClient`, around `:443`)
- **Java Reference**: `kafka/clients/src/test/java/org/apache/kafka/clients/admin/MockAdminClient.java:118-120`:
  `public static Builder create() { return new Builder(); }`. It is used by
  `streams/.../StreamThreadStateStoreProviderTest.java:196`
  (`MockAdminClient.create().build()`) and `connect/.../TopicAdminTest.java:858`.
- **What is wrong**: Java offers two ways to get a Builder: `new
  MockAdminClient.Builder()` and `MockAdminClient.create()`. Rust has only
  `Builder::new()`. Code translated from those Java tests, or from anything
  written as `MockAdminClient.create()...build()`, has no counterpart.
  - The FFI rustdoc at `src/ffi/admin.rs:~516` even describes the entry point as
    "exactly as Java's `MockAdminClient.create().numBrokers(n).build()`", naming
    a method the Rust API lacks.
  - The rest of Java's construction surface is covered: `Builder` with all
    setters, `build()`, and the private constructor. The two public constructors
    are excluded by decision.
- **Failure scenario**: A user porting `MockAdminClient.create().numBrokers(3).build()`
  finds no `MockAdminClient::create()`.
- **Expected fix**: Add `pub fn create() -> Builder { Builder::new() }` to
  `impl MockAdminClient`, with a rustdoc citing `:118-120`.
  - It is a static factory, not a constructor, so the name stays `create`
    (CLAUDE.md §2's `new` rule does not apply). It no longer clashes with
    anything, now that `create(num_brokers)` is gone.
  - Add a test that `MockAdminClient::create().build()` equals the
    `Builder::new().build()` defaults.
  - Optionally, have the FFI and backend call sites use it, so they read like the
    Java they cite.

- **Resolution**: Fixed in `fixup! admin: translate MockAdminClient.Builder and Java's DEFAULT_CLUSTER_ID` (f67cd7e8). Added `MockAdminClient::create() -> Builder`, returning `Builder::new()`, with rustdoc citing `MockAdminClient.java:118-120`. `kafka_admin_MockAdminClient_new` now calls `MockAdminClient::create().set_num_brokers(n).and_then(Builder::build)`, so its doc ("as Java's `MockAdminClient.create().numBrokers(n).build()`") describes the code literally. New test `create_returns_a_fresh_builder` checks that every built field equals `Builder::new().build()`.

### Issue 5: `set_brokers` has an invented error path ("Too many brokers: n")

- **Severity**: Low (a divergence with no Java counterpart; it cannot be reached in
  practice)
- **File**: `src/admin/mock_admin_client.rs:250-258` (`Builder::set_brokers`)
- **Java Reference**: `MockAdminClient.java:144-148` (`numBrokers(brokers.size())`),
  and `java.util.Collection.size()`: "If this collection contains more than
  `Integer.MAX_VALUE` elements, returns `Integer.MAX_VALUE`."
- **What is wrong**: `i32::try_from(brokers.len()).map_err(|_|
  Error::local_illegal_argument(format!("Too many brokers: {}", ..)))` adds an
  error with invented text at a point where Java cannot throw. Java's `size()`
  saturates at `Integer.MAX_VALUE`. That matters most to the "same failure
  points" requirement of this phase, where every other error is traced to a JDK
  source line.
- **Failure scenario**: None in practice: it takes more than 2^31 `Node`s. The cost
  is a documented error variant, and an error message, that do not exist in Java.
- **Expected fix**: Translate `size()` as defined:
  `let count = i32::try_from(brokers.len()).unwrap_or(i32::MAX);`. Say so in a
  comment citing the `Collection.size()` contract, and drop the invented message.
  `set_brokers` still returns `Result`, for the real `numBrokers` failure.

- **Resolution**: Fixed in the same fixup. `set_brokers` now uses `i32::try_from(brokers.len()).unwrap_or(i32::MAX)`, like `Collection.size()` saturating at `Integer.MAX_VALUE`, so its only errors are those of `set_num_brokers` (Java's `numBrokers`). The `Result` return type stays. No test asserted the removed message.

## Review of Phase 6 (`describeTopics` by name via `DescribeTopicPartitions`): `f23f233b`, `dad0004a`, `5e0ca485`

### Issue 6: a `describeTopics` by name that hits `UnsupportedVersion` after `close()` has begun never completes

- **Severity**: Medium (Bug: a future that never resolves; CLAUDE.md §5 says this is worse than an explicit error)
- **File**: `src/admin/kafka_admin_client.rs:5350-5360` (the `describeTopicPartitions` `handle_failure`), together with `src/admin/internals/admin_client_runnable.rs:788-791` (`fail_call`'s closing branch)
- **Java Reference**: `KafkaAdminClient.java:904-913` (`Call.fail`: the `runnable.closing` check comes before `handleUnsupportedVersionException`), `:1132` / `:1474` (`closing` is written only in `run()`'s `finally`), `:2311-2323` (the fallback and `handleFailure`), `:1598-1601` (`call()` rejects with IllegalState)
- **What is wrong**: The new `handle_failure` ignores a code-35 error because it assumes `handle_unsupported_version` has already issued the Metadata fallback, which will complete the futures. That only holds if `fail_call` actually calls the hook. In Rust `fail_call` goes straight to `handle_failure` whenever `ShutdownSignal::closing` is set, and `close()` sets that flag at once (`kafka_admin_client.rs:5028`). Java's `runnable.closing` is set only when the I/O thread exits. So during the grace period of `close(timeout)`, Rust skips the fallback, and the one hook left to complete the futures swallows the error. Nothing else holds them: the call has left every queue, so `fail_all_remaining` never sees it. This is the known early stop of retries once `close()` starts, which the plan leaves out of scope. Before this phase it made a call fail early; this phase's code-35 skip turns it into a hang.
- **Failure scenario**: The broker is older than 3.8, so ApiVersions has no `DescribeTopicPartitions`. The user calls `describe_topics_with_topic_names(["t"])`; `describeCluster` answers and `describeTopicPartitions` is queued. The user then calls `close(30s)`. The send produces a version-mismatch response, `fail_call` sees `closing` and calls `handle_failure(UnsupportedVersion)`, which returns without completing anything. The future for `"t"` never resolves. With no active external call left, the loop exits and `close()` returns, but a caller waiting on the future hangs forever.
  - Java in the same case: `closing` is false, so `handleUnsupportedVersionException` calls `runnable.call(metadataCall)`. The hard-shutdown deadline is set, so that call's `handleFailure` fails every topic future with `IllegalStateException("Cannot accept new calls when AdminClient is closing.")`.
  - Reproduced with a scratch unit test on HEAD. I prepared the describeCluster response, pumped until it was handled, then stored `closing = true` and `hard_shutdown_deadline_ms = now + 30_000` (what `close(30s)` stores), prepared `prepare_unsupported_version_response()` and pumped 10 times. Result: the future is not done, the UVE response was consumed, and `has_active_external_calls` is false.
- **Expected fix**: Either option works:
  1. Make the skip depend on the fallback having actually been issued. `handle_uv` sets a flag in `DescribeTopicPartitionsState`, and `handle_failure` ignores the error only when the error is code 35 **and** the flag is set. Otherwise it fails the futures with the error. This keeps the early stop of retries out of scope and never leaves a future pending. Java can only reach `handleFailure(UVE)` after the fallback was issued, because `closing` is only true once the thread has exited and no more responses are handled.
  2. Remove the early stop of retries: `fail_call` checks a flag that mirrors Java's `runnable.closing`, set only by `run()`'s `finally`. The fallback then runs, and the gate fails it with Java's IllegalState. This changes retry behaviour during the grace period for every call, so it needs the Manager's decision.

  Add a regression test like the scratch probe above. It should assert the future resolves; with option 2 it resolves with the exact IllegalState message.

- **Resolution**: Fixed in a fixup of `dad0004a`, per the Manager's decision (the early stop of retries and `fail_call` untouched). `DescribeTopicPartitionsState` now records `metadata_fallback_issued`, set by the one shared `issue_metadata_fallback` closure that `handle_uv` calls. `handle_failure` skips a code-35 error only if that flag is set. Otherwise (the close grace period, where `fail_call` skipped `handle_uv`) it issues the fallback itself through `DriverContext::call` → `runnable_call` → `admit_new_call`, which rejects it, so every topic future fails with "Cannot accept new calls when AdminClient is closing.", as in Java. Outside close nothing changes (the flag is always set before `handle_failure` sees code 35). Regression test `an_unsupported_version_during_close_fails_the_by_name_describe` (uses `close_with_timeout(30s)` and `run_once` via `pump`). It fails with `Elapsed` (the future never completes) when the `!issued` branch is removed.

### Issue 7: the rustdoc of `describe_cluster_with_nodes_handle` is attached to the wrong function

- **Severity**: Low (documentation)
- **File**: `src/admin/kafka_admin_client.rs:454-473` and `:533`
- **Java Reference**: n/a
- **What is wrong**: The paragraph that starts "`describeCluster`, also returning the completable handle behind `DescribeClusterResult.nodes()`" and ends "…so the handle is returned alongside the public result." sits directly on top of the doc of `handle_describe_topics_by_names_with_describe_topic_partitions_api`, with no blank line between them. Rustdoc therefore joins the two into one comment. `describe_cluster_with_nodes_handle` (`:533`) has no doc at all.
- **Failure scenario**: A reader or rustdoc sees `handle_describe_topics_by_names_…` described as "`describeCluster`, also returning the completable handle…", and the DoD #7 justification for the extra handle is missing from the function it justifies.
- **Expected fix**: Move the first paragraph (`:454-462`) onto `describe_cluster_with_nodes_handle`.

- **Resolution**: Fixed in the same fixup. The describeCluster paragraph now sits on `describe_cluster_with_nodes_handle`, and `handle_describe_topics_by_names_with_describe_topic_partitions_api` keeps only its own doc.

