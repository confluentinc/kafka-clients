# Translation Design: KAFKA-19831 — Improved error handling in DefaultStateUpdater

**AK commit:** `c48c50d3e806f2d0dedbff7570ed11687799dd41`
**AK branch:** trunk
**PR:** #39
**Rust branch:** `kafka-translate/c48c50d3e806f2d0dedbff7570ed11687799dd41`

---

## 1. Summary of the Apache Kafka commit

The commit fixes two related reliability issues in **Kafka Streams** internals:

### 1a. `DefaultStateUpdater` — handle `maybeCheckpoint` failures

`DefaultStateUpdater.StateUpdaterThread` checkpoints tasks in several paths:

| Call site | Method | Previous behaviour |
|---|---|---|
| `prepareUpdatingTaskForRemoval` | `task.maybeCheckpoint(true)` | Unguarded — any `RuntimeException` propagated out and killed the thread |
| `pauseTask` | `task.maybeCheckpoint(true)` | Only `StreamsException` was caught; other runtime exceptions were unguarded |
| `maybeCompleteRestoration` | `task.maybeCheckpoint(true)` | Only `StreamsException` was caught |
| `removeCheckpointForCorruptedTask` | `task.maybeCheckpoint(true)` | Already guarded with a `StreamsException` swallow |
| `maybeCheckpointTasks` | `task.maybeCheckpoint(false)` | Already guarded per-task with `StreamsException` |

The fix ensures that each call to `maybeCheckpoint` is surrounded by a `StreamsException` catch (and, where appropriate, a broader `RuntimeException` catch) so that a failure in a single task's checkpoint does not crash the updater thread and does not silently drop the task.

### 1b. `TaskManager#shutdownStateUpdater` — avoid indefinite hang when thread is dead

`shutdownStateUpdater` calls `stateUpdater.shutdown(Duration.ofMinutes(1L))` after draining tasks. `DefaultStateUpdater#shutdown` in turn calls `stateUpdaterThread.join(timeout)`. If the updater thread has already terminated unexpectedly (e.g., due to an unhandled exception), the join completes quickly, but the code was structured in a way that could block waiting for futures that would never complete because the dead thread never processes the remove actions.

The fix adds a guard: if the `StateUpdaterThread` is not alive at shutdown time, the pending `CompletableFuture`s are completed exceptionally (or the remove step is skipped), so `shutdownStateUpdater` returns promptly rather than hanging for minutes.

---

## 2. Files changed upstream

```
streams/src/main/java/org/apache/kafka/streams/processor/internals/DefaultStateUpdater.java   (+38 / -20)
streams/src/main/java/org/apache/kafka/streams/processor/internals/TaskManager.java            (+20 / -4)
streams/src/test/java/org/apache/kafka/streams/processor/internals/DefaultStateUpdaterTest.java
streams/src/test/java/org/apache/kafka/streams/processor/internals/TaskManagerTest.java
streams/src/test/java/org/apache/kafka/streams/StateUpdaterFailureIntegrationTest.java  (new)
```

---

## 3. Scope analysis for the Rust client

The Rust repository is a translation of the **Apache Kafka client** library
(`clients/` in the upstream tree). The two files changed by this commit belong
to **Kafka Streams** (`streams/processor/internals`), which is a separate
application-level stream processing framework layered on top of the client.

The current Rust codebase has no Streams layer:
- `src/` contains only client modules (network, protocol, producer, common, …).
- No `streams/`, `processor/`, or `state_updater` module exists.
- The milestones defined in `design/history/MILESTONES.md` cover the client
  through performance optimisation and the translation agent itself; Streams is
  not in scope.

**Conclusion: no translation work is required for this commit.**

---

## 4. Decision

| Item | Decision |
|---|---|
| `DefaultStateUpdater` (Streams) | **Skip** — not in scope (no Streams layer in Rust repo) |
| `TaskManager` (Streams) | **Skip** — not in scope |
| Tests | **Skip** — no corresponding Rust modules |

This PR can be merged as-is (containing only the kafka submodule pointer bump
commit). No Actor work is needed.

---

## 5. Future consideration

When/if the Rust project expands to include a Kafka Streams equivalent, the
following patterns from this commit should be kept in mind:

- Every call to `maybe_checkpoint` inside the state-updater task should be
  wrapped in error handling that moves the failing task to the failed-tasks
  queue rather than propagating the error to the updater task itself.
- The `shutdown_state_updater` equivalent should check whether the updater
  task is still alive before waiting on futures tied to task removal, to
  avoid hanging indefinitely if the task has already died.
