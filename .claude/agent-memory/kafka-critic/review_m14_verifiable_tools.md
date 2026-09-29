---
name: review-m14-verifiable-tools
description: M14 verifiable-clients (producer/consumer tools) review — SIGTERM shutdown gap, JVM-shutdown-hook translation traps
metadata:
  type: project
---

Milestone 14 translates Kafka's system-test tools (`VerifiableProducer`,
`VerifiableConsumer`) into `tools/verifiable-clients/` (own workspace crate),
plus `StringDeserializer` into the main crate. JSON stdout is the "wire
contract" (ducktape parses each line to a dict, so field ORDER is not
load-bearing; event `name`s and field names ARE).

**Load-bearing trap: JVM shutdown hook ≠ tokio::signal::ctrl_c.**
- ducktape's clean shutdown of a verifiable client sends **SIGTERM by default**
  (`kafka/tests/kafkatest/services/verifiable_client.py:232`
  `self.conf.get("kill_signal", signal.SIGTERM)`; `SIGKILL` only for unclean).
- Java's `Runtime.addShutdownHook` fires on BOTH SIGINT and SIGTERM.
- `tokio::signal::ctrl_c()` on Unix is **SIGINT only**. Translating the hook as
  a ctrl_c task means the harness's default clean shutdown (SIGTERM) kills the
  process abruptly → no `shutdown_requested`/`shutdown_complete`, no graceful
  close. The harness waits for `shutdown_complete` (`verifiable_client.py:62`),
  so a clean-shutdown test hangs/fails. Fix = also handle
  `SignalKind::terminate()`. This affects BOTH bins (producer Phase 1 + consumer
  Phase 2); PLAN §4 only names ctrl_c and the Phase-1 review missed it. Filed as
  Critic-65 P2-1 (MEDIUM).
- Secondary: Java's hook also runs on NORMAL exit, so Java prints
  `shutdown_requested` even on the max-messages path. Rust ctrl_c task doesn't
  fire on normal exit → omitted (P2-2, LOW).

**Phase 2 was otherwise clean and faithful.** Verified: `records_consumed.count`
uses FULL `records.count()` not truncated size (Java VC.java:176); `subList`
truncation + `maxOffset+1` commit + cross-partition early-break correct;
`commit_sync` wakeup→recurse-once→rethrow preserved via `Box::pin(recurse).await?`
then `Err(wakeup)`; EventReporter split (stateless unit struct as
`Arc<dyn ConsumerRebalanceListener>+OffsetCommitCallback>`) is §31-faithful
(callbacks on caller task, no spawn); `poll(i64::MAX ms)` safe because client
`calculate_deadline_ms` saturating_adds; StringDeserializer `from_utf8_lossy` ==
Java `new String(bytes,UTF_8)` (both U+FFFD on malformed); dropped configurable
`*.deserializer.encoding` acceptable (UTF-8-only system tests).

**Test-gap heuristic that recurred:** offset-math tests asserted state
(`consumed_messages`, committed offsets) but never the emitted JSON line, so a
full-vs-truncated `count` regression would pass. Push for a test that captures
the actual event JSON, not just internal counters (DoD §12).
