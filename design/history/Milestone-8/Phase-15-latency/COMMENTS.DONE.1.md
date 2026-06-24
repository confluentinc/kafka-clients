# Critic review — consumer steady-state latency fixes (RESOLVED)

Commits reviewed: `0a37f87`, `a038aeb`. Reference:
`design/current/consumer-latency-findings.md`. The critic confirmed both
commits faithful and sound (wakeup race, §31 rebalance-drain bound, no-spin
guard, deadline honoring all correct). One behavior deviation was reported
(Issue 1) and is now fixed.

## Issue 1 (RESOLVED): `pollForFetches` dropped Java's `retryBackoffMs` clamp

Java's `pollForFetches` (`AsyncKafkaConsumer.java:1888-1904`) reduces the
pre-`awaitWakeup` poll timeout to `retry.backoff.ms` (100 ms) when there are no
assigned partitions or any assigned partition lacks a valid position, so the
consumer does not park for the full `maximum_time_to_wait` while positions are
being looked up (offset reset / committed fetch, possibly backing off). The Rust
`poll_for_fetches` omitted it; because `OffsetsRequestManager` does not shrink
`maximum_time_to_wait`, the `await_wakeup` block could park up to
`MAX_POLL_TIMEOUT_MS` (5 s) during the join / post-rebalance window. Not a
correctness bug (data still arrives via the fetch-response wakeup once positions
are valid), but a faithful-translation gap that lengthens worst-case poll latency
in exactly that window — overlapping the known initial-join latency area.

**Resolution (fixup of `a038aeb`):** ported the Java clamp into
`poll_for_fetches`. After computing `poll_timeout_ms = min(maximum_time_to_wait_ms,
remaining)`, if `poll_timeout_ms > retry_backoff_ms`, lock `SubscriptionState`,
take `assigned_partitions()`, and if it is empty OR any partition has
`!has_valid_position(tp)`, clamp `poll_timeout_ms = retry_backoff_ms`. Uses the
already-cached `retry_backoff_ms` field (documented as "used by
`poll_for_fetches`"). No `.await` is held across the guard (§16). Line-for-line
mirror of the Java reduction. Full lib suite 1711 passed; clippy clean;
steady-state latency unchanged (the clamp only fires while positions are invalid,
which does not occur in steady state).
