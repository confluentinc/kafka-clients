---
name: review-m8-phase19
description: Phase 19 SSL try_read + Arc<str> selector ids — clean; verification heuristics for sync-non-blocking and Arc<str> intern changes
metadata:
  type: project
---

Phase 19 (commits ed48998, 1e28380) — two CPU optimizations on the consumer
receive/poll hot path. Reviewed CLEAN.

**Why these were safe (the join-stall §10 test that matters):** A sync,
never-`.await`ed function is cancel-safe by construction — it cannot be dropped
mid-flight at an await point, so it cannot strand a node in `Connecting` (the
join-stall root cause). To clear an SSL `try_read`-type change against §10:
verify (1) no `.await` in the body, (2) the only socket op is non-blocking
(`TcpStream::try_read` via `TryReadAdapter`, not a blocking read), (3) the
selector wakeup/Notify/poll machinery is literally untouched by the diff.

**try_read vs async read parity check:** The fast-path sync `try_read` was
verbatim-identical to the existing async `read` body (read_tls →
process_new_packets → reader().read(dst), same tcp_eof/WouldBlock/Ok(0)
branches). When a sync fast-path duplicates an async path, diff the two bodies
line-for-line — divergence in the n==0/tcp_eof/WouldBlock matrix is the bug to
hunt.

**has_bytes_buffered re-poll invariant (SSL):** `has_bytes_buffered()` =
`!conn.wants_read()`. rustls `wants_read()` is false whenever decoded plaintext
remains buffered. So when the Phase-3 drain loop fills `dst` and `break`s with
leftover plaintext, the channel is re-added to `channels_with_buffered_read`
(selector poll_channel_reads post-read) and re-polled. Flipping
`supports_try_read()` to true does NOT touch this; confirm the buffered-read
tracking path is unchanged.

**Arc<str> intern change checklist (Fix 2):** (a) `&str` lookups work via
`Arc<str>: Borrow<str>` — remove/contains_key/get/get_mut sites need `.as_str()`
or `&**id`; (b) `intern_id` must reuse the map Arc (get_key_value + Arc::clone)
and only `Arc::from` on the not-in-map fallback (= where Java allocates a
String) — no NEW per-poll allocation; (c) PartialEq on Arc<str> compares str
contents, so dedup comparisons (`*s == id`) are behavior-identical to the old
String compare; (d) keeping `connected`/`disconnected` as String for the
`Selectable` trait return types is an acceptable documented deviation — they're
cleared/repopulated per poll, not the dominant clone.

The dominant per-poll clone these perf fixes target is
`channels.keys().cloned()` in selector poll Pass-1 (profiled 2.65% malloc/free).
