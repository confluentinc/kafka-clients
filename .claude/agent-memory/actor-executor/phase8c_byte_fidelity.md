---
name: phase8c_byte_fidelity
description: kafka-console-consumer harness patterns + clippy doc-comment attachment gotcha surfaced during Phase 8c byte-fidelity work
metadata:
  type: project
---

Phase 8c landed end-to-end byte fidelity for the producer via
`docker exec kafka-console-consumer.sh` invoked from
`tests/integration/producer_smoke_test.rs::consume_records`. Pure ASCII
fixtures (`k{i:04}` / `v{i:04}`) — the default string deserializer's
lossy UTF-8 decode is identity on those bytes.

**Why:** CLAUDE.md §12 demands zero-copy from `ProducerRecord` to the
wire. Production-side audits (Phases 6b/6d/7g) checked the path but
the broker-boundary audit was missing. This phase closed that gap
without translating a Rust consumer.

**How to apply:**
1. `kafka-console-consumer` defaults to deprecated `--property` —
   prefer `--formatter-property` to suppress deprecation warnings
   from polluting stdout (the parser sees stdout).
2. `DefaultMessageFormatter` joins printed fields with the configured
   `key.separator`, NOT tab. Line shape with `print.partition=true` +
   `print.key=true` + `key.separator=\x1F` is
   `Partition:<n>\x1F<key>\x1F<value>\n`. Three-field splitn, not
   `split_once('\t')`.
3. Pick `\x1F` (ASCII unit separator) as the separator — cannot
   appear in printable ASCII payloads.
4. `kafka-console-consumer` exits status=1 on `--timeout-ms` even
   after a successful read of all `--max-messages` — do NOT assert
   `status.success()`. Assert parsed count == max_messages instead.
5. Sync helper (`Command::output` blocks); wrap in
   `tokio::task::spawn_blocking` from async tests so the runtime
   doesn't stall up to `timeout_ms`.

**Clippy doc-attachment trap surfaced by Nit 1 fix:** A `cfg`-gated
`type` alias positioned between a struct's rustdoc and the struct
declaration causes clippy's `empty_line_after_doc_comments` to
attach the struct's rustdoc to the type alias — silently truncating
the struct's rendered docs. Fix: move the type alias below the
struct (after its closing brace) with its own `///` rustdoc and a
`// ----` separator banner. Don't try to fix it with non-doc
comments BETWEEN the struct rustdoc and the next item; clippy
considers any item between the doc block and its closing newline as
the documented item.

Integration-test suite (5 tests) wall-clock against localhost Docker:
**14.83 s** end-to-end. Useful budget data for the upcoming 8f
3-consecutive-run flakiness gate (each run < 30 s → 3 runs < 1.5 min).
