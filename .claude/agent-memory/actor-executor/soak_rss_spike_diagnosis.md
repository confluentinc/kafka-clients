---
name: soak-rss-spike-diagnosis
description: Why the python-soak-ht RSS spikes on broker rolls — C-extension BatchNode overhead, not the Rust client; plus the repro method that works
metadata:
  type: project
---

Diagnosed 2026-08-14 (branch `dev/python-soak-client`). Full writeup in
`COMMENTS.0.md` at repo root. Non-obvious things worth keeping:

**The cause is `bindings/python/_confluentkafka.c`, not the Rust client.**
`BatchNode` is a fixed 44,016-byte struct (five parallel `[1100]` pointer
arrays) allocated per *drain cycle*. The send task drains every 10 ms, so at
80 msg/s that is ~1 node per record — 43 KiB of bookkeeping per outstanding
record regardless of payload size. `Producer_poll_futures_thread` drains the
pending list strictly in order and frees a node only after all its futures
resolve, so one record retrying to a dead broker pins every later node.

**The 1000-record backpressure bound is ineffective and this is easy to
misread.** `PRODUCER_MAX_ACCUMULATED_RECORDS` gates `accumulated_records`,
which the send task resets to 0 on every drain. It bounds the pre-drain
staging queue only — never outstanding records. This is why `producer.outq`
can read 6536 against a "1000" bound; the two counters measure different
things.

**`tracemalloc` is NOT a Python-vs-native discriminator in these bindings.**
The C extension allocates with `PyMem_RawMalloc`, a traced domain, so
extension memory shows up in `tracemalloc` too. Use
`take_snapshot().statistics('lineno')` and read the bytes-per-block instead —
44,032 B/block is the `BatchNode` fingerprint, attributed to
`producer.py:258` (the `_lib.Producer_send` call site).

**Repro method — `docker stop` reproduces nothing.** A graceful broker
shutdown moves leadership before the socket closes; `outq` never exceeds 2 and
RSS stays flat. You need `docker pause` (frozen broker, connection stays open)
so delivery reports actually stall. Pausing 2 of 3 brokers gives a full stall.

**Separating retention from a leak:** after drain, `tracemalloc` returns to
baseline while RSS does not; `libc.malloc_trim(0)` via ctypes then releases
most of it (404 -> 103 MiB). glibc holds freed 43 KiB chunks because they are
below the 128 KiB `M_MMAP_THRESHOLD`. Trim never fully returns to baseline —
that residue is the ratcheting baseline seen in Grafana.

**Ruled out with experiments, don't re-litigate without new evidence:** the
HI fetch tuning (`fetch.max.bytes=50MB`), the Rust fetch path, and the Rust
`BufferPool`. A consumer-only process with HI tuning draining a 170 MB backlog
through a broker outage peaked at +36 MiB and stayed flat.

**Container recipe:** `ckr-soak:dev` = `ckr-pytest:dev` + the soak
`requirements.txt` (psutil, confluent-kafka). Bind-mount the host repo at `/w`
with a named volume for `/w/target` seeded from the image's build cache, and
set `LD_LIBRARY_PATH=/w/target/release` for anything importing
`bindings/python/producer.py` directly. The soak imports as bare `producer` /
`consumer` modules, not a `confluent_kafka_rust` package.

Related: [[python_soak_client_notes]].
