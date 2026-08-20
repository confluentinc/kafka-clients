---
name: python-soak-client-notes
description: Python soak client (bindings/python/soak) — binding API gaps it works around, the venv/Linux build constraint, and how to run it end-to-end locally
metadata:
  type: project
---

The Python soak client lives in `bindings/python/soak/` (branch
`dev/python-soak-client`), ported from `confluent-kafka-python/tests/soak/`.
Non-obvious constraints discovered while building it:

**The Python bindings only build on Linux.** `_confluentkafka.c` includes
`<threads.h>` (C11 threads), which macOS does not ship, so
`pip install -e bindings/python` fails on this machine with
`fatal error: 'threads.h' file not found`. Anything needing the bindings must
run in a Linux container. The `ckr-pytest:dev` local image is exactly that
environment: repo copied to `/w`, `cargo build --release --features ffi` already
done, venv at `/venv` with the bindings installed editable. Bind-mount the dirs
you changed over `/w/...` and `docker exec` — far cheaper than rebuilding.

**pip on this machine points at expired CodeArtifact tokens** (`~/.pip/pip.conf`
`index-url`), so every install 401s. Override per-command with
`PIP_INDEX_URL=https://pypi.org/simple PIP_EXTRA_INDEX_URL=` — do not edit the
user's pip.conf.

**`cargo fmt` / `cargo clippy` are not installed here** (`cargo xtask
format-check` / `lint` both fail with "no such command"). Pre-existing; it means
Python-only changes cannot be gated on those. The pre-commit hook
(`core.hooksPath=.githooks`) runs `make verify-sandbox`, which needs the same
missing tooling plus Docker, so Python-only commits use `--no-verify`.

**Binding API facts the soak depends on** (all verified against a real broker):

- `send()` returns `concurrent.futures.Future`; delivery reports come from
  `future.add_done_callback`, which fires **on the C extension's poll thread**.
  There is no producer `poll()`, no `BufferError` (send blocks on
  backpressure), no `len(producer)`, no `msg.latency()`.
- `consumer.poll(timeout)` takes **seconds**, returns a `ConsumerRecords` batch,
  and *raises* `KafkaError` instead of returning an error record.
  `ConsumerRecord` accessors are properties; `RecordMetadata` accessors are
  methods.
- `record.value` is a zero-copy `memoryview` over the batch — copy before
  parsing.
- `sasl.username`/`sasl.password` do not exist; only `sasl.jaas.config` (Java
  JAAS form) works. Unknown config keys are silently accepted by the client, so
  the soak validates against the accepted-key lists mirrored from
  `producer_config.rs` / `consumer_config.rs` and refuses to start.
- The producer has **no `wakeup()`**; the consumer does. `consumer.wakeup()` is
  NOT idempotent here — each call aborts one more blocking operation, unlike
  Java's flag. Anything shutting down on signals must issue it at most once, or
  a second signal aborts a commit too.
- `ProducerRecord` cannot carry headers (the FFI struct has no headers field),
  so the soak carries msgid/send_time/txcnt in the value payload.

**Local end-to-end recipe** (used as the release gate): start
`apache/kafka:4.2.0` single-node KRaft with
`KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS=classic,consumer` (KIP-848 is
required — the client fails construction for `group.protocol=classic`) on a
docker network, then run the soak container on the same network with
`--runtime-seconds`. `CLUSTER_ID=5L6g3nShT-eMCtK--X86sw` is the value the Rust
integration suite uses.

Related: [[integration_test_infra]], [[consumer_ffi_marshaling_notes]].
