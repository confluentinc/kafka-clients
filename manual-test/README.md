# Manual test: share consumer against a real Kafka 4.2 broker

Drives the KIP-932 **share consumer** end-to-end against a live broker using the
current changes — the Python bindings (`_confluentkafka.c` + `share_consumer.py`),
which sit on the C FFI, which sits on the Rust client:

```
produce.py ─┐                 share_consume.py ─┐
            │ producer.py                       │ share_consumer.py   (Python wrapper)
            └─► _confluentkafka.c  ◄─────────────┘                    (CPython C-extension)
                     │  confluent_kafka.h (cbindgen)
                     └─► src/ffi/  ─► src/producer / src/consumer      (Rust)
                                     └─► apache/kafka:4.2.0 (Docker)
```

## Prerequisites

- Docker.
- The Rust cdylib built: `cargo build --features ffi --release`
  (produces `target/release/libconfluent_kafka.dylib` + the header).
- Python 3.

## 1. Start the broker (Kafka 4.2, KIP-932 share groups enabled)

```sh
cd manual-test
docker compose up -d
docker compose logs -f kafka   # wait for "Kafka Server started", then Ctrl-C
```

## 2. One-time broker setup (topic + earliest delivery for the demo group)

Share groups default to `share.auto.offset.reset=latest`, so records produced
before the consumer joins are NOT delivered. There is no client-side API to
change this, so set it broker-side for the demo group (KIP-932 group config):

```sh
K=kafka-share-test; BIN=/opt/kafka/bin; BS=localhost:9092
docker exec $K $BIN/kafka-topics.sh   --bootstrap-server $BS --create --topic share-test --partitions 1 --replication-factor 1
docker exec $K $BIN/kafka-configs.sh  --bootstrap-server $BS --entity-type groups --entity-name share-test-group --alter --add-config share.auto.offset.reset=earliest
```

(Alternatively, skip the `earliest` step and just run the consumer FIRST, then the
producer — `latest` then delivers records produced after the consumer joined.)

## 3. Build the Python extension

macOS note: the Xcode CLT SDK ships no C11 `<threads.h>` (the pre-existing
producer half of `_confluentkafka.c` needs it), so pass a shim on the include
path. On Linux this shim is unnecessary.

```sh
cd ..                                   # repo root
python3 -m venv manual-test/.venv
manual-test/.venv/bin/python -m pip install -U pip setuptools wheel

# macOS: create a tiny C11-threads-over-pthreads shim dir with threads.h, then:
SHIM=/path/to/shim                      # dir containing threads.h (macOS only)
( cd bindings/python
  CONFLUENT_KAFKA_LIB_DIR="$PWD/../../target/release" CFLAGS="-I$SHIM" \
    ../../manual-test/.venv/bin/python -m pip install -e . )
# Linux: drop the CFLAGS shim entirely.

manual-test/.venv/bin/python -c "import _confluentkafka, share_consumer, producer; print('ok')"
```

## 4. Run (continuous — Ctrl-C to stop)

Both scripts run until interrupted: the producer streams records on an interval;
the share consumer polls → acknowledges → commits in a loop.

```sh
PY=manual-test/.venv/bin/python

# terminal 1 — produce a record every 0.5s (arg = interval seconds, default 1.0)
$PY manual-test/produce.py 0.5

# terminal 2 — consume continuously (arg = instance name, default "c1")
$PY manual-test/share_consume.py c1

# optional terminal 3 — a second consumer in the SAME share group; KIP-932 splits
# records cooperatively across c1 and c2
$PY manual-test/share_consume.py c2

# OR: watch the acknowledgement-commit callback fire — commits async and prints
# each `>> ACK-COMMIT CALLBACK` line with the committed offsets
$PY manual-test/share_consume_callback.py cb

# OR: watch delivery_count climb — RELEASE each record until dc>=3, then ACCEPT,
# so the broker redelivers the SAME offset as dc=1 -> 2 -> 3. Uses its own
# 'latest' group, so start THIS first, then the producer:
$PY manual-test/share_consume_release.py       # terminal A (start first)
$PY manual-test/produce.py 1.5                 # terminal B
```

Each consumer prints `recv ...@<offset> key=... value=... dc=<delivery_count>`
then `committed N ack(s)`. `kafka-share-groups.sh --describe --group
share-test-group` shows the group's lag. `share_consume.py` uses **explicit**
acknowledgement mode so `acknowledge()` is exercised (the default `implicit`
auto-accepts on poll/commit and rejects explicit `acknowledge()`).

## Teardown

```sh
cd manual-test && docker compose down -v
```
