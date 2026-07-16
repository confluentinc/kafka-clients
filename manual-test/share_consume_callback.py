#!/usr/bin/env python3
# Copyright 2025 Confluent Inc.  (Apache-2.0)
"""Continuously consume from 'share-test', acknowledging each record and
committing ASYNCHRONOUSLY, with a registered acknowledgement-commit callback so
you can watch the callbacks fire.

Unlike share_consume.py (which uses commit_sync and gets the result inline), this
uses commit_async(): the commit completes in the background and the ONLY way to
observe it is the registered callback. The callback fires on the caller's own
thread while a later poll() drains the completed-ack events (KIP-932 / §31), so
you'll see the `>> ACK-COMMIT CALLBACK` lines interleaved just before the next
batch.

Run (from the venv built per manual-test/README.md), with produce.py running:
    python manual-test/share_consume_callback.py [name]     # default "cb"

Ctrl-C to stop.
"""
import sys

from share_consumer import AcknowledgeType, KafkaShareConsumer

BOOTSTRAP = "localhost:9092"
TOPIC = "share-test"
GROUP = "share-test-group"
POLL_TIMEOUT_S = 1.0


def main() -> None:
    name = sys.argv[1] if len(sys.argv) > 1 else "cb"
    committed_total = 0  # running count, updated inside the callback

    def on_ack_commit(offsets, error):
        """offsets: dict[TopicIdPartition, set[int]]; error: KafkaError | None.

        Runs on THIS thread during a poll()/commit() drain — safe to touch
        Python state and print."""
        nonlocal committed_total
        if error is not None:
            print(f"[{name}] >> ACK-COMMIT CALLBACK error: {error}")
            return
        for tip, offs in offsets.items():
            committed_total += len(offs)
            print(
                f"[{name}] >> ACK-COMMIT CALLBACK: {tip.topic}[{tip.partition}] "
                f"committed offsets {sorted(offs)} (callback total {committed_total})"
            )

    consumer = KafkaShareConsumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP,
            "client.id": f"share-test-consumer-{name}",
            "share.acknowledgement.mode": "explicit",
        }
    )
    try:
        consumer.set_acknowledgement_commit_callback(on_ack_commit)
        consumer.subscribe([TOPIC])
        print(f"[{name}] subscribed; ack-commit callback registered; consuming (Ctrl-C to stop)...")
        while True:
            batch = list(consumer.poll(POLL_TIMEOUT_S))
            if not batch:
                continue
            for r in batch:
                val = bytes(r.value).decode() if r.value is not None else None
                print(f"[{name}] recv {r.topic}[{r.partition}]@{r.offset} value={val!r} dc={r.delivery_count}")
                consumer.acknowledge(r, AcknowledgeType.ACCEPT)
            # ASYNC commit: no inline result — completion arrives via the
            # registered callback (the `>>` lines), on a later poll drain.
            consumer.commit_async()
            print(f"[{name}] commit_async() submitted for {len(batch)} record(s) — awaiting callback...")
    except KeyboardInterrupt:
        print(f"\n[{name}] stopping (callback committed {committed_total} offset(s) total)...")
    finally:
        try:
            # close() also drains any final pending ack-commit callback.
            consumer.close()
        except KeyboardInterrupt:
            pass  # a second Ctrl-C during close; already stopping


if __name__ == "__main__":
    main()
