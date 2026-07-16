#!/usr/bin/env python3
# Copyright 2025 Confluent Inc.  (Apache-2.0)
"""Continuously consume from 'share-test' via the Rust-backed Python share
consumer until Ctrl-C: subscribe -> poll -> per-record acknowledge -> commit,
forever.

Uses EXPLICIT acknowledgement mode so the acknowledge() calls are exercised (the
default 'implicit' mode auto-accepts on poll/commit and rejects explicit
acknowledge()).

Run (from the venv built per manual-test/README.md):
    python manual-test/share_consume.py [name]      # default "c1"

Run SEVERAL at once with different names (in separate terminals) to watch a share
group cooperatively split records across consumers:
    python manual-test/share_consume.py c1
    python manual-test/share_consume.py c2

Group 'share-test-group' has share.auto.offset.reset=earliest set broker-side
(see README). Ctrl-C to stop.
"""
import sys

from share_consumer import AcknowledgeType, KafkaShareConsumer

BOOTSTRAP = "localhost:9092"
TOPIC = "share-test"
GROUP = "share-test-group"  # the group we set share.auto.offset.reset=earliest on
POLL_TIMEOUT_S = 1.0


def main() -> None:
    name = sys.argv[1] if len(sys.argv) > 1 else "c1"
    consumer = KafkaShareConsumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP,
            "client.id": f"share-test-consumer-{name}",
            "share.acknowledgement.mode": "explicit",
        }
    )
    total = 0
    try:
        consumer.subscribe([TOPIC])
        print(f"[{name}] subscribed to '{TOPIC}' (group={GROUP}); consuming (Ctrl-C to stop)...")
        while True:
            batch = list(consumer.poll(POLL_TIMEOUT_S))
            if not batch:
                continue
            for r in batch:
                total += 1
                key = bytes(r.key).decode() if r.key is not None else None
                val = bytes(r.value).decode() if r.value is not None else None
                print(
                    f"[{name}] recv {r.topic}[{r.partition}]@{r.offset} "
                    f"key={key!r} value={val!r} dc={r.delivery_count}"
                )
                # EXPLICIT mode: acknowledge each delivered record.
                consumer.acknowledge(r, AcknowledgeType.ACCEPT)
            consumer.commit_sync()
            print(f"[{name}] committed {len(batch)} ack(s) (total {total})")
    except KeyboardInterrupt:
        print(f"\n[{name}] stopping (consumed {total})...")
    finally:
        try:
            consumer.close()
        except KeyboardInterrupt:
            pass  # a second Ctrl-C during close; already stopping


if __name__ == "__main__":
    main()
