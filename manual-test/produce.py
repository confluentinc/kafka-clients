#!/usr/bin/env python3
# Copyright 2025 Confluent Inc.  (Apache-2.0)
"""Continuously produce records to 'share-test' until Ctrl-C, using the
Rust-backed Python producer.

Run (from the venv built per manual-test/README.md):
    python manual-test/produce.py [interval_seconds]    # default 1.0

Emits key-N / value-N on an interval forever, waiting for each broker ack.
Ctrl-C to stop (flushes + closes cleanly).
"""
import itertools
import sys
import time

from _confluentkafka import ProducerRecord
from producer import KafkaProducer

BOOTSTRAP = "localhost:9092"
TOPIC = "share-test"


def main() -> None:
    interval = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    producer = KafkaProducer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "client.id": "share-test-producer",
            "acks": "all",
        }
    )
    print(f"producing to '{TOPIC}' every {interval}s (Ctrl-C to stop)...")
    sent = 0
    try:
        for i in itertools.count():
            record = ProducerRecord(TOPIC, f"value-{i}".encode(), f"key-{i}".encode())
            md = producer.send(record).result(timeout=30)
            sent += 1
            print(f"produced #{i} -> {TOPIC}[{md.partition()}]@{md.offset()}")
            time.sleep(interval)
    except KeyboardInterrupt:
        print(f"\nstopping producer (sent {sent})...")
    finally:
        try:
            producer.flush()
            producer.close()
        except KeyboardInterrupt:
            pass  # already stopping; ignore Ctrl-C during shutdown


if __name__ == "__main__":
    main()
