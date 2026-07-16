#!/usr/bin/env python3
# Copyright 2025 Confluent Inc.  (Apache-2.0)
"""Consume from 'share-test' and RELEASE each record (instead of ACCEPT) until its
delivery_count reaches a threshold, then ACCEPT it — so you can watch the broker
redeliver the SAME offset with delivery_count climbing 1 -> 2 -> 3.

RELEASE (KIP-932) puts a delivered record back for another attempt; the broker
re-acquires it on a later fetch with delivery_count incremented. ACCEPT finishes
it. This exercises the acknowledge=RELEASE redelivery path and makes the
delivery_count field visibly change (the ACCEPT-only scripts keep it at 1).

Uses its OWN group ('share-test-release-group') on the default 'latest' reset, so
it only sees records produced AFTER it joins. Run it FIRST, then start the
producer:

    # terminal 1
    python manual-test/share_consume_release.py            # then, once it prints "subscribed"
    # terminal 2
    python manual-test/produce.py 1.5

Args: [name] [max_attempts]   (defaults "rel" and 3 — ACCEPT at dc>=3). Ctrl-C to stop.
"""
import sys

from share_consumer import AcknowledgeType, KafkaShareConsumer

BOOTSTRAP = "localhost:9092"
TOPIC = "share-test"
GROUP = "share-test-release-group"  # separate group, default 'latest' reset
POLL_TIMEOUT_S = 1.0


def main() -> None:
    name = sys.argv[1] if len(sys.argv) > 1 else "rel"
    max_attempts = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    consumer = KafkaShareConsumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP,
            "client.id": f"share-test-consumer-{name}",
            "share.acknowledgement.mode": "explicit",
        }
    )
    released = 0
    accepted = 0
    try:
        consumer.subscribe([TOPIC])
        print(
            f"[{name}] subscribed to '{TOPIC}' (group={GROUP}); RELEASE until "
            f"delivery_count>={max_attempts}, then ACCEPT. Start produce.py now. (Ctrl-C to stop)"
        )
        while True:
            batch = list(consumer.poll(POLL_TIMEOUT_S))
            if not batch:
                continue
            for r in batch:
                dc = r.delivery_count
                val = bytes(r.value).decode() if r.value is not None else None
                if dc is not None and dc >= max_attempts:
                    consumer.acknowledge(r, AcknowledgeType.ACCEPT)
                    accepted += 1
                    print(f"[{name}] @{r.offset} value={val!r} dc={dc} -> ACCEPT   (done; accepted {accepted})")
                else:
                    consumer.acknowledge(r, AcknowledgeType.RELEASE)
                    released += 1
                    print(f"[{name}] @{r.offset} value={val!r} dc={dc} -> RELEASE  (redeliver expected; released {released})")
            consumer.commit_sync()
    except KeyboardInterrupt:
        print(f"\n[{name}] stopping (released {released}, accepted {accepted})...")
    finally:
        try:
            consumer.close()
        except KeyboardInterrupt:
            pass  # a second Ctrl-C during close; already stopping


if __name__ == "__main__":
    main()
