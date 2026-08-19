# Original confluent-kafka-python examples

These are the **unmodified** upstream examples from
[confluent-kafka-python's `examples/`](https://github.com/confluentinc/confluent-kafka-python/tree/master/examples),
kept here purely as a reference so the adaptations in the parent
[examples/](../) directory can be compared against their originals.

They target the `confluent_kafka` package (librdkafka), **not** this client's
bindings, and will not run against it — the delivery-callback shape,
`poll()`/`flush()` semantics, `Message` vs `ConsumerRecord`, and config
handling all differ. See the parent [README.md](../README.md) for the exact
differences and the adapted, runnable versions.

| File | Adapted version |
|---|---|
| `producer.py` | [../producer_example.py](../producer_example.py) |
| `auto_producer.py` | [../auto_producer_example.py](../auto_producer_example.py) |
| `consumer.py` | [../consumer_example.py](../consumer_example.py) |
