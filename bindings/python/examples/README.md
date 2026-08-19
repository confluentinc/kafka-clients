# Examples

Command-line producer/consumer examples, adapted from
[confluent-kafka-python's `examples/`](https://github.com/confluentinc/confluent-kafka-python/tree/master/examples)
to this client's bindings. See the top-level [bindings/python/README.md](../README.md)
for how to build the bindings before running these.

Differences from the confluent-kafka-python originals are called out as
comments in each file; the main ones are:

- `send()` takes `on_delivery=callback(metadata, exception)` — Java's
  `Callback` — instead of confluent-kafka-python's `callback(err, msg)`.
  Exactly one argument is meaningful (`metadata` on success, `exception` on
  failure), and the callback fires **exactly once per record**.
- `on_delivery` runs on the producer's completion thread the moment the
  record completes — it is NOT deferred until a `poll()`/`flush()` call as in
  confluent-kafka-python (there is no `poll()` on the producer). Keep it
  short and non-blocking.
- `flush()` does **not** wait for `on_delivery` callbacks — the callbacks are
  dispatched by a separate background thread it doesn't join. `close()` (e.g.
  via a `with` block) *does* drain every outstanding send and run its
  `on_delivery` before returning, so rely on `close()` for the
  "all delivered" guarantee, not `flush()`.
- `RecordMetadata` doesn't carry the delivered payload back the way
  confluent-kafka-python's `Message` does — capture the value by closure in
  the delivery callback if you need to log/use it.
- `poll()` returns a batch (`ConsumerRecords`) and raises `KafkaError` on
  failure, instead of returning a single `Message` whose `.error()` must be
  checked.
- `ConsumerRecord` fields (`topic`, `partition`, `offset`, `key`, `value`)
  are properties, not methods.
- The rebalance listener is a Java-style `ConsumerRebalanceListener` object
  (`on_partitions_assigned` / `on_partitions_revoked`), passed as
  `subscribe(topics, listener=...)` — the analog of confluent-kafka-python's
  `on_assign=`. Its methods run on the Rust dispatcher thread; to call back
  into the consumer from inside one, use `consumer.handle()`.
- Config dict values must be strings (e.g. `'6000'`, not `6000`).
- The consumer requires `group.protocol=consumer` in its config: this client
  implements only the KIP-848 group protocol and rejects the classic protocol
  at construction. (The confluent-kafka-python original does not set this.)
- `stats_cb` and manual offset storage (`store_offsets` /
  `enable.auto.offset.store`) are not available, so the consumer example
  relies on default auto-commit.

## Files

The adapted scripts are named `*_example.py` **on purpose**: this client's
bindings are imported as bare top-level modules `producer` / `consumer`, so an
example file named `producer.py` would shadow the module it imports (Python
puts a script's own directory first on `sys.path`). The `*_example.py` names
avoid that collision so the examples run from any directory.

- `producer_example.py` — reads lines from stdin, sends each as a message.
- `auto_producer_example.py` — generates one message per `1/rate` seconds
  until interrupted (Ctrl-C), instead of reading stdin.
- `consumer_example.py` — subscribes to topics and prints each message
  received.
- [`old/`](old/) — the unmodified original confluent-kafka-python examples
  these were adapted from, kept for side-by-side reference. They target the
  `confluent_kafka` (librdkafka) package and will not run against this
  client.

## Running

```bash
source ../../../venv/bin/activate   # from bindings/python/examples/
python producer_example.py localhost:9092 my-topic
python auto_producer_example.py localhost:9092 my-topic 10   # 10 msgs/sec
python consumer_example.py localhost:9092 my-group my-topic
```

Stop any of them with Ctrl-C.
