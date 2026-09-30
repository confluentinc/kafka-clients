# Examples

| Example | What it does |
|---|---|
| `producer.py` | Reads lines from stdin and sends each one to a topic. The delivery callback prints where each message landed, with its value. |
| `producer_context_manager.py` | The same, in a `with` block: leaving the block flushes the producer, then closes it. |
| `consumer.py` | Subscribes a consumer (KIP-848 group protocol), prints every message it reads, and prints every change to its assignment. |
| `consumer_context_manager.py` | The same, in a `with` block: leaving the block closes the consumer. |
| `async_producer.py` | `producer_context_manager.py` on asyncio: `AsyncKafkaProducer` in an `async with` block. |
| `async_consumer.py` | `consumer_context_manager.py` on asyncio: `AsyncKafkaConsumer` in an `async with` block, with a listener whose methods are coroutines. |

## Running them

Build the client into the repository's venv, then start a broker:

```bash
make devel-build-python        # from the repository root
docker run -d --rm --name kafka -p 9092:9092 apache/kafka:4.3.1
```

```bash
cd bindings/python/examples
../../../venv/bin/python consumer.py localhost:9092 my-group demo      # Ctrl+C to stop
echo "hello" | ../../../venv/bin/python producer.py localhost:9092 demo
```

The other producers take the same arguments as `producer.py`, and the other consumers the same as
`consumer.py`.

Start a second consumer in the same group to see a rebalance: each consumer prints the partitions
it is assigned and the ones revoked from it.

## Notes

- **Delivery callback.** The client calls it as `callback(metadata, exception)`, on the producer's
  background completion thread (on the event loop for `AsyncKafkaProducer`). `RecordMetadata` carries
  no key or value, so the producers bind each record to the callback with `functools.partial` and print
  its value from there.
- **Async send.** Awaiting `send()` waits for buffer space and returns the future of the delivery, so
  `await (await p.send(record=…))` gives the `RecordMetadata`.
- **Rebalance listener.** Its methods run on the thread calling `poll()`, and the rebalance waits for
  them to return. On `AsyncKafkaConsumer` they may be `async def`: the consumer awaits them on the
  event loop. With the KIP-848 protocol, `on_partitions_assigned` gets only the partitions added in
  that step, which can be none; the examples also print the whole assignment after each step.
- **Ctrl+C.** `asyncio.run` cancels the async examples' task; the cancelled `poll()` ends, and leaving
  the `async with` block closes the client.
- **Offsets.** With auto-commit (the default), the consumer commits the offsets of the records
  `poll()` returned every `auto.commit.interval.ms` (5 seconds), before a rebalance takes partitions
  away, and on `close()`.
