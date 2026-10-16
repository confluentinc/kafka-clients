# Examples

| Example | What it does |
|---|---|
| `producer.py` | Reads lines from stdin and sends each one to a topic. The delivery callback prints where each message landed, with its value. |
| `producer_context_manager.py` | The same, in a `with` block: leaving the block flushes the producer, then closes it. |
| `consumer.py` | Subscribes a consumer (KIP-848 group protocol), prints every message it reads, and prints every change to its assignment. |
| `consumer_context_manager.py` | The same, in a `with` block: leaving the block closes the consumer. |
| `async_producer.py` | `producer_context_manager.py` on asyncio: `AsyncKafkaProducer` in an `async with` block. |
| `async_consumer.py` | `consumer_context_manager.py` on asyncio: `AsyncKafkaConsumer` in an `async with` block, with a listener whose methods are coroutines. |
| `admin.py` | Creates a topic with `Admin`, reads each topic's `KafkaFuture` with `get()` (a failure arrives as `ExecutionError`, the error being its `__cause__`), and lists the topics. |
| `async_admin.py` | The same on asyncio: `AsyncAdmin` in an `async with` block; a call returns its result at once and each `AsyncKafkaFuture` is awaited through `get()`. |

## Running them

Build the client into the repository's venv, then start a broker:

```bash
make devel-build-python        # from the repository root
docker run -d --rm --name kafka -p 9092:9092 apache/kafka:4.3.1
```

```bash
cd python/examples
../../venv/bin/python consumer.py localhost:9092 my-group demo      # Ctrl+C to stop
echo "hello" | ../../venv/bin/python producer.py localhost:9092 demo
```

`admin.py` and `async_admin.py` take `<bootstrap-brokers> <topic>`, as `producer.py` does.

The other producers take the same arguments as `producer.py`, and the other consumers the same as
`consumer.py`.

Start a second consumer in the same group to see a rebalance: each consumer prints the partitions
it is assigned and the ones revoked from it.

## Notes

- **Delivery callback.** The client calls it as `callback(metadata, exception)`, on the producer's
  background completion thread (on the event loop for `AsyncKafkaProducer`). `RecordMetadata` carries
  no key or value, so the producers bind each record to the callback with `functools.partial` and print
  its value from there.
- **Send future.** `send()` returns a `KafkaFuture` of the delivery: `p.send(record=…).get()` gives the
  `RecordMetadata`. On `AsyncKafkaProducer`, awaiting `send()` waits for buffer space and returns an
  `AsyncKafkaFuture`, so `await (await p.send(record=…)).get()` gives it.
- **Rebalance listener.** Its methods run on the thread calling `poll()`, and the rebalance waits for
  them to return. On `AsyncKafkaConsumer` they may be `async def`: the consumer awaits them on the
  event loop. With the KIP-848 protocol, `on_partitions_assigned` gets only the partitions added in
  that step, which can be none; the examples also print the whole assignment after each step.
- **Ctrl+C.** `asyncio.run` cancels the async examples' task; the cancelled `poll()` ends, and leaving
  the `async with` block closes the client.
- **Offsets.** With auto-commit (the default), the consumer commits the offsets of the records
  `poll()` returned every `auto.commit.interval.ms` (5 seconds), before a rebalance takes partitions
  away, and on `close()`.
