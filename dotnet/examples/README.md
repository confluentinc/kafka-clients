# Examples

| Example | What it does |
|---|---|
| `Producer` | Reads lines from stdin and sends each one to a topic. The delivery callback prints where each message landed, with its value. |
| `ProducerUsing` | The same, in a `using` block: leaving the block closes the producer, which first waits for every delivery. |
| `Consumer` | Subscribes a consumer (KIP-848 group protocol), prints every message it reads, and prints every change to its assignment. |
| `ConsumerUsing` | The same, in a `using` block: leaving the block closes the consumer. |
| `AsyncProducer` | `ProducerUsing` with `AsyncKafkaProducer` in an `await using` block, and a delivery callback that returns a `Task`. |
| `AsyncConsumer` | `ConsumerUsing` with `AsyncKafkaConsumer` in an `await using` block, with a listener whose methods return a `Task`. |
| `AdminClient` | Creates a topic with `Admin`, reads each topic's `KafkaFuture` with `Get()` (a failure arrives as `ExecutionException`, the error being its `InnerException`), and lists the topics. |
| `AsyncAdminClient` | The same with `AsyncAdmin` in an `await using` block; a call returns its result at once and each `AsyncKafkaFuture` is awaited through `Get()`. |

## Running them

Build the client, then start a broker:

```bash
make -C dotnet build-dotnet        # from the repository root
docker run -d --rm --name kafka -p 9092:9092 apache/kafka:4.4.0
```

```bash
cd dotnet/examples
dotnet run -c Release --project Consumer -- localhost:9092 my-group demo      # Ctrl+C to stop
echo "hello" | dotnet run -c Release --project Producer -- localhost:9092 demo
```

The producers and the admin examples take `<bootstrap-brokers> <topic>`, and the consumers
`<bootstrap-brokers> <group> <topic1> <topic2> ..`.

Start a second consumer in the same group to see a rebalance: each consumer prints the partitions
it is assigned and the ones revoked from it.

`-c Release` matters: the examples reference the client's source, and a Release build loads the
native library `make -C dotnet build-dotnet` builds, while a Debug build looks for a debug build of
it. The examples target `net8.0` and run on .NET 8 or any later runtime.

To build an example against the `Confluent.Kafka` NuGet package instead, replace the
`ProjectReference` in its `.csproj` with the `PackageReference` in the comment beside it.

## Notes

- **Delivery callback.** The producer calls `ICallback.OnCompletion(metadata, exception)` on its
  background completion thread, `exception` being `null` on success. `AsyncKafkaProducer` also takes
  an `IAsyncCallback`, whose `Task` the producer waits for before it handles its next completion.
  `RecordMetadata` carries no key or value, so the producers give each record its own callback object,
  which prints the value.
- **Send future.** `Send()` returns a `KafkaFuture` of the delivery: `producer.Send(record).Get()`
  gives the `RecordMetadata`. On `AsyncKafkaProducer`, awaiting `Send()` waits for buffer space and
  returns an `AsyncKafkaFuture`, so `await (await producer.Send(record)).Get()` gives it.
- **Rebalance listener.** On `KafkaConsumer` its methods run on the thread calling `Poll()`; on
  `AsyncKafkaConsumer` they run on the consumer's own thread while `Poll()` runs. Either way the
  rebalance waits for them to return (for the async listener, for its `Task` to end). With the
  KIP-848 protocol, `OnPartitionsAssigned` gets only the partitions added in that step, which can be
  none; the examples also print the whole assignment after each step. A listener that handles
  `OnPartitionsLost` itself implements `IConsumerRebalanceListenerWithPartitionsLost`
  (`IAsyncConsumerRebalanceListenerWithPartitionsLost` for the async consumer); without it, a lost
  partition is reported to `OnPartitionsRevoked`, as in Java.
- **Ctrl+C.** The consumer examples handle `Console.CancelKeyPress` and set `e.Cancel = true`, which
  keeps the process running so the consumer is closed. `Consumer` and `ConsumerUsing` call
  `Wakeup()`, which makes the `Poll()` in progress throw `WakeupException`; `AsyncConsumer` cancels
  the token it passes to `Poll()`, whose task then throws `OperationCanceledException`. The producer
  and admin examples do not handle it: Ctrl+C ends the process at once, without closing the client,
  so messages not yet delivered are lost. End the producers' input with Ctrl+D (Ctrl+Z, then Enter,
  on Windows).
- **Offsets.** With auto-commit (the default), the consumer commits the offsets of the records
  `Poll()` returned every `auto.commit.interval.ms` (5 seconds), before a rebalance takes partitions
  away, and on `Close()`.
