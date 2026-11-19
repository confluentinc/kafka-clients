# .NET performance harnesses

| Project | Measures |
|---|---|
| `PerfV3` | This binding, over the Rust core (`Confluent.Kafka` from `dotnet/src`) |
| `PerfV2` | The published confluent-kafka-dotnet (the `Confluent.Kafka` NuGet package, over librdkafka) |
| `PerformanceCommon` | The engines both share: the producer, consumer and transactional benchmarks, metrics and results |
| `Confluent.Kafka.PerformanceTests` | Unit tests of the engines, and a p99 smoke test against a Docker broker |

Both harnesses are configured by environment variables and select the workload with `MODE`:

| `MODE` | Runs |
|---|---|
| `producer` (default) | The producer benchmark |
| `consumer` | The consumer benchmark, reading a load produced by Kafka's `kafka-producer-perf-test.sh` |
| `transactional` | The transactional producer: `TXN_MODE=produce`, or `eos` (consume, produce, send the offsets, commit) |
| `version` | Prints `key: value` lines: client, versions, .NET runtime, GC mode and TieredPGO |

`ASYNC=True` selects the async API, else the sync one: the async producer and consumer, and on v3 the async
transaction calls. On v2 the transaction calls and the transactional consumer stay blocking, and `ASYNC` only
sends the records through `ProduceAsync`.
Each run writes `results.json` (`RESULTS_FILE`) and a `metrics.jsonl` line per window (`METRICS_FILE`), with the
same keys as the Python harnesses in `python/test/performance`.

## Run them through `cargo xtask perf-cloud`

The `dotnet` group runs four arms: `dotnet-sync-rust` and `dotnet-async-rust` (PerfV3), `dotnet-sync-librdkafka`
and `dotnet-async-librdkafka` (PerfV2). From `rust/`:

```sh
cargo xtask perf-cloud build --group dotnet
cargo xtask perf-cloud check --group dotnet --batch dotnet-producer-1
```

`build` checks for the `dotnet` SDK, builds the Rust FFI library and publishes both harnesses for `net10.0`, each
into its own `bin/publish` folder. The driver runs `dotnet <harness>.dll` with `MODE` and `ASYNC` set per arm and
workload, and removes inherited `DOTNET_*` (except `DOTNET_ROOT`) and `COMPlus_*` variables, so a runner's GC
settings apply to no arm. The tests and batches are in `tools/perf-cloud/`.

## Default configurations

- **librdkafka (PerfV2)** queues up to `BUFFER_MEMORY` MiB (32 by default, as the driver passes to every producing
  client) as `queue.buffering.max.kbytes`, with no message cap. A produce never blocks the caller: a full queue is
  retried after 1 ms, in the sync and the async arm alike.
- **The Rust core (PerfV3)** runs `buffer.memory.allocation.strategy=incremental` on the producer workload. It
  runs `full` where the core does not run incremental: with a `COMPRESSION_TYPE` other than `none`, or a
  `BATCH_SIZE` below 16 KiB. An explicit `BUFFER_MEMORY_ALLOCATION_STRATEGY` wins. The transactional workload
  runs `full`. The harness prints the effective strategy and writes it to `results.json`
  (`buffer_memory_allocation_strategy`); the report shows it per client.
- **Serializers and deserializers (PerfV3).** Both producer arms send `byte[]` keys and values through the
  built-in byte-array serializers. The async arm passes their `Async` classes, which the producer calls inside
  `Send` without awaiting, so both arms serialize the same way. Both consumer arms read each key and value as a
  `ReadOnlyMemory<byte>` over the fetched batch, without copying it, and count its length: the sync arm through
  the built-in `ReadOnlyMemoryDeserializer`, the async arm through `AsyncReadOnlyMemoryDeserializer`, which the
  consumer calls without awaiting, so both arms deserialize the same way. Each polled batch stays alive until the
  garbage collector releases it, and each key and value that has bytes costs one memory manager. No arm measures
  the awaited path of an `IAsyncDeserializer<T>` that is not a built-in.
- The transactional workload forces idempotence and `acks=all`.
- **The consumer** takes the client defaults for every setting the test leaves out: the driver passes
  `USE_DEFAULTS=True`. Run outside the driver, without `USE_DEFAULTS`, the harnesses tune the fetch settings
  (4 MiB) and read in batches of `CONSUMER_BATCH_SIZE` records (2500 by default; PerfV3 as `max.poll.records`).
  `MAX_POLL_RECORDS` overrides it, and PerfV2 also reads `POLL_SINGLE` (one record per `Consume` call).
- **`DO_VERIFY`** (default `True`) also applies to the consumer: both harnesses read every byte of each polled
  key and value, counting the bits set, and check that the record's partition is not negative. PerfV3 reads the
  bytes in place in the fetched batch, PerfV2 in the `byte[]` copies its client hands out. The summary and
  `results.json` (`verify`) report the records read, the bits set and the failures, over every record polled,
  the warmup's included; a negative partition fails the run. `DO_VERIFY=False` reads only the lengths.

## Run them by hand

```sh
BOOTSTRAP_SERVERS=localhost:9092 CLIENT_VERSION=3 make producer-perf-test-dotnet
BOOTSTRAP_SERVERS=localhost:9092 CLIENT_VERSION=2 make consumer-perf-test-dotnet
make -C dotnet perf-unit-test-dotnet
```
