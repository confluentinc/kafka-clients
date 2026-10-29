# Data-integrity tests

These tests check that what the Rust client writes is what it reads back. Each
record must come back byte for byte. No acknowledged record may be lost, and no
record may be duplicated, reordered, or visible when its transaction aborted.
Only the Rust client is exercised: the brokers are Apache Kafka, and nothing
else touches the data.

There are two forms, on one harness (`tests/common/integrity/`):

- **The integration tests** (`tests/integration/integrity_test.rs`). These are
  short scenarios, each a few seconds to a minute, run with the rest of the
  integration suite against the pooled test broker:

  ```sh
  cargo test --features integration-tests --test integration -- integrity_test::
  ```

  Its `self_tests` module checks the harness itself and needs no broker.

- **The long run** (`tests/integrity/`). Every stream at once for half an hour
  by default, against a local three-broker cluster or an external one.
  The run is `#[ignore]`d, so `cargo test` skips it; start it by hand:

  ```sh
  cargo xtask integrity-test
  ```

## How a record checks itself

Every record carries an `x-integrity` header, always its last one. The header
holds:

- the run id, the writer, the producer incarnation and the sequence number;
- the transaction intent (none, commit or abort);
- the creation timestamp;
- a SHA-256 digest over all of the above, the key, the value and every other
  header.

The digest does not depend on Kafka's own CRC, so it catches corruption
anywhere between the application and the application: serialization, batching,
compression, the wire, the fetch path and decompression.

Keys, values and headers come from a generator seeded by `(seed, writer,
sequence)`. They cover null and empty keys and values, null header values,
repeated and non-ASCII header keys, and compressible and random data, at many
sizes. When a digest does not match, the harness generates the record again to
report what changed, for example `value byte 812 is 0x3f, expected 0x3e`.

## What is checked

Every send is recorded in a ledger. Each send gets an expectation from its
outcome:

| Outcome | Expectation |
|---|---|
| acknowledged, or in a committed transaction | exactly once |
| failed with a retriable error, or the commit outcome is unknown | at most once |
| rejected, or in an aborted transaction | never |

While the run is going, the live consumer group is checked:

- **Corruption**: a record whose identity header or digest is wrong.
- **Aborted records**: under `read_committed`, no record of an aborted
  transaction is visible.
- **Order**: within a partition, a writer's sequence numbers only increase.
- **Double writes**: one record is never stored at two offsets.
- **Placement**: a record is read at the partition and offset its
  acknowledgement reported.
- **Committed offsets**: a restarted member never reads below the committed
  offset, and reading the committed offset back never gives less than was
  committed.

At the end, a fresh consumer with no group reads every partition from the
beginning to its end offset. This final read-back checks the exact count of
every record against its expectation, so it finds lost, duplicated and leaked
records.

A run that tested nothing does not pass. Each scenario states its
requirements, for example a minimum number of records per stream, committed
and aborted transactions, clean restarts and hard stops, and every partition
read. A watchdog stops the run when nothing is sent or read for too long.

## The long run

The long run has these streams, each with a writer on a topic of its own:

| Stream | What it does |
|---|---|
| `none`, `gzip`, `snappy`, `lz4`, `zstd` | One idempotent writer per compression codec, at `RATE_PER_WRITER` records per second |
| `txn` | A transactional writer: 50 records per transaction, one in four aborted |
| `large` | Values of 128 KiB to 900 KiB, at `LARGE_RATE` records per second |
| `copy` | A writer on `copy-in`, and a consume-transform-produce copier from `copy-in` to `copy`, using `send_offsets_to_transaction`, with one in five transactions aborted |

The streams run all at once for `DURATION_S`: about 30,000 records per second
in total at the default rates (the `txn` writer is held back by its commit
round trips), with every producer and a group reading every topic loading the
cluster together.

Every writer replaces its producer every `PRODUCER_RESTART_S`. One consumer
group of `CONSUMERS` members reads every topic. A
member restarts every `CONSUMER_RESTART_S`, alternately cleanly and hard:

- A clean restart commits what was read and calls `close()`.
- A hard restart reads one more batch without committing it and stops polling,
  as a stuck or crashed application would. Its partitions move to another
  member once `MAX_POLL_INTERVAL_S` expires, and that member must read the
  uncommitted batch again.

  A process crash cannot be simulated in process: an unclosed consumer's
  background task runs on its own thread and keeps heartbeating, as Java's does.
  So the abandoned consumer is closed only when the run stops.

At the end, the writers stop and the group catches up. Then the final
read-back runs and the verdict is printed. The drain and the read-back come on top of `DURATION_S`: the read-back
reads every record again, which took 10 to 30 minutes after a 30-minute run.

### Against a local cluster

Leave `BOOTSTRAP_SERVERS` unset. The test starts three combined
broker/controller containers, with every topic replicated three ways and
`min.insync.replicas=2`. Docker must be running. The image is `apache/kafka`,
at tag `INTEGRATION_TEST_BROKER_TAG`, which `cargo xtask integrity-test` sets
to 4.4.0 when it is unset (the integration tests' default is older).

```sh
cargo xtask integrity-test
DURATION_S=600 cargo xtask integrity-test       # a ten-minute run
```

### Against an external cluster

Set `BOOTSTRAP_SERVERS` and the security settings:

```sh
BOOTSTRAP_SERVERS=broker.example.com:9092 \
SECURITY_PROTOCOL=SASL_SSL \
SASL_MECHANISM=PLAIN \
SASL_USERNAME=... \
SASL_PASSWORD=... \
cargo xtask integrity-test
```

For anything else, `CLIENT_CONFIG_FILE` names a Java-style properties file of
client settings. The variables above override that file.

The run creates its topics as `<TOPIC_PREFIX>-<run id>-<stream>` and deletes
them at the end unless `KEEP_TOPICS=true`. The credentials need these
permissions:

- create, delete, read and write on those topics;
- read on groups with the same prefix;
- write and describe on transactional ids with the same prefix.

### Settings

| Variable | Default | Meaning |
|---|---|---|
| `BOOTSTRAP_SERVERS` | unset (local cluster) | The cluster to run against |
| `SECURITY_PROTOCOL` | unset | `security.protocol` |
| `SASL_MECHANISM` | unset | `PLAIN`, `SCRAM-SHA-256` or `SCRAM-SHA-512`; `PLAIN` when only the credentials are set |
| `SASL_USERNAME`, `SASL_PASSWORD` | unset | The SASL credentials |
| `CLIENT_CONFIG_FILE` | unset | A properties file of extra client settings |
| `DURATION_S` | 1800 | How long the writers run |
| `RATE_PER_WRITER` | 5000 | Records per second per writer, except `large` |
| `LARGE_RATE` | 5 | Records per second of the `large` writer |
| `PARTITIONS` | 6 | Partitions per topic |
| `REPLICATION_FACTOR` | 3 | Replication factor of each topic |
| `CONSUMERS` | 3 | Members of the live group |
| `CONSUMER_RESTART_S` | 60 | How often each member restarts |
| `MAX_POLL_INTERVAL_S` | 30 | The group's `max.poll.interval.ms`, in seconds: how long a hard-stopped member holds its partitions |
| `PRODUCER_RESTART_S` | 90 | How often each writer replaces its producer |
| `DRAIN_S` | 300 | How long the group may take to catch up, and how long the final read-back may go without reading a record |
| `INTEGRITY_SEED` | random | The payload seed; set it to replay the same data |
| `TOPIC_PREFIX` | `integrity` | The prefix of the topic, group and transactional id names |
| `KEEP_TOPICS` | `false` | Keep the topics of an external run |
| `REPORT_DIR` | `target/integrity-runs` | Where the report is written |
| `INTEGRATION_TEST_BROKER_TAG` | `4.4.0` (via xtask) | The `apache/kafka` image tag of the local cluster |
| `RUST_LOG` | unset | The client's log filter, for example `confluent_kafka=info` |

### The report

Every 30 seconds the run prints a status line: records sent, acknowledged,
read and redelivered, and the number of failures. At the end it prints the
verdict, with the reason for each failure and up to a few examples of each.

Everything is also written to `REPORT_DIR/<run id>/`:

- `verdict.txt`: the printed verdict.
- `report.json`: every counter, the examples, the unmet requirements and the
  settings, without the credentials.

The test fails unless the verdict passes. To replay a failed run's data, use the seed printed at its start,
`INTEGRITY_SEED=<seed>`.
