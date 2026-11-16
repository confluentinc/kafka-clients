# Verifiable clients

`VerifiableProducer` and `VerifiableConsumer` are ports of Kafka's
`org.apache.kafka.tools.VerifiableProducer` and `VerifiableConsumer` over the
.NET client's blocking `KafkaProducer` and `KafkaConsumer`. They take the same
arguments and print the same JSON events, so Kafka's ducktape system tests
(`kafkatest`) can run against the .NET client in place of the Java tools,
through `VerifiableClientApp`. The Python tools in `python/verifiable_clients`
do the same for the Python client, and these follow them where Java leaves a
choice.

The client implements only the KIP-848 `consumer` group protocol, so only the
`group_protocol=consumer` parametrizations of a test can pass; with
`--group-protocol classic` (the tool's default, as Java's) the consumer fails at
construction with `Confluent.Kafka.UnsupportedOperationException`.

## Tests

From the repository root:

    make -C dotnet test-verifiable-clients-dotnet

This runs the tools' tests on net8.0 and net10.0. They need no broker, only the
Rust library, which the target's `build-dotnet` prerequisite builds. The
projects are in `Confluent.Kafka.sln`, so `make -C dotnet test-dotnet` runs the
tests too. The tests that send signals run on Unix only.

## Running the system tests

These steps were tested with the system tests of Kafka 4.4.0-rc4 (tag
`4.4.0-rc4`, commit `1156b2752`, the repository's `kafka` submodule). These
cases passed with the .NET tools on 2026-11-16, on `ISOLATED_KRAFT` and, for the
consumer tests, with `group_protocol=consumer`:

| Case, in `tests/kafkatest/tests/client/` | Other parameters | Result |
|---|---|---|
| `consumer_test.py` `test_broker_rolling_bounce` | none | passed, 2 min 54 s |
| `consumer_test.py` `test_consumer_bounce` | `clean_shutdown=True`, `bounce_mode=all`, `enable_assignment_batching=True` | passed, 43 s |
| `consumer_test.py` `test_fencing_static_consumer` | `num_conflict_consumers=1`, `fencing_stage=stable` | passed, 1 min 27 s |
| `compression_test.py` `test_compressed_topic` | `compression_types=[snappy, gzip, lz4, zstd, none]` | passed, 1 min 19 s |

### Kafka's system-test libraries

The nodes run Kafka from a checkout, which `systemTestLibs` builds. From the
repository root, copy the `kafka` submodule to `<kafka>`, an absolute path
outside the repository, so Gradle's output stays out of it, and build it;
without a JDK on the host, in a JDK container (about 2.5 minutes; the volume
keeps Gradle's downloads):

    mkdir -p <kafka> && git -C kafka archive HEAD | tar -x -C <kafka>
    docker run --rm --memory=6g -v <kafka>:/kafka -v kafka-gradle-cache:/root/.gradle \
        -w /kafka eclipse-temurin:17-jdk-jammy \
        ./gradlew --no-daemon --max-workers=4 systemTestLibs

### Building the tools for the nodes

The ducker-ak nodes are Ubuntu 22.04 on the host's architecture. Build the
Linux native library with the `ubuntu:22.04` command of
[`python/verifiable_clients/README.md`](../../../python/verifiable_clients/README.md):
it writes `rust/target-linux/release/libconfluent_kafka.so`, and one build
serves both bindings. The .NET tools need only its `cargo build`, without the
Python packages and the `setup.py` step.

Then, from the repository root, publish both tools self-contained for the
host's architecture (`<arch>` is `arm64` or `x64`) into a directory under the
Kafka checkout's `tests/` (mounted on every node as `/opt/kafka-dev/tests/`):

    for tool in VerifiableProducer VerifiableConsumer; do
      dotnet publish dotnet/tools/VerifiableClients/$tool -c Release -f net8.0 \
          -r linux-<arch> --self-contained --artifacts-path <dir> \
          -p:NativeLibraryPath="$PWD/rust/target-linux/release/libconfluent_kafka.so" \
          -p:NativeLibraryFileName=libconfluent_kafka.so \
          -o <kafka>/tests/dotnet-clients
    done

The two `-p:` properties put the Linux library next to the executables in place
of the host's. The build happens in `<dir>`, any directory outside the
repository, so the repository's `bin/` and `obj/` directories are left
untouched. The output carries the .NET runtime and the library, and the tools
use invariant globalization, so the nodes need no .NET runtime, no ICU and no
`LD_LIBRARY_PATH`.

### Running a case

Save these globals as `<kafka>/tests/dotnet-globals.json`:

    {
      "VerifiableProducer": {
        "class": "kafkatest.services.verifiable_client.VerifiableClientApp",
        "exec_cmd": "/opt/kafka-dev/tests/dotnet-clients/VerifiableProducer",
        "pids": "pgrep -f /opt/kafka-dev/tests/dotnet-clients/VerifiableProducer",
        "kill_signal": 15
      },
      "VerifiableConsumer": {
        "class": "kafkatest.services.verifiable_client.VerifiableClientApp",
        "exec_cmd": "/opt/kafka-dev/tests/dotnet-clients/VerifiableConsumer",
        "pids": "pgrep -f /opt/kafka-dev/tests/dotnet-clients/VerifiableConsumer",
        "kill_signal": 15
      }
    }

The executable is the .NET process itself, so the signal reaches the tool.
`pids` is the pattern `VerifiableClientApp` would derive from `exec_cmd`,
spelled out so that an `exec_cmd` starting with `env` (see Logging) still works.

From `<kafka>`, bring the cluster up with `tests/docker/ducker-ak up -n 11`:
ducker01 only runs ducktape, so a case needs one node more than its
`@cluster(num_nodes=...)`; 11 covers the cases above, and the default, 14,
works too. The first `up` builds the nodes' image, in about 20 minutes.

Run one case at a time, selected with ducktape's `@` parameters, so the
`classic` parametrizations, which cannot pass, are left out. The selector does
not survive `ducker-ak test`'s shell quoting, so call ducktape on the first
node:

    docker exec -w /opt/kafka-dev ducker01 ducktape \
        --cluster-file tests/docker/build/cluster.json \
        '<selector>' \
        --globals /opt/kafka-dev/tests/dotnet-globals.json

The selectors of the cases above:

    tests/kafkatest/tests/client/consumer_test.py::OffsetValidationTest.test_broker_rolling_bounce@{"metadata_quorum":"ISOLATED_KRAFT","group_protocol":"consumer"}
    tests/kafkatest/tests/client/consumer_test.py::OffsetValidationTest.test_consumer_bounce@{"clean_shutdown":true,"bounce_mode":"all","metadata_quorum":"ISOLATED_KRAFT","group_protocol":"consumer","enable_assignment_batching":true}
    tests/kafkatest/tests/client/consumer_test.py::OffsetValidationTest.test_fencing_static_consumer@{"num_conflict_consumers":1,"fencing_stage":"stable","metadata_quorum":"ISOLATED_KRAFT","group_protocol":"consumer"}
    tests/kafkatest/tests/client/compression_test.py::CompressionTest.test_compressed_topic@{"compression_types":["snappy","gzip","lz4","zstd","none"],"metadata_quorum":"ISOLATED_KRAFT"}

A selector must name every parameter of the case, with JSON booleans (`true`,
not `True`). `ducktape --collect-only <file>` lists a file's cases with their
parameters.

The results go to `<kafka>/results/`. A service's `start()` first removes the
tool's directory on each of its nodes, so a failed test's capture file (see
Logging) holds only the runs since the service last started: when a test starts
a service again, as the fencing test does its conflict consumers, the earlier
runs (the fenced first run) are not in it. A node bounced with `stop_node` and
`start_node`, as in `test_consumer_bounce`, appends to the same file.

### What cannot pass yet

- Classic group protocol: see above. `client/pluggable_test.py` runs it too:
  its consumer is given no group protocol, so the service passes no
  `--group-protocol`.
- SSL: the client reads PEM stores only, and the tests generate JKS
  truststores and keystores, so every case with an SSL client fails at
  startup.
- SASL: the client implements only the PLAIN mechanism, so GSSAPI and SCRAM
  cases fail at construction. A SASL_PLAINTEXT client gets its login
  configuration as a multi-line `sasl.jaas.config` in the properties file,
  which the tools read; a SASL_SSL client, and the delegation-token test, get
  it only as a JAAS file named in `KAFKA_OPTS`
  (`-Djava.security.auth.login.config`), which only a JVM reads.
- Tests that copy records with Java's transactional message copier use a Java
  tool these globals do not replace. Tests that run an older release's clients
  get these tools with that release's arguments, which name the brokers with
  `--broker-list` (the consumer's before 4.0, the producer's before 2.5); the
  tools take only `--bootstrap-server`, so they exit at argument parsing.

## Logging

The tools log to stderr at `WARNING` by default; set
`VERIFIABLE_CLIENT_LOG_LEVEL` (for example to `DEBUG`) to change it. At `DEBUG`
the consumer also logs the wakeup that ends its poll at shutdown. The Rust core
logs to stderr at the level `RUST_LOG` sets (`RUST_LOG=debug`, for example). On
the nodes, set both in `exec_cmd`, as in
`"env VERIFIABLE_CLIENT_LOG_LEVEL=DEBUG RUST_LOG=debug /opt/kafka-dev/tests/dotnet-clients/VerifiableConsumer"`.

The ducktape services append stderr to the stdout capture file
(`verifiable_consumer.stdout`, `verifiable_producer.stdout`), after the JSON
stream they parse. Those files are collected into the test's results only when
the test fails.

## Signals

The tools stop cleanly on SIGTERM (the globals' `kill_signal` 15) and on
SIGINT, as Java's shutdown hook stops them: the producer closes and prints
`shutdown_complete` and `tool_data`; the consumer prints `shutdown_requested`,
wakes its poll, closes and prints `shutdown_complete`. Later signals are
ignored, and the tool exits 0. A tool started with `&` from a non-interactive
shell, as ducktape starts them, inherits SIGINT as ignored, and .NET keeps it
ignored, as the JVM does: stop it with SIGTERM, so keep `kill_signal` at 15. On
Windows only the console control events map to them (Ctrl+C to SIGINT).
