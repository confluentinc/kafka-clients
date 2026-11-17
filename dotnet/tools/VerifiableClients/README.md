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

The steps use three shell variables. Set them first, in the shell you run the
steps from; the values are examples:

    # Replace these values with your own.
    export KAFKA=$HOME/kafka-systest-4.4                 # a copy of Kafka, outside the repository
    export PUBLISH_DIR=$HOME/kafka-systest-dotnet-build  # where dotnet publish builds, outside the repository
    export ARCH=arm64                                    # the host's architecture: arm64 or x64

Run each command from the repository root.

### 1. Build Kafka's system-test libraries

The nodes run Kafka from a checkout, which `systemTestLibs` builds. Copy the
`kafka` submodule to `$KAFKA`, outside the repository so Gradle's output stays
out of it, and build it; without a JDK on the host, in a JDK container (about
2.5 minutes; the volume keeps Gradle's downloads):

    mkdir -p "$KAFKA" && git -C kafka archive HEAD | tar -x -C "$KAFKA"
    docker run --rm --memory=6g -v "$KAFKA":/kafka -v kafka-gradle-cache:/root/.gradle \
        -w /kafka eclipse-temurin:17-jdk-jammy \
        ./gradlew --no-daemon --max-workers=4 systemTestLibs

The build ends with `BUILD SUCCESSFUL`. Run it before step 5: the build and
the cluster each need much of Docker's memory.

### 2. Build the Linux native library

The ducker-ak nodes are Ubuntu 22.04 on the host's architecture, so the native
library is built in an `ubuntu:22.04` container:

    docker run --rm -v "$PWD":/work -w /work/rust \
        -e CARGO_TARGET_DIR=/work/rust/target-linux -e CARGO_BUILD_JOBS=8 \
        -e HOST_OWNER="$(id -u):$(id -g)" ubuntu:22.04 bash -c '
      set -e
      apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
          gcc g++ make cmake clang nasm perl pkg-config git
      curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
      . "$HOME/.cargo/env"
      cargo build -p confluent-kafka --release --lib --features ffi
      chown -R "$HOST_OWNER" /work/rust/target-linux /work/rust/target/include'

It writes `rust/target-linux/release/libconfluent_kafka.so` (`rust/target-linux`
is in `.gitignore`). `--features ffi` builds the C API the .NET binding calls.
The build also writes the C headers to `rust/target/include`, shared with the
host's build, and runs as root, so the last line gives the files back to you.
`CARGO_BUILD_JOBS` limits the build's parallel jobs; lower it if Docker runs
out of memory. The same library serves the Python tools, whose
[`README.md`](../../../python/verifiable_clients/README.md) adds the extension
build to this command.

### 3. Publish the tools

Publish both tools self-contained for the host's architecture into a directory
under the Kafka checkout's `tests/` (mounted on every node as
`/opt/kafka-dev/tests/`):

    for tool in VerifiableProducer VerifiableConsumer; do
      dotnet publish dotnet/tools/VerifiableClients/$tool -c Release -f net8.0 \
          -r linux-$ARCH --self-contained --artifacts-path "$PUBLISH_DIR" \
          -p:NativeLibraryPath="$PWD/rust/target-linux/release/libconfluent_kafka.so" \
          -p:NativeLibraryFileName=libconfluent_kafka.so \
          -o "$KAFKA/tests/dotnet-clients"
    done

The two `-p:` properties put the Linux library next to the executables in place
of the host's. The build happens in `$PUBLISH_DIR`, so the repository's `bin/` and
`obj/` directories are left untouched. The output carries the .NET runtime and
the library, and the tools use invariant globalization, so the nodes need no
.NET runtime, no ICU and no `LD_LIBRARY_PATH`.

Check that a tool starts on Ubuntu 22.04; this prints
`usage: verifiable-producer` and the options:

    docker run --rm -v "$KAFKA/tests/dotnet-clients":/t ubuntu:22.04 /t/VerifiableProducer --help

### 4. Save the globals

Save the globals in the Kafka copy's `tests/`:

    cat > "$KAFKA/tests/dotnet-globals.json" <<'EOF'
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
    EOF

The executable is the .NET process itself, so the signal reaches the tool.
`pids` is the pattern `VerifiableClientApp` would derive from `exec_cmd`,
spelled out so that an `exec_cmd` starting with `env` (see Logging) still works.

### 5. Bring the cluster up

    (cd "$KAFKA" && tests/docker/ducker-ak up -n 11)

ducker01 only runs ducktape, so a case needs one node more than its
`@cluster(num_nodes=...)`. The largest case in this README asks for 10 nodes,
so 11 covers them all, and the default, 14, works too. The first `up` builds
the nodes' image, in about 20 minutes.

### 6. Run the cases

Run either one case or every case the client can run. Select cases with
ducktape's `@` parameters, so the `classic` parametrizations, which cannot
pass, are left out. A selector does not survive `ducker-ak test`'s shell
quoting, so call ducktape on the first node.

#### One case

    docker exec -w /opt/kafka-dev ducker01 ducktape \
        --cluster-file tests/docker/build/cluster.json \
        '<selector>' \
        --globals /opt/kafka-dev/tests/dotnet-globals.json

The selectors of the four cases in the table at the start of this section:

    tests/kafkatest/tests/client/consumer_test.py::OffsetValidationTest.test_broker_rolling_bounce@{"metadata_quorum":"ISOLATED_KRAFT","group_protocol":"consumer"}
    tests/kafkatest/tests/client/consumer_test.py::OffsetValidationTest.test_consumer_bounce@{"clean_shutdown":true,"bounce_mode":"all","metadata_quorum":"ISOLATED_KRAFT","group_protocol":"consumer","enable_assignment_batching":true}
    tests/kafkatest/tests/client/consumer_test.py::OffsetValidationTest.test_fencing_static_consumer@{"num_conflict_consumers":1,"fencing_stage":"stable","metadata_quorum":"ISOLATED_KRAFT","group_protocol":"consumer"}
    tests/kafkatest/tests/client/compression_test.py::CompressionTest.test_compressed_topic@{"compression_types":["snappy","gzip","lz4","zstd","none"],"metadata_quorum":"ISOLATED_KRAFT"}

A selector must name every parameter of the case, with JSON booleans (`true`,
not `True`). `ducktape --collect-only <file>` lists a file's cases with their
parameters.

#### Every case the client can run

[`system-tests.txt`](system-tests.txt) lists, one selector per line, the 53
cases of Kafka 4.4.0-rc4 that use these tools and that the client can run. From
the 14 test files below, it keeps:

- the `group_protocol=consumer` variant of each case that has a
  `group_protocol` parameter;
- in `security_test.py`, only the case with a PLAINTEXT client;
- in `security_rolling_upgrade_test.py`, only
  `test_enable_separate_interbroker_listener`, as the other cases use an SSL or
  SASL client;
- every case of the other files.

| Test file, in `tests/kafkatest/tests/` | .NET tools | Cases |
|---|---|---|
| `client/consumer_test.py` | producer and consumer | 24 |
| `core/replication_replica_failure_test.py` | producer and consumer | 1 |
| `core/security_test.py` | producer and consumer | 2 |
| `client/compression_test.py` | producer | 1 |
| `core/fetch_from_follower_test.py` | producer | 1 |
| `core/reassign_partitions_test.py` | producer | 4 |
| `core/log_dir_failure_test.py` | producer | 4 |
| `core/throttling_test.py` | producer | 2 |
| `core/snapshot_test.py` | producer | 4 |
| `core/eligible_leader_replicas_test.py` | producer | 1 |
| `core/security_rolling_upgrade_test.py` | producer | 1 |
| `core/quorum_reconfiguration_test.py` | producer | 2 |
| `core/get_offset_shell_test.py` | producer | 5 |
| `tools/replica_verification_test.py` | producer | 1 |

Where only the producer is a .NET tool, Kafka's Java tools (`ConsoleConsumer`,
or the tool the test is about) read the records.

Copy the list next to the globals:

    cp dotnet/tools/VerifiableClients/system-tests.txt "$KAFKA/tests/dotnet-system-tests.txt"

Check that every selector still matches a case; this prints
`Collected 53 tests`:

    docker exec -w /opt/kafka-dev ducker01 bash -c \
        'mapfile -t S < tests/dotnet-system-tests.txt
         ducktape --collect-only "${S[@]}" | grep Collected'

A Kafka release that renames a test parameter makes a selector match nothing;
`ducktape --collect-only <file>` shows the file's current cases and parameters.

Run them all in one ducktape session, which takes a few hours (on macOS, put
`caffeinate -i` before `docker exec` so the host does not sleep):

    docker exec -w /opt/kafka-dev ducker01 bash -c \
        'mapfile -t S < tests/dotnet-system-tests.txt
         ducktape --cluster-file tests/docker/build/cluster.json \
                  --globals /opt/kafka-dev/tests/dotnet-globals.json "${S[@]}"' \
        2>&1 | tee "$KAFKA/dotnet-system-tests.log"

To run a part of the list, filter it, for example
`mapfile -t S < <(grep snapshot_test tests/dotnet-system-tests.txt)`.

### 7. Read the results

Each ducktape session writes a directory under `$KAFKA/results/`, named by
date and session number (`2026-11-16--001`). This prints the newest session's
`report.txt`, which gives each case's result:

    RUN=$(ls -td "$KAFKA"/results/20*/ | head -1) && cat "$RUN/report.txt"

Each failed case's directory in it holds the tools' capture files (see
Logging) and the broker logs.

A service's `start()` first removes the tool's directory on each of its nodes,
so a failed test's capture file holds only the runs since the service last
started: when a test starts a service again, as the fencing test does its
conflict consumers, the earlier runs (the fenced first run) are not in it. A
node bounced with `stop_node` and `start_node`, as in `test_consumer_bounce`,
appends to the same file.

### 8. Tear the cluster down

    (cd "$KAFKA" && tests/docker/ducker-ak down)

This removes the nodes and their network.

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
(`verifiable_consumer.stdout`, `verifiable_producer.stdout`), interleaved with
the JSON stream they parse. Those files are collected into the test's results
only when the test fails.

## Signals

The tools stop cleanly on SIGTERM (the globals' `kill_signal` 15) and on
SIGINT, as Java's shutdown hook stops them: the producer closes and prints
`shutdown_complete` and `tool_data`; the consumer prints `shutdown_requested`,
wakes its poll, closes and prints `shutdown_complete`. Later signals are
ignored, and the tool exits 0. A tool started with `&` from a non-interactive
shell, as ducktape starts them, inherits SIGINT as ignored, and .NET keeps it
ignored, as the JVM does: stop it with SIGTERM, so keep `kill_signal` at 15. On
Windows only the console control events map to them (Ctrl+C to SIGINT).
