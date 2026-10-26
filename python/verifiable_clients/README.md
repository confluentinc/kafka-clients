# Verifiable clients

`verifiable_producer.py` and `verifiable_consumer.py` are translations of Kafka's
`org.apache.kafka.tools.VerifiableProducer` and `VerifiableConsumer` over the
`confluent_kafka` Python client. They take the same arguments and print the
same JSON events, so Kafka's ducktape system tests (`kafkatest`) can run against
the Python client in place of the Java tools, through `VerifiableClientApp`.
The Rust tools in `rust/tools/verifiable-clients` do the same for the Rust
client, and the Python tools follow them where Java leaves a choice (see the
module docstrings).

The client implements only the KIP-848 `consumer` group protocol, so only the
`group_protocol=consumer` parametrizations of a test can pass; with
`--group-protocol classic` (the tool's default, as Java's) the consumer fails at
construction with `confluent_kafka.UnsupportedOperationError`.

## Tests

From the repository root, with the root venv set up (`make init`) and the Rust
library and the extension built (`make build-python`):

    . venv/bin/activate
    pip install './python[dev]'      # mypy and pytest
    make -C python test-verifiable-clients

This type checks the tools with `mypy --strict` and runs their tests, which
need the built extension but no broker. `make -C python test` runs them too,
after building and installing the dev extras itself.

## Running the system tests

The ducker-ak nodes are Ubuntu 22.04 (Python 3.10) on the host's architecture,
so the tools run on a Linux build of the library and of the extension, made in
an Ubuntu 22.04 container from the repository root:

    docker run --rm -v "$PWD":/work -w /work/rust \
        -e CARGO_TARGET_DIR=/work/rust/target-linux \
        -e HOST_OWNER="$(id -u):$(id -g)" ubuntu:22.04 bash -c '
      apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
          gcc g++ make cmake clang nasm perl pkg-config git python3-dev python3-setuptools
      curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
      . "$HOME/.cargo/env"
      cargo build -p confluent-kafka --release --lib --features ffi
      cd /work/python && CONFLUENT_KAFKA_LIB_DIR=/work/rust/target-linux/release \
          python3 setup.py build_ext --build-lib /work/rust/target-linux/python-ext \
              --build-temp /work/rust/target-linux/python-ext-tmp
      chown -R "$HOST_OWNER" /work/rust/target-linux /work/rust/target/include'

`--features ffi` is required: without it the C headers and the FFI symbols the
extension links against are not built. The build writes the C headers to
`rust/target/include`, shared with the host's build, and runs as root, so the
last step gives the files back to you; without it a later build on a Linux
host fails to rewrite the headers. `rust/target-linux` is in `.gitignore`.

Copy into a directory under the Kafka checkout's `tests/` (mounted on every
node as `/opt/kafka-dev/tests/`), here `tests/python-clients/`:

    python-clients/
      confluent_kafka/                           # python/confluent_kafka, without __pycache__
      _confluentkafka.cpython-310-<arch>-linux-gnu.so
                                                 # rust/target-linux/python-ext/; <arch> is the
                                                 # host's, aarch64 or x86_64
      libconfluent_kafka.so                      # rust/target-linux/release/
      _common.py  throughput_throttler.py  verifiable_producer.py  verifiable_consumer.py

The scripts import `confluent_kafka` and the extension from their own
directory. The extension's runpath is the build container's, so the globals
set `LD_LIBRARY_PATH`:

    {
      "VerifiableProducer": {
        "class": "kafkatest.services.verifiable_client.VerifiableClientApp",
        "exec_cmd": "env LD_LIBRARY_PATH=/opt/kafka-dev/tests/python-clients python3 /opt/kafka-dev/tests/python-clients/verifiable_producer.py",
        "pids": "pgrep -f /opt/kafka-dev/tests/python-clients/verifiable_producer.py",
        "kill_signal": 15
      },
      "VerifiableConsumer": {
        "class": "kafkatest.services.verifiable_client.VerifiableClientApp",
        "exec_cmd": "env LD_LIBRARY_PATH=/opt/kafka-dev/tests/python-clients python3 /opt/kafka-dev/tests/python-clients/verifiable_consumer.py",
        "pids": "pgrep -f /opt/kafka-dev/tests/python-clients/verifiable_consumer.py",
        "kill_signal": 15
      }
    }

The `pids` entries are needed: `VerifiableClientApp`'s default pattern is the
whole `exec_cmd`, which starts with `env` and matches no running process.

Save it as `tests/python-globals.json` and bring the cluster up with
`tests/docker/ducker-ak up`. Run one case at a time, selected with ducktape's
`@` parameters, so the `classic` parametrizations, which can only fail after a
120 s startup timeout, are left out. The selector does not survive
`ducker-ak test`'s shell quoting, so call ducktape on the first node:

    docker exec -w /opt/kafka-dev ducker01 ducktape \
        --cluster-file tests/docker/build/cluster.json \
        'tests/kafkatest/tests/client/consumer_test.py::OffsetValidationTest.test_broker_rolling_bounce@{"metadata_quorum":"ISOLATED_KRAFT","group_protocol":"consumer"}' \
        --globals /opt/kafka-dev/tests/python-globals.json

`ducktape --collect-only <file>` lists a file's cases with their parameters.

### What cannot pass yet

- Classic group protocol: see above.
- SSL: the client cannot read the JKS truststore and keystore the tests
  generate ("No certificates found in truststore file"), so every case with
  an SSL client fails at startup.
- SASL: the client implements only the PLAIN mechanism, so GSSAPI and SCRAM
  cases fail at construction. A SASL_PLAINTEXT client gets its login
  configuration as a multi-line `sasl.jaas.config` in the properties file,
  which the tools read; a SASL_SSL client, and the delegation-token test, get
  it only as a JAAS file named in `KAFKA_OPTS`
  (`-Djava.security.auth.login.config`), which only a JVM reads.
- Tests that copy records with Java's transactional message copier, or run
  older Kafka releases, use Java tools these globals do not replace.

## Logging

The tools log to stderr at `WARNING` by default; set
`VERIFIABLE_CLIENT_LOG_LEVEL` (for example to `DEBUG`) to change it. The
ducktape services append stderr to the stdout capture file
(`verifiable_consumer.stdout`, `verifiable_producer.stdout`), after the JSON
stream they parse. Those files are collected into the test's results only
when the test fails.
