<!--
Copyright 2026 Confluent Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Verifiable clients, chaos and soak

The JavaScript binding has a producer only, so the tools here are the
producer's: the verifiable producer for Kafka's system tests, the producer
workload of the chaos harness, and a producer soak. Each runs over the public
API (`@confluentinc/kafka-javascript/producer`), which the files import by the
package's own name (Node's self-reference), so they run from this checkout
once the package is built (`make -C javascript build`).

| File | What it is | Python counterpart |
|---|---|---|
| `verifiable_producer.js` | `org.apache.kafka.tools.VerifiableProducer` | `python/verifiable_clients/verifiable_producer.py` |
| `throughput_throttler.js` | `org.apache.kafka.server.util.ThroughputThrottler` | `python/verifiable_clients/throughput_throttler.py` |
| `_common.js` | argument parser, properties reader, `printJson`, shutdown hook | `python/verifiable_clients/_common.py` |
| `chaos_workload.js` | the chaos harness's `ChaosWorkloadService`, producer only | `python/grpc_chaos.py` |
| `soak/soakclient.js`, `soak/soak_metrics.js` | the producer half of the soak | `python/soak/soakclient.py`, `soak_metrics.py` |
| `test/` | their tests (`node --test`) | `python/verifiable_clients/test/`, `python/soak/test/` |

## Verifiable producer

`verifiable_producer.js` takes Java's arguments and prints Java's JSON events
(the same names, fields and field order), so Kafka's ducktape tests run it in
place of the Java tool through `VerifiableClientApp`:

    {
      "VerifiableProducer": {
        "class": "kafkatest.services.verifiable_client.VerifiableClientApp",
        "exec_cmd": "node /opt/kafka-dev/tests/js-clients/javascript/verifiable_clients/verifiable_producer.js",
        "pids": "pgrep -f /opt/kafka-dev/tests/js-clients/javascript/verifiable_clients/verifiable_producer.js",
        "kill_signal": 15
      }
    }

Copy a Linux build of `javascript/` (its `package.json`, `dist/`,
`native/build/Release/` and this directory) under the Kafka checkout's `tests/`,
built as `python/verifiable_clients/README.md` describes for the Python tools;
the nodes need Node.js 22.14 or later. There is no verifiable consumer: the
binding has no consumer yet, so the consumer side of a system test must use
the Java, Python or Rust tool.

Where the JavaScript tool differs from Java's:

- `exception` in `producer_send_error` names the client's class by the import
  path that exports it (`class @confluentinc/kafka-javascript/common.KafkaError`),
  or by its bare name for a class the client does not export.
- `avg_throughput` is `0` when the run ends in its first millisecond, where
  Java prints `"Infinity"` (as the Python tool does).
- A negative `--message-create-time` is a `producer_send_error`, not the end
  of the producing thread (as the Rust and Python tools do).
- Command-line errors carry Python's argparse messages, since argparse4j is a
  port of argparse.

`VERIFIABLE_CLIENT_LOG_LEVEL` (`DEBUG`, `INFO`, `WARNING`, the default, or
`ERROR`) sets the tool's own log level on stderr; the core logs to stderr
under `RUST_LOG`.

## Chaos

`chaos_workload.js` is the `ChaosWorkloadService` of
`rust/multilanguage-test-server/proto/chaos_service.proto` for the JavaScript
gRPC server (`javascript/grpc-server/`, image
`confluent-kafka-rust/javascript-grpc-server`, which copies the module in).
Its producer loop is the asyncio Python server's, step for step: it sends on
the harness's schedule, awaiting each `send()` to its hand-over only, and
reports every record from its delivery callback. The harness runs it as

    cargo xtask chaos --workload producer:javascript --workload consumer:rust --cycles 3

(`Backend::Js` in `rust/tests/chaos/workload.rs`, over `BackendKind::Js`);
`consumer:javascript` is rejected before a run starts. The server registers
the service with

    const { ChaosWorkloadService } = require('../verifiable_clients/chaos_workload');
    const chaos = new ChaosWorkloadService({ errorToProto: kafkaErrorToProto });
    server.addService(pkg.ChaosWorkloadService.service, chaos.handlers());

and calls `await chaos.stopAll()` at shutdown, before it closes its producers.

## Soak

`soak/soakclient.js` is the producer half of the Python soak, run the same way
and with the same flags, configuration file and JSONL metrics schema (prefix
`kafka.client.soak.rust_javascript.`):

    node soak/soakclient.js -i soak1 -t my-soak-topic -r 80 -f ccloud.config \
        --variant 848-normal --payload-size 50

With no consumer, it judges what a producer can: every record's delivery
callback runs exactly once (a missing or a second report fails the run), and
`close()` succeeds. A partition's delivered offsets that go backwards are
counted (`producer.offset_regression`) but do not fail it, since a retry
without idempotence may reorder batches. The records carry the Python soak's
`SoakRecord` payload, so a Python or .NET soak consumer on the same topic does
the end-to-end loss accounting. Its exit codes are the Python soak's (0 PASS,
1 a delivery-report failure, 2 a configuration the client rejects, 4 a
shutdown that wedged past `--shutdown-timeout`), so `python/soak/run.sh`'s
restart policy applies to it.

Not ported from the Python soak: the consumer and its accounting, topic
creation (the binding has no admin client), the OpenTelemetry pipeline (the
package has no OpenTelemetry dependency; the JSONL file is written always),
and the EC2, bootstrap, build and supervisor scripts.

## Tests

    make -C javascript test-verifiable-clients

builds the package and runs `test/*.test.js`. They need no broker: the
producer logic runs over a stand-in for Java's `MockProducer`, which the
binding does not translate yet (`test/_mock_producer.js`), and the scripts run
end to end against an unreachable broker.
