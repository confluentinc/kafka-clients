# Examples

Runnable programs that use the client against a real Kafka broker:
`producer.js`, which produces one record, and `admin/`, one example per call
of the admin client, 45 in all. They import the package by its own name
(`require('@confluentinc/kafka-javascript/admin')`), which resolves inside
this directory through `package.json`'s `exports`, so they run without an
`npm install`, against the compiled `dist/`: build first, from the repository
root:

```bash
make -C rust build-all-features
(cd javascript && npm ci)
make -C javascript build
```

They use the client as an app does: they await what it returns, close it in
a `finally`, and read a failed key's error as the `cause` of the
`ExecutionError` its future's `get()` rejects with. `make -C javascript
typecheck-examples` type-checks them against the package's own types, and
`make -C javascript lint-rules` lints them.

## Start a broker

A single-node Kafka in Docker. On a one-broker cluster the internal topics'
replication factor must be lowered to 1, which the settings below do (the
transaction calls need the transaction-state topic):

```bash
docker run -d --name kafka-examples -p 9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  apache/kafka:4.3.1
```

Tear down with `docker rm -f kafka-examples`, which also deletes the topics.

Some calls need more than this broker has, and report the broker's error
instead (exit code 1): the ACL calls need an authorizer, the delegation-token
calls a token secret and a SASL listener, `alterReplicaLogDirs` a second log
directory, and a reassignment or an election more than one broker.

## The producer example

```bash
node javascript/examples/producer.js localhost:9092 js-demo k1 hello-from-node
```

It takes the brokers and the topic, and optionally a key and a value, as
positional arguments, and prints the record's `RecordMetadata`. Without the
two arguments it prints its usage and exits with 2.

## The admin examples

```bash
node javascript/examples/admin/<rpc>.js [-X key=value]... [--flag] [--name=value]... [<argument>]...
```

`<rpc>` is the call's Java name in snake_case (`create_topics`,
`describe_consumer_groups`, `list_offsets`, ...). For example:

```bash
node javascript/examples/admin/create_topics.js orders:3:1
node javascript/examples/admin/describe_topics.js orders
node javascript/examples/admin/list_topics.js
node javascript/examples/admin/delete_topics.js orders
```

Every admin example shares one command line (`admin/common.js`):

- `KAFKA_BOOTSTRAP_SERVERS` names the brokers, `localhost:9092` by default.
- `-X key=value` sets a client configuration key, Java's name, and may be
  repeated: `-X security.protocol=SASL_SSL -X sasl.mechanism=PLAIN`.
- `--flag` and `--name=value` are the example's own options; `--help` prints
  its usage, which lists them.
- The rest are its positional arguments.

The exit code is:

| Code | Meaning |
|---|---|
| 0 | every call succeeded (and `--help`) |
| 1 | a call, or one of its keys, failed: the error is printed, and the example goes on with the other keys |
| 2 | the command line is wrong: the problem and the usage are printed |

Each example is the JavaScript twin of the Rust example of the same name in
`rust/examples/admin/` (`create_topics.js` and `create_topics.rs`): it takes
the same options and arguments, and prints the same lines in the same order,
so a command carries over between the clients. `test/integration/admin_examples.test.js`
checks that the two directories name the same examples, and runs each
JavaScript one against the scratch broker of `make -C javascript
test-integration`.
