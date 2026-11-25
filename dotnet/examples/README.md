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
| `AdminApis` | One example per Admin API, 45 in all: `AdminApis <bootstrap-brokers> <example> ..` runs the one named. See [Admin API examples](#admin-api-examples). |

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

The producers, `AdminClient` and `AsyncAdminClient` take `<bootstrap-brokers> <topic>`, the consumers
`<bootstrap-brokers> <group> <topic1> <topic2> ..`, and `AdminApis` the name of an example and that
example's own arguments (below).

Start a second consumer in the same group to see a rebalance: each consumer prints the partitions
it is assigned and the ones revoked from it.

`-c Release` matters: the examples reference the client's source, and a Release build loads the
native library `make -C dotnet build-dotnet` builds, while a Debug build looks for a debug build of
it. The examples target `net10.0` and run on .NET 10 or any later runtime.

To build an example against the `Confluent.Kafka` NuGet package instead, replace the
`ProjectReference` in its `.csproj` with the `PackageReference` in the comment beside it.

## Admin API examples

The `AdminApis` project holds one example per Admin API, each in its own file named after the `IAdmin`
method it calls. The first argument is the brokers, the second the example, and the rest are that
example's own:

```bash
cd dotnet/examples
dotnet run -c Release --project AdminApis -- localhost:9092 CreateTopics orders:3:1
dotnet run -c Release --project AdminApis -- localhost:9092 DescribeTopics orders
dotnet run -c Release --project AdminApis -- localhost:9092 DescribeConfigs --help   # that example's arguments
dotnet run -c Release --project AdminApis -- --help                                  # the list of examples
```

The example's name is matched without regard to case or separators: `CreateTopics`, `create_topics`
and `create-topics` are the same. `-X key=value`, after the name, sets a client configuration
property and can be repeated, for example `-X security.protocol=SASL_SSL`. With
`-X bootstrap.controllers=..` the client connects to the KRaft controllers instead of the brokers, and
the first argument is not used. A bare `--` ends the options, so an argument that starts with `--`,
such as a password, goes after it: `AlterUserScramCredentials -- UPSERT alice SCRAM-SHA-256 8192 --secret`.

| Area | Examples |
|---|---|
| Topics | `CreateTopics`, `ListTopics`, `DescribeTopics`, `DeleteTopics`, `CreatePartitions` |
| Records and offsets | `DeleteRecords`, `ListOffsets` |
| Cluster and configs | `DescribeCluster`, `DescribeConfigs`, `IncrementalAlterConfigs`, `ListConfigResources`, `DescribeFeatures`, `UpdateFeatures`, `UnregisterController` |
| Replicas | `DescribeLogDirs`, `DescribeReplicaLogDirs`, `AlterReplicaLogDirs`, `AlterPartitionReassignments`, `ListPartitionReassignments`, `ElectLeaders` |
| Groups | `ListGroups`, `DescribeConsumerGroups`, `DescribeClassicGroups`, `ListConsumerGroupOffsets`, `AlterConsumerGroupOffsets`, `DeleteConsumerGroupOffsets`, `DeleteConsumerGroups`, `RemoveMembersFromConsumerGroup` |
| ACLs | `CreateAcls`, `DescribeAcls`, `DeleteAcls` |
| Quotas and SCRAM | `DescribeClientQuotas`, `AlterClientQuotas`, `DescribeUserScramCredentials`, `AlterUserScramCredentials` |
| Delegation tokens | `CreateDelegationToken`, `DescribeDelegationToken`, `RenewDelegationToken`, `ExpireDelegationToken` |
| Transactions | `ListTransactions`, `DescribeTransactions`, `DescribeProducers`, `AbortTransaction`, `FenceProducers`, `ForceTerminateTransaction` |

Every call returns its Result at once. An example reads each of the Result's `KafkaFuture`s with
`Get()` and prints one line per key (topic, partition, group, ..): the outcome on stdout, a failure on
stderr.

Exit codes: `0` success, `1` the client was not created or an operation failed, `2` a mistake on the
command line (the message and the example's usage are printed).

Some examples need more than the broker started above. The `apache/kafka` image uses its default
configuration only while no `KAFKA_*` variable is set; once one is set, every setting has to be given.
This command starts one broker with what the single-broker examples need:

```bash
docker run -d --rm --name kafka -p 9092:9092 -p 9094:9094 \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,SASL_PLAINTEXT://0.0.0.0:9094,CONTROLLER://0.0.0.0:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092,SASL_PLAINTEXT://localhost:9094 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_LOG_DIRS=/tmp/kafka-logs-0,/tmp/kafka-logs-1 \
  -e KAFKA_AUTHORIZER_CLASS_NAME=org.apache.kafka.metadata.authorizer.StandardAuthorizer \
  -e KAFKA_SUPER_USERS='User:ANONYMOUS;User:admin' \
  -e KAFKA_DELEGATION_TOKEN_SECRET_KEY=examples-secret \
  -e KAFKA_SASL_ENABLED_MECHANISMS=PLAIN -e KAFKA_OPTS=-Dexamples.unused=1 \
  -e KAFKA_LISTENER_NAME_SASL__PLAINTEXT_PLAIN_SASL_JAAS_CONFIG='org.apache.kafka.common.security.plain.PlainLoginModule required user_admin="admin-secret";' \
  apache/kafka:4.4.0
```

- **ACLs** need an authorizer (`KAFKA_AUTHORIZER_CLASS_NAME`) and the client's principal among the
  super users (`User:ANONYMOUS` over PLAINTEXT).
- **Delegation tokens** need `KAFKA_DELEGATION_TOKEN_SECRET_KEY` on the broker and a client that
  connects with SASL or two-way TLS: here the SASL listener on port 9094, which the image only
  accepts with `KAFKA_OPTS` set. The client supports the PLAIN mechanism:

  ```bash
  dotnet run -c Release --project AdminApis -- localhost:9094 CreateDelegationToken \
    -X security.protocol=SASL_PLAINTEXT -X sasl.mechanism=PLAIN \
    -X 'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret";'
  ```

- **Replica moves** need a broker with several `log.dirs` (`KAFKA_LOG_DIRS`).
- **Transactions** need a transactional producer running, and on a single broker the two
  `KAFKA_TRANSACTION_STATE_LOG_*` settings.
- **Reassignments and preferred leader elections** need several brokers, which this command does not
  start.

## Notes

- **Delivery callback.** The producer calls `ICallback.OnCompletion(metadata, exception)` on its
  background completion thread, `exception` being `null` on success. `AsyncKafkaProducer` also takes
  an `IAsyncCallback`, whose `Task` the producer waits for before it handles its next completion.
  `RecordMetadata` carries no key or value, so the producers give each record its own callback object,
  which prints the value.
- **Send future.** `Send()` returns a `KafkaFuture` of the delivery: `producer.Send(record).Get()`
  gives the `RecordMetadata`. On `AsyncKafkaProducer`, awaiting `Send()` waits for buffer space and
  returns an `AsyncKafkaFuture`, so `await (await producer.Send(record)).Get()` gives it.
- **Serializers.** The blocking clients take an `ISerializer<T>` or an `IDeserializer<T>`
  (`StringSerializer`, `StringDeserializer`). The async clients take an `IAsyncSerializer<T>` or an
  `IAsyncDeserializer<T>`, so `AsyncProducer` and `AsyncConsumer` pass the `Async` class of the same
  built-in (`AsyncStringSerializer`, `AsyncStringDeserializer`), which the client calls without
  awaiting. The package README describes serializers of one's own.
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
