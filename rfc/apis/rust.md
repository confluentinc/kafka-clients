# RFC: Rust client API

- **Status:** Proposed
- **Audience:** users of Confluent's Rust client

## Summary

The Rust client is the core from which the clients for the other languages are built.

The client is translated from the Apache Kafka Java client. By deriving the Rust client from the Apache Kafka Java client, it benefits from complete, sustained parity with Apache Kafka with the aim of eliminating the delay between features being delivered in Apache Kafka and being available in non-Java clients. Best practice and guidance for using the Apache Kafka Java client also apply to the Rust client.

In some cases, the derivation of the Rust client is evident in the detail of the Rust interfaces. A balance is being struck between following the Java interface and creating a natural Kafka client interface for Rust.

## Distribution

* **Crate:** published to `crates.io`
* **Install:** `cargo add confluent-kafka`
* **Import:** `confluent_kafka`, e.g. `use confluent_kafka::producer::KafkaProducer;`

## API shape

The client is translated from the Apache Kafka Java client and follows Rust conventions. The governing translation rules live in this repository's `CLAUDE.md`. Those rules apply to the Rust core only; there is no equivalent per-language translation document for the other clients, because the Python, .NET, JavaScript, and C/C++ clients are thin bindings over the core's C ABI and inherit its behavior rather than re-deriving it from Java.

The Rust client keeps the same architecture, package structure, and names as the Java source, adapted to Rust conventions. These principles govern every translation decision.

At the API level the key points for the Rust core are:

* **Async, Tokio-based I/O.** Methods that block in Java are `async` in Rust; methods that do not block in Java stay synchronous.

* **Java-aligned names and structure,** adapted to Rust naming (modules, `snake_case` methods, `Error` types in place of Java exceptions).

* **Result-based error handling** with a flat `Error` enum that carries error details and hierarchy predicates such as `is_retriable_error`, rather than exceptions.

* **Idiomatic ownership and zero-copy** on the hot paths (produce and consume), so the Rust client is a first-class client in its own right and not only a substrate for the bindings.

## Relationship to the bindings

The Python, .NET, JavaScript, and C/C++ clients are thin bindings over this core. Keeping the core's API aligned with Java is what lets those bindings present a Java-aligned API in each language without per-language protocol work.

## The Rust client interface

### Naming conventions

The names in Java have a clear mapping to their equivalents in Rust. The following table gives some examples:

| Java                                               | Rust                                                |
|----------------------------------------------------|-----------------------------------------------------|
| `package org.apache.kafka.clients.consumer`        | `module consumer`                                   |
| `class ProducerRecord` (PascalCase)                | `ProducerRecord struct/enum` (PascalCase)           |
| `method maybeThrowAnyException` (camelCase)        | `maybe_return_any_error` (snake_case)               |
| `const CommonClientConfigs.RETRY_BACKOFF_EXP_BASE` | `CommonClientConfigs::RETRY_BACKOFF_EXP_BASE`     |
| `throw / throws`                                   | `return Err(...) / Result<T, Error>`                |
| Java "thread"                                      | Rust "task" (e.g. in log messages)                  |

### Concurrency and callbacks

A method that blocks in Java becomes `async` in Rust. A method that must hand work to the client's background task (for example `Consumer::pause`, `assign` or `seek_to_beginning`) is also `async`, even though its Java equivalent does not block. Pure reads of local state (for example `assignment()` or `paused()`) stay synchronous.

Where Java uses `thread.join()` or `Future.get()` to block until completion, the Rust application must actually `.await` the handle.

### Overloaded methods

Rust has no method overloading, so every Java overloaded method must be given a distinct Rust name. The Rust names are derived from the Java signature by appending the Java argument or type names. For example, the Java method `KafkaConsumer.commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout)` is one of 4 overloaded methods. The Rust equivalent is:

```rust
async fn commit_sync_with_offsets_timeout(
    &mut self,
    offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    timeout: Duration,
) -> Result<(), Error>;
```

In the most complicated cases where more than three parameters would appear in a derived name, an Options structure is introduced (with the disadvantage that the compiler can no longer catch invalid combinations of parameters).

### Getters and setters

A Java getter and setter that share the same method name are translated as `<field_name>` for the getter and `set_<field_name>` for the setter — for example `AbstractOptions.timeoutMs(Integer) / timeoutMs()` become `set_timeout_ms(..) / timeout_ms()` in Rust.

A Rust setter method uses `set_<field_name>` even when there is no corresponding getter with the same name. For example, `CreateTopicsOptions.validateOnly(boolean)` becomes `set_validate_only(..)` although its getter is the differently named `shouldValidateOnly()` → `should_validate_only()`. Where Java's pair is symmetric, as in `CreatePartitionsOptions`, the getter takes the plain `validate_only()`.

### Error handling

Every API which can fail returns `Result<T, Error>`. `Error (common::Error)` is a single flat enum with no Java counterpart: Rust cannot express Java's exception class hierarchy, so one type holds both `KafkaException`'s subclasses and the generic `java.lang / java.util` runtime exceptions that sit beside it. Java's `KafkaException` base class maps to an embedded `KafkaError struct`; every other exception is a variant carrying its own struct.

Rust can propagate the error to the caller automatically, without all the conditions needed in Golang and similarly to Java exception propagation, by appending `?` to the expression returning the error — provided the called function's error type is assignable to the calling function's. Almost every API returns the top-level `Error` enum, so this holds. The exception is the `ProducerRecord` constructors, which return `LocalIllegalArgumentError` and must be wrapped explicitly: `ProducerRecord::with_partition_key(..).map_err(Error::LocalIllegalArgument)?`.

#### Inspecting an error

* `message()` — the text, translating `Throwable.getMessage()`.

* Match a specific error as an enum variant, e.g. `Error::Timeout(_)` or `Error::RecordTooLarge(_)`.

#### The ErrorHierarchy predicates

Because the hierarchy is flattened, every intermediate (non-leaf) Java class is recoverable as an inherent predicate method on `Error` — the reader has no other way to see the extends chain. The family of predicates is: `is_kafka_error`, `is_api_error`, `is_retriable_error`, `is_refresh_retriable_error`, `is_invalid_metadata_error`, `is_invalid_configuration_error`, `is_authentication_error`, `is_authorization_error`, and so on.

The predicates are not complements of one another: `SerializationException` and `WakeupException` are `KafkaException`s that are not `ApiException`s, so they answer true to `is_kafka_error()` and false to `is_api_error()`.

## Examples
Each example shows the same operation in the Java API and its Rust translation.

### Producer example - send a record and block for its acknowledgement

#### Java

```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
producer.send(new ProducerRecord<>("my-topic", "key", "value")).get();
producer.close();
```

#### Rust

```rust
let props = HashMap::from([
    ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
]);
let config = ProducerConfig::new(&props)?;
let producer = KafkaProducer::<String, String>::new(
    config, Box::new(StringSerializer::new()), Box::new(StringSerializer::new()),
)?;
let record = ProducerRecord::with_key("my-topic".to_string(), Some("key".to_string()), Some("value".to_string()));
producer.send(record).await?.get().await?;
producer.close().await?;
```

### Consumer example - subscribe to topics and poll a batch of records in a loop

Java's subscribe has six overloads with an empty parameter-name intersection, so no Rust method keeps the plain name subscribe: the topic-collection form is `subscribe_with_topics`.

#### Java

```java
KafkaConsumer<byte[], byte[]> consumer =
    new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
consumer.subscribe(Arrays.asList("foo", "bar"));
while (true) {
    ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
    for (ConsumerRecord<byte[], byte[]> record : records)
        System.out.printf("offset = %d%n", record.offset());
}
```

#### Rust

```rust
let mut consumer = KafkaConsumer::new::<Vec<u8>, Vec<u8>>(
    config, Box::new(ByteArrayDeserializer::new()), Box::new(ByteArrayDeserializer::new()),
)?;
consumer.subscribe_with_topics(vec!["foo".into(), "bar".into()]).await?;
loop {
    let records = consumer.poll(Duration::from_millis(100)).await?;
    for record in &records {
        println!("offset = {}", record.offset());
    }
}
```

### Consumer example - with a rebalance listener

Subscribe with a rebalance callback that fires when partitions are assigned or revoked (e.g. to commit offsets before losing a partition), then poll a batch. In Java, this is a `ConsumerRebalanceListener` passed to subscribe; in Rust an `Arc<dyn ConsumerRebalanceListener>` passed to `subscribe_with_topics_listener`.

The listener runs on the caller's poll task in every client: Java invokes it inside `poll()`, and the Rust translation preserves that — the callback executes on the task that calls `poll()`, never on the background task. Because `poll` borrows the consumer as `&mut self`, a Rust listener cannot capture the consumer itself as a Java listener does; instead it captures the `ConsumerHandle` returned by `consumer.handle()`, which exposes the operations that are safe to call from inside a callback (`commit_sync`, `seek_*`, `pause`/`resume`, `position`, ...). In the future, the `RebalanceListener` interface from KIP-1306 will resolve this more neatly.

#### Java

```java
consumer.subscribe(Arrays.asList("foo", "bar"), new ConsumerRebalanceListener() {
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        consumer.commitSync(); // flush offsets before the partitions move
    }
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.println("assigned: " + partitions);
    }
});
while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
    for (ConsumerRecord<String, String> record : records)
        System.out.printf("offset = %d, value = %s%n", record.offset(), record.value());
}
```

#### Rust

```rust
struct CommittingRebalanceListener {
    consumer: ConsumerHandle,
}
#[async_trait]
impl ConsumerRebalanceListener for CommittingRebalanceListener {
    async fn on_partitions_revoked(&self, _partitions: &[TopicPartition]) -> Result<(), Error> {
        self.consumer.commit_sync().await // flush offsets before the partitions move
    }
    async fn on_partitions_assigned(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        println!("assigned: {partitions:?}");
        Ok(())
    }
}

let listener: Arc<dyn ConsumerRebalanceListener> =
    Arc::new(CommittingRebalanceListener { consumer: consumer.handle() });
consumer
    .subscribe_with_topics_listener(vec!["foo".into(), "bar".into()], listener)
    .await?;
loop {
    let records = consumer.poll(Duration::from_millis(100)).await?;
    for record in &records {
        println!("offset = {}, value = {:?}", record.offset(), record.value());
    }
}
```

### AdminClient - Create a topic and block until the request completes or fails.

Java declares each admin operation as a pair — a default `<method_name>(args)` forwarding to `<method_name>(args, new <MethodName>Options())` — so Rust has a pair too: the no-options form owns the plain name and the options-taking form carries the `_with_options` suffix. `NewTopic`'s `(name, int, short)` and `(name, Optional<Integer>, Optional<Short>)` constructors differ only by `Optional`, so they collapse to a single Rust constructor taking `Option`.

#### Java

```java
CreateTopicsResult result = admin.createTopics(
    Set.of(new NewTopic("my-topic", 12, (short) 3)));
result.values().get("my-topic").get(); // block until created or failed
```

#### Rust

```rust
let result = admin.create_topics(&[
    NewTopic::with_num_partitions_replication_factor("my-topic", Some(12), Some(3)),
]);
result.all().get().await?; // block until created or failed
```

When you need to override the default options, use `create_topics_with_options` instead:

```rust
let result = admin.create_topics_with_options(
    &[NewTopic::with_num_partitions_replication_factor("my-topic", Some(12), Some(3))],
    CreateTopicsOptions::new().set_validate_only(true),
);
result.all().get().await?; // block until created or failed
```

## Interface Details

### Producer interface

The equivalent of the Java `Producer<K, V>` interface is a trait. Applications use `KafkaProducer<K, V>` which implements the trait, just like in Java.

Every method that blocks in Java is `async` in Rust. The Rust type system is very different than Java’s. The trait's asynchronous methods return `impl Future<Output = ...> + Send` rather than being declared `async fn`, so the futures are `Send` even in generic code over `impl Producer<K, V>`, and the send path allocates no boxed future per record. `KafkaProducer` implements them as ordinary `async fn`s.

Returning `impl Future` makes `Producer<K, V>` not dyn-compatible. To hold different producer implementations behind one type, as a Java `List<Producer<K, V>>` would, use `DynProducer<K, V>`: every `Producer` implements it automatically, and it returns each future boxed, so only callers that go through `dyn` pay for the allocation.

The `Producer<K, V>` trait has looser restrictions than the consumer equivalent. The methods of the `Producer<K, V>` trait take a non-mutable reference to the producer. The producer instance is shareable across threads.

```rust
pub trait Producer<K, V>: Send + Sync {
    fn init_transactions(&self) -> impl Future<Output = Result<(), Error>> + Send;
    fn begin_transaction(&self) -> Result<(), Error>;
    fn send_offsets_to_transaction(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        group_metadata: &dyn ConsumerGroupMetadata,
    ) -> impl Future<Output = Result<(), Error>> + Send;
    fn commit_transaction(&self) -> impl Future<Output = Result<(), Error>> + Send;
    fn abort_transaction(&self) -> impl Future<Output = Result<(), Error>> + Send;
    fn send(
        &self,
        record: ProducerRecord<K, V>,
    ) -> impl Future<Output = Result<KafkaFuture<RecordMetadata>, Error>> + Send;
    fn send_with_callback(
        &self,
        record: ProducerRecord<K, V>,
        callback: Option<Callback>,
    ) -> impl Future<Output = Result<KafkaFuture<RecordMetadata>, Error>> + Send;
    fn flush(&self) -> impl Future<Output = Result<(), Error>> + Send;
    fn partitions_for(&self, topic: &str) -> impl Future<Output = Result<Vec<PartitionInfo>, Error>> + Send;
    fn metrics(&self) -> HashMap<MetricName, Arc<KafkaMetric>>;
    fn close(&self) -> impl Future<Output = Result<(), Error>> + Send;
    fn close_with_timeout(&self, timeout: Duration) -> impl Future<Output = Result<(), Error>> + Send;
}
```

The application uses `KafkaProducer<K, V>`.

```rust
impl<K, V> KafkaProducer<K, V> {
    pub fn new(
        config: ProducerConfig,
        key_serializer: Box<dyn Serializer<K> + Send + Sync>,
        value_serializer: Box<dyn Serializer<V> + Send + Sync>,
    ) -> Result<Self, Error>;
}

// Send a record with borrowed byte-slice key/value, bypassing serialization.
// This is the zero-copy path for callers that already have `&[u8]` data.
impl KafkaProducer<Vec<u8>, Vec<u8>> {
    pub async fn send(
        &self,
        record: ProducerRecord<&[u8], &[u8]>,
        callback: Option<Callback>,
    ) -> Result<KafkaFuture<RecordMetadata>, Error>;
}
```

The `ProducerRecord` looks like this:

```rust
impl<K, V> ProducerRecord<K, V> {
    pub fn new(topic: String, value: Option<V>) -> Self;
    pub fn with_key(topic: String, key: Option<K>, value: Option<V>) -> Self;
    pub fn with_partition_key(
        topic: String,
        partition: Option<i32>,
        key: Option<K>,
        value: Option<V>,
    ) -> Result<Self, LocalIllegalArgumentError>;
    pub fn with_partition_key_headers(
        topic: String,
        partition: Option<i32>,
        key: Option<K>,
        value: Option<V>,
        headers: RecordHeaders,
    ) -> Result<Self, LocalIllegalArgumentError>;
    pub fn with_partition_timestamp_key(
        topic: String,
        partition: Option<i32>,
        timestamp: Option<i64>,
        key: Option<K>,
        value: Option<V>,
    ) -> Result<Self, LocalIllegalArgumentError>;
    pub fn with_options(options: ProducerRecordOptions<K, V>)
        -> Result<Self, LocalIllegalArgumentError>;
    pub fn topic(&self) -> &str;
    pub fn partition(&self) -> Option<i32>;
    pub fn timestamp(&self) -> Option<i64>;
    pub fn key(&self) -> Option<&K>;
    pub fn value(&self) -> Option<&V>;
    pub fn headers(&self) -> &RecordHeaders;
    pub fn headers_mut(&mut self) -> &mut RecordHeaders;
    pub fn into_parts(self)
        -> (String, Option<i32>, Option<i64>, RecordHeaders, Option<K>, Option<V>);
}
```

### Consumer interface

The equivalent of the Java `Consumer<K, V>` interface is a trait. Applications use `KafkaConsumer<K, V>` which implements the trait, just like in Java.

Every method that blocks in Java is `async` in Rust. The Rust type system is very different than Java’s. The trait is an `#[async_trait]`, so each async method actually returns `Pin<Box<dyn Future<Output = Result<..., Error>> + Send + '_>>`. Unlike Java's `KafkaFuture`, a Rust future is lazy: the operation does not start until it is `.await`ed.

The `Consumer<K, V>` trait and its type parameters are both `Send + 'static` because their ownership can be transferred between threads and their lifetime can exist for the duration of the program.

Most of the methods of the `Consumer<K, V>` trait take a mutable reference to the consumer. This is because, just like in Java, the consumer instance is not shareable across threads.

Only the new KIP-848 consumer group rebalance protocol (`group.protocol=consumer`) is supported. Note that `group.protocol` still defaults to `classic` (matching Java), and `KafkaConsumer::new` returns an `UnsupportedVersion` error for it, so every consumer configuration must set `group.protocol=consumer` explicitly.

```rust
#[async_trait]
pub trait Consumer<K, V>: Send + 'static
where
    K: Send + 'static,
    V: Send + 'static,
{
    // --- State reads: do not block in Java, so synchronous here. ---
    fn assignment(&self) -> HashSet<TopicPartition>;
    fn subscription(&self) -> HashSet<String>;
    fn paused(&self) -> HashSet<TopicPartition>;
    fn group_metadata(&self) -> Arc<dyn ConsumerGroupMetadata>;
    fn client_id(&self) -> &str;
    fn current_lag(&self, topic_partition: &TopicPartition) -> Option<i64>; // currently always returns None
    fn metrics(&self) -> HashMap<MetricName, Arc<KafkaMetric>>;
    // --- Subscription and assignment ---
    async fn subscribe_with_topics(&mut self, topics: Vec<String>) -> Result<(), Error>;
    async fn subscribe_with_topics_listener(
        &mut self,
        topics: Vec<String>,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> Result<(), Error>;
    async fn subscribe_with_pattern(&mut self, pattern: SubscriptionPattern) -> Result<(), Error>;
    async fn subscribe_with_pattern_listener(
        &mut self,
        pattern: SubscriptionPattern,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> Result<(), Error>;
    async fn assign(&mut self, partitions: Vec<TopicPartition>) -> Result<(), Error>;
    async fn unsubscribe(&mut self) -> Result<(), Error>;
    // --- Fetching ---
    async fn poll(&mut self, timeout: Duration) -> Result<ConsumerRecords<K, V>, Error>;
    // --- Committing ---
    async fn commit_sync(&mut self) -> Result<(), Error>;
    async fn commit_sync_with_timeout(&mut self, timeout: Duration) -> Result<(), Error>;
    async fn commit_sync_with_offsets(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<(), Error>;
    async fn commit_sync_with_offsets_timeout(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        timeout: Duration,
    ) -> Result<(), Error>;
    async fn commit_async(&mut self) -> Result<(), Error>;
    async fn commit_async_with_callback(&mut self, callback: Arc<dyn OffsetCommitCallback>) -> Result<(), Error>;
    async fn commit_async_with_offsets_callback(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        callback: Arc<dyn OffsetCommitCallback>,
    ) -> Result<(), Error>;
    // --- Positioning ---
    async fn seek_with_offset(&mut self, partition: TopicPartition, offset: i64) -> Result<(), Error>;
    async fn seek_with_offset_and_metadata(
        &mut self,
        partition: TopicPartition,
        offset_and_metadata: OffsetAndMetadata,
    ) -> Result<(), Error>;
    async fn seek_to_beginning(&mut self, partitions: &[TopicPartition]) -> Result<(), Error>;
    async fn seek_to_end(&mut self, partitions: &[TopicPartition]) -> Result<(), Error>;
    async fn position(&mut self, partition: &TopicPartition) -> Result<i64, Error>;
    async fn position_with_timeout(&mut self, partition: &TopicPartition, timeout: Duration) -> Result<i64, Error>;
    async fn committed(
        &mut self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, Error>;
    async fn committed_with_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, Error>;
    // --- Metadata and offset lookup ---
    async fn partitions_for(&mut self, topic: &str) -> Result<Vec<PartitionInfo>, Error>;
    async fn partitions_for_with_timeout(
        &mut self,
        topic: &str,
        timeout: Duration,
    ) -> Result<Vec<PartitionInfo>, Error>;
    async fn list_topics(&mut self) -> Result<HashMap<String, Vec<PartitionInfo>>, Error>;
    async fn list_topics_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<HashMap<String, Vec<PartitionInfo>>, Error>;
    async fn offsets_for_times(
        &mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
    ) -> Result<HashMap<TopicPartition, OffsetAndTimestamp>, Error>;
    async fn offsets_for_times_with_timeout(
        &mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, OffsetAndTimestamp>, Error>;
    async fn beginning_offsets(&mut self, partitions: &[TopicPartition]) -> Result<HashMap<TopicPartition, i64>, Error>;
    async fn beginning_offsets_with_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, i64>, Error>;
    async fn end_offsets(&mut self, partitions: &[TopicPartition]) -> Result<HashMap<TopicPartition, i64>, Error>;
    async fn end_offsets_with_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, i64>, Error>;
    // --- Flow control ---
    async fn pause(&mut self, partitions: &[TopicPartition]) -> Result<(), Error>;
    async fn resume(&mut self, partitions: &[TopicPartition]) -> Result<(), Error>;
    // --- Lifecycle ---
    async fn enforce_rebalance(&mut self) -> Result<(), Error>;
    async fn enforce_rebalance_with_reason(&mut self, reason: &str) -> Result<(), Error>;
    async fn close(&mut self) -> Result<(), Error>;
    // Java's deprecated close(Duration) is not translated; pass a timeout through CloseOptions.
    async fn close_with_options(&mut self, options: CloseOptions) -> Result<(), Error>;
    fn wakeup(&self);
    fn handle(&self) -> ConsumerHandle;
}
```

The application uses `KafkaConsumer<K, V>`.

```rust
impl KafkaConsumer {
    pub fn new<K, V>(
        config: ConsumerConfig,
        key_deserializer: Box<dyn Deserializer<K>>,
        value_deserializer: Box<dyn Deserializer<V>>,
    ) -> Result<Box<dyn Consumer<K, V>>, Error>
    where
        K: Send + Sync + 'static,
        V: Send + Sync + 'static;
}
```

The `ConsumerRecord` looks like this:

```rust
impl<K, V> ConsumerRecord<K, V> {
    pub fn new(
        topic: impl Into<Arc<str>>,
        partition: i32,
        offset: i64,
        key: Option<K>,
        value: Option<V>,
    ) -> Self;
    pub fn with_options(options: ConsumerRecordOptions<K, V>) -> Self;
    pub fn topic(&self) -> &str;
    pub fn partition(&self) -> i32;
    pub fn offset(&self) -> i64;
    pub fn timestamp(&self) -> i64;
    pub fn timestamp_type(&self) -> TimestampType;
    pub fn serialized_key_size(&self) -> i32;
    pub fn serialized_value_size(&self) -> i32;
    pub fn key(&self) -> Option<&K>;
    pub fn value(&self) -> Option<&V>;
    pub fn headers(&self) -> &RecordHeaders;
    pub fn leader_epoch(&self) -> Option<i32>;
    pub fn delivery_count(&self) -> Option<i16>;
}
```

And `ConsumerRecords` which is returned when the consumer is polled looks like this:

```rust
impl<K, V> ConsumerRecords<K, V> {
    // Java's deprecated ConsumerRecords(Map) constructor is not translated.
    pub fn with_next_offsets(
        records: IndexMap<TopicPartition, Vec<ConsumerRecord<K, V>>>,
        next_offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Self;
    pub fn empty() -> Self;
    pub fn records_partition(&self, partition: &TopicPartition) -> &[ConsumerRecord<K, V>];
    pub fn records_topic<'a>(&'a self, topic: &'a str) -> impl Iterator<Item = &'a ConsumerRecord<K, V>> + 'a;
    pub fn partitions(&self) -> impl Iterator<Item = &TopicPartition>;
    pub fn count(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn next_offsets(&self) -> &HashMap<TopicPartition, OffsetAndMetadata>;
}
```

## More comprehensive examples
These are complete, compilable examples which you can also find in the [`rust/examples`](../../rust/examples) directory.

### Producer example

This producer sends 10 records and waits for each to be acknowledged.

```rust
use std::collections::HashMap;
use std::process::ExitCode;

use confluent_kafka::common::Error;
use confluent_kafka::common::serialization::StringSerializer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;

use uuid::Uuid;

const TOPIC_NAME: &str = "my-topic";
const NUM_RECORDS: usize = 10;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("producer failed: {e}");
            ExitCode::FAILURE
        },
    }
}

async fn run() -> Result<(), Error> {
    let producer = create_kafka_producer()?;
    println!("Kafka producer created successfully.");

    let result = produce_records(&producer).await;
    let close_result = producer.close().await;
    result.and(close_result)
}

async fn produce_records(producer: &KafkaProducer<String, String>) -> Result<(), Error> {
    for i in 0..NUM_RECORDS {
        let key = format!("key-{i}");
        let value = format!("value-{i}");

        println!("Producing record: key={key}, value={value}");
        let record = ProducerRecord::with_key(TOPIC_NAME.to_string(), Some(key), Some(value));

        // Synchronous produce: `send` returns a future for the record's delivery,
        // and awaiting `get` blocks until the broker has acknowledged it (or the
        // delivery has failed) before the next record is sent.
        let future = producer.send(record).await?;
        match future.get().await {
            Ok(metadata) => println!(
                "Delivered record {i} to topic {}, partition {} at offset {}",
                metadata.topic(),
                metadata.partition(),
                metadata.offset()
            ),
            Err(error) => {
                eprintln!("Failed to deliver record {i}: {error}");
                return Err(error);
            },
        }
    }
    Ok(())
}

fn bootstrap_servers() -> String {
    std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_string())
}

fn create_kafka_producer() -> Result<KafkaProducer<String, String>, Error> {
    let props = HashMap::from([
        (ProducerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(), bootstrap_servers()),
        (
            ProducerConfig::CLIENT_ID_CONFIG.to_string(),
            format!("client-{}", Uuid::new_v4()),
        ),
    ]);

    println!("Kafka producer properties: {props:?}");

    let config = ProducerConfig::new(&props)?;
    KafkaProducer::new(config, Box::new(StringSerializer::new()), Box::new(StringSerializer::new()))
}
```

### Producer with callback example

This producer sends 10 records and uses a delivery callback to track the completion of the records. 

```rust
use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::Mutex;
use confluent_kafka::common::Error;
use confluent_kafka::common::serialization::StringSerializer;
use confluent_kafka::producer::Callback;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;
use uuid::Uuid;
const TOPIC_NAME: &str = "my-topic";
const NUM_RECORDS: usize = 10;
#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("producer failed: {e}");
            ExitCode::FAILURE
        },
    }
}
async fn run() -> Result<(), Error> {
    let producer = create_kafka_producer()?;
    println!("Kafka producer created successfully.");
    // Delivery callbacks run on the producer's background task, so the first
    // delivery error is recorded here and checked once `close()` has flushed
    // every outstanding record and invoked every callback.
    let first_delivery_error: Arc<Mutex<Option<Error>>> = Arc::new(Mutex::new(None));
    let result = produce_records(&producer, &first_delivery_error).await;
    let close_result = producer.close().await;
    let delivery_result = match first_delivery_error.lock().unwrap().take() {
        Some(e) => Err(e),
        None => Ok(()),
    };
    result.and(delivery_result).and(close_result)
}
async fn produce_records(
    producer: &KafkaProducer<String, String>,
    first_delivery_error: &Arc<Mutex<Option<Error>>>,
) -> Result<(), Error> {
    for i in 0..NUM_RECORDS {
        let key = format!("key-{i}");
        let value = format!("value-{i}");
        println!("Producing record: key={key}, value={value}");
        let record = ProducerRecord::with_key(TOPIC_NAME.to_string(), Some(key), Some(value));
        let callback = delivery_callback(i, Arc::clone(first_delivery_error));
        producer.send_with_callback(record, Some(callback)).await?;
    }
    Ok(())
}
fn delivery_callback(index: usize, first_delivery_error: Arc<Mutex<Option<Error>>>) -> Callback {
    // Per the `Callback` contract, on failure `metadata` is still `Some` but holds a
    // sentinel `-1` offset (and `-1` partition if none could be resolved), so `error`
    // must be checked first to tell success from failure.
    Box::new(move |metadata, error| match error {
        Some(error) => {
            eprintln!("Failed to deliver record {index}: {error}");
            first_delivery_error.lock().unwrap().get_or_insert_with(|| error.clone());
        },
        None => match metadata {
            Some(metadata) => println!(
                "Delivered record {index} to topic {}, partition {} at offset {}",
                metadata.topic(),
                metadata.partition(),
                metadata.offset()
            ),
            None => eprintln!("Record {index} completed with neither metadata nor error"),
        },
    })
}
fn bootstrap_servers() -> String {
    std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_string())
}
fn create_kafka_producer() -> Result<KafkaProducer<String, String>, Error> {
    let props = HashMap::from([
        (ProducerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(), bootstrap_servers()),
        (
            ProducerConfig::CLIENT_ID_CONFIG.to_string(),
            format!("client-{}", Uuid::new_v4()),
        ),
    ]);
    println!("Kafka producer properties: {props:?}");
    let config = ProducerConfig::new(&props)?;
    KafkaProducer::new(config, Box::new(StringSerializer::new()), Box::new(StringSerializer::new()))
}
```

### Consumer example

This consumer polls for records with a 30-second timeout. It stops if no records are received within the timeout. If records are received, it prints the record content, and then commits the records just received. A rebalance listener prints every partition assignment, revocation and loss, and commits the consumed offsets before partitions are revoked.

```rust
use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use confluent_kafka::common::Error;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::serialization::StringDeserializer;
use confluent_kafka::consumer::Consumer;
use confluent_kafka::consumer::ConsumerConfig;
use confluent_kafka::consumer::ConsumerHandle;
use confluent_kafka::consumer::ConsumerRebalanceListener;
use confluent_kafka::consumer::GroupProtocol;
use confluent_kafka::consumer::KafkaConsumer;

use uuid::Uuid;

const TOPIC_NAME: &str = "my-topic";
const GROUP_ID: &str = "my-group";

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("consumer failed: {e}");
            ExitCode::FAILURE
        },
    }
}

async fn run() -> Result<(), Error> {
    let mut consumer = create_kafka_consumer()?;
    println!("Kafka consumer created successfully.");

    // The listener runs inside `poll()`, which already borrows the consumer
    // mutably, so it calls back into the consumer through a `ConsumerHandle`.
    let listener: Arc<dyn ConsumerRebalanceListener> = Arc::new(CommitOnRevokeListener { consumer: consumer.handle() });
    consumer
        .subscribe_with_topics_listener(vec![TOPIC_NAME.to_string()], listener)
        .await?;
    println!("Subscribed to topic: {TOPIC_NAME}");

    let handle = consumer.handle();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await; // needs tokio's "signal" feature
        handle.wakeup();
    });

    let result = consume_loop(&mut *consumer).await;
    let close_result = consumer.close().await;
    result.and(close_result)
}

async fn consume_loop(consumer: &mut dyn Consumer<String, String>) -> Result<(), Error> {
    loop {
        let records = match consumer.poll(Duration::from_secs(30)).await {
            Ok(records) => records,
            Err(e) if e.is_retriable_error() => {
                eprintln!("transient poll error, retrying: {e}");
                continue;
            },
            Err(Error::Wakeup(_)) => return Ok(()),
            Err(e) => return Err(e),
        };

        if records.is_empty() {
            println!("No records consumed in this poll interval.");
            return Ok(());
        }

        for record in records {
            println!("Consumed record: key={:?}, value={:?}", record.key(), record.value());
        }
        consumer.commit_sync().await?;
    }
}

/// Prints every change to the assignment, and commits the offsets consumed so
/// far before partitions are revoked, so the member that takes them over
/// resumes where this one stopped.
struct CommitOnRevokeListener {
    consumer: ConsumerHandle,
}

#[async_trait]
impl ConsumerRebalanceListener for CommitOnRevokeListener {
    async fn on_partitions_revoked(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        println!("Partitions revoked: {partitions:?}");
        self.consumer.commit_sync().await
    }

    async fn on_partitions_assigned(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        println!("Partitions assigned: {partitions:?}");
        Ok(())
    }

    async fn on_partitions_lost(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        // Lost partitions may already belong to another member, so a commit
        // could fail or overwrite theirs: only report them.
        println!("Partitions lost: {partitions:?}");
        Ok(())
    }
}

fn bootstrap_servers() -> String {
    std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_string())
}

fn create_kafka_consumer() -> Result<Box<dyn Consumer<String, String>>, Error> {
    let props = HashMap::from([
        (ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(), bootstrap_servers()),
        (ConsumerConfig::GROUP_ID_CONFIG.to_string(), GROUP_ID.to_string()),
        (
            ConsumerConfig::GROUP_PROTOCOL_CONFIG.to_string(),
            GroupProtocol::Consumer.to_string(),
        ),
        (
            ConsumerConfig::CLIENT_ID_CONFIG.to_string(),
            format!("client-{}", Uuid::new_v4()),
        ),
    ]);

    println!("Kafka consumer properties: {props:?}");

    let config = ConsumerConfig::new(&props)?;
    KafkaConsumer::new(config, Box::new(StringDeserializer::new()), Box::new(StringDeserializer::new()))
}
```