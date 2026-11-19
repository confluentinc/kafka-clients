# Confluent.Kafka

Confluent's .NET client for Apache Kafka. All the Kafka logic, from batching and partitioning to
retries and the group protocol, runs in the Confluent Kafka Rust client, which this package ships
as a native library. Its API follows the Apache Kafka Java client. Each client has a blocking form
and an async form. Each type is in the namespace of its Java package, without `org.apache.kafka`
and `clients`.

This is a pre-release of a new major version. Its API is not the API of Confluent.Kafka 2.x (the
librdkafka-based client).

## Namespaces

- <xref:Confluent.Kafka.Producer>: the producer, its records and its delivery callbacks.
- <xref:Confluent.Kafka.Consumer>: the consumer, its records, rebalance listeners and offset
  commit callbacks.
- <xref:Confluent.Kafka.Admin>: the admin client, its options and its results.
- <xref:Confluent.Kafka.Common>: the futures, `TopicPartition`, `KafkaException` and the
  namespaces below it (serialization, headers, errors, configuration, ACLs).
- <xref:Confluent.Kafka>: the exceptions that stand for Java's built-in exceptions.
