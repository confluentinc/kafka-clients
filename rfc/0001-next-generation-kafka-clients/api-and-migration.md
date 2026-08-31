# API alignment and migration

Part of [RFC-0001](README.md).

## Why align with the Java API

The new client aligns its API and configuration with the Apache Kafka Java client. A
shared contract across languages means documentation, examples, and answers found online
transfer between languages; developers moving between C/C++, Python, JavaScript, .NET, and
Java reuse their knowledge; and each new KIP maps from Java to every language without
per-language API design.

This is a deliberate departure from librdkafka's API. Keeping librdkafka's surface would
perpetuate the divergence from Java that causes friction today, especially around
differing configuration defaults.

## Categories of change

Migrating an existing application involves targeted updates, not a rewrite. The changes
fall into three categories:

1. **Configuration.** Config keys align with the Java client. librdkafka-specific keys
   (for example `message.max.bytes`) map to their Java equivalents (for example
   `max.request.size`). A full mapping is published, and the migration tooling translates
   configuration automatically. Any divergence from the Java client's configuration is
   introduced through a KIP.
2. **API surface.** Method names and signatures align with the Java client. For example,
   Python's `Consumer.poll()` returns a single message today, while Java's
   `KafkaConsumer.poll()` returns a collection of records; the new client follows Java's
   convention. Callback signatures, error handling, and admin operations are updated
   similarly.
3. **Behavior.** Default behaviors generally align with Java. In some cases the new client
   adopts newer defaults that improve on both librdkafka and Java; for example, the
   KIP-848 consumer group rebalance protocol is enabled by default.

## Example

A Python consumer loop changes because `poll()` returns a batch of records:

```python
# Before (librdkafka-based)          # After (new client)
msg = consumer.poll(1.0)             records = consumer.poll(1.0)
if msg is not None:                  for msg in records:
    process(msg.value())                 process(msg.value())
    consumer.commit()                consumer.commit_sync()
```

## Migration support

- **Migration guides**, per language, with before/after examples for common patterns:
  consumer loops, producer callbacks, error handling, admin operations, serialization, and
  Schema Registry integration.
- **A config mapping reference** that maps every librdkafka config key to its Java
  equivalent, published in a machine-readable form for automated translation.
- **AI-assisted migration tooling** that scans an application, identifies the required
  changes, and guides the developer through them, including config translation, API
  updates, and behavioral differences. It keeps the developer in control: each change is
  explained and can be approved, edited, or skipped.
- **Deprecation warnings.** The new client logs a warning when a deprecated librdkafka
  config key is used, naming the modern equivalent, so teams can migrate incrementally.

## Schema Registry

Schema Registry support is a separate serializer/deserializer library installed alongside
the client, as it is today for Python, .NET, JavaScript, and Go. That model is unchanged;
the new client reuses the existing Schema Registry libraries. C/C++ remains the exception,
as librdkafka never shipped a Schema Registry serializer of its own.
