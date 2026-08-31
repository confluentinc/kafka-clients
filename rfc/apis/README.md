# RFC: Client APIs

- **Status:** Draft
- **Audience:** users and contributors of Confluent's Kafka clients

## Summary

Align each client's public API and configuration with the Apache Kafka Java client. This
umbrella document states the shared principles; a separate proposal per language covers the
concrete API and the migration from the current librdkafka-based client.

| Language | Proposal |
| --- | --- |
| Python | [python.md](python.md) |
| .NET | [dotnet.md](dotnet.md) |
| JavaScript | [javascript.md](javascript.md) |
| C/C++ | [c-cpp.md](c-cpp.md) |
| Rust | [rust.md](rust.md) |

Go is being evaluated separately and is not covered here.

## Why align with the Java API

A shared contract across languages means documentation, examples, and answers found online
transfer between languages; developers moving between C/C++, Python, JavaScript, .NET, and
Java reuse their knowledge; and each new KIP maps from Java to every language without
per-language API design. This is a deliberate departure from librdkafka's API, whose
divergence from Java (especially in configuration defaults) is a recurring source of
friction today.

## Categories of change

Each per-language proposal describes its changes in three categories:

1. **Configuration.** Config keys align with the Java client. librdkafka-specific keys (for
   example `message.max.bytes`) map to their Java equivalents (for example
   `max.request.size`). A full mapping is published, and the migration tooling translates
   configuration automatically. Any divergence from the Java configuration is introduced
   through a KIP.
2. **API surface.** Method names and signatures align with the Java client. For example,
   Java's `KafkaConsumer.poll()` returns a collection of records rather than a single
   message; the wrapper clients follow that convention. Callback signatures, error
   handling, and admin operations are updated similarly.
3. **Behavior.** Default behaviors generally align with Java. In some cases the new client
   adopts newer defaults that improve on both librdkafka and Java; for example, the KIP-848
   consumer group rebalance protocol is enabled by default.

## Migration

Migrating an existing application involves targeted updates, not a rewrite. Shared support
across languages:

- **Migration guides** with before/after examples for common patterns.
- **A config mapping reference** from every librdkafka config key to its Java equivalent, in
  a machine-readable form.
- **AI-assisted migration tooling** that scans an application, identifies the required
  changes, and guides the developer through them, keeping the developer in control of each
  change.
- **Deprecation warnings** logged when a deprecated librdkafka config key is used, naming
  the modern equivalent.

## Schema Registry

Schema Registry support is provided by Confluent's existing per-language serializer and
deserializer (serdes) libraries, installed alongside the client. The new clients support
Schema Registry exactly as the librdkafka-based clients do today: the same serdes libraries,
attached to produce and consume the same way, with no change required by this RFC.

Each per-language proposal includes a Schema Registry example:
[Python](python.md#schema-registry), [.NET](dotnet.md#schema-registry), and
[JavaScript](javascript.md#schema-registry). C/C++ is the exception, as librdkafka never
shipped a Schema Registry serializer of its own (see [c-cpp.md](c-cpp.md#schema-registry)).

## Relationship to the other RFCs

- The clients are versioned per [Client versioning](../versioning.md); the API
  changes are why the first release is a new major version.
- The clients are published per [GitHub repository structure](../repository-structure.md).
