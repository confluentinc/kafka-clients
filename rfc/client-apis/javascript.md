# RFC: JavaScript client API

- **Status:** Draft
- Part of [Client APIs](README.md).

## Summary

The next major version of the JavaScript client aligns its API and configuration with the
Apache Kafka Java client, over the new Rust core, published from the same
`confluent-kafka-javascript` repository and `@confluentinc/kafka-javascript` npm package.

## Package and distribution

- **Package name:** `@confluentinc/kafka-javascript` (unchanged).
- **Install:** `npm install @confluentinc/kafka-javascript`.
- npm's default caret range pins to a major version, so upgrading to the new major is a
  deliberate step. For example, `"@confluentinc/kafka-javascript": "^1.10.0"` in
  `package.json` accepts `1.x` releases but not `4.0.0`; a developer moves to the new client
  by changing the range to `^4.0.0`. The legacy client remains available on its legacy
  branch for its support window.

## API changes

Illustrative, not exhaustive; the full surface is developed on this proposal.

- **Promise-based API** aligned with the Java shapes, including the batch-oriented poll
  result and the explicit commit distinction.
- **A single Java-aligned API; the KafkaJS-compatibility surface is not carried forward.**
  The current client offers a KafkaJS-shaped compatibility API. The new client drops it in
  favor of one Java-aligned API, so there is a single client surface to learn and document.
  Existing KafkaJS-compatibility and KafkaJS applications are converted to the Java-aligned
  API with the AI-assisted migration tooling, or manually using the migration guide.
- **Configuration** keys align with Java; librdkafka-specific keys map to their Java
  equivalents.

## Schema Registry

Schema Registry is a separate serdes library, the existing
[`@confluentinc/schemaregistry`](https://www.npmjs.com/package/@confluentinc/schemaregistry)
package, installed alongside the client and unchanged from today. A serializer or
deserializer is attached when producing and consuming, exactly as it is now; only the
surrounding client API follows the Java-aligned shapes.

## Migration

Uses the shared migration guide, config mapping, and AI-assisted tooling described in
[Client APIs](README.md#migration). Migration from KafkaJS and from the current
librdkafka-based client are both in scope for the guide.

## Open questions

- Any language-native additions beyond the Java-aligned API.
- Package size and cold-start behavior in serverless environments (for example AWS Lambda),
  and whether the native binding is distributed in a way that works under restricted
  install policies.
