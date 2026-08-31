# RFC: JavaScript client API

- **Status:** Proposed
- Part of [Client APIs](README.md).

## Summary

The next major version of the JavaScript client aligns its API and configuration with the
Apache Kafka Java client, over the new Rust core, published from the same
`confluent-kafka-javascript` repository and `@confluentinc/kafka-javascript` npm package.

## Package and distribution

- **Package name:** `@confluentinc/kafka-javascript` (unchanged).
- **Install:** `npm install @confluentinc/kafka-javascript`.
- npm caret ranges pin to a major version by default, so upgrading to the new major is a
  deliberate step. The legacy client remains available on its legacy branch for its support
  window.

## API changes

Illustrative, not exhaustive; the full surface is developed on this proposal.

- **Promise-based API** aligned with the Java shapes, including the batch-oriented poll
  result and the explicit commit distinction.
- **KafkaJS-compatibility surface.** The client offers a KafkaJS-shaped API to ease
  migration from KafkaJS. How much of that surface the new client keeps, and how it maps
  onto the Java-aligned API, is a design point to settle here.
- **Configuration** keys align with Java; librdkafka-specific keys map to their Java
  equivalents.

## Migration

Uses the shared migration guide, config mapping, and AI-assisted tooling described in
[Client APIs](README.md#migration). Migration from KafkaJS and from the current
librdkafka-based client are both in scope for the guide.

## Open questions

- The completeness of the KafkaJS-compatible surface (for example event-emitter methods)
  and any language-native additions.
- Package size and cold-start behavior in serverless environments (for example AWS Lambda),
  and whether the native binding is distributed in a way that works under restricted
  install policies.
