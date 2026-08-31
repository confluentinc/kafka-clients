# RFC: Python client API

- **Status:** Draft
- Part of [Client APIs](README.md).

## Summary

The next major version of the Python client aligns its API and configuration with the
Apache Kafka Java client, over the new Rust core, published from the same
`confluent-kafka-python` repository and PyPI package.

## Package and distribution

- **Package name:** `confluent-kafka` (unchanged).
- **Install:** `pip install confluent-kafka`.
- The legacy librdkafka-based client remains available on its legacy branch and existing
  releases for its support window.

## API changes

Illustrative, not exhaustive; the full surface is developed on this proposal.

- **Consumer poll returns a batch.** Java's `KafkaConsumer.poll()` returns a collection of
  records, so the Python consumer loop changes from one message per call to iterating a
  batch:

  ```python
  # Before (librdkafka-based)          # After (new client)
  msg = consumer.poll(1.0)             records = consumer.poll(1.0)
  if msg is not None:                  for msg in records:
      process(msg.value())                 process(msg.value())
      consumer.commit()                consumer.commit_sync()
  ```

- **Commit methods** align with Java's explicit sync/async distinction
  (`commit_sync` / `commit_async`).
- **Callbacks, error handling, and admin operations** align with the Java shapes.
- **Configuration** keys align with Java; librdkafka-specific keys map to their Java
  equivalents (see [Client APIs](README.md#categories-of-change)).

## Migration

Uses the shared migration guide, config mapping, and AI-assisted tooling described in
[Client APIs](README.md#migration). The two major versions can be installed side by side
during migration.

## Open questions

- Whether to add an idiomatic `asyncio` API as a language-native capability. This has no
  Apache Kafka equivalent and would be additive; see the contribution model for how
  language-native features are handled.
