# RFC: .NET client API

- **Status:** Draft
- Part of [Client APIs](README.md).

## Summary

The next major version of the .NET client aligns its API and configuration with the Apache
Kafka Java client, over the new Rust core, published from the same `confluent-kafka-dotnet`
repository and `Confluent.Kafka` NuGet package.

## Package and distribution

- **Package name:** `Confluent.Kafka` (unchanged).
- **Install:** `dotnet add package Confluent.Kafka`.
- .NET version specifiers pin to a major version, so upgrading to the new major is a
  deliberate step. The legacy client remains available on its legacy branch for its support
  window.

## API changes

Illustrative, not exhaustive; the full surface is developed on this proposal.

- **Consumer** methods align with Java, including the batch-oriented poll result and the
  explicit commit distinction.
- **Producer** and **Admin** method names, signatures, and error handling align with Java.
- **Configuration** keys align with Java; librdkafka-specific keys map to their Java
  equivalents.
- **Interop with idiomatic .NET** (async/await, cancellation tokens, `IAsyncEnumerable`
  where natural) is a design point to settle on this proposal.

## Migration

Uses the shared migration guide, config mapping, and AI-assisted tooling described in
[Client APIs](README.md#migration).

## Open questions

- Whether to expose a batch-consume convenience (a `max.poll.records`-style API) and to
  surface producer queue depth as a first-class API. These are language-native ergonomics
  with no direct Java equivalent.
- Native AOT compatibility and Windows/IIS deployment behavior are known pain points on the
  current stack and should be validated against the new core.
