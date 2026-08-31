# RFC-0001: Next-generation Confluent Kafka clients

- **Status:** Proposed
- **Audience:** users and contributors of Confluent's non-Java Kafka clients (Python,
  .NET, JavaScript, C/C++) and this Rust client
- **Discussion:** the pull request that introduces this RFC

## Summary

Confluent is building a new generation of its non-Java Kafka clients on a shared **Rust
core** that is translated from the Apache Kafka Java client. The Rust core is a native
Rust client and, through thin bindings, also powers the Python, .NET, JavaScript, and
C/C++ clients. It replaces the librdkafka C core that underpins those clients today.

The intended outcomes are:

- **Parity within weeks of Java.** New KIPs and features are translated from the Java
  reference implementation into the Rust core and rippled into every binding, instead of
  being re-implemented by hand per language.
- **A memory-safe core.** Rust removes a class of memory-safety bugs that recur in C.
- **A single, Java-aligned API** across languages, so knowledge, examples, and
  documentation transfer between them.
- **One place to contribute.** All new-client code lives in one repository.

## Motivation

The non-Java clients are a large share of Kafka usage, and today they trail the Java
client. New Kafka capabilities can take months to reach non-Java users, and some never
arrive. The root cause is structural, not a lack of effort:

- Every feature is implemented once in C (librdkafka) and then wrapped separately in each
  language, which multiplies the work and the surface area for divergence.
- The C codebase is complex and slow to ramp on, which limits the pool of people who can
  maintain it.
- Bug fixes must be re-derived in C rather than inherited from the upstream Java client's
  ongoing maintenance.

Deriving the client from the Java reference implementation changes the maintenance model.
Fixes and features flow from Java into the Rust core and outward to the bindings, the
memory-safe core removes recurring bug classes, and any engineer who knows the Java client
can work on the Rust core. This is the reasoning behind building a new core rather than
continuing to patch the C one.

## Goals

- Feature and behavior parity with the Java client, reaching all supported languages.
- A Java-aligned public API and configuration across languages.
- A migration path for existing librdkafka-based applications, with tooling and guides.
- A clear, supported deprecation timeline for the librdkafka-based clients.
- A single contribution point and issue backlog for the new client.

## Non-goals

- **Drop-in ABI/API compatibility with librdkafka.** The new client aligns with the Java
  client, so it is not a source- or binary-compatible replacement for librdkafka. See
  [api-and-migration.md](api-and-migration.md).
- **The Go client.** Go is being evaluated separately and is not covered by this RFC.
- **A frozen public API today.** This project is an early-stage preview; APIs may still
  change ahead of GA (see [CONTRIBUTING.md](../../CONTRIBUTING.md)).

## Proposal

The proposal is described in five parts, each in its own document:

| Topic | Document | What it covers |
| --- | --- | --- |
| Architecture | [architecture.md](architecture.md) | The `confluentinc/kafka-clients` monorepo, the read-only per-language mirror repositories, and how the C/C++ client ships. |
| Versioning | [versioning.md](versioning.md) | Semantic versioning aligned to Apache Kafka `MAJOR.MINOR`, and how patches are encoded. |
| API and migration | [api-and-migration.md](api-and-migration.md) | Alignment with the Java API, the categories of breaking change, and migration tooling. |
| Deprecation | [deprecation.md](deprecation.md) | The support and end-of-life timeline for the librdkafka-based clients. |
| Governance and licensing | [governance-and-licensing.md](governance-and-licensing.md) | How contributions are made and reviewed, and how the code is licensed. |

## How features are built

Features are translated from the Apache Kafka Java client into the Rust core, then the
resulting interface changes are rippled into the language bindings. AI with a
human-in-the-loop makes this translation economical, but a human reviews and merges every
change; a merged pull request is the record of that review. See
[AI_POLICY.md](../../AI_POLICY.md).

## Open questions

- **C/C++ repository.** Whether the C/C++ client is published as build artifacts from the
  monorepo (with the librdkafka repository deprecated) or continues to be published from
  the librdkafka repository. A repository that holds only build artifacts and no source
  diverges from how the language bindings are mirrored; this is unresolved. See
  [architecture.md](architecture.md#cc-client).
- **Rust crate name.** The published crate name for the native Rust client is not yet
  fixed.
- **Classic consumer protocol.** The preview targets the KIP-848 consumer group protocol,
  which requires brokers on Apache Kafka 4.0 or later. Whether to add classic-protocol
  support depends on preview feedback.

## Feedback

This RFC is **Proposed**. Comments on the scope, the API direction, the repository model,
and the migration and deprecation plans are all welcome on the pull request or via a
referencing issue.
