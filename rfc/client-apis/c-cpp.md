# RFC: C/C++ client API

- **Status:** Draft
- Part of [Client APIs](README.md).

## Summary

The C/C++ client is produced by the Rust core as build artifacts (a C library and headers),
with an API aligned to the Apache Kafka Java client. It is a new library with a different
name than `librdkafka`, and it is not a drop-in `librdkafka` ABI replacement.

## Distribution

C/C++ users consume the Kafka client directly rather than as a dependency of a higher-level
wrapper. Two consumption paths, mirroring how librdkafka is used today:

- include the C sources/headers as a subproject and add them to the build (for example
  CMake), or
- install a development package and include the provided C headers for the new API.

The library name and the exact publishing location are open (see below and the
[Client repository structure](../client-repository-structure.md) proposal).

## API changes

- The C API aligns with the Java client's concepts and naming, adapted to C conventions.
  Because it aligns with Java rather than preserving librdkafka's API, existing C/C++ code
  faces the same categories of migration work as the other languages: configuration key
  changes, API-surface updates, and behavioral alignment.
- **Configuration** keys align with Java; librdkafka-specific keys map to their Java
  equivalents.
- **Schema Registry** is unchanged: librdkafka never shipped a Schema Registry serializer,
  and the new C/C++ client does not either.

## Coexistence

The new C library carries a different name than `librdkafka`, so an application can link
both during migration.

## Migration

Uses the shared migration guide and config mapping described in
[Client APIs](README.md#migration).

## Open questions

- The new C library's name.
- Where the C/C++ artifacts are published: from the monorepo, or from the `librdkafka`
  repository. This is the open question raised in
  [Client repository structure](../client-repository-structure.md#cc-client); a repository
  that holds only artifacts and no source is an anti-pattern to avoid.
- The supported platform matrix for prebuilt artifacts.
