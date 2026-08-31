# RFC: Rust client API

- **Status:** Proposed
- Part of [Client APIs](README.md).

## Summary

The native Rust client is the core from which the other languages are built. It is a new,
greenfield client: there is no prior Confluent Rust client to migrate from. Its API is an
idiomatic Rust mapping of the Apache Kafka Java client.

## Distribution

- **Crate:** published to crates.io. The crate name is not yet fixed (see open questions).
- **Install:** `cargo add <crate>`.

## API shape

The client is translated from the Apache Kafka Java client and follows Rust conventions.
The governing translation rules live in this repository's `CLAUDE.md`; at the API level the
key points are:

- **Async, Tokio-based I/O.** Methods that block in Java are `async` in Rust; methods that
  do not block in Java stay synchronous.
- **Java-aligned names and structure**, adapted to Rust naming (modules, `snake_case`
  methods, `Error` types in place of Java exceptions).
- **`Result`-based error handling** with a `KafkaError` that carries an error code and
  predicates such as `is_retriable` / `is_fatal`, rather than exceptions.
- **Idiomatic ownership and zero-copy** on the hot paths (produce and consume), so the Rust
  client is a first-class client in its own right and not only a substrate for the bindings.

Because there is no prior Rust client, there is no migration story here; this proposal is
about the shape of a new API rather than a change to an existing one.

## Relationship to the bindings

The Python, .NET, JavaScript, and C/C++ clients are thin bindings over this core. Keeping
the core's API aligned with Java is what lets those bindings present a Java-aligned API in
each language without per-language protocol work.

## Open questions

- The published crate name.
- Which parts of the API are considered stable for a 1.0-style commitment versus still
  evolving during preview.
- How much idiomatic-Rust surface (builders, typed config) to add on top of the
  Java-aligned core.
