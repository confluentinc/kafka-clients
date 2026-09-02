# RFC: Client versioning

- **Status:** Proposed
- **Audience:** users and contributors of Confluent's Kafka clients

## Summary

Version the next-generation clients with semantic versioning aligned to Apache Kafka:
`MAJOR.MINOR` tracks the Apache Kafka version, and `PATCH` encodes three independent fix
streams in one SemVer-valid integer.

## Motivation

The clients are derived from the Apache Kafka Java client, so their capabilities track an
Apache Kafka version. Aligning the version number to Apache Kafka makes that relationship
legible to users and lets the Rust core and every binding share one version. The first
generation is a new major version per language, which signals the breaking API changes
described in the [Client APIs](apis/README.md) proposals.

## MAJOR.MINOR tracks Apache Kafka

`MAJOR.MINOR` tracks the Apache Kafka version the client is built against. For example,
`4.5` corresponds to Apache Kafka 4.5. The Rust core and every language binding share the
same `MAJOR.MINOR`. As in Apache Kafka, new features and KIPs land only in a `MAJOR.MINOR`
release, never in a patch.

## PATCH encodes three fix streams

`PATCH` stays a single, SemVer-valid integer but encodes three independent streams of
fixes:

```
PATCH = AK_patch * 100 + core_patch * 10 + binding_patch
```

- **Hundreds digit:** the Apache Kafka patch release the client is built on.
- **Tens digit:** a fix in the Rust core that is not tied to an Apache Kafka patch, which
  flows to every binding.
- **Ones digit:** a fix specific to a single language binding.

### Worked examples

| Version | Meaning |
| --- | --- |
| `4.3.0` | Targets Apache Kafka 4.3.0, no core or binding fixes. |
| `4.3.1` | A binding-only fix on Apache Kafka 4.3.0. |
| `4.3.10` | A Rust core fix on Apache Kafka 4.3.0 that flows to all bindings. |
| `4.3.100` | Rebuilt on Apache Kafka 4.3.1, no core or binding fixes yet. |
| `4.3.123` | Apache Kafka 4.3.1, Rust core patch 2, binding patch 3. |

Ordering stays correct because each Apache Kafka patch occupies its own hundreds block:
Apache Kafka 4.3.0 releases occupy `4.3.0`–`4.3.99`, and 4.3.1 releases start at `4.3.100`,
so `4.3.99 < 4.3.100`.

The trade-off is that the core and per-binding patch streams are each a single digit (0–9)
within one Apache Kafka minor release.

## Defaults and divergence

Because `MAJOR.MINOR` tracks Apache Kafka, default values aim to match the Java client.
Where a default is chosen to diverge, it is chosen in the user's interest, kept
forward-compatible with the corresponding Java release, and tracked through a KIP. For
example, the new client defaults to the KIP-848 consumer group protocol.

## Open questions

- Whether a single patch digit per stream per Apache Kafka minor release is enough in
  practice, or whether some streams need more room.
- How pre-release (preview) builds are tagged relative to this scheme.
