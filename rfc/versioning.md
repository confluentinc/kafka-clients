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

Each digit resets to 0 when any digit to its left increments. Incrementing the core patch
resets the binding patch; a new Apache Kafka patch resets both.
A higher `PATCH` always contains every earlier fix and SemVer-aware upgrades stay safe.

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

### Use cases

Here are some examples of how the versioning applies for use cases:

| Case | Example | What happens |
| --- | --- | --- | 
| No leading zeros | `4.3.021` is really `4.3.21` | SemVer forbids leading zeros in `PATCH` reducing readability. |
| Resets carry fixes forward | `4.0.101` upgrades safely to `4.0.200` | Bumping the Apache Kafka patch resets core and binding to 0, but the Apache Kafka 4.0.2 rebuild carries the earlier binding fix forward. |

## Defaults and divergence

Because `MAJOR.MINOR` tracks Apache Kafka, default values aim to match the Java client.
Where a default is chosen to diverge, it is chosen in the user's interest, kept
forward-compatible with the corresponding Java release, and tracked through a KIP. For
example, the new client defaults to the KIP-848 consumer group protocol.

## Versions before the proposal is adopted

Until this proposal is adopted, the Rust crates and the Python packages are on separate
version lines, and `cargo xtask bump-version` (`make bump-version`) keeps them so:

- **Rust crates** follow plain semantic versioning from the first release (`0.1.1`,
  `0.2.0-beta.1`, ...), not aligned to Apache Kafka. `cargo xtask api-compat` enforces
  Cargo's semver rule against the last release tag.
- **Python packages** (`python/pyproject.toml`, `tools/translation_agent`) follow their own
  PEP 440 line, which already tracks Apache Kafka (`4.4.0a1.dev1`). It only moves up: the
  command refuses any Python version lower, in PEP 440 order, than the one a package has.
  Without `--python-version` (`PYTHON_VERSION=`) the packages get the PEP 440 form of the
  crate version, which is refused while that is the lower one; pass the Python version
  explicitly, for example `make bump-version VERSION=0.2.0 PYTHON_VERSION=4.4.0`.

Versions only move forward: the command refuses a version below the current one, in SemVer
order for the crates and PEP 440 order for the Python packages, where a pre-release comes
before its release, so a pre-release `X.Y.Z-beta.N` is cut before `X.Y.Z`, never after.
