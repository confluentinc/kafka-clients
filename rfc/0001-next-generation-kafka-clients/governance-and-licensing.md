# Governance and licensing

Part of [RFC-0001](README.md).

## Contribution model

The new client's repository, `confluentinc/kafka-clients`, is open source. Anyone can read
the source and open, view, and comment on issues and pull requests. The day-to-day
expectations for issues and pull requests are in [CONTRIBUTING.md](../../CONTRIBUTING.md).

Because Apache Kafka features arrive through translation from the Java client rather than
through community pull requests, community contributions are, by their nature, the changes
that are *not* in Apache Kafka. Two kinds are expected and welcome:

- **Language and runtime support**, for example adding a new Python or Node.js version or a
  new .NET target framework. These are maintenance changes to a single binding.
- **Language-native capabilities**, for example an idiomatic async API in a language that
  has no Apache Kafka equivalent.

These land as pull requests to the affected binding's subfolder in the monorepo and are
reviewed and merged by maintainers. Because they do not come from Apache Kafka, they are
not gated on the translation pipeline; a binding-specific fix ships on that binding's patch
stream (see [versioning.md](versioning.md)), and a larger capability ships in a
`MAJOR.MINOR` release.

One guardrail keeps this from fragmenting the clients: the shared, cross-language API and
configuration stay aligned with the Java client, so a change that would diverge that
shared surface still requires a KIP (see
[api-and-migration.md](api-and-migration.md)). Additive, language-idiomatic features that
layer on top of the shared client do not diverge that contract and are handled within the
binding.

## Merge rights

Merge access starts with a small group of core maintainers, as is common for early-stage
projects. Following an Apache-style meritocratic model, contributors with a sustained
track record can be nominated as committers who review and merge pull requests, so the
project is not gated solely on Confluent employees over time.

## Licensing

The code uses two licenses, split by where it comes from:

- **Apache 2.0** for the Rust core and the language bindings. This is all code translated
  from Apache Kafka, and it matches upstream Apache Kafka.
- **Confluent Community License (CCL)** for code translated from Confluent's client
  plugins (for example enterprise authentication). These ship as separate, per-language
  plugin packages installed alongside the open-source client.

The two licenses live in separate packages and repositories so the boundary stays clear:
the Apache 2.0 core and bindings accept community contributions; the CCL plugins do not.

## AI-generated contributions

Features are translated from the Java client with AI assistance, and a human reviews and
merges every change. The repository's [AI_POLICY.md](../../AI_POLICY.md) documents the
policy on AI-generated contributions; a merged pull request is the record of
human-in-the-loop review.

## Relationship to the Apache Software Foundation

A longer-term aim is to donate the Apache 2.0 core and bindings to the Apache Software
Foundation. That would formalize the path for contributors to earn merge rights, provide
long-term governance, and strengthen the project's standing in the Kafka community. The
licensing and contribution model above is chosen to keep that path open.
