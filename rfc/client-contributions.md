# RFC: Client contributions

- **Status:** Draft
- **Audience:** contributors to Confluent's Kafka clients

## Summary

Establish how the community contributes to the new clients: where issues and pull requests
are raised, what kinds of contribution are expected, how review and merge rights work, and
the licensing terms (including CLA) for contributions. This RFC states the model; the
operational day-to-day guidance lives in `CONTRIBUTING.md`.

## Where issues and pull requests are raised

All new-client code lives in the `confluentinc/kafka-clients` monorepo, so that is where its
issues and pull requests are handled. See
[Client repository structure](client-repository-structure.md#filing-issues-and-pull-requests)
for the mechanics; in short:

- **Issues.** New-client issues are filed in `confluentinc/kafka-clients`. The read-only
  mirror repositories direct reporters there through notes in `CONTRIBUTING.md` and issue
  templates. Issues about the legacy client are filed on the mirror repository's legacy
  branch.
- **Pull requests.** New-client pull requests go to `confluentinc/kafka-clients`. The mirror
  repositories accept pull requests only for legacy fixes; a pull request opened against a
  mirror cannot be merged into the monorepo, because one is not a fork of the other.

## What to contribute

Apache Kafka features arrive through translation from the Java client, not through community
pull requests, so community contributions are, by their nature, the changes that are *not*
in Apache Kafka:

- **Language and runtime support** (for example a new Python or Node.js version, or a new
  .NET target framework).
- **Language-native capabilities** that make a binding idiomatic and have no Apache Kafka
  equivalent.

These land as pull requests to the affected binding's subfolder in the monorepo. One
guardrail keeps the clients from diverging: the shared, cross-language API and configuration
stay aligned with the Java client, so a change that would diverge that shared surface
requires a KIP (see [Client APIs](client-apis/README.md)).

## Review and merge rights

- The repository is open source: anyone can read the source and open, view, and comment on
  issues and pull requests.
- Community pull requests are accepted for merge starting at GA.
- Merge access starts with a small group of core maintainers, as is common for early-stage
  projects. Following an Apache-style meritocratic model, contributors with a sustained
  track record can be nominated as committers who review and merge, so the project is not
  gated solely on Confluent employees over time.
- The concrete pull-request expectations (reference an issue, pass CI, include tests, follow
  existing style, stay scoped, stay responsive, and the stale-PR policy) live in
  `CONTRIBUTING.md`.

## Licensing and CLA

- Contributions to the Apache 2.0 core and bindings are accepted under Apache 2.0, which by
  default covers any contribution submitted for inclusion in the work (see
  [Apache License 2.0, section 5](https://www.apache.org/licenses/LICENSE-2.0#contributions)).
- Large contributions may require the contributor to sign a Contributor License Agreement
  (CLA), consistent with
  [Apache's contributor agreement guidance](https://www.apache.org/licenses/contributor-agreements.html).
- Licensing of the code itself is covered by [Client licensing](client-licensing.md); the
  CCL plugins are out of scope of these community-contribution terms.

## AI-generated contributions

Features are translated from the Java client with AI assistance, and a human reviews and
merges every change; a merged pull request is the record of that human-in-the-loop review.
The repository's `AI_POLICY.md` documents the policy on AI-generated contributions, including
the commit trailer used to disclose AI assistance.

## Relationship to the other RFCs

- Where issues and pull requests go: [Client repository structure](client-repository-structure.md).
- The shared-API guardrail for contributions: [Client APIs](client-apis/README.md).
- Contribution licensing terms: [Client licensing](client-licensing.md).
