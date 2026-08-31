# RFC: Client contributions

- **Status:** Proposed
- **Audience:** contributors to Confluent's Kafka clients

## Summary

Establish how the community contributes to the new clients: where issues and pull requests
are raised, what kinds of contribution are expected, how review and merge rights work, and
the licensing terms (including CLA) for contributions. This RFC states the model; the
operational day-to-day guidance lives in `CONTRIBUTING.md`.

## Where issues and pull requests are raised

All new-client code lives in the `confluentinc/kafka-clients` monorepo (see
[Client repository structure](repository-structure.md)), so that is where its issues
and pull requests are handled:

- **Issues.** New-client issues are filed in `confluentinc/kafka-clients`. The read-only
  mirror repositories direct reporters there through notes in `CONTRIBUTING.md` and issue
  templates. Issues about the legacy client are filed on the mirror repository's legacy
  branch.
- **Pull requests.** New-client pull requests go to `confluentinc/kafka-clients`. The mirror
  repositories accept pull requests only for legacy fixes; a pull request opened against a
  mirror cannot be merged into the monorepo, because one is not a fork of the other.

The mirrors keep issues and pull requests enabled while their legacy branch is still
supported; they may be disabled after that support window ends.

## What to contribute

Apache Kafka features arrive through translation from the Java client, not through community
pull requests, so community contributions are, by their nature, the changes that are *not*
in Apache Kafka:

- **Language and runtime support** (for example a new Node.js version, a new .NET target framework).
- **Language-native capabilities** that make a binding idiomatic and have no Apache Kafka
  equivalent (for example free threading support for Python).

These land as pull requests to the affected binding's subfolder in the monorepo. One
guardrail keeps the clients from diverging: the shared, cross-language API and configuration
stay aligned with the Java client, so a change that would diverge that shared surface
requires a KIP (see [Client APIs](apis/README.md)).

## Review and merge rights

- The repository is open source, and contributions from the community are welcome today
  through issues and pull requests.
- For now, merge access is limited to a small group of core maintainers, as is common for
  early-stage open-source projects. Expanding who can merge is a goal the project will grow
  into as its tooling and review process mature.
- The concrete pull-request expectations (reference an issue, pass CI, include tests, follow
  existing style, stay scoped, stay responsive, and the stale-PR policy) live in
  `CONTRIBUTING.md`.

## Licensing and CLA

- Most contributions to the Apache 2.0 core and bindings are accepted under the project's
  Apache 2.0 license without a separate agreement.
- For large contributions, the project may ask the contributor to sign a Contributor License
  Agreement (CLA).
- Licensing of the code itself is covered by [Client licensing](licensing.md); the
  CCL plugins are out of scope of these community-contribution terms.

## AI-assisted contributions

Contributors may use any tools, including AI coding assistants. The bar is the same for every
contribution, AI-assisted or not:

- You understand and stand behind what you submit, and you are accountable for it while it is
  under review.
- It is your original work: do not submit third-party material you do not have the rights to,
  including AI output that reproduces someone else's licensed code.
- You respond to maintainers as yourself, rather than handing a review conversation to an
  unsupervised agent.

Disclosing which tools you used is not required. A short note is helpful when AI assistance
was substantial, and `CONTRIBUTING.md` offers optional commit trailers (`Co-Authored-By` /
`Assisted-By` / `Generated-By`) to signal tool use. The full policy is in `AI_POLICY.md`.

## Relationship to the other RFCs

- The monorepo and mirror model that issues and pull requests route through: [Client repository structure](repository-structure.md).
- The shared-API guardrails for contributions: [Client APIs](apis/README.md).
- Contribution licensing terms: [Client licensing](licensing.md).
