# RFC: Community contributions

- **Status:** Proposed
- **Audience:** contributors to Confluent's Kafka clients

## Summary

Establish how users contribute code, where issues and pull requests are raised, what kinds of
contribution are encouraged, how review and merge rights work, and the licensing terms
(including CLA) for contributions. Feedback here will be used to update `CONTRIBUTING.md`.

## Where issues and pull requests are raised

Note: This requires an understanding of the repository structure. Please read
`repository-structure.md` for more information.

The monorepo, which is located at `confluentinc/kafka-clients`, is home for all issues and
pull requests across clients. Issues and pull requests should not be opened in the
language-specific read-only repositories.

For example, an issue with the Python client should be raised in `confluentinc/kafka-clients`.
Not in `confluentinc/confluent-kafka-python`.

Users browsing one of the mirrors are directed here through `CONTRIBUTING.md` and issue
templates.

## What to contribute

Changes in Apache Kafka are automatically translated from the Java client, not through community pull
requests. Community contributions are the changes *not* in Apache Kafka:

- **Bug fixes and performance enhancements**, for example, fixing memory leaks.
- **Language and runtime support**, for example, a new Node.js version, a new .NET target
  framework.
- **Language-native capabilities** that make a binding idiomatic and have no Apache Kafka
  equivalent (for example free threading support for Python).

## Review and merge rights

- The repository is open source, and contributions from the community are welcome through
  issues and pull requests.
- For now, merge access is limited to a small group of core maintainers, as is common for
  early-stage open-source projects. Expanding who can merge is a goal the project will grow
  into as its tooling and review process mature.
- The concrete pull-request expectations (reference an issue, pass CI, include tests, follow
  existing style, stay scoped, stay responsive, and the stale-PR policy) live in
  `CONTRIBUTING.md`.

## Licensing and CLA

This project aims to use the same approach to licensing as Apache Kafka.

- Most contributions to the Apache 2.0 core and bindings are accepted under the project's
  Apache 2.0 license without a separate agreement.
- For large contributions, the project may ask the contributor to sign a Contributor License
  Agreement (CLA).
- Licensing of the code itself is covered by [Source code licensing](licensing.md).

## AI-assisted contributions

Users may use any tools, including AI coding assistants. The quality bar is the same for every
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
