# RFCs

This directory holds design proposals (RFCs) for the next-generation Confluent Kafka
clients. An RFC captures a significant change (architecture, public API, repository model,
or release process) so the community can review it before it is implemented.

## Background

Confluent is building a new generation of its non-Java Kafka clients on a shared **Rust
core** translated from the Apache Kafka Java client. The Rust core is a native Rust client
and, through thin bindings, also powers the Python, .NET, JavaScript, and C/C++ clients,
replacing the librdkafka C core those clients use today. The aim is feature and behavior
parity with the Java client within weeks of a Java release, a memory-safe core, a
Java-aligned API across languages, and one place to contribute.

The proposals below break that effort into focused pieces that can be reviewed on their own.

## Proposals

| Proposal | Summary | Status |
| --- | --- | --- |
| [GitHub repository structure](repository-structure.md) | One `confluentinc/kafka-clients` monorepo, mirrored to read-only per-language repositories. | Proposed |
| [Client versioning](versioning.md) | Semantic versioning aligned to Apache Kafka `MAJOR.MINOR`, with a patch encoding. | Proposed |
| [Client APIs](apis/README.md) | Align each client's API with the Java client. Proposal: [Rust](apis/rust.md). | Draft |
| [Source code licensing](licensing.md) | Apache 2.0 for the Rust core and language bindings (the CCL plugins are out of scope). | Proposed |
| [Community contributions](contributions.md) | Where issues and pull requests are raised, the contribution model, and licensing/CLA terms. | Proposed |
| [Client release and distribution](release-and-distribution.md) | Release cadence aligned to Apache Kafka, per-registry publishing, prebuilt binaries, and supply-chain integrity. | Proposed |
| [AI-assisted migration](ai-assisted-migration.md) | An agentskills.io skill, model-agnostic, with a deterministic config mapping plus AI for API and behavioral changes. | Proposed |

## Status values

- **Draft:** under active writing, not yet ready for broad review.
- **Proposed:** open for community feedback.
- **Accepted:** agreed and ready to guide implementation.
- **Superseded / Rejected:** kept for the record, with a pointer to what replaced it or why
  it was declined.

## How to comment

Comment on the proposal's pull request, or open an issue that references it. Substantive
changes to a Proposed RFC are made through follow-up commits on its pull request so the
discussion and the text stay together.

## Adding a proposal

1. Add a `topic.md` (or a `topic/` directory with a `README.md` when it needs multiple
   documents, as the Client APIs proposal does).
2. Add a row to the table above with status **Draft**.
3. Open a pull request. Move the status to **Proposed** when it is ready for review.
