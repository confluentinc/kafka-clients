# RFCs

This directory holds design proposals (RFCs) for the next-generation Confluent Kafka
clients. An RFC captures a significant change (architecture, public API, repository
model, or release process) so the community can review it before it is implemented.

## Why RFCs

Large or foundational changes benefit from a written proposal and an open discussion
before code lands. RFCs give contributors context, a place to raise concerns, and a
durable record of a decision and its rationale. This complements the day-to-day flow
described in [CONTRIBUTING.md](../CONTRIBUTING.md): most work happens through issues
and pull requests, and an RFC is the tool for the small set of changes that are big
enough to warrant a proposal first.

## Status values

- **Draft:** under active writing, not yet ready for broad review.
- **Proposed:** open for community feedback.
- **Accepted:** agreed and ready to guide implementation.
- **Superseded / Rejected:** kept for the record, with a pointer to what replaced it or
  why it was declined.

## Index

| RFC | Title | Status |
| --- | --- | --- |
| 0001 | [Next-generation Confluent Kafka clients](0001-next-generation-kafka-clients/README.md) | Proposed |

## How to comment

Comment on the RFC's pull request, or open an issue that references it. Substantive
changes to a Proposed RFC are made through follow-up commits on its pull request so the
discussion and the text stay together.

## Adding an RFC

1. Copy the structure of an existing RFC into a new `NNNN-short-title/` directory.
2. Write `README.md` as the entry point (summary, motivation, proposal). Split long
   sections into sibling documents when a single file becomes hard to read.
3. Add a row to the index above with status **Draft**.
4. Open a pull request. Move the status to **Proposed** when it is ready for review.
