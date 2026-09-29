# RFC: AI-assisted migration

- **Status:** Proposed
- **Audience:** users and contributors of Confluent's Kafka clients

## Summary

Provide an AI-assisted tool that helps developers migrate existing librdkafka-based
applications to the new client. This RFC focuses on the technology and framework choices the
community would weigh in on: how the tool is delivered, which agents and models it runs on,
and how it handles code and data. It also illustrates, with a representative session, what the
migration experience could look like.

## Motivation

The new client aligns its API with the Java client, so migrating an existing application
requires targeted changes (see [Client APIs](apis/README.md)). Migration is the main
adoption cost, so reducing it with tooling matters. The technology choices below affect
whether users can trust, run, extend, and self-host that tooling.

## Scope

In scope: the delivery mechanism, agent/harness compatibility, model support, distribution
and licensing, the code-and-data handling model, and an indicative view of what the migration
experience could look like.

Out of scope, to be determined separately: the exact set of transformations, the final user
experience, and per-language coverage. This RFC sketches those only enough to frame the
technology choices.

## Delivery: an agent skill

Deliver the migration tool as an **agent skill** conforming to the
[agentskills.io](https://agentskills.io/) standard, so it runs inside a developer's existing
AI coding agent rather than as a separate bespoke script. It is intended to run in any
compatible agent, for example Claude Code, Codex, and GitHub Copilot.

Rationale: developers already use coding agents; a portable skill meets them where they are,
avoids a separate install, and lets the same migration logic run across agents.

## Model support

The tool is **model-agnostic**: it targets a capability bar rather than a specific model, and
developers bring their own model, provider, and API keys. A validated reference model set may
be published, but the tool is not tied to one vendor.

Self-hosted and local models are supported, so teams can migrate without sending source code
to a third-party provider and can manage cost.

## Deterministic core plus AI assistance

Split the work into a deterministic layer and an AI layer. An agent skill can bundle its own
tools, so the skill ships deterministic programs that the agent invokes for the mechanical
work and calls the model only where code understanding is required:

- **Deterministic:** configuration-key translation is driven by a published, machine-readable
  librdkafka-to-Java config mapping (see [Client APIs](apis/README.md)) and applied by
  a bundled tool, so config changes are mechanical and reproducible.
- **AI-assisted:** API-surface and behavioral changes, which need code understanding, are
  handled by the model.

Rationale: keeping the mechanical parts deterministic makes the tool more predictable,
testable, and trustworthy, and narrows what the model is responsible for.

## Code and data handling

The tool runs wherever the developer's agent runs, on a local machine or in a hosted agent
environment such as Claude Code on the web, and code is processed by whichever model provider
the developer configures. This matters for organizations with source-code and data-egress
constraints.

Community input wanted on the default data-handling posture and what the tool logs.

## Distribution and licensing

The skill is open source, distributed from the `confluentinc/agent-skills` repository, so the
community can read, run, fork, and contribute to the migration logic. Its license is aligned
with Apache 2.0 license from the [agent-skills repository](https://github.com/confluentinc/agent-skills/blob/main/LICENSE).

## Use cases and impacted code

Rather than one general migration flow, the tool addresses a set of distinct migration
problems. It scans an application and, for impacted code, identifies the required changes and
guides the developer through them while keeping the developer in control. Each area has its
own split between the deterministic and AI-assisted layers and its own review step.

- **Configuration translation.** Map librdkafka configuration keys to their Java-aligned
  equivalents (for example `message.max.bytes` to `max.request.size`), driven by the
  published, machine-readable config mapping and applied deterministically. The developer
  reviews a diff of translated keys, including any key that has no direct equivalent.
- **API interface changes.** Suggest changes to sections of code where method names or
  signatures need to be updated, for example a loop that reads a single message moving to
  Java's `poll()` that returns a batch of records, along with updated callback and admin call
  signatures. Each change is presented to the developer to accept.
- **Behavioral differences.** Identify places where a default or behavior changes, for
  example the KIP-848 consumer group protocol being enabled by default, and either adjust the
  code or flag the difference for the developer to decide. Differences are shown to the user
  with an explanation in order to determine what change is appropriate for their application.
- **Unsupported or removed surface.** Detect librdkafka-specific features or configuration
  with no direct equivalent in the new client and surface them for manual handling rather
  than guessing, so a migration fails instead of silently producing broken code.
- **Post-migration verification.** After changes are applied, help the developer confirm the
  result before they build and run their test suite.

## What the migration experience could look like (indicative)

The precise experience is still to be determined, but the representative session below shows
the intent. The tool scans the codebase, summarizes what needs to change across the problem
areas above, and walks the developer through each refactor one at a time. This example
migrates a Python application.

```
$ /start-kafka-client-migration

Scanning your codebase for confluent-kafka usage...

Found 3 files with confluent-kafka imports:
  src/consumers/order_consumer.py    -- Consumer with manual commit
  src/producers/event_producer.py    -- Producer with delivery callback
  src/admin/topic_manager.py         -- AdminClient operations

Summary of required changes:
  ┌─────────────────────────┬───────┐
  │ Category                │ Count │
  ├─────────────────────────┼───────┤
  │ Config key updates      │    11 │
  │ API surface changes     │     6 │
  │ Behavioral changes      │     2 │
  │ Total refactors         │    19 │
  └─────────────────────────┴───────┘

  2 behavioral changes need your review:
    • enable.auto.commit default changes from true to false
    • Rebalance callback signature changes

Ready to begin. I'll walk you through each change one at a time.

─────────────────────────────────────────────────
Refactor 1 of 19 -- src/consumers/order_consumer.py
Category: API surface change
─────────────────────────────────────────────────

Consumer.poll() return type changes from a single Message to a
ConsumerRecords collection. Your current code handles one message
at a time; the new API returns a batch.

  Diff (lines 31-44):

      consumer.subscribe(['orders'])
      while True:
    -     msg = consumer.poll(1.0)
    -     if msg is None:
    -         continue
    -     if msg.error():
    -         logger.error(f"Consumer error: {msg.error()}")
    -         continue
    -     process_order(msg.value().decode('utf-8'))
    -     consumer.commit()
    +     records = consumer.poll(Duration.ofSeconds(1))
    +     for msg in records:
    +         process_order(msg.value().decode('utf-8'))
    +     consumer.commit()
      consumer.close()

  This change also updates commit() → commitSync() to align with
  the Java client's explicit sync/async commit distinction.

Apply this change? [y/n/edit/skip]
> y

✓ Applied. Moving to refactor 2 of 19...
```

The tool explains each change, shows the before and after in context, and lets the developer
approve, edit, or skip it. For large codebases the developer can run it in batch mode to apply
all changes at once and review the resulting diff.

## Relationship to the other RFCs

- The API changes that migration addresses, and the machine-readable config mapping the
  deterministic layer uses: [Client APIs](apis/README.md).
- Licensing of the tool: [Source code licensing](licensing.md).
