# RFC: AI-assisted migration

- **Status:** Draft
- **Audience:** users and contributors of Confluent's Kafka clients

## Summary

Provide an AI-assisted tool that helps developers migrate existing librdkafka-based
applications to the new client. This RFC focuses on the technology and framework choices the
community would weigh in on — how the tool is delivered, which agents and models it runs on,
and how it handles code and data — rather than the detailed capabilities and user experience,
which are still being determined.

## Motivation

The new client aligns its API with the Java client, so migrating an existing application
requires targeted changes (see [Client APIs](client-apis/README.md)). Migration is the main
adoption cost, so reducing it with tooling matters. The technology choices below affect
whether the community can trust, run, extend, and self-host that tooling, so they are worth
settling early.

## Scope

In scope: the delivery mechanism, agent/harness compatibility, model support, distribution
and licensing, and the code-and-data handling model.

Out of scope, to be determined separately: the exact set of transformations, the precise user
experience, and per-language coverage. This RFC sketches those only enough to frame the
technology choices.

## Delivery: an agent skill, not a bespoke tool

Deliver the migration tool as an **agent skill** conforming to the
[agentskills.io](https://agentskills.io/) standard, so it runs inside a developer's existing
AI coding agent rather than as a separate bespoke application. It is intended to run in any
compatible agent, for example Claude Code, Codex, and GitHub Copilot.

Rationale: developers already use coding agents; a portable skill meets them where they are,
avoids a separate install, and lets the same migration logic run across agents.

## Model support

The tool is **model-agnostic**: it targets a capability bar rather than a specific model, and
developers bring their own model, provider, and API keys. A validated reference model set may
be published, but the tool is not tied to one vendor.

Community input wanted on which models are validated, whether a minimum capability bar is
defined, and whether local or self-hosted models are supported for offline or
privacy-sensitive use.

## Deterministic core plus AI assistance

Split the work into a deterministic layer and an AI layer:

- **Deterministic:** configuration-key translation is driven by a published, machine-readable
  librdkafka-to-Java config mapping (see [Client APIs](client-apis/README.md)), so config
  changes are mechanical and reproducible.
- **AI-assisted:** API-surface and behavioral changes, which need code understanding, are
  handled by the model.

Rationale: keeping the mechanical parts deterministic makes the tool more predictable,
testable, and trustworthy, and narrows what the model is responsible for.

## Code and data handling

The tool runs locally in the developer's agent, and code is processed by whichever model
provider the developer configures; no Confluent-hosted processing is required. This matters
for organizations with source-code and data-egress constraints.

Community input wanted on the default data-handling posture, whether an offline/local-model
path is a first-class supported mode, and what the tool logs.

## Distribution and licensing

The skill is open source, distributed from a public repository (for example the existing
`agent-skills` repository), so the community can read, run, fork, and contribute to the
migration logic. Its license is aligned with [Client licensing](client-licensing.md).

## Capabilities and user experience (indicative, to be determined)

At a high level, and subject to change, the tool scans an application, identifies the required
changes (config translation, API updates, behavioral differences), and guides the developer
through them while keeping the developer in control. The concrete transformations, the
interaction model (for example per-change review versus batch application), and per-language
coverage are out of scope here and will be detailed separately.

## Relationship to the other RFCs

- The API changes that migration addresses, and the machine-readable config mapping the
  deterministic layer uses: [Client APIs](client-apis/README.md).
- Licensing of the tool: [Client licensing](client-licensing.md).

## Open questions

- Which agent harnesses and standards to support beyond agentskills.io (for example MCP).
- Which models are validated, and whether local or self-hosted models are a supported mode.
- The default code-and-data-handling posture and the extent of offline support.
- The skill's license and repository.
