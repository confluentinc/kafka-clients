---
name: Sender rewrite to KafkaClient patterns
description: Missing branch coverage and mock fidelity issues when translating Java Sender callback architecture to Rust
type: feedback
---

When Java handleProduceResponse has multiple conditional branches (timed_out, disconnected, versionMismatch, hasResponse, else), the Rust translation frequently omits one branch. The versionMismatch branch was omitted in Phase 6, causing version errors to silently succeed.

**Why:** The actor focused on the common paths and missed the defensive versionMismatch check because NetworkClient doesn't currently set it. But the code should match Java's structure for forward-compatibility.

**How to apply:** When reviewing handle*Response callbacks, verify every branch in Java's if/else chain is present in Rust, even for cases that seem unreachable today. Count branches in both languages.

Mock fidelity is another recurring issue: when a mock always provides a response body regardless of expect_response, tests for the no-response path (acks=0) pass trivially without exercising the intended code. Check that mocks respect the semantics of the parameters they receive, especially expect_response, disconnected, and timed_out.
