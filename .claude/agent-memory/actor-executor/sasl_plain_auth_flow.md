---
name: SASL PLAIN authentication flow
description: Key behavioral differences between Java and Rust SASL PLAIN auth, especially state machine transitions
type: project
---

For PLAIN mechanism, the Java `saslClient.isComplete()` returns true after the initial token exchange (evaluateChallenge), and `sendSaslClientToken` returns false (no additional token to send). This means `noResponsesPending=true`, so PLAIN goes directly from INTERMEDIATE to COMPLETE -- never entering CLIENT_COMPLETE state.

**Why:** CLIENT_COMPLETE is only used by challenge-response mechanisms (e.g., SCRAM) where the client sends a final response and waits for the server's acknowledgment. PLAIN has a single token exchange.

**How to apply:** When implementing SCRAM support, CLIENT_COMPLETE will need to be used. For PLAIN, the Intermediate state handler should always go to Complete after receiving the server's success response.

Also: `RequestBuilder` trait must be imported to call `build()` on `ApiVersionsRequestBuilder` -- it's not inherent.
