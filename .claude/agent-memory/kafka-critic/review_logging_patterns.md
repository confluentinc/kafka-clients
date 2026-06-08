---
name: Logging infrastructure patterns
description: LogContext wiring gaps and message content mismatches found in logging review
type: project
---

LogContext wiring is the primary gap pattern in the logging implementation. Components that receive LogContext::empty() when they should receive the producer's LogContext:
- NetworkClient (both constructors)
- ClusterConnectionStates (via NetworkClient)
- SaslChannelBuilder -> SaslClientAuthenticator

**Why:** Java threads LogContext through every constructor; the Rust translation adds LogContext fields but doesn't always wire them through the constructor chain.

**How to apply:** When reviewing LogContext additions, check the full constructor chain from KafkaProducer down to leaf components. Look for `LogContext::empty()` calls in constructors that should receive the real context. Also watch for bare `log::*!` calls that bypassed migration to `kafka_*!` macros.
