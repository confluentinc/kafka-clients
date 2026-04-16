---
name: Milestone 3 Phase 1 review patterns
description: Config default-vs-constant mismatch pattern found in SaslConfig — actor defines correct constant but uses different value in Default impl
type: feedback
---

When reviewing config types translated from Java constant-only classes (like SaslConfigs, SslConfigs), watch for **default value mismatches**: the Rust code may define the correct Java constant (e.g., `DEFAULT_SASL_MECHANISM = "GSSAPI"`) but then use a different value in the `Default` trait implementation (e.g., `mechanism: "PLAIN"`). The actor may rationalize this as "what Rust users would want" but it creates silent behavioral divergence from Java.

**Why:** The SaslConfig default mechanism was "PLAIN" in Rust but "GSSAPI" in Java, contradicting the constant defined in the same file. This pattern where constants and defaults diverge is subtle and easy to miss.

**How to apply:** When reviewing any struct that has both constants and a Default impl, verify that the Default impl uses the defined constants rather than hardcoded values.
