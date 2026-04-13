---
name: ProduceRequest/Response translation patterns
description: Java ApiError.fromThrowable semantics lost when translating getErrorResponse to take &Errors instead of Throwable
type: feedback
---

ProduceRequest/Response translation (Milestone 2 Phase 2) has a recurring pattern with Java exception-to-error adaptation:

1. **ApiError.fromThrowable message suppression**: Java's `getErrorResponse` uses `ApiError.fromThrowable(e)` which suppresses the error_message when it matches the default for the error code. When Rust takes `&Errors` instead of a `Throwable`, there is never a custom message, so `error_message` should always be `None`. The Actor set it to `Some(error.message())` which is always redundant and causes wire format differences.

**Why:** The Actor translated the method signature differently (taking `&Errors` instead of a Throwable analogue) but didn't adjust the body logic to account for the fact that `ApiError.fromThrowable` has message-suppression logic that would always suppress when given only a bare error code.

**How to apply:** When reviewing `get_error_response` methods on any request type, check whether the error_message follows Java's `ApiError.fromThrowable` semantics. If the Rust API takes `&Errors`, the message should almost always be `None`.
