---
name: java-exception-hierarchy-dosend-split
description: doSend's catch chain splits on ApiException vs bare KafkaException vs IllegalStateException — the flat KafkaError enum collapses one of those, and where the carve-out belongs
metadata:
  type: project
---

`KafkaProducer.doSend` decides *sync throw vs failed future* by exception class,
and the crate's flat `KafkaError` cannot express one of the three cases.

Java's chain (`KafkaProducer.java:1056-1081`):
`catch (ApiException)` → failed future **+ `maybeTransitionToErrorState`**;
`catch (KafkaException)` → rethrow; `catch (Exception)` → rethrow. Neither
rethrowing block touches the error state.

`ApiException extends KafkaException`, not the reverse — so a **bare**
`new KafkaException(...)` is rethrown, not futured. `IllegalStateException` is
not a `KafkaException` at all.

**Why it bites:** `KafkaError::is_api_exception()` is the crate's `instanceof
ApiException` test and gets `IllegalState` right, but a bare `KafkaException` is
spelled `Errors::UnknownServerError` (a convention set in `record_accumulator.rs`
and `transaction_manager.rs::maybe_fail_with_error`) — which `is_api_exception()`
reports as *true*. Misrouting it doesn't just pick the wrong surface: it runs
`maybe_transition_to_error_state`, which **overwrites `last_error`** on an
abortable producer and destroys the cause the app needs at abort time.

**How to apply:** at a site where the errors are raised locally (never by a
broker), `UnknownServerError` is unambiguously the bare-`KafkaException` stand-in
— carve it out beside `is_api_exception()` there, with a comment. Do NOT try to
fix it in `KafkaError`; the two conventions are inconsistent crate-wide and
resolving that is its own piece of work.

`TransactionManagerTest.testFailIfNotReadyForSend*` (Java 262-300) is the
authority on which class each state raises: `assertThrows(KafkaException.class)`
for abortable/fatal, `IllegalStateException.class` for no-producer-id and
no-ongoing-transaction.

**Corollary — sync-vs-future does NOT tell local from remote.** I got this wrong
once and a Critic caught it. Every *client-side* rejection on the send path is an
`ApiException` too (`RecordTooLargeException` from `ensureValidRecordSize`,
`InvalidTopicException` from `waitOnMetadata`), so it is delivered through the
**future**, exactly like a broker error — in Java as much as here. To prove a
record actually reached the broker, assert the wire **error code**: a broker
`MESSAGE_TOO_LARGE` gives `Errors::MessageTooLarge`, whereas the local
`KafkaError::RecordTooLarge` carries no `KafkaGenericError` and its `error()`
degrades to `UnknownServerError`. Generally: `KafkaError::error()` returns
`UnknownServerError` for every variant without a `KafkaGenericError`
(`IllegalArgument`, `IllegalState`, `Timeout`, `RecordTooLarge`, `Serialization`,
`Wakeup`, `ConcurrentModification`, `TransactionAborted`), so that code means
"no wire code", not "the broker said UNKNOWN_SERVER_ERROR".
