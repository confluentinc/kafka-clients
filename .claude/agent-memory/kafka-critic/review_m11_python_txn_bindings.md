---
name: review-m11-python-txn-bindings
description: Critic patterns for reviewing Python/C-extension producer-txn bindings — FP traps in CPython marshaling, verifying error-surface tests without running them, DoD#3 hook-coverage gaps
metadata:
  type: project
---

Milestone 11 Python producer-transaction bindings review (Critic 54, branch
`milestone11-producer-transactions-python`, 4 commits 9a-9d). Phase came back
sound: C-extension memory/lifecycle correct; 1 should-fix DoD#3 coverage gap,
1 nit. Reusable verification patterns below.

**FP trap — CPython marshaling DECREF-then-retain-pointer is SAFE and is the
house idiom.** `py_Producer_send_offsets_to_transaction` and the template
`py_Consumer_commit_sync_offsets_async` (`_confluentkafka.c:1849`) both do:
`PySequence_GetItem`(new ref) → `PyArg_ParseTuple(item,"siL|iO",...)` → store the
`const char* t`/`metas[i]` → `Py_XDECREF(item)` in the loop. The retained
`const char*` stays valid because the **caller's list** holds every tuple (and
thus every string) alive for the whole FFI call, and the FFI copies into owned
Rust data synchronously before returning. Header comment at `_confluentkafka.c`
~:1598 documents this. Do NOT flag it as use-after-free.

**Verifying error-surface tests you can't run: `KafkaError::with_message(error,
msg)` ALWAYS builds `Generic(...)`** (`src/common/kafka_error.rs:363-364`),
preserving `code()`/`message()`. It never routes into the string-payload variants
(`Timeout`/`IllegalState`/`ConcurrentModification`/`TransactionAborted`) — those
come only from the dedicated constructors (`KafkaError::timeout(msg)` :446, etc.).
So an injected mock error (`set_commit_transaction_error(code,msg)` →
`mock_error` → `with_message`) keeps code-based semantics:
`txn_requires_abort()` = `requires_abort || error==TransactionAbortable`
(`kafka_error.rs:129`, `errors.rs:460`); `is_fatal()` = the `fatal` field
(false for `with_message`). Codes: `TransactionAbortable=120`, `RequestTimedOut=7`
(`errors.rs`). This lets you adjudicate abortable/non-abortable test assertions
without running the suite.

**tp_flags without `Py_TPFLAGS_HAVE_GC` is CORRECT for an extension type holding
only a raw C pointer and no PyObject members** (cannot be in a ref cycle). Use
`PyObject_New`/`tp_free`, not the GC variants; no `tp_traverse`/`tp_clear` needed.
`ConsumerGroupMetadataObject` does this right. An explicit `_destroy` FFI wrapper
is dead code when freeing lives in `tp_dealloc` — omitting it is correct.

**Handle-ownership check for "value object carrying a C handle":** confirm the
producer/consumer FFI *clones* out of the borrowed handle rather than taking it
(`send_offsets_to_transaction_inner` does `group_metadata_ref(gm).clone()`,
`src/ffi/producer.rs:2999`), and that each accessor call boxes a **fresh** handle
(`kafka_consumer_Consumer_group_metadata` → `box_group_metadata`,
`consumer.rs:3752`). Then: single owner (Python obj), freed once in `tp_dealloc`,
no aliasing, object safely outlives its source consumer.

**`grpc_server.py:_take_producer` is a PEEK (`.get`), not a pop** — despite the
name. Repeated txn RPCs on one `producer_id` are fine; not a bug.

**DoD#3 gap pattern (the real finding):** when a phase adds mock introspection
hooks to cover Java tests *behaviourally*, a blanket deviation note ("the
introspector-only / fenceProducer tests are covered behaviourally / not exposed")
can be **over-broad**. Enumerate each Java test in the mapped category. Here the
`sent_offsets`/`committed_offset` hooks were added, but the offset-lifecycle-on-
abort tests that use them (`MockProducerTest.shouldDropConsumerGroupOffsetsOnAbort...`
:529, `shouldResetSentOffsetsFlagOnlyWhenBeginningNewTransaction` :464, etc.) were
not translated and fell outside the note's two justified categories. Confirm the
underlying behaviour is correct first (`mock_producer.rs:911` clears staged
offsets on abort) so the finding is "coverage gap", not "hidden bug".

`fenceProducer` genuinely has no FFI symbol (grep `src/ffi/producer.rs`), so those
Java tests are correctly untranslatable in a Python-only phase.
