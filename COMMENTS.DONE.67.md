# COMMENTS.DONE.67 — Critic 67 findings resolved (fixup of 21b7a328)

All 4 findings from COMMENTS.67.md (Critic 67 review of P4) fixed. Verdict was
PASS / 0 blockers; these were low-severity. Note (C22) needed no code change.

## F1 — MockProducer.client_instance_id unset: raise NotImplementedError (was KafkaError)
RESOLVED. `_MockCore.client_instance_id` (mock_producer.py) now raises
`NotImplementedError("clientInstanceId not set")` — Java throws
`UnsupportedOperationException` (MockProducer.java:406); no UnsupportedOperationError
JDK analog exists on this surface / no FFI id, so Python's semantic counterpart
carries Java's exact message. C25 addendum records the owner choice
(NotImplementedError vs a hand-written root UnsupportedOperationError analog). Tests:
test_client_instance_id_unset_raises, test_client_instance_id_returns_set_id.

## F2 — test asserted the opposite of its name; real background-thread contract uncovered
RESOLVED. Renamed test_on_delivery_runs_on_non_caller_thread →
test_mock_on_delivery_fires_synchronously_on_caller_thread (asserts the mock's
Java-faithful synchronous inline completion). A broker-free real-KafkaProducer
background-thread test proved unreliable (no deterministic fast-fail; close can
block), so the real contract's coverage is documented as P7-integration in the
module docstring ("Thread-contract coverage boundary").

## F3 — transaction-no-timeout test covered only 3 of 5 methods
RESOLVED. test_transaction_methods_have_no_timeout now checks all five
(init_transactions, begin_transaction, send_offsets_to_transaction,
commit_transaction, abort_transaction) on BOTH Producer and AsyncProducer.

## F4 — close_timeout_async deviated from CLAUDE.md §2 presence-rule
RESOLVED (option b — break the ABI, fold the param). Deleted
kafka_producer_Producer_close_timeout_async; added timeout_ms to the bare
kafka_producer_Producer_close_async (async) AND the sync kafka_producer_Producer_close
(Java close(Duration) sync form). The C-ext py_Producer_close_async takes an
optional timeout_ms (legacy 2-arg call still works, default -1). All callers
aligned (new-package producer.py/async_producer.py, C tests, Rust FFI tests). The
legacy producer.py call is unchanged (default -1 = the flushing close it always did).

## Note (C22) — pure-Python MockProducer vs FFI-backed core mock
No change needed (owner decision, accurately recorded in C22). The Critic verified
the pure-Python mock does NOT diverge from MockProducer.java except F1 (now fixed).
