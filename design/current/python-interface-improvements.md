# Python interface improvements — parked list

Ideas for making the Python client surface more idiomatic than strict Java parity gives. The
spec applies Java's names and types by default (audit rules §0, R1); each item here is a place
where that rule produced an un-Pythonic shape. Nothing on this list is applied to the spec until
it has been discussed and ruled on.

Format per item: where · Java shape (what the spec has now) · proposed Python shape · why.

| # | Where | Java shape (spec today) | Proposed Python shape | Why | Added |
|---|---|---|---|---|---|
| 1 | `RecordMetadata` | `offset() -> int` with `has_offset() -> bool`; `timestamp() -> int` with `has_timestamp() -> bool` (sentinel `-1` when absent) | `offset() -> int \| None`, `timestamp() -> int \| None`; drop the `has_*` pair | Python expresses "absent" with `None`, not a sentinel plus a predicate; one call instead of two | 2026-09-09 |
| 2 | `MockConsumer` | `set_poll_exception(*, error)` (Java `setPollException`) | `set_poll_error(*, error)` — extend the `Exception → Error` rename from class names to method names that embed the word | Consistency with the `…Error` class names the method takes | 2026-09-09 |
| 3 | Keyword-only rule, a few fields | Every argument keyword-only, including an error's message (`KafkaError(message="boom")`) and `ProducerRecord`'s `topic` / `partition` | Revisit allowing positional use for a few well-known leading fields: the error message (`KafkaError("boom")`), `ProducerRecord(topic, …)`, and similar | Python convention for exceptions (`raise X("msg")`, `super().__init__(msg)`); very common constructors read better positionally. Owner, 2026-09-25: keep keyword-only for now, revisit in the next phase of interface updates | 2026-09-25 |
| 4 | Config-route typing | A serde named in `configs` (`"value.deserializer": "app.OrderDeser"`) is invisible to the type checker, so the client is typed `[bytes, bytes]` and no annotation can correct it | Allow explicit type parameters on the config route only, `KafkaConsumer[str, Order](configs=…)`, like Java's `new KafkaConsumer<String, Order>(props)`, via type-variable defaults (check `mypy` support first) | Correct typing for users who configure serdes by name. Owner, 2026-09-25: keep the argument route as the typed path for now | 2026-09-25 |
| 5 | Async-ness rule | A method is `async def` on the async classes iff Java waits in it, waiting on the background thread included — so `AsyncKafkaConsumer.current_lag()` is `async def` (Java `addAndGet(CurrentLagEvent)`, `AsyncKafkaConsumer.java:1505`), although it only reads cached state and never waits on the network | Revisit whether a method that only waits on the background task (no network wait) should stay a plain `def`, as the spec first had `current_lag` | `await` on a cheap read is unusual in Python. Owner, 2026-09-25: Java decides for now, revisit in the second pass of the interfaces | 2026-09-25 |
