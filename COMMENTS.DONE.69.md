# Critic 69 — Phase P6 review — RESOLVED (Actor 69)

## Finding 1 — Producer backpressure lost its only unit-testable path — FIXED

**Resolution.** The critic is right: backpressure is a live contract of the real
producer (the reason `send` is `async`, spec principle 5), not retired mock
plumbing, and `Producer_test_set_paused` (`_confluentkafka.c:884`) still exists.

Re-expressed against the **real** clients in
`bindings/python/test/unit/test_producer_family.py::TestProducerBackpressure`:
a `KafkaProducer` / `AsyncKafkaProducer` pointed at an unreachable bootstrap,
with the send task paused via `Producer_test_set_paused`, accumulates records
broker-free. Three tests now cover the broker-free half of the contract:

1. `test_sync_send_blocks_on_full_and_close_unblocks` — fill to
   `PRODUCER_MAX_ACCUMULATED_RECORDS - 1`; the crossing send blocks the calling
   thread on `full`; `close()` (paused) fires the pending space waiter and
   releases it.
2. `test_async_send_suspends_on_full_and_close_unblocks` — the crossing send
   suspends the coroutine (never blocks the loop); `close()` releases it.
3. `test_async_below_bound_never_suspends` — below the bound, no send suspends.

The drop comment in the migration section was corrected: it no longer says the
subject is gone; it points at `TestProducerBackpressure`.

**Integration-only leg (C44).** The pure "resume-on-drain" case — un-pause and
the send task actually DELIVERS the backlog so the crossing send resolves *by
drain* rather than by `close()` — needs a real broker: once un-paused, a real
producer at an unreachable bootstrap blocks its send task trying to deliver, so
any `close()` afterward hangs (verified, even `close(timeout=0)`). That
assertion is left to the gRPC/multilanguage integration arm and logged as C44.
This is a faithful split, not a silent drop.

Verified: `pytest test/unit/test_producer_family.py` — 85 passed (was 82; the 3
new backpressure tests run in ~0.9s and tear down cleanly).

---

## Note 1 — C43 package-name collision (perf tests) — ACKNOWLEDGED

Correct and already logged as an **open owner item** in C43. The in-tree package
is named `confluent_kafka` and shadows the pip-installed reference
`confluent-kafka` the perf suite benchmarks against; the perf tests are the only
surviving legacy importers and are **not** in `make verify`. No Actor action —
the owner picks option (a) env-aliased reference import, (b) benchmark our client
only, or (c) defer to the spec §11 compat-module decision.

## Note 2 — C42 metric Long/Int fidelity relaxation — ACKNOWLEDGED

Correct. The new public `metrics()` returns `dict[MetricName, Metric]` whose
`metric_value()` no longer carries the Rust `Long`-vs-`Int` distinction, so
`grpc_translate._metric_to_proto` picks the proto value oneof from the Python
value type (`int` → `long_value`). Unobservable to the integration metric
assertions (structural on name/group/tags; `test-integration-python` 232/0).
Noted for a future integration test that asserts a metric's numeric kind — it
must be written with this loss in mind. Logged in C42.

## Note 3 — `_member` callable-heuristic shim — ACKNOWLEDGED

Correct that `_member(obj, name)` (`grpc_translate.py`) returning
`attr() if callable(attr) else attr` is a heuristic. It is safe today: the new
value types expose accessors consistently as methods (rule 15) and the
paused-admin legacy types consistently as bare attributes, and no new value type
exposes a bare non-callable field the same helper reads. The latent fragility is
noted; no change required now. Should a future value type break the invariant,
the fix is to give the producer/consumer and admin paths separate helpers rather
than the shared heuristic.
