---
name: Phase 7d review patterns
description: KafkaProducer send-path translation review notes — interceptor fan-out audit, catch-arm-collapse divergence, Sender::wakeup-not-wired-to-producer pattern, deferred-config-key silent-drop hazard
type: project
---

KafkaProducer Phase 7d (`do_send` + `partition` + `AppendCallbacks` +
`wait_on_metadata` + `impl Producer`) review takeaways. Carry into
Phase 7e/7f reviews.

**Why:** Send-path translations have layered fan-out semantics
(callback → interceptor.onAck → user.onCompletion) that Java distinguishes
by exception type, but Rust's `Result` flattens. Spotting where Java's
class-hierarchy fan-out collapses in Rust is the high-yield axis here.

**How to apply:**

1. **Catch-arm fan-out audit.** Java's `doSend` has 4 catch arms
   (`ApiException`, `InterruptedException`, `KafkaException`,
   `Exception`); only `ApiException` fires the user callback, the rest
   only fire `interceptors.onSendError` and re-throw. Rust collapses
   into a single `match Err(_)` arm — by default this fires the user
   callback for *every* error, including ones Java would only re-throw.
   This is a behavior divergence, not a bug, but worth flagging as a
   Suggestion. The cleanest classifier would be a
   `KafkaError::is_api_exception()` discriminator. Until then, document
   the divergence in the catch-arm rustdoc.

2. **Interceptor double-fire trap.** When Java's catch block fires the
   user callback *directly* (bypassing `AppendCallbacks.onCompletion`),
   it does so to avoid calling `interceptors.onAcknowledgement` twice
   (once via `AppendCallbacks` and once via `onSendError`). The Rust
   translation must extract the user callback from the `AppendCallbacks`
   wrapper and fire it directly — never `append_cb.on_completion(...)`
   in the catch path. The Phase 7d Round-1 test
   `send_returns_record_too_large_and_fires_interceptor_on_send_error`
   pins this contract by counting `on_acknowledgement(error)`
   invocations: must be exactly 1, not 2.

3. **`Sender::wakeup` not reachable from `KafkaProducer` after spawn.**
   Once the constructor moves the Sender into `tokio::spawn`, the
   producer no longer holds a reference to call `wakeup` on. Java's
   `waitOnMetadata` (line 1129) and `doSend` (line 1050) both call
   `sender.wakeup()`. Rust replaces both with a no-op. The latency cost
   is bounded (Sender's poll loop re-fetches metadata on every tick,
   max-block-ms bounds the wait). This is acceptable as a
   *Suggestion-at-most* if documented in NOTES.md and the Sender's
   poll cycle ticks reasonably often. Resolution path: Arc-wrapped
   `wakeup` handle extracted pre-spawn, or `tokio::sync::Notify` driven
   into the run loop. Both are non-trivial refactors.

4. **Deferred-config-key silent drop.** A config key (e.g.
   `partitioner.class`) that's *defined* in the schema but never
   consumed by the constructor will not show up in
   `config.log_unused()` because `unused()` checks
   "user-supplied minus consumed", not "supplied AND non-null AND
   schema-known but unconsumed". A user-supplied custom partitioner is
   silently ignored with no warning. Flag as Suggestion: add an
   explicit `touch()` + warn in the constructor for any deferred key
   that has a non-default value, until the feature is wired up.

5. **`set_read_only(record.headers())` is a no-op in Rust by-value
   model.** Java flips headers to read-only at L1026 to prevent
   user/interceptor mutation after partitioning. Rust's `do_send` takes
   `record: ProducerRecord` by value, so the user has no live reference.
   The omission is correct in Rust but should be documented inline so
   future reviewers don't think it was missed.

6. **`producerMetrics.recordMetadataWait` (Java L1154) is one of the
   metric-stub omissions.** Don't flag — the project-wide pattern is
   to skip metrics until a follow-up.

7. **`ApplicationException` vs `KafkaException` mapping in catches.**
   When the Java code has a multi-arm catch that distinguishes by
   class, ALWAYS check whether the Rust translation collapses them
   (one `Err`-match) and whether that loses behavior. Even when the
   error types are equivalent in Rust's flat enum, the *behavior* of
   each catch arm may not be — record this in a Suggestion if the user
   callback or interceptor fan-out differs.

8. **Test counts the right thing for fan-out bugs.** A single counter
   for `on_acknowledgement` (or `on_send_error`) fires is the cheapest
   pin for the double-fire bug. Be wary of tests that only assert
   "an error fired" without counting — they'll pass with the bug.

9. **`Arc<dyn Trait>` upcast for callback handing.** When `Arc<Foo>`
   needs to be passed as `Arc<dyn FooTrait>`, a single `Arc::clone`
   does the upcast — this is *not* a clone of the underlying data,
   just a refcount bump. Don't flag in performance audits.

10. **Trait methods deferring with explicit `Err`.** For Phase 7d's
    "every other Producer method returns UnsupportedOperation",
    look for any `unimplemented!()` / `todo!()` / `panic!()` macros —
    those violate CLAUDE.md rule 5 and rule 10 (no panic in public API).
    Phase 7d uses explicit `Err(KafkaError::UnsupportedOperation(...))`
    which is correct.

11. **MockTime + wall-clock-deadline await contract.** When a test
    uses `MockTime` for `time.milliseconds()` but the awaited future
    uses `Instant::now()` for its deadline, both clocks run
    independently. The `wait_on_metadata` test that uses MockTime
    times out via the wall-clock-anchored `await_update` deadline,
    not via Java's `time.milliseconds()-now_ms >= max_wait` check
    (which would never fire on MockTime). This is the right
    translation — Java's `MockTime.waitObject` advances both clocks,
    which has no Rust analogue. Acceptable; record it in module docs
    (already done in `producer_metadata.rs`).
