---
name: critic1-ffi-contract-round
description: Critic-1 FFI round — check Java BEFORE deciding doc-vs-code, Rust moves what Java shares (callback hand-back), and the app-side listener mirror that a listener-less subscribe failed to clear
metadata:
  type: project
---

Critic 1's review of the FFI callback-bridging layer (phases 1-5) produced 4
issues; all fixed in `8a30e27` / `3002047` / `7b913ea` on `ffi-callback-bridging`.
Three patterns generalize.

1. **When documented != actual, read Java before deciding which one is wrong.**
   Both directions of the `subscribe_with_listener` release-timing bug turned out
   to be *Java-faithful core behaviour* with a wrong doc:
   - `SubscriptionState.subscribe(Set, Optional)` is
     `registerRebalanceListener(listener); setSubscriptionType(...); ...` — the
     register happens BEFORE the call that throws, so a failed subscribe keeps
     the registration. Any "on error we clean up" claim about a translated
     Java method needs this ordering check.
   - `AsyncKafkaConsumer.subscribeInternal(Collection, Optional)` maps an empty
     collection to `unsubscribe()` and silently drops the listener, so a
     *successful* call can release it, and `MockConsumer` (no such
     short-circuit) legitimately differs. Java asymmetries between the real and
     mock impls are contract, not bugs — document them, don't "fix" them.
   Generalizable: an FFI resource-release doc must describe *when the core drops
   its last reference*, not "on success / on error". Ownership transfer and
   release timing are separate facts and Rust's `Drop` makes the second one
   observable where Java's GC hid it.

2. **Rust moves what Java shares — that breaks callback obligations silently.**
   `KafkaProducer.doSend` keeps its own `callback` reference alive for the whole
   method, so `catch (ApiException e) { callback.onCompletion(nullMetadata, e); }`
   fires no matter how deep inside `RecordAccumulator.append` the failure
   happened. Rust's `Callback = Box<dyn FnOnce>` is *moved* into `append`, so
   every `?` on an error path dropped it unfired.
   Fix shape worth reusing: make the error type hand the moved resource back —
   `Err(AppendError { error, callback: Option<Callback> })`, `Some` exactly when
   nothing else took ownership. That makes exactly-once structural instead of a
   convention. Give the new (Java-less) type a **manual `Debug`** reporting
   `callback_returned: bool` — a boxed `FnOnce` is unformattable, and `Debug` on
   the error is what keeps every existing `.unwrap()`/`.expect()` call site
   compiling, which is the difference between a 4-file diff and a 40-site one.
   Audit rule: **any `?` between "callback moved in" and "callback parked in a
   batch/thunk" is a dropped callback.** Same question applies to buffers —
   the same error paths were also skipping Java's
   `finally { free.deallocate(buffer); }`, leaking pool accounting in the exact
   scenario (pool starved) where that compounds into a hang.

3. **A duplicated piece of shared state must be cleared, not just set.**
   `AsyncKafkaConsumer` mirrors `SubscriptionState`'s rebalance listener app-side
   (because §31 invokes the callback on the app task). All three subscribe paths
   wrote the mirror as `if let Some(l) = listener { *slot = Some(l) }`, so a
   listener-*less* `subscribe(topics)` left the mirror pointing at the replaced
   listener while Java's single slot got cleared
   (`registerRebalanceListener(Optional.empty())`). `leave_group_on_close` reads
   that mirror, so close would have invoked the replaced listener. The rebalance
   path was accidentally safe only because its *gating* reads
   `SubscriptionState::rebalance_listener()`.
   Rule: whenever Java has one field and Rust has two copies, every Java write —
   **including the write-`None` ones** — must reach both. Grep for
   `if let Some(x) = ... { *slot = Some(x) }` as the tell.

**Verification notes for this branch.** Mutation-checking the two new
"contract" tests was cheap and both times decisive: the producer test fails
`1 != 0` when the handed-back callback is discarded, and the C arm 5 test fails
when the `if let Some` mirror write is restored (the latter needs a
`cargo build --release --features ffi` ~1m20s + a direct `cc` link, no cmake).
Producer integration tests exist ONLY under `multilanguage-tests`:
`tests/integration/main.rs` gates `mod producer_test` on that feature, which
makes the file's own `rust_only_fallback` module (cfg'd `not(multilanguage-tests)`)
dead code — so `cargo test --features integration-tests producer_test` matches
zero tests and is not the way to verify a core producer change.

See [[ffi-callback-bridging-phase5]], [[ffi-callback-bridging-phase2]],
[[multilanguage-suite-on-macos]], [[critic3-wakeup-and-logstate-round]].
