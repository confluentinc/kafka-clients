## Definition of Done

Every change must at least pass all of the following before it is considered complete from the Actor or reviewed by the Critic:

1. Be consistent with CLAUDE.md and all Claude rules linked or not linked in it.

2. Are all methods from the translated classes implemented?

3. Are all test using those classes translated? Never skip a test that is present in the Java codebase except if there are tests that are present in the Java codebase but not translated and they are not relevant to the Rust codebase, explain why they are not relevant and why they can be skipped. When translating tests also verify:
   - Dedicated per-message-type test files beyond the main `*Test.java` (e.g. `SimpleExampleMessageTest`, `NullableStructMessageTest`) are not missed
   - `@RepeatedTest(N)` annotations become loops in Rust, not single invocations
   - Error message content is asserted, not just `is_err()` — error messages are part of the behavioral contract
   - Wire protocol types have byte-level encoding tests against known vectors, not just round-trip tests — a consistently wrong encoding passes round-trips but is wire-incompatible with Java

4. Are there blockers for doing that? In case implement the needed classes as well.

5. Are all unit and integration tests passing? If there are any failing test fix them before considering the change done.

6. Are there duplicated classes that were translated twice? In case remove the duplicated code and keep only one implementation.

7. Are there structs or traits that aren't present in Java codebase? Avoid adding new structs or traits that aren't present in Java codebase. In case they are needed to implement the Rust client, explain why they are needed and how they are used.

8. Are there any TODO or FIXME left in the code? In case finish everything that should be done before considering the change done.

9. Are unit tests, integration tests, Python, C tests passing?
   Use `make verify` to run all tests and format checks and lint checks. If there are any failing test or check fix them before considering the change done.

10. **Hot-path allocation audit**: For any class that sits on the producer send path, verify there are no avoidable per-message heap allocations: no intermediate copy buffers, no identifier `String` clones, no `Box<dyn Future>` per send. If the translated class is not on the send path, this check can be skipped.

11. **Consumer trait surface check** (when translating consumer files):
    - The top-level `Consumer<K, V>` trait declares its asynchronous methods as returning `impl Future + Send` (no `#[async_trait]`), and has the sealed, dyn-compatible `DynConsumer<K, V>` companion blanket-implemented for every `Consumer` (`consumer-threading.md` §2). Every `Consumer` method has a `DynConsumer` counterpart and a forwarding method on `KafkaConsumer`.
    - `KafkaConsumer::new` returns the concrete `KafkaConsumer<K, V>`, which holds the private `AsyncKafkaConsumer` delegate and forwards to its `Consumer` impl. No public enum dispatch wrapping `AsyncKafkaConsumer` / `MockConsumer`; code holding several implementations uses `Box<dyn DynConsumer<K, V>>`.
    - Per-record traits (`Deserializer`, `Serializer`, anything on the `Fetcher` / `FetchCollector` path) do NOT use `#[async_trait]` and box no future per record: the serde traits are async (`impl Future + Send`) and their `Dyn*` companions return an `InlineFuture` (`consumer-threading.md` §2, §27); other per-record code uses sync `fn` or generic dispatch.
    - No `block_on`-wrapped sync façade for any async consumer method (see `consumer-threading.md` §1).
    - Receivers follow Java's `acquireAndEnsureOpen()` guard (`consumer-threading.md` §1, §2): every `Consumer` / `DynConsumer` method takes `&mut self` unless Java leaves it unguarded (`metrics`, `wakeup`; `client_instance_id`, `register_metric_for_subscription` and `unregister_metric_from_subscription` once translated, added to the lint's allowlist) or it is the Rust-only `handle`; those take `&self`. On `KafkaConsumer` the guarded sync getters panic with Java's message on a closed consumer (and `group_metadata` without `group.id`), per CLAUDE.md §12.5 (Panics policy); `MockConsumer` follows Java's `MockConsumer`. `KafkaConsumer` and `MockConsumer` stay `Send + !Sync` and `ConsumerHandle` `Send + Sync`, pinned by tests. `cargo xtask lint-custom` (`check-consumer-receiver`) enforces the receivers.
    - Every future returned by a `Consumer` method is `Send`; where `#[async_trait]` is used (the `ConsumerRebalanceListener` / `OffsetCommitCallback` callback traits), it keeps the default `Send` bound (no `?Send`).

12. **Test-fixture fidelity**: When a unit-test fixture stands in for a production closure / callback (`wakeup_fn`, `signal_close_fn`, ...), the fixture must invoke the *same* primitives production does — preferably by calling the same builder function the production constructor uses, so the two cannot diverge. A fixture that substitutes a working primitive for a broken one turns the test into a proof about the fixture.