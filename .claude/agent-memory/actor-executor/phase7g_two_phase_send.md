---
name: Phase 7g two-phase send shape
description: How KafkaFuture<T> + KafkaFutureOps trait wrap FutureRecordMetadata for the Java-parity Producer::send return shape
type: project
---

Phase 7g restored Java parity on `Producer::send` by returning
`Result<KafkaFuture<RecordMetadata>, KafkaError>`. This is a load-
bearing shape choice that other phases (8a.1 perf test, Phase 9 admin
KafkaFuture<U> uses) depend on.

**Why:** Phase 7b collapsed the two phases into a single
`async fn -> Result<RecordMetadata, _>` and suggested callers
`tokio::spawn` the returned future for fire-and-forget — this
contradicted CLAUDE.md rule 11 (per-message tokio::spawn on send
path is the explicit anti-pattern) and changed the Java public-API
contract (rule 4). Phase 7g reverts the collapse.

**How to apply:** When other classes need a Java `Future<T>` shape
(admin client futures, KafkaFuture<Void>, etc.):

1. The wrapper `KafkaFuture<T>` lives in `src/common/kafka_future.rs`.
   Generic over `T: Send + 'static`. Public methods: `get() -> Result<T, KafkaError>`,
   `get_timeout(Duration)`, `is_done()`.
2. Underneath is `pub(crate) trait KafkaFutureOps<T: Send>: Send + Sync`
   with two methods: `get<'a>() -> Pin<Box<dyn Future + Send + 'a>>` and
   `is_done() -> bool`. Object-safe by design so the wrapper can hold
   `Arc<dyn KafkaFutureOps<T>>`.
3. To plug a concrete future type in: `impl KafkaFutureOps<T> for
   ConcreteType { ... }` and have the inherent `get()` be a concrete
   `async fn`. The trait impl just `Box::pin(self.get())`s.
4. The per-send allocation cost is one `Arc<dyn KafkaFutureOps<T>>` —
   matches Java's per-`Future` JVM allocation. The boxed-future on
   `KafkaFutureOps::get()` is per-`.get()`-call, not per-send.

**Pitfalls to avoid:**

- Don't add cancellation methods to the trait — Java's
  `FutureRecordMetadata.cancel()` always returns false; the producer
  surface doesn't need them. Admin/consumer surface will re-add later.
- `KafkaFutureOps` is `pub(crate)`. Don't make it `pub` — Java's
  abstract methods are not a public extension point.
- Test sites using `producer.send(record).await.expect_err(...)`
  don't need `.get()`-chaining: the outer `Result` is exactly what
  the sync-throw assertion checks. Only success-path tests need to
  chain `.await?.get().await`.
- Mock the integration-test side-channel: `producer_smoke_test.rs`
  spawn-task body had to become `.await?.get().await` for Java parity.
  `performance_test.rs` is currently disabled (no `mod` in
  `tests/integration/main.rs`) — it expects a 2-arg `send(record, None)`
  signature which is Phase 8a.1's revisit, not 7g's.
