# Critic 123 — resolved items

## 123.1 · Low · `dotnet/tests/Confluent.Kafka.IntegrationTests/TransactionsBrokerTests.cs:40`: a second Kafka container now starts in parallel with the first

**Issue.**
- `TransactionsBrokerTests` takes the broker through `IClassFixture<KafkaBrokerFixture>`, and so does `KafkaProducerBrokerTests.cs:40`.
- xUnit creates one class fixture per test class and, by default, runs each class as its own collection in parallel. The project has no `xunit.runner.json` or `CollectionBehavior`.
- So `IntegrationTests` now starts **two** `apache/kafka` JVM containers, concurrently, where it used to start one.
- Python's broker tests share one session-scoped `kafka_broker` (`python/test/performance/conftest.py:61`).
- The plan's §5 already flags the broker tests as memory-heavy (the OOM at default thread count). The CI `verify-dotnet` job is said to run 20–26 minutes against a 30-minute limit.
- The tests passed locally (16 / 16 per STATUS), so this is a cost and risk issue, not a failure.

**Suggested fix.**
- Share one broker across both classes with an xUnit collection fixture: `[CollectionDefinition("broker")] class BrokerCollection : ICollectionFixture<KafkaBrokerFixture> {}`, and `[Collection("broker")]` on both classes, keeping the constructor injection as it is.
- This matches Python's single broker. It also serializes the two classes, which the unique topic and transactional ids allow.
- If the parallel second broker is intentional, state it in the class summary.

**Resolution.**
- New `tests/Confluent.Kafka.IntegrationTests/BrokerCollection.cs`: `[CollectionDefinition("broker")]` with `ICollectionFixture<KafkaBrokerFixture>`.
- `KafkaProducerBrokerTests` and `TransactionsBrokerTests` drop `IClassFixture` and carry `[Collection(BrokerCollection.Name)]`. Their constructor injection is unchanged.
- The fixture's only state is the container and its start gate. Each test creates its own Guid-suffixed topics and transactional and group ids, so the two classes cannot collide.
- Rerun against Docker (Release): 16 / 16 on net8.0 and 16 / 16 on net10.0, none skipped, `dotnet format --verify-no-changes` clean.
- Each test process now starts one broker. A `docker ps` sampler saw at most 2 `apache/kafka:4.2.0` containers at once, one per TFM process, since `dotnet test` runs the two TFMs side by side. Before this fix the peak would have been up to 4.
- The run takes about 48 s per TFM, against 30 s before, because the two classes now run one after the other.
