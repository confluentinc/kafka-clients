// Copyright 2025 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// M17/P1 S10: idempotence configuration reaches the core unaltered through both real producer
/// constructors (D9), the <c>KafkaProducerTest</c> subset that runs broker-free (222, 238, 341,
/// 413, 1329, 2054), and D13's empty-map rows. Every test runs on
/// <see cref="KafkaProducer{TKey, TValue}"/> (<c>"sync"</c>) and
/// <see cref="AsyncKafkaProducer{TKey, TValue}"/> (<c>"async"</c>); the bootstrap address has no
/// listener, so nothing reaches a broker. Codes and messages are the core's, measured at CP5.
/// </summary>
/// <remarks>
/// Java reads the effective configuration back; the binding has no config introspection. The
/// effective <c>enable.idempotence</c> is observable all the same (<see cref="AssertIdempotence"/>):
/// with it off the core builds no transaction manager, so <c>InitTransactions()</c> on a
/// producer without <c>transactional.id</c> reports <see cref="NoTransactionManager"/>, and with it
/// on the manager reports <see cref="NotTransactional"/>. The <c>acks</c>, <c>retries</c> and
/// <c>max.in.flight</c> readbacks have no observable: N/A.
/// </remarks>
public sealed class PublicProducerIdempotenceTests
{
    private const string NoTransactionManager =
        "Cannot use transactional methods without enabling transactions by setting the transactional.id configuration property";

    private const string NotTransactional = "Transactional method invoked on a non-transactional producer.";

    private const string TransactionalIdNeedsIdempotence = "Cannot set a transactional.id without also enabling idempotence.";

    private const string AcksMustBeAll =
        "Must set acks to all in order to use the idempotent producer. Otherwise we cannot guarantee idempotence.";

    private const string RetriesMustBeNonZero = "Must set retries to non-zero when using the idempotent producer.";

    private const string MaxInFlightAtMostFive =
        "To use the idempotent producer, max.in.flight.requests.per.connection must be set to at most 5. Current value is 6.";

    private const string InitTimedOut =
        "Timeout expired after 2000ms while awaiting InitProducerId. InitTransactions timed out - did not complete "
        + "coordinator discovery or receive the InitProducerId response within max.block.ms.";

    private const string BeginAfterInitTimeout =
        "Cannot attempt operation `beginTransaction` because the previous call to `initTransactions` timed out and must be retried";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    // ---- D9: constructor validation (and KPT 238 / 341 / 413's invalid halves) ----

    /// <summary>
    /// D9 rows 1-5 and every <c>invalidProps*</c> of KPT 238 / 341 / 413: the combination fails in
    /// the constructor with the core's config error and the message its validation produces for
    /// that combination. <paramref name="source"/> names the row, so two sources with the same
    /// settings (D9 row 5 and KPT 413 <c>invalidProps1</c>) are separate cases and both run.
    /// </summary>
    [Theory]
    [MemberData(nameof(InvalidCombinations))]
    public void Constructor_WithAnInvalidCombination_FailsWithTheCoresMessage(string flavour, string source, string settings, string message)
    {
        _ = source; // A label only: it keeps the case IDs distinct.
        KafkaException failure = Assert.Throws<KafkaException>(() => Create(flavour, Parse(settings)).Dispose());

        Assert.Equal(-10, failure.Code);
        Assert.Equal(message, failure.Message);
    }

    /// <summary>The invalid combinations, each with its source row, once per flavour.</summary>
    public static IEnumerable<object[]> InvalidCombinations()
    {
        (string Source, string Settings, string Message)[] rows =
        {
            ("D9 row 1", "transactional.id=t,enable.idempotence=false", TransactionalIdNeedsIdempotence),
            ("D9 row 2", "transactional.id=t,acks=1", TransactionalIdNeedsIdempotence),
            ("D9 row 3", "enable.idempotence=true,acks=1", AcksMustBeAll),
            ("D9 row 4", "enable.idempotence=true,retries=0", RetriesMustBeNonZero),
            ("D9 row 5", "max.in.flight.requests.per.connection=6", MaxInFlightAtMostFive),

            ("KPT 238 invalidProps", "acks=0,enable.idempotence=false,transactional.id=transactionalId", TransactionalIdNeedsIdempotence),
            ("KPT 238 invalidProps2", "acks=1,enable.idempotence=true", AcksMustBeAll),
            ("KPT 238 invalidProps3", "acks=0,transactional.id=transactionalId", TransactionalIdNeedsIdempotence),

            ("KPT 341 invalidProps", "retries=0,enable.idempotence=false,transactional.id=transactionalId", TransactionalIdNeedsIdempotence),
            ("KPT 341 invalidProps2", "retries=0,enable.idempotence=true", RetriesMustBeNonZero),
            ("KPT 341 invalidProps3", "retries=0,transactional.id=transactionalId", TransactionalIdNeedsIdempotence),

            // invalidProps1 has D9 row 5's settings; the source label keeps it a separate case.
            ("KPT 413 invalidProps1", "max.in.flight.requests.per.connection=6", MaxInFlightAtMostFive),
            ("KPT 413 invalidProps2", "max.in.flight.requests.per.connection=5,enable.idempotence=false,transactional.id=transactionalId", TransactionalIdNeedsIdempotence),
            ("KPT 413 invalidProps3", "max.in.flight.requests.per.connection=6,enable.idempotence=true", MaxInFlightAtMostFive),
            ("KPT 413 invalidProps4", "max.in.flight.requests.per.connection=6,transactional.id=transactionalId", MaxInFlightAtMostFive),
        };
        foreach (string flavour in new[] { "sync", "async" })
        {
            foreach ((string source, string settings, string message) in rows)
            {
                yield return new object[] { flavour, source, settings, message };
            }
        }
    }

    // ---- KPT 238 / 341 / 413's valid halves ----

    /// <summary>
    /// Every <c>validProps*</c> of KPT 238 / 341 / 413 without a <c>transactional.id</c> constructs,
    /// and its effective idempotence is the one Java reads back, through the probe.
    /// </summary>
    [Theory]
    [MemberData(nameof(ValidCombinations))]
    public async Task Constructor_WithAValidCombination_HasTheExpectedIdempotence(string flavour, string settings, bool idempotent)
    {
        using IRealProducer producer = Create(flavour, Parse(settings));

        await AssertIdempotence(producer, idempotent);
    }

    /// <summary>The valid combinations without a <c>transactional.id</c>, once per flavour.</summary>
    public static IEnumerable<object[]> ValidCombinations()
    {
        (string Settings, bool Idempotent)[] rows =
        {
            // KPT 238 validProps / 3 / 4 / 5 (validProps2 carries a transactional.id: below).
            ("acks=0,enable.idempotence=false", false),
            ("acks=all,enable.idempotence=false", false),
            ("acks=0", false),
            ("acks=1", false),

            // KPT 341 validProps / 2.
            ("retries=0,enable.idempotence=false", false),
            ("retries=0", false),

            // KPT 413 validProps.
            ("max.in.flight.requests.per.connection=6,enable.idempotence=false", false),

            // The defaults: idempotent.
            (string.Empty, true),
        };
        foreach (string flavour in new[] { "sync", "async" })
        {
            foreach ((string settings, bool idempotent) in rows)
            {
                yield return new object[] { flavour, settings, idempotent };
            }
        }
    }

    /// <summary>
    /// KPT 238 <c>validProps2</c> and KPT 222 <c>testOverwriteAcksAndRetriesForIdempotentProducers</c>,
    /// partial: with only <c>transactional.id</c> set the producer constructs (so idempotence
    /// defaulted on — the constructor rejects a transactional id without it, rows above), and its
    /// effective <c>client.id</c> is <c>producer-&lt;transactional.id&gt;</c>, seen as the
    /// <c>client-id</c> tag on every metric.
    /// </summary>
    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public void Constructor_WithOnlyATransactionalId_DerivesTheClientId(string flavour)
    {
        using IRealProducer producer = Create(flavour, Parse("transactional.id=transactionalId"));

        IReadOnlyDictionary<MetricName, IMetric> metrics = producer.Metrics();

        Assert.NotEmpty(metrics);
        Assert.All(metrics.Keys, name => Assert.Equal("producer-transactionalId", name.Tags["client-id"]));
    }

    // ---- D9 rows 6-9, and D13 ----

    /// <summary>
    /// D9 row 6 and D13: with idempotence off and no <c>transactional.id</c> there is no transaction
    /// manager, and each of the five members reports so — including <c>SendOffsetsToTransaction</c>
    /// with an empty map, because the manager check precedes the empty check.
    /// </summary>
    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task EveryControlMember_WithIdempotenceOff_ReportsNoTransactionManager(string flavour)
    {
        using IRealProducer producer = Create(flavour, Parse("enable.idempotence=false"));

        await AssertCoreError(producer.InitTransactions, -4, NoTransactionManager);
        await AssertCoreError(() => Inline(producer.BeginTransaction), -4, NoTransactionManager);
        await AssertCoreError(() => producer.SendOffsetsToTransaction(NoOffsets(), Group()), -4, NoTransactionManager);
        await AssertCoreError(() => producer.SendOffsetsToTransaction(OneOffset(), Group()), -4, NoTransactionManager);
        await AssertCoreError(producer.CommitTransaction, -4, NoTransactionManager);
        await AssertCoreError(producer.AbortTransaction, -4, NoTransactionManager);
    }

    /// <summary>
    /// D9 rows 7-9 and D13: an idempotent producer without a <c>transactional.id</c> rejects the
    /// four state members as non-transactional; a non-empty <c>SendOffsetsToTransaction</c> is
    /// rejected too (measured); an empty one succeeds, as Java returns before any transaction
    /// check.
    /// </summary>
    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task ControlMembers_OnAnIdempotentNonTransactionalProducer_AreRejected_ButAnEmptyMapSucceeds(string flavour)
    {
        using IRealProducer producer = Create(flavour, Parse(string.Empty));

        await AssertCoreError(producer.InitTransactions, -4, NotTransactional);
        await AssertCoreError(() => Inline(producer.BeginTransaction), -4, NotTransactional);
        await AssertCoreError(producer.CommitTransaction, -4, NotTransactional);
        await AssertCoreError(producer.AbortTransaction, -4, NotTransactional);
        await AssertCoreError(() => producer.SendOffsetsToTransaction(OneOffset(), Group()), -4, NotTransactional);

        await Run(producer.SendOffsetsToTransaction(NoOffsets(), Group()));
    }

    // ---- KPT 1329 / 2054: the init timeout ----

    /// <summary>
    /// D9 row 10 and KPT 1329 <c>testInitTransactionTimeout</c>: with no broker,
    /// <c>InitTransactions()</c> times out after <c>max.block.ms</c> with a retriable error, and a
    /// second call is accepted — it times out the same way rather than being refused as an invalid
    /// transition.
    /// </summary>
    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task InitTransactions_WithNoBroker_TimesOut_AndCanBeRetried(string flavour)
    {
        using IRealProducer producer = Create(flavour, Parse("transactional.id=bad-transaction,max.block.ms=2000"));

        for (int attempt = 0; attempt < 2; attempt++)
        {
            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(() => Run(producer.InitTransactions()));
            Assert.Equal(7, failure.Code);
            Assert.Equal(InitTimedOut, failure.Message);
            Assert.True(failure.IsRetriable, $"attempt {attempt}: the timeout is not retriable");
        }
    }

    /// <summary>
    /// KPT 2054 <c>testOnlyCanExecuteCloseAfterInitTransactionsTimeout</c>: after the timeout,
    /// <c>BeginTransaction()</c> is refused, and the producer still closes within the bound. Java's
    /// <c>close(Duration.ofMillis(0))</c> has no .NET form (the ABI has no timed close), so a bounded
    /// <c>Dispose</c> stands in.
    /// </summary>
    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task BeginTransaction_AfterAnInitTimeout_IsRefused_AndTheProducerCloses(string flavour)
    {
        IRealProducer producer = Create(flavour, Parse("transactional.id=bad-transaction,max.block.ms=2000"));
        try
        {
            KafkaException timedOut = await Assert.ThrowsAsync<KafkaException>(() => Run(producer.InitTransactions()));
            Assert.Equal(7, timedOut.Code);
            Assert.Equal(InitTimedOut, timedOut.Message);

            await AssertCoreError(() => Inline(producer.BeginTransaction), -4, BeginAfterInitTimeout);
        }
        finally
        {
            TestTimeout.Run(producer.Dispose, s_deadline);
        }
    }

    // ---- helpers ----

    // The effective idempotence, through the transaction manager's presence (the class remarks).
    private static Task AssertIdempotence(IRealProducer producer, bool idempotent) =>
        AssertCoreError(producer.InitTransactions, -4, idempotent ? NotTransactional : NoTransactionManager);

    private static Dictionary<string, string> Parse(string settings)
    {
        Dictionary<string, string> config = new Dictionary<string, string> { ["bootstrap.servers"] = "localhost:9092" };
        foreach (string pair in settings.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries))
        {
            string[] parts = pair.Split('=');
            config[parts[0]] = parts[1];
        }

        return config;
    }

    private static IRealProducer Create(string flavour, Dictionary<string, string> config) => flavour switch
    {
        "sync" => new SyncProducer(new KafkaProducer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray)),
        "async" => new AsyncProducer(new AsyncKafkaProducer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray)),
        _ => throw new ArgumentOutOfRangeException(nameof(flavour), flavour, "unknown flavour"),
    };

    private static Dictionary<TopicPartition, OffsetAndMetadata> NoOffsets() => new Dictionary<TopicPartition, OffsetAndMetadata>();

    private static Dictionary<TopicPartition, OffsetAndMetadata> OneOffset() =>
        new Dictionary<TopicPartition, OffsetAndMetadata> { [new TopicPartition("t", 0)] = new OffsetAndMetadata(42) };

#pragma warning disable CS0618 // Java's tests build the metadata with the obsolete constructor too
    private static ConsumerGroupMetadata Group() => new ConsumerGroupMetadata("group");
#pragma warning restore CS0618

    private static async Task AssertCoreError(Func<Task> operation, int code, string message)
    {
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(() => Run(operation()));
        Assert.Equal(code, failure.Code);
        Assert.Equal(message, failure.Message);
    }

    private static Task Run(Task task) => TestTimeout.Run(() => task, s_deadline);

    private static Task Inline(Action action)
    {
        try
        {
            action();
            return Task.CompletedTask;
        }
        catch (Exception failure)
        {
            return Task.FromException(failure);
        }
    }

    /// <summary>
    /// The two real producers behind one shape. The sync adapter runs the member inline and reports
    /// its throw through the returned <see cref="Task"/>; its <see cref="IDisposable.Dispose"/> is
    /// bounded, since a real producer's close is a native call.
    /// </summary>
    private interface IRealProducer : IDisposable
    {
        Task InitTransactions();

        void BeginTransaction();

        Task SendOffsetsToTransaction(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata);

        Task CommitTransaction();

        Task AbortTransaction();

        IReadOnlyDictionary<MetricName, IMetric> Metrics();
    }

    private sealed class SyncProducer : IRealProducer
    {
        private readonly KafkaProducer<byte[], byte[]> _producer;

        public SyncProducer(KafkaProducer<byte[], byte[]> producer) => _producer = producer;

        public Task InitTransactions() => Inline(_producer.InitTransactions);

        public void BeginTransaction() => _producer.BeginTransaction();

        public Task SendOffsetsToTransaction(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata) =>
            Inline(() => _producer.SendOffsetsToTransaction(offsets, groupMetadata));

        public Task CommitTransaction() => Inline(_producer.CommitTransaction);

        public Task AbortTransaction() => Inline(_producer.AbortTransaction);

        public IReadOnlyDictionary<MetricName, IMetric> Metrics() => _producer.Metrics();

        public void Dispose() => TestTimeout.Run(_producer.Dispose, s_deadline);
    }

    private sealed class AsyncProducer : IRealProducer
    {
        private readonly AsyncKafkaProducer<byte[], byte[]> _producer;

        public AsyncProducer(AsyncKafkaProducer<byte[], byte[]> producer) => _producer = producer;

        public Task InitTransactions() => _producer.InitTransactions();

        public void BeginTransaction() => _producer.BeginTransaction();

        public Task SendOffsetsToTransaction(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata) =>
            _producer.SendOffsetsToTransaction(offsets, groupMetadata);

        public Task CommitTransaction() => _producer.CommitTransaction();

        public Task AbortTransaction() => _producer.AbortTransaction();

        public IReadOnlyDictionary<MetricName, IMetric> Metrics() => _producer.Metrics();

        public void Dispose() => TestTimeout.Run(_producer.Dispose, s_deadline);
    }
}
