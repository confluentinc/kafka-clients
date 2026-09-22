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
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The M9/P2 consumer <see cref="IConsumerCommon.Metrics"/> surface — Java
/// <c>Map&lt;MetricName, ? extends Metric&gt; metrics()</c> — through the <b>public</b>
/// consumer types (sync + async, real + mock), broker-free. Metrics is a non-blocking sync
/// state read on <see cref="IConsumerCommon"/>, so every consumer flavor inherits it.
/// </summary>
/// <remarks>
/// <para>
/// The mock's <c>metrics()</c> is an <b>empty</b> map (Java <c>MockConsumer</c> parity), so
/// the mock legs prove the <c>count == 0</c> marshal path. The real KIP-848 consumer
/// registers its full metric set at construction (fetch / kafka-consumer / heartbeat /
/// offset-commit / rebalance / async sensors), so a broker-free real consumer's map is
/// <b>non-empty</b> — the real legs prove the populated copy-out and the
/// <see cref="MetricName"/>-key value identity end to end.
/// </para>
/// <para>
/// The per-value-kind boxed-type mapping (double / string / long / int → the CLR runtime
/// type of <see cref="IMetric.Value"/>) is proven at the ABI level by the Rust FFI test
/// <c>metric_map_carries_all_value_kinds_and_tags</c> and by inspection of
/// <c>MetricMapMarshal.ReadValue</c>; the real consumer's sensors are all <c>Double</c>-kind,
/// so the real legs additionally assert every value is a valid boxed numeric/string.
/// </para>
/// </remarks>
public sealed class PublicConsumerMetricsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static Dictionary<string, string> RealConfig(string groupId) => new()
    {
        ["bootstrap.servers"] = "localhost:9092",
        ["group.protocol"] = "consumer",
        ["group.id"] = groupId,
    };

    // ---- Mock: empty map (Java MockConsumer parity), sync + async ----

    [Fact]
    public void Metrics_AsyncMock_IsEmpty()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        IReadOnlyDictionary<MetricName, IMetric> metrics = consumer.Metrics();
        Assert.NotNull(metrics);
        Assert.Empty(metrics);
    }

    [Fact]
    public void Metrics_SyncMock_IsEmpty()
    {
        using MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        Assert.Empty(consumer.Metrics());
    }

    // ---- Real consumer: populated map, well-formed entries (sync + async) ----

    [Fact]
    public void Metrics_AsyncRealConsumer_IsPopulatedAndWellFormed()
    {
        using AsyncKafkaConsumer<byte[], byte[]> consumer = new AsyncKafkaConsumer<byte[], byte[]>(
            RealConfig("metrics-async"), Serdes.ByteArray, Serdes.ByteArray);

        AssertPopulatedAndWellFormed(consumer.Metrics());
    }

    [Fact]
    public void Metrics_SyncRealConsumer_IsPopulatedAndWellFormed()
    {
        using KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(
            RealConfig("metrics-sync"), Serdes.ByteArray, Serdes.ByteArray);

        AssertPopulatedAndWellFormed(consumer.Metrics());
    }

    [Fact]
    public void Metrics_RealConsumer_KeyHasValueIdentity_DescriptionExcluded()
    {
        // Round-trip the MetricName-key value identity end to end: pick a real key, rebuild it
        // with the SAME name/group/tags but a DIFFERENT description, and index the same entry —
        // proving the dictionary keys on value identity and excludes description.
        using AsyncKafkaConsumer<byte[], byte[]> consumer = new AsyncKafkaConsumer<byte[], byte[]>(
            RealConfig("metrics-key-identity"), Serdes.ByteArray, Serdes.ByteArray);

        IReadOnlyDictionary<MetricName, IMetric> metrics = consumer.Metrics();
        Assert.NotEmpty(metrics);

        foreach (KeyValuePair<MetricName, IMetric> entry in metrics)
        {
            MetricName original = entry.Key;
            MetricName rebuilt = new MetricName(
                original.Name,
                original.Group,
                original.Description + "-mutated",
                new Dictionary<string, string>(RebuildTags(original.Tags), StringComparer.Ordinal));

            Assert.True(metrics.ContainsKey(rebuilt));
            Assert.Same(entry.Value, metrics[rebuilt]);
            return; // one entry is enough to prove the contract.
        }
    }

    // ---- Post-dispose → ObjectDisposedException (sync + async) ----

    [Fact]
    public void Metrics_AfterDispose_ThrowsObjectDisposed()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Dispose();
        Assert.Throws<ObjectDisposedException>(() => consumer.Metrics());
    }

    [Fact]
    public async Task Metrics_AfterDisposeAsync_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await consumer.DisposeAsync();
        Assert.Throws<ObjectDisposedException>(() => consumer.Metrics());
    }

    // ---- Reachable via the interface (single-owner, non-generic base) ----

    [Fact]
    public void Metrics_ReachableViaIConsumerCommon()
    {
        using MockConsumer<byte[], byte[]> mock = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        IConsumerCommon consumer = mock;
        Assert.Empty(consumer.Metrics());
    }

    // ---- Concurrent access → InvalidOperationException with the exact message ----

    [Fact]
    public async Task Metrics_ConcurrentWithInFlightPoll_ThrowsInvalidOperationWithExactMessage()
    {
        // Deterministic concurrent-use proof (DoD §3 error-message content). Unlike the mock
        // (whose poll does NOT block, so it can't hold the single-owner guard for a controllable
        // window — the documented D-Q4 ceiling), a REAL consumer's poll BLOCKS: Consumer_poll_async
        // acquires the core's single-owner guard SYNCHRONOUSLY before Poll() returns, and releases
        // it only when the poll future resolves (broker-free: at the timeout or on Wakeup). So the
        // guard is provably held while the poll is in flight, and a concurrent sync Metrics() is
        // rejected by the core with a null handle → InvalidOperationException. No race: the acquire
        // happens on this thread before Poll() returns.
        using AsyncKafkaConsumer<byte[], byte[]> consumer = new AsyncKafkaConsumer<byte[], byte[]>(
            RealConfig("metrics-concurrent"), Serdes.ByteArray, Serdes.ByteArray);

        // Subscribe (resolves broker-free, releasing the guard) so the following poll blocks.
        await TestTimeout.Run(() => consumer.Subscribe(new[] { "metrics-concurrent-probe" }), s_deadline);

        Task<ConsumerRecords<byte[], byte[]>> inFlight = consumer.Poll(TimeSpan.FromSeconds(20));
        try
        {
            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() => consumer.Metrics());
            Assert.Equal("KafkaConsumer is not safe for multi-threaded access.", ex.Message);
        }
        finally
        {
            await DrainInFlightPollAsync(consumer, inFlight);
        }
    }

    private static void AssertPopulatedAndWellFormed(IReadOnlyDictionary<MetricName, IMetric> metrics)
    {
        Assert.NotNull(metrics);
        Assert.NotEmpty(metrics);

        foreach (KeyValuePair<MetricName, IMetric> entry in metrics)
        {
            MetricName name = entry.Key;
            IMetric metric = entry.Value;

            Assert.NotNull(name);
            Assert.NotNull(name.Name);
            Assert.NotNull(name.Group);
            Assert.NotNull(name.Description);
            Assert.NotNull(name.Tags);

            Assert.NotNull(metric);
            // IMetric.Name must equal (by value identity) the dictionary key.
            Assert.Equal(name, metric.Name);

            // Value is a boxed CLR value whose runtime type IS the kind (D2): one of the four.
            object value = metric.Value;
            Assert.NotNull(value);
            Assert.True(
                value is double || value is string || value is long || value is int,
                $"Unexpected metric value type {value.GetType()} for {name.Name}");
        }
    }

    private static Dictionary<string, string> RebuildTags(IReadOnlyDictionary<string, string> tags)
    {
        Dictionary<string, string> copy = new Dictionary<string, string>(tags.Count, StringComparer.Ordinal);
        foreach (KeyValuePair<string, string> tag in tags)
        {
            copy[tag.Key] = tag.Value;
        }

        return copy;
    }

    // Interrupt + await the in-flight real-consumer poll so the guard releases and the
    // subsequent Dispose has no stranded op. Best-effort: the poll faults with a Wakeup
    // KafkaException; a drain timeout is swallowed (Dispose still tears the consumer down).
    private static async Task DrainInFlightPollAsync(AsyncKafkaConsumer<byte[], byte[]> consumer, Task<ConsumerRecords<byte[], byte[]>> inFlight)
    {
        consumer.Wakeup();
        try
        {
            await TestTimeout.Run(
                async () =>
                {
                    try
                    {
                        await inFlight;
                    }
                    catch (KafkaException)
                    {
                        // Expected: Wakeup faults the in-flight poll.
                    }
                },
                s_deadline);
        }
        catch (TimeoutException)
        {
            // Best-effort — the using-scope Dispose still tears the consumer down.
        }
    }
}
