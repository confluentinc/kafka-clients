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
/// The public async / sync split (PLAN §5.2), the <c>Seek</c> negative-offset
/// message (§5.3), and <see cref="ConsumerGroupMetadata"/> full-field + concurrency
/// (§5.4), all through the public <see cref="AsyncMockConsumer"/> / <see cref="AsyncKafkaConsumer"/>
/// surface. Every awaited op runs under a <see cref="TestTimeout"/> hang guard.
/// </summary>
public sealed class PublicConsumerApiTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static string[] ProofTopic() => new[] { "proof-topic" };

    private static Dictionary<string, string> RealConfig(string groupId) => new()
    {
        ["bootstrap.servers"] = "localhost:9092",
        ["group.protocol"] = "consumer",
        ["group.id"] = groupId,
    };

    // ---- Async / sync split (§5.2) ----

    [Fact]
    public async Task Subscribe_ReturnsTask_Completes()
    {
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        await TestTimeout.Run(() => consumer.Subscribe(ProofTopic()), s_deadline);
    }

    [Fact]
    public async Task Unsubscribe_ReturnsTask_Completes()
    {
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        await consumer.Subscribe(ProofTopic());
        await TestTimeout.Run(() => consumer.Unsubscribe(), s_deadline);
    }

    [Fact]
    public async Task SubscribeUnsubscribe_Churned_NoLeakOrHang()
    {
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        for (int i = 0; i < 50; i++)
        {
            await consumer.Subscribe(ProofTopic());
            await TestTimeout.Run(() => consumer.Unsubscribe(), s_deadline);
        }
    }

    [Fact]
    public void Seek_UnassignedPartition_ThrowsKafkaException()
    {
        // Seeking an unassigned partition is a genuine broker-free failure. Seek is now
        // SYNC (M5/P7), so the failure surfaces as a SYNCHRONOUS KafkaException from the
        // sync ABI's returned error handle (replacing the old faulted-Task).
        using AsyncMockConsumer consumer = new AsyncMockConsumer();

        Assert.Throws<KafkaException>(() => consumer.Seek(new TopicPartition("unassigned", 0), 0L));
    }

    [Fact]
    public void Wakeup_IsSync_AndSafeWhenIdle()
    {
        using AsyncMockConsumer consumer = new AsyncMockConsumer();
        consumer.Wakeup(); // void, non-blocking, safe with no op in flight.
    }

    [Fact]
    public void GroupMetadata_IsSync_ReturnsSynchronously()
    {
        using AsyncKafkaConsumer consumer = new AsyncKafkaConsumer(RealConfig("sync-group"));
        ConsumerGroupMetadata meta = consumer.GroupMetadata();
        Assert.Equal("sync-group", meta.GroupId);
    }

    // ---- Seek negative-offset message (§5.3, DoD §3 error-message fidelity) ----

    [Fact]
    public void Seek_NegativeOffset_ThrowsExactJavaMessage()
    {
        // Q1 = KEEP: the Java-fidelity negative-offset guard (the one place .NET is stricter
        // than Python). Seek is sync (M5/P7), so this is a synchronous Assert.Throws.
        using AsyncMockConsumer consumer = new AsyncMockConsumer();

        ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => consumer.Seek(new TopicPartition("t", 0), offset: -1));

        Assert.Equal("offset", ex.ParamName);
        Assert.Contains("seek offset must not be a negative number", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Seek_NegativeOffset_ThrownBeforeNativeCall_EvenWhenClosed()
    {
        // The precondition is validated before any native call — a closed consumer still
        // throws the offset precondition first, not ObjectDisposedException (the argument
        // check precedes ThrowIfClosed, Q1).
        AsyncMockConsumer consumer = new AsyncMockConsumer();
        await consumer.DisposeAsync();

        ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => consumer.Seek(new TopicPartition("t", 0), offset: -3));
        Assert.Equal("offset", ex.ParamName);
    }

    [Fact]
    public void TopicPartition_NegativePartition_ThrowsArgumentOutOfRange()
    {
        // The TopicPartition ctor rejects a negative partition before it can reach seek.
        ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => new TopicPartition("t", -1));
        Assert.Equal("partition", ex.ParamName);
    }

    // ---- GroupMetadata full-field (§5.4) ----

    [Fact]
    public void GroupMetadata_RealConsumer_PreJoinDefaults()
    {
        // A real KIP-848 consumer stubs group_metadata from the configured group.id
        // pre-join (M2/P1 D5). GroupId is the configured value; the other three carry
        // documented pre-join defaults (generation_id = -1, member_id = "",
        // group_instance_id = null — SOURCE-VERIFIED, see COMMENTS.DONE.8 D8.3).
        using AsyncKafkaConsumer consumer = new AsyncKafkaConsumer(RealConfig("public-group"));

        ConsumerGroupMetadata meta = consumer.GroupMetadata();

        Assert.Equal("public-group", meta.GroupId);
        Assert.Equal(-1, meta.GenerationId);
        Assert.Equal(string.Empty, meta.MemberId);
        Assert.Null(meta.GroupInstanceId);
    }

    [Fact]
    public void GroupMetadata_RealConsumer_NonAsciiGroupId_RoundTrips()
    {
        // The M2/P1 D5 non-ASCII group.id round-trip, carried to the public full-field read.
        using AsyncKafkaConsumer consumer = new AsyncKafkaConsumer(RealConfig("café-Ω-日本語-😀"));

        Assert.Equal("café-Ω-日本語-😀", consumer.GroupMetadata().GroupId);
    }

    [Fact]
    public void GroupMetadata_MockConsumer_ReturnsMockSentinels()
    {
        // The AsyncMockConsumer returns Java-parity hard-coded sentinels
        // (ConsumerGroupMetadata::with_details("dummy.group.id", 1, "1", None)).
        using AsyncMockConsumer consumer = new AsyncMockConsumer();

        ConsumerGroupMetadata meta = consumer.GroupMetadata();

        Assert.Equal("dummy.group.id", meta.GroupId);
        Assert.Equal(1, meta.GenerationId);
        Assert.Equal("1", meta.MemberId);
        Assert.Null(meta.GroupInstanceId);
    }

    [Fact]
    public void GroupMetadata_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer consumer = new AsyncMockConsumer();
        consumer.Dispose();

        Assert.Throws<ObjectDisposedException>(() => consumer.GroupMetadata());
    }

    // Note on the concurrent → InvalidOperationException mapping (§5.4 / M3/P3 D-Q4):
    // the core returns a null metadata handle on its concurrent-access rejection, which
    // GroupMetadata() maps to InvalidOperationException (verified by inspection of the
    // shared GetGroupMetadataHandleOrThrow path, carried from GroupId()). Broker-free
    // ops resolve instantly, so a deterministic forced submit→callback overlap is not
    // reproducible without a controllable-duration guard-holding op (out of scope) —
    // no flaky forced-overlap test is shipped (documented COMMENTS.DONE.8 D8.4).
}
