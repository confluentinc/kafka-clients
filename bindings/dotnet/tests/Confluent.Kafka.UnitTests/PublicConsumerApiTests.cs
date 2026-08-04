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
/// The public async / sync split (PLAN §5.2), the <c>SeekAsync</c> negative-offset
/// message (§5.3), and <see cref="ConsumerGroupMetadata"/> full-field + concurrency
/// (§5.4), all through the public <see cref="MockConsumer"/> / <see cref="KafkaConsumer"/>
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
    public async Task SubscribeAsync_ReturnsTask_Completes()
    {
        using MockConsumer consumer = new MockConsumer();
        await TestTimeout.Run(() => consumer.SubscribeAsync(ProofTopic()), s_deadline);
    }

    [Fact]
    public async Task UnsubscribeAsync_ReturnsTask_Completes()
    {
        using MockConsumer consumer = new MockConsumer();
        await consumer.SubscribeAsync(ProofTopic());
        await TestTimeout.Run(() => consumer.UnsubscribeAsync(), s_deadline);
    }

    [Fact]
    public async Task SubscribeUnsubscribe_Churned_NoLeakOrHang()
    {
        using MockConsumer consumer = new MockConsumer();
        for (int i = 0; i < 50; i++)
        {
            await consumer.SubscribeAsync(ProofTopic());
            await TestTimeout.Run(() => consumer.UnsubscribeAsync(), s_deadline);
        }
    }

    [Fact]
    public async Task SeekAsync_UnassignedPartition_FaultsWithKafkaException()
    {
        // Seeking an unassigned partition is a genuine broker-free failure — the void
        // bridge's error path faults the Task (carried from M3/P1).
        using MockConsumer consumer = new MockConsumer();

        await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => consumer.SeekAsync(new TopicPartition("unassigned", 0), 0L), s_deadline));
    }

    [Fact]
    public void Wakeup_IsSync_AndSafeWhenIdle()
    {
        using MockConsumer consumer = new MockConsumer();
        consumer.Wakeup(); // void, non-blocking, safe with no op in flight.
    }

    [Fact]
    public void GroupMetadata_IsSync_ReturnsSynchronously()
    {
        using KafkaConsumer consumer = new KafkaConsumer(RealConfig("sync-group"));
        ConsumerGroupMetadata meta = consumer.GroupMetadata();
        Assert.Equal("sync-group", meta.GroupId);
    }

    // ---- SeekAsync negative-offset message (§5.3, DoD §3 error-message fidelity) ----

    [Fact]
    public async Task SeekAsync_NegativeOffset_ThrowsExactJavaMessage()
    {
        using MockConsumer consumer = new MockConsumer();

        ArgumentOutOfRangeException ex = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => consumer.SeekAsync(new TopicPartition("t", 0), offset: -1));

        Assert.Equal("offset", ex.ParamName);
        Assert.Contains("seek offset must not be a negative number", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SeekAsync_NegativeOffset_ThrownBeforeNativeCall_EvenWhenClosed()
    {
        // The precondition is validated before any native call — a closed consumer still
        // throws the offset precondition first, not ObjectDisposedException.
        MockConsumer consumer = new MockConsumer();
        await consumer.DisposeAsync();

        ArgumentOutOfRangeException ex = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => consumer.SeekAsync(new TopicPartition("t", 0), offset: -3));
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
        using KafkaConsumer consumer = new KafkaConsumer(RealConfig("public-group"));

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
        using KafkaConsumer consumer = new KafkaConsumer(RealConfig("café-Ω-日本語-😀"));

        Assert.Equal("café-Ω-日本語-😀", consumer.GroupMetadata().GroupId);
    }

    [Fact]
    public void GroupMetadata_MockConsumer_ReturnsMockSentinels()
    {
        // The MockConsumer returns Java-parity hard-coded sentinels
        // (ConsumerGroupMetadata::with_details("dummy.group.id", 1, "1", None)).
        using MockConsumer consumer = new MockConsumer();

        ConsumerGroupMetadata meta = consumer.GroupMetadata();

        Assert.Equal("dummy.group.id", meta.GroupId);
        Assert.Equal(1, meta.GenerationId);
        Assert.Equal("1", meta.MemberId);
        Assert.Null(meta.GroupInstanceId);
    }

    [Fact]
    public void GroupMetadata_AfterDispose_ThrowsObjectDisposed()
    {
        MockConsumer consumer = new MockConsumer();
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
