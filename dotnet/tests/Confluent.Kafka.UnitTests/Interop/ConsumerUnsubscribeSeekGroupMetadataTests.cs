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

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The M4/P4a <c>NativeConsumer</c> edits driven through the internal surface: the new
/// <c>UnsubscribeWithCallback</c> void wire (<c>Consumer_unsubscribe_async</c>), the sync
/// <c>Seek</c> negative-<b>offset</b> precondition (Java-fidelity, Q1; M5/P7 made Seek sync),
/// and <c>GroupMetadata()</c> (the full four-field owned-handle read). Every awaited op
/// runs under a <see cref="TestTimeout"/> hang guard.
/// </summary>
public sealed class ConsumerUnsubscribeSeekGroupMetadataTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static string[] ProofTopic() => new[] { "proof-topic" };

    private static Dictionary<string, string> RealConsumerConfig(
        string groupId, string? groupInstanceId = null)
    {
        var config = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "localhost:9092",
            ["group.protocol"] = "consumer",
            ["group.id"] = groupId,
        };
        if (groupInstanceId is not null)
        {
            config["group.instance.id"] = groupInstanceId;
        }

        return config;
    }

    // ---- UnsubscribeWithCallback (the one new void wire) ----

    [Fact]
    public async Task UnsubscribeWithCallback_OnMock_Completes()
    {
        using NativeConsumer consumer = NativeConsumer.CreateMock();

        await consumer.SubscribeWithCallback(ProofTopic());
        await TestTimeout.Run(() => consumer.UnsubscribeWithCallback(), s_deadline);
    }

    [Fact]
    public async Task UnsubscribeWithCallback_WithoutSubscribe_Completes()
    {
        // Unsubscribing when not subscribed is a no-op that still resolves the Task.
        using NativeConsumer consumer = NativeConsumer.CreateMock();

        await TestTimeout.Run(() => consumer.UnsubscribeWithCallback(), s_deadline);
    }

    [Fact]
    public async Task UnsubscribeWithCallback_Churned_NoLeakOrHang()
    {
        // Churn subscribe → unsubscribe: the void bridge (batch _destroy is N/A here,
        // but the per-op GCHandle) must be freed exactly once per op. A leak / double
        // free would corrupt the allocator over many iterations.
        using NativeConsumer consumer = NativeConsumer.CreateMock();

        for (int i = 0; i < 100; i++)
        {
            await consumer.SubscribeWithCallback(ProofTopic());
            await TestTimeout.Run(() => consumer.UnsubscribeWithCallback(), s_deadline);
        }
    }

    [Fact]
    public async Task UnsubscribeWithCallback_AfterDispose_ThrowsObjectDisposed()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        await consumer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.UnsubscribeWithCallback());
    }

    // ---- Seek (sync, M5/P7) negative-offset precondition (Java-fidelity, DoD §3 message) ----

    [Fact]
    public void Seek_NegativeOffset_ThrowsWithExactJavaMessage()
    {
        using NativeConsumer consumer = NativeConsumer.CreateMock();

        ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => consumer.Seek("proof-topic", 0, offset: -1));

        // The exact Java message is part of the behavioral contract (DoD §3), kept under Q1.
        Assert.Equal("offset", ex.ParamName);
        Assert.Contains("seek offset must not be a negative number", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Seek_NegativeOffset_ThrownBeforeNativeCall_EvenWhenClosed()
    {
        // Preconditions are validated BEFORE the FFI call (ffi §B5). Even a closed
        // consumer throws the offset precondition (ArgumentOutOfRangeException), not
        // ObjectDisposedException — the argument check precedes ThrowIfClosed inside
        // the sync Seek (Q1).
        NativeConsumer consumer = NativeConsumer.CreateMock();
        await consumer.DisposeAsync();

        ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => consumer.Seek("proof-topic", 0, offset: -5));
        Assert.Equal("offset", ex.ParamName);
    }

    [Fact]
    public void Seek_NegativePartition_ThrowsArgumentOutOfRange()
    {
        // The partition precondition is carried unchanged from M3/P1.
        using NativeConsumer consumer = NativeConsumer.CreateMock();

        ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => consumer.Seek("proof-topic", partition: -1, offset: 0));
        Assert.Equal("partition", ex.ParamName);
    }

    // ---- GroupMetadata() full-field read (M2/P1 D5 + PLAN dec. 5) ----

    [Fact]
    public void GroupMetadata_RealConsumer_PreJoin_ReturnsConfiguredGroupIdAndDefaults()
    {
        // A real KIP-848 consumer stubs group_metadata from the configured group.id
        // before join (M2/P1 D5). The reachable broker-free field is GroupId (the
        // configured value); generation_id = -1 and member_id = "" are the documented
        // pre-join defaults (UNKNOWN_GENERATION_ID / UNKNOWN_MEMBER_ID in the core);
        // group.instance.id is unset here → GroupInstanceId is null.
        using NativeConsumer consumer = NativeConsumer.Create(RealConsumerConfig("proof-group"));

        ConsumerGroupMetadata meta = consumer.GroupMetadata();

        Assert.Equal("proof-group", meta.GroupId);
        Assert.Equal(-1, meta.GenerationId);
        Assert.Equal(string.Empty, meta.MemberId);
        Assert.Null(meta.GroupInstanceId);
    }

    [Fact]
    public void GroupMetadata_RealConsumer_NonAsciiGroupId_RoundTrips()
    {
        // The M2/P1 D5 non-ASCII group.id round-trip, carried to the full-field read.
        using NativeConsumer consumer = NativeConsumer.Create(RealConsumerConfig("grüße-café-Ω-日本語-😀"));

        ConsumerGroupMetadata meta = consumer.GroupMetadata();

        Assert.Equal("grüße-café-Ω-日本語-😀", meta.GroupId);
    }

    [Fact]
    public void GroupMetadata_RealConsumer_WithGroupInstanceId_IsNullPreJoin()
    {
        // SOURCE-VERIFIED broker-free behavior (M2/P1 D5 precedent): the pre-join stub
        // is ConsumerGroupMetadata::new(group_id) (async_kafka_consumer.rs:1639), which
        // sets group_instance_id = None regardless of the configured group.instance.id
        // — the configured value only surfaces POST-JOIN via the state notifier (against
        // a broker). So broker-free, GroupInstanceId is null even when configured. Only
        // GroupId is reachable pre-join; the other three carry documented defaults. This
        // asserts the reachable truth (a flaky "reads back instance-7" would need a
        // broker) and proves the group_instance_id accessor's null → null mapping.
        using NativeConsumer consumer = NativeConsumer.Create(
            RealConsumerConfig("static-group", groupInstanceId: "instance-7"));

        ConsumerGroupMetadata meta = consumer.GroupMetadata();

        Assert.Equal("static-group", meta.GroupId);
        Assert.Null(meta.GroupInstanceId);
    }

    [Fact]
    public void GroupMetadata_MockConsumer_ReturnsMockSentinels()
    {
        // The MockConsumer returns Java-parity hard-coded sentinels
        // (ConsumerGroupMetadata::with_details("dummy.group.id", 1, "1", None)) — it
        // ignores config. Asserts the full-field marshal off those deterministic values.
        using NativeConsumer consumer = NativeConsumer.CreateMock();

        ConsumerGroupMetadata meta = consumer.GroupMetadata();

        Assert.Equal("dummy.group.id", meta.GroupId);
        Assert.Equal(1, meta.GenerationId);
        Assert.Equal("1", meta.MemberId);
        Assert.Null(meta.GroupInstanceId);
    }

    [Fact]
    public void GroupMetadata_AfterDispose_ThrowsObjectDisposed()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        consumer.Dispose();

        Assert.Throws<ObjectDisposedException>(() => consumer.GroupMetadata());
    }

    // ---- CloseWithCallback() wiring (PLAN decision 6: latch → close → destroy, surface error) ----

    [Fact]
    public async Task CloseWithCallback_OnMock_ReturnsWithoutHang()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();

        await TestTimeout.Run(async () => await consumer.CloseWithCallback(), s_deadline);
    }

    [Fact]
    public async Task CloseWithCallback_ThenDisposeAsync_IsIdempotent()
    {
        // CloseWithCallback takes the one-shot latch and destroys; a subsequent DisposeAsync
        // loses the latch and no-ops (no double-destroy).
        NativeConsumer consumer = NativeConsumer.CreateMock();

        await consumer.CloseWithCallback();
        await consumer.DisposeAsync();
        consumer.Dispose();
    }

    [Fact]
    public async Task CloseWithCallback_UseAfterClose_ThrowsObjectDisposed()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        await consumer.CloseWithCallback();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.SubscribeWithCallback(ProofTopic()));
    }
}
