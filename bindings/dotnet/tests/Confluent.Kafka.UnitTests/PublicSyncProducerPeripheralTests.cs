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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Public-surface tests for the M11/P4 <b>sync</b> producer peripherals —
/// <see cref="IProducer.Flush"/> and <see cref="IProducer.PartitionsFor(string)"/> — over the sync C
/// ABI (<c>Producer_flush</c> / <c>Producer_partitions_for</c>, ffi §A2 sync-op auto-ref), exercised
/// through the public <see cref="MockProducer"/> / <see cref="IProducer"/> surface (PLAN §7).
/// </summary>
/// <remarks>
/// <b>Honest mock reachability (PLAN §2).</b> On a <see cref="MockProducer"/>
/// <see cref="IProducer.PartitionsFor(string)"/> succeeds broker-free but returns an <b>empty</b>
/// list for every topic (the mock ctor builds an empty cluster) — a success with an empty result,
/// not a fault; a populated list is integration-only. Every blocking op runs under a
/// <see cref="TestTimeout"/> hang guard.
/// </remarks>
public sealed class PublicSyncProducerPeripheralTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "sync-peripheral-topic";

    // ---- Flush ----

    [Fact]
    public void Flush_OnMock_Succeeds()
    {
        using MockProducer producer = new MockProducer();

        // No pending sends → the flush resolves immediately (broker-free) without hanging.
        TestTimeout.Run(() => producer.Flush(), s_deadline);
    }

    [Fact]
    public void Flush_AfterDispose_ThrowsObjectDisposed()
    {
        MockProducer producer = new MockProducer();
        producer.Dispose();

        Assert.Throws<ObjectDisposedException>(() => producer.Flush());
    }

    // ---- PartitionsFor (empty on the mock; empty topic forwarded, null rejected) ----

    [Fact]
    public void PartitionsFor_OnMock_ReturnsEmptyList()
    {
        using MockProducer producer = new MockProducer();

        IReadOnlyList<PartitionInfo> partitions = null!;
        TestTimeout.Run(() => partitions = producer.PartitionsFor(Topic), s_deadline);

        Assert.NotNull(partitions);
        Assert.Empty(partitions);
    }

    [Fact]
    public void PartitionsFor_EmptyTopic_ForwardedNotRejected_ReturnsEmptyList()
    {
        using MockProducer producer = new MockProducer();

        // Empty topic is FORWARDED (Java/Python-faithful — the binding guards only null); the mock
        // returns an empty list, proving the empty topic reached the core rather than being rejected.
        IReadOnlyList<PartitionInfo> partitions = null!;
        TestTimeout.Run(() => partitions = producer.PartitionsFor(string.Empty), s_deadline);

        Assert.NotNull(partitions);
        Assert.Empty(partitions);
    }

    [Fact]
    public void PartitionsFor_NullTopic_ThrowsArgumentNull()
    {
        using MockProducer producer = new MockProducer();

        // Default message (ArgumentNullException(nameof(topic))) → assert ParamName only.
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => producer.PartitionsFor(null!));
        Assert.Equal("topic", ex.ParamName);
    }

    [Fact]
    public void PartitionsFor_NullTopic_ThrownBeforeDisposedCheck_EvenWhenClosed()
    {
        // The null-topic guard in PartitionsForSync precedes ThrowIfClosed, so a disposed producer +
        // null topic surfaces ArgumentNullException, NOT ObjectDisposedException (verified source).
        MockProducer producer = new MockProducer();
        producer.Dispose();

        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => producer.PartitionsFor(null!));
        Assert.Equal("topic", ex.ParamName);
    }

    [Fact]
    public void PartitionsFor_AfterDispose_ThrowsObjectDisposed()
    {
        MockProducer producer = new MockProducer();
        producer.Dispose();

        Assert.Throws<ObjectDisposedException>(() => producer.PartitionsFor(Topic));
    }

    [Fact]
    public void PartitionsFor_CalledRepeatedly_Succeeds()
    {
        using MockProducer producer = new MockProducer();

        // Each call frees its owned PartitionInfoList root (ffi §B2) — repeated calls must not leak
        // or fault; the copy-out returns a fresh empty list each time.
        for (int i = 0; i < 5; i++)
        {
            IReadOnlyList<PartitionInfo> partitions = null!;
            TestTimeout.Run(() => partitions = producer.PartitionsFor(Topic), s_deadline);
            Assert.Empty(partitions);
        }
    }
}
