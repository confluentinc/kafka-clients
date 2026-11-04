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

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Producer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The sync producer's Flush and PartitionsFor on <see cref="MockProducer{TKey, TValue}"/>: the mock's cluster is
/// empty, so PartitionsFor returns an empty list; an empty topic reaches the core, a null one is rejected first.
/// Every blocking call is bounded.
/// </summary>
public sealed class PublicSyncProducerPeripheralTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "sync-peripheral-topic";

    [Fact]
    public void Flush_OnMock_Succeeds()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        TestTimeout.Run(() => producer.Flush(), s_deadline);
    }

    [Fact]
    public void Flush_AfterDispose_ThrowsObjectDisposed()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(producer);

        Assert.Throws<ObjectDisposedException>(() => producer.Flush());
    }

    [Fact]
    public void PartitionsFor_OnMock_ReturnsEmptyList()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        IReadOnlyList<PartitionInfo> partitions = null!;
        TestTimeout.Run(() => partitions = producer.PartitionsFor(Topic), s_deadline);

        Assert.NotNull(partitions);
        Assert.Empty(partitions);
    }

    [Fact]
    public void PartitionsFor_EmptyTopic_ForwardedNotRejected_ReturnsEmptyList()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        // An empty topic reaches the core, as in Java and Python; only null is rejected.
        IReadOnlyList<PartitionInfo> partitions = null!;
        TestTimeout.Run(() => partitions = producer.PartitionsFor(string.Empty), s_deadline);

        Assert.NotNull(partitions);
        Assert.Empty(partitions);
    }

    [Fact]
    public void PartitionsFor_NullTopic_ThrowsArgumentNull()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        // ArgumentNullException sets no custom message, so ParamName is the asserted contract.
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => producer.PartitionsFor(null!));
        Assert.Equal("topic", ex.ParamName);
    }

    [Fact]
    public void PartitionsFor_NullTopic_ThrownBeforeDisposedCheck_EvenWhenClosed()
    {
        // The null check precedes the closed check, so a closed producer with a null topic throws
        // ArgumentNullException.
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(producer);

        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => producer.PartitionsFor(null!));
        Assert.Equal("topic", ex.ParamName);
    }

    [Fact]
    public void PartitionsFor_AfterDispose_ThrowsObjectDisposed()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(producer);

        Assert.Throws<ObjectDisposedException>(() => producer.PartitionsFor(Topic));
    }

    [Fact]
    public void PartitionsFor_CalledRepeatedly_Succeeds()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        // Repeated calls free their owned list root once each and return a fresh empty list.
        for (int i = 0; i < 5; i++)
        {
            IReadOnlyList<PartitionInfo> partitions = null!;
            TestTimeout.Run(() => partitions = producer.PartitionsFor(Topic), s_deadline);
            Assert.Empty(partitions);
        }
    }
}
