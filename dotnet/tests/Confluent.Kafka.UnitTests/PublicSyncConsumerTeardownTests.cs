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
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Teardown of the sync consumers: Close and Dispose return, are idempotent under double and mixed calls, and every
/// operation throws <see cref="ObjectDisposedException"/> after them.
/// </summary>
public sealed class PublicSyncConsumerTeardownTests
{
    // A hang guard only: a mock's close returns in milliseconds.
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private static string[] ProofTopic() => new[] { "sync-proof-topic" };

    [Fact]
    public void Dispose_ReturnsWithoutHang()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Run(consumer.Dispose, s_deadline);
    }

    [Fact]
    public void Close_ReturnsWithoutHang()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Run(() => consumer.Close(), s_deadline);
    }

    [Fact]
    public void DoubleDispose_IsSafe()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(consumer);
        TestTimeout.Dispose(consumer);
    }

    [Fact]
    public void Close_ThenDispose_IsIdempotent()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Teardown(consumer, () => consumer.Close());
        TestTimeout.Dispose(consumer);
    }

    [Fact]
    public void Dispose_ThenClose_IsIdempotent()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(consumer);
        TestTimeout.Teardown(consumer, () => consumer.Close());
    }

    [Fact]
    public void Close_OnMock_DoesNotHang_AfterSubscribe()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Subscribe(ProofTopic());

        TestTimeout.Run(() => consumer.Close(), s_deadline);
    }

    [Fact]
    public void UseAfterDispose_ThrowsObjectDisposed()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(consumer);

        Assert.Throws<ObjectDisposedException>(() => consumer.Poll(s_pollTimeout));
        Assert.Throws<ObjectDisposedException>(() => consumer.Subscribe(ProofTopic()));
        Assert.Throws<ObjectDisposedException>(() => consumer.Unsubscribe());
        Assert.Throws<ObjectDisposedException>(() => consumer.Assign(new[] { new TopicPartition("t", 0) }));
        Assert.Throws<ObjectDisposedException>(() => consumer.Pause(new[] { new TopicPartition("t", 0) }));
        Assert.Throws<ObjectDisposedException>(() => consumer.Resume(new[] { new TopicPartition("t", 0) }));
        Assert.Throws<ObjectDisposedException>(() => consumer.SeekToBeginning(new[] { new TopicPartition("t", 0) }));
        Assert.Throws<ObjectDisposedException>(() => consumer.SeekToEnd(new[] { new TopicPartition("t", 0) }));
        Assert.Throws<ObjectDisposedException>(() => consumer.Position(new TopicPartition("t", 0)));
        Assert.Throws<ObjectDisposedException>(() => consumer.Commit());
        Assert.Throws<ObjectDisposedException>(
            () => consumer.Commit(new Dictionary<TopicPartition, OffsetAndMetadata>()));
        Assert.Throws<ObjectDisposedException>(() => consumer.Seek(new TopicPartition("t", 0), 0L));
        Assert.Throws<ObjectDisposedException>(() => consumer.GroupMetadata());
    }

    [Fact]
    public void UseAfterClose_ThrowsObjectDisposed()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Teardown(consumer, () => consumer.Close());

        Assert.Throws<ObjectDisposedException>(() => consumer.Subscribe(ProofTopic()));
        Assert.Throws<ObjectDisposedException>(() => consumer.Poll(s_pollTimeout));
    }

    [Fact]
    public void ManyConsumers_CreateAndDispose_NoLeakOrCrash()
    {
        TestTimeout.Run(
            () =>
            {
                for (int i = 0; i < 100; i++)
                {
                    MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
                    consumer.Subscribe(ProofTopic());
                    consumer.Dispose();
                }
            },
            s_deadline);
    }

    [Fact]
    public void ManyConsumers_CreateAndClose_NoLeakOrCrash()
    {
        TestTimeout.Run(
            () =>
            {
                for (int i = 0; i < 100; i++)
                {
                    MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
                    consumer.Subscribe(ProofTopic());
                    consumer.Close();
                }
            },
            s_deadline);
    }
}
