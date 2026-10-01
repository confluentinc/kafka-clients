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
/// M5/P8a — synchronous-consumer teardown (PLAN §8): <see cref="IConsumer.Close()"/> /
/// <see cref="IConsumer.Close(TimeSpan)"/> / <see cref="IDisposable.Dispose"/> return without
/// hanging, are idempotent under double / mixed calls (the shared <c>TryBeginClose</c> latch),
/// and every public op throws <see cref="ObjectDisposedException"/> after teardown. Every close
/// / dispose runs under a <see cref="TestTimeout"/> hang guard (the teardown-returns
/// regression). There is no <c>DisposeAsync</c> — this is the synchronous surface.
/// </summary>
public sealed class PublicSyncConsumerTeardownTests
{
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
    public void CloseWithTimeout_ReturnsWithoutHang()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Run(() => consumer.Close(TimeSpan.FromSeconds(5)), s_deadline);
    }

    [Fact]
    public void CloseWithTimeout_Zero_ReturnsWithoutHang()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Run(() => consumer.Close(TimeSpan.Zero), s_deadline);
    }

    [Fact]
    public void DoubleDispose_IsSafe()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Dispose();
        consumer.Dispose();
    }

    [Fact]
    public void Close_ThenDispose_IsIdempotent()
    {
        // Close takes the one-shot latch and destroys; a subsequent Dispose loses the latch and
        // no-ops (closed-flag idempotence) — no double-close / double-destroy.
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Close();
        consumer.Dispose();
    }

    [Fact]
    public void Dispose_ThenClose_IsIdempotent()
    {
        // Dispose wins the latch; a subsequent Close loses it and no-ops (no throw).
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Dispose();
        consumer.Close();
    }

    [Fact]
    public void CloseWithTimeout_ThenClose_IsIdempotent()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Close(TimeSpan.FromSeconds(1));
        consumer.Close();
        consumer.Dispose();
    }

    [Fact]
    public void Close_OnMock_DoesNotHang_AfterSubscribe()
    {
        // Close surfaces a close KafkaException (unlike Dispose, which swallows it). Broker-free
        // a mock close succeeds — the error-surfacing path (throw) is by inspection (CloseSync
        // rethrows FromHandle's exception; Dispose swallows). Here the reachable assertion is a
        // broker-free Close completes without faulting.
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Subscribe(ProofTopic());

        TestTimeout.Run(() => consumer.Close(), s_deadline);
    }

    [Fact]
    public void UseAfterDispose_ThrowsObjectDisposed()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Dispose();

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
        // IConsumerCommon members after dispose.
        Assert.Throws<ObjectDisposedException>(() => consumer.Seek(new TopicPartition("t", 0), 0L));
        Assert.Throws<ObjectDisposedException>(() => consumer.GroupMetadata());
    }

    [Fact]
    public void UseAfterClose_ThrowsObjectDisposed()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Close();

        Assert.Throws<ObjectDisposedException>(() => consumer.Subscribe(ProofTopic()));
        Assert.Throws<ObjectDisposedException>(() => consumer.Poll(s_pollTimeout));
    }

    [Fact]
    public void ManyConsumers_CreateAndDispose_NoLeakOrCrash()
    {
        // Create/dispose many sync consumers — the handle-leak / double-free detector at the
        // public surface (SafeHandle ReleaseHandle → Consumer_destroy). Wrapped in the
        // TestTimeout hang guard like the rest of this class, so a teardown that stopped
        // returning fails the run instead of hanging it.
        //
        // Every call in the loop is synchronous, so it returns before Dispose and the
        // reference count is 1 at teardown — the destroy is immediate. The deferred-destroy
        // path is covered by PublicConsumerHandleProtectionTests.
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
