// Copyright 2026 Confluent Inc.
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
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// An async <c>Poll</c> that ends with a wakeup is made again only when the core's own wakeup ended it and that wakeup
/// was the consumer's leftover one: a <see cref="WakeupException"/> a callback threw, or one injected into a mock,
/// is thrown as the same instance even while a leftover wakeup of the consumer's own is pending.
/// </summary>
public sealed class PublicAsyncConsumerOwnWakeupTests
{
    private const string Topic = "own-wakeup";
    private const int Partition = 0;

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task Poll_ACarriedWakeupException_IsThrownAsTheSameInstance_WhileTheConsumersOwnWakeupIsLeft()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        LeaveTheConsumersOwnWakeup(consumer, armTheCore: false);
        WakeupException raised = new WakeupException();
        consumer.SetPollException(raised);

        Exception? thrown = await Record.ExceptionAsync(
            () => TestTimeout.Run(() => consumer.Poll(TimeSpan.Zero), s_deadline));

        Assert.Same(raised, thrown);
    }

    [Fact]
    public async Task Poll_EndedByTheConsumersOwnLeftoverWakeup_IsMadeAgain()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, Partition, offset: 0, key: null, value: Encoding.UTF8.GetBytes("v"));
        LeaveTheConsumersOwnWakeup(consumer, armTheCore: true);

        ConsumerRecords<byte[], byte[]> records =
            await TestTimeout.Run(() => consumer.Poll(TimeSpan.Zero), s_deadline);

        Assert.Equal(1, records.Count());
    }

    private static async Task<AsyncMockConsumer<byte[], byte[]>> ReadyToPoll()
    {
        AsyncMockConsumer<byte[], byte[]> consumer =
            new AsyncMockConsumer<byte[], byte[]>(new AsyncByteArrayDeserializer(), new AsyncByteArrayDeserializer());
        await TestTimeout.Run(() => consumer.Assign(new[] { new TopicPartition(Topic, Partition) }), s_deadline);
        await TestTimeout.Run(() => consumer.Seek(new TopicPartition(Topic, Partition), offset: 0), s_deadline);
        return consumer;
    }

    // What a cancelled operation leaves when the call its wakeup was sent to ended first: the wakeup marked as the
    // consumer's own and, with armTheCore, still armed in the core.
    private static void LeaveTheConsumersOwnWakeup(object consumer, bool armTheCore)
    {
        const BindingFlags Private = BindingFlags.NonPublic | BindingFlags.Instance;
        NativeConsumer native = (NativeConsumer)consumer.GetType().GetField("_native", Private)!.GetValue(consumer)!;
        typeof(NativeConsumer).GetField("_ownWakeLeft", Private)!.SetValue(native, true);
        if (armTheCore)
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.ConsumerWakeup(native.Handle, ref outErr);
            Assert.Equal(IntPtr.Zero, outErr);
        }
    }
}
