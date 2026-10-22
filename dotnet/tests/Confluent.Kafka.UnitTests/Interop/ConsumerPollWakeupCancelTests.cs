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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Wakeup and cancellation on <see cref="NativeConsumer"/>'s PollWithCallback: a poll after a wakeup faults once with
/// the Wakeup <see cref="KafkaException"/>, then the next returns the queued record, as Java's WakeupException; a
/// pre-cancelled token throws <see cref="OperationCanceledException"/> before the native call.
/// </summary>
/// <remarks>
/// The mock's poll never waits, so a wakeup sent while a poll is in flight cannot be tested here.
/// </remarks>
public sealed class ConsumerPollWakeupCancelTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private const string Topic = "wakeup-topic";
    private const int Partition = 0;

    private static Task<NativeConsumer> MockReadyToPoll()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        consumer.Assign(new[] { (Topic, Partition) });
        consumer.Seek(Topic, Partition, offset: 0);
        return Task.FromResult(consumer);
    }

    [Fact]
    public async Task Wakeup_ThenPoll_FaultsOnce_ThenReusable()
    {
        NativeConsumer consumer = await MockReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, Partition, offset: 0, Encoding.UTF8.GetBytes("k"), Encoding.UTF8.GetBytes("v"));

        consumer.Wakeup();
        KafkaException ex = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => consumer.PollWithCallback<byte[], byte[]>(s_pollTimeout, Serdes.ByteArray, Serdes.ByteArray), s_deadline));
        Assert.False(string.IsNullOrEmpty(ex.Message));

        ConsumerRecords<byte[], byte[]> records = await TestTimeoutResult(consumer.PollWithCallback<byte[], byte[]>(s_pollTimeout, Serdes.ByteArray, Serdes.ByteArray));
        Assert.Single(records);
    }

    [Fact]
    public async Task PollWithCallback_PreCanceledToken_ThrowsOperationCanceled()
    {
        NativeConsumer consumer = await MockReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => consumer.PollWithCallback<byte[], byte[]>(s_pollTimeout, Serdes.ByteArray, Serdes.ByteArray, cts.Token));
    }

    private static async Task<ConsumerRecords<byte[], byte[]>> TestTimeoutResult(Task<ConsumerRecords<byte[], byte[]>> op)
    {
        ConsumerRecords<byte[], byte[]> result = default!;
        await TestTimeout.Run(async () => result = await op, s_deadline);
        return result;
    }
}
