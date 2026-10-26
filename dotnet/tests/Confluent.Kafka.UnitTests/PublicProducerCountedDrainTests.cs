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
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The completion thread's drain is a use of the producer's handle: it is counted while it runs, a close does not
/// refuse it, and once the handle is destroyed a drain is refused without calling into it.
/// </summary>
public sealed class PublicProducerCountedDrainTests
{
    private const BindingFlags Private = BindingFlags.NonPublic | BindingFlags.Instance;

    private const string Topic = "counted-drain-topic";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task Drain_IsCountedAsAUse_CloseDoesNotRefuseIt_AndTheDestroyDoes()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(
            Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        NativeProducer native = NativeOf(producer);
        using ManualResetEventSlim entered = new ManualResetEventSlim();
        using ManualResetEventSlim release = new ManualResetEventSlim();
        Task closing;
        try
        {
            _ = producer.Send(
                new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0),
                new Parking(entered, release));
            Assert.True(producer.CompleteNext());
            Assert.True(entered.Wait(s_deadline), "the delivery callback never ran in the drain");

            Assert.Equal(1, UseCount(native));

            closing = Task.Run(producer.Close);
            Assert.True(SpinWait.SpinUntil(() => CloseBegan(native), s_deadline), "the close never began");
            Assert.Equal(1, UseCount(native));
            Assert.False(closing.IsCompleted, "the close completed while the drain was still running");
        }
        finally
        {
            release.Set();
        }

        await TestTimeout.Run(() => closing, s_deadline);

        Assert.Equal(0, UseCount(native));
        Assert.True(Destroyed(native), "the destroy did not mark the handle destroyed");
        Assert.Equal(0, DrainOf(native)());
    }

    private static NativeProducer NativeOf(MockProducer<byte[], byte[]> producer) =>
        (NativeProducer)typeof(MockProducer<byte[], byte[]>).GetField("_native", Private)!.GetValue(producer)!;

    private static object UsesOf(NativeProducer native) =>
        typeof(NativeProducer).GetField("_uses", Private)!.GetValue(native)!;

    private static int UseCount(NativeProducer native)
    {
        object uses = UsesOf(native);
        return (int)uses.GetType().GetField("_count", Private)!.GetValue(uses)!;
    }

    private static bool Destroyed(NativeProducer native)
    {
        object uses = UsesOf(native);
        return (bool)uses.GetType().GetField("_destroyed", Private)!.GetValue(uses)!;
    }

    private static bool CloseBegan(NativeProducer native) =>
        (int)typeof(NativeProducer).GetField("_closed", Private)!.GetValue(native)! != 0;

    private static Func<int> DrainOf(NativeProducer native)
    {
        CompletionThread completion =
            (CompletionThread)typeof(NativeProducer).GetField("_completion", Private)!.GetValue(native)!;
        return (Func<int>)typeof(CompletionThread).GetField("_drain", Private)!.GetValue(completion)!;
    }

    /// <summary>A delivery callback that signals it is running, then waits until it is released.</summary>
    private sealed class Parking : IDeliveryCallback
    {
        private readonly ManualResetEventSlim _entered;
        private readonly ManualResetEventSlim _release;

        internal Parking(ManualResetEventSlim entered, ManualResetEventSlim release)
        {
            _entered = entered;
            _release = release;
        }

        public void OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            _entered.Set();
            _release.Wait(s_deadline);
        }
    }
}
