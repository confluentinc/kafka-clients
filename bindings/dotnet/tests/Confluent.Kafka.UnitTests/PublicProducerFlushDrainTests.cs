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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// M11/P3.1 slice S6 — <c>Flush</c> drains the accumulator (§3.5). This is a deliberate divergence
/// <b>toward Java</b> and away from the Python anchor: Python's <c>flush()</c> never signals its
/// send thread (<c>record_batches_new_record_cnd</c> has exactly three signal sites and none is a
/// flush entry point), so it can return while records are still buffered inside the binding —
/// although from the caller's view those records <b>were</b> sent, because <c>send()</c> returned.
/// </summary>
/// <remarks>
/// <b>This is the test the anchor lacks</b> — Python's own <c>test_flush</c> asserts only that it
/// does not raise. The assertion here is on <c>HistoryCount()</c>, i.e. on what reached the
/// <em>core</em>, because that is precisely what "the accumulator was drained" means and it is
/// deterministic. Asserting instead that the send <c>Task</c>s are resolved would be asserting one
/// extra hop — the completion pump's — which <c>Flush</c> does not and should not promise.
/// </remarks>
public sealed class PublicProducerFlushDrainTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "flush-drain-topic";

    private const int SendCount = 16;

    [Fact]
    public async Task Flush_DrainsAccumulatorRecords_TheyHaveReachedTheCoreWhenItReturns()
    {
        using AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        Task<RecordMetadata>[] sends = Fire(producer);

        await TestTimeout.Run(() => producer.Flush(), s_deadline);

        Assert.Equal(SendCount, producer.HistoryCount());

        // And they still resolve normally afterwards — the flush drained, it did not consume.
        await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);
    }

    // ⚠ THE CONTROL FOR THE TEST ABOVE LIVES IN Interop/SendAccumulatorTests, deliberately:
    // WithoutADrain_TheRecordsHaveNotReachedTheCore. It is what makes "history is 16 after Flush"
    // meaningful rather than possibly just "the window elapsed", so it has to hold — and here it
    // could not. The public producer reads its window from the environment (10 ms) and that window
    // is FREE-RUNNING (§3.3): the batch thread's deadline comes from the top of its own loop, so a
    // drain can land microseconds after the first append. A GC pause or thread-pool hiccup inside
    // the 16-send window flipped the assertion to 16, and a flake in the control would read as a
    // product regression in the thing it controls for. The accumulator-level form takes an explicit
    // 60 s window, so only an explicit drain can move the records — and it asserts BOTH halves
    // (nothing before the drain, everything after). Setting the environment variable here instead
    // is not an option: this assembly runs its test classes in PARALLEL (see AssemblyInfo), so a
    // process-wide override would reach producers other tests are constructing.

    [Fact]
    public async Task Flush_WithNothingBuffered_StillFlushesTheCore()
    {
        // The accumulator exists but is empty (its drain returns a completed task), and the
        // send-less case where no accumulator was ever started — both must still reach the core
        // flush rather than short-circuit.
        using AsyncMockProducer<byte[], byte[]> neverSent =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.Run(() => neverSent.Flush(), s_deadline);

        using AsyncMockProducer<byte[], byte[]> drained =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        Task<RecordMetadata>[] sends = Fire(drained);
        await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);

        await TestTimeout.Run(() => drained.Flush(), s_deadline);
        Assert.Equal(SendCount, drained.HistoryCount());
    }

    [Fact]
    public async Task Flush_PreCanceledToken_ThrowsSynchronouslyEvenWithRecordsBuffered()
    {
        // The precondition contract survives the drain: ffi §A5 wants it thrown before anything
        // else happens, and moving the drain in front of the core flush must not turn it into a
        // faulted Task.
        using AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        Task<RecordMetadata>[] sends = Fire(producer);

        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        Func<object> flush = () => producer.Flush(cts.Token);
        Assert.Throws<OperationCanceledException>(flush);

        await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);
    }

    [Fact]
    public async Task Flush_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        Task<RecordMetadata>[] sends = Fire(producer);
        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);

        foreach (Task<RecordMetadata> send in sends)
        {
            Assert.True(send.IsCompleted);
        }

        Func<object> flush = () => producer.Flush();
        Assert.Throws<ObjectDisposedException>(flush);
    }

    private static Task<RecordMetadata>[] Fire(AsyncMockProducer<byte[], byte[]> producer)
    {
        Task<RecordMetadata>[] sends = new Task<RecordMetadata>[SendCount];
        for (int i = 0; i < SendCount; i++)
        {
            sends[i] = producer.Send(new ProducerRecord<byte[], byte[]>(
                Topic, Encoding.UTF8.GetBytes($"value-{i}"), partition: 0));
        }

        return sends;
    }
}
