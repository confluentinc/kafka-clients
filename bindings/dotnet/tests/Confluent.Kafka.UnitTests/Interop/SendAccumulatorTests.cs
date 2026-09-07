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
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// M11/P3.1 slice S3 — the concurrency core: the node chain, the free-running window, the
/// threshold wake, and the chunking rule (§3.3/§3.4). Driven against a real
/// <see cref="SendAccumulator"/> over a broker-free <c>MockProducer</c>, with explicit settings
/// rather than environment overrides, so the timing assertions are deterministic and no test
/// mutates process-wide state that a concurrently-running test class could observe.
/// </summary>
/// <remarks>
/// <b>Every timing assertion is a BOUND, never an expected value</b> (§3.3). The window is
/// free-running — the batch thread takes its deadline from its own clock at the top of each
/// iteration, unrelated to when a record arrived — so a sub-threshold batch waits
/// <c>0..window</c> <em>uniformly</em>. A test that asserted "about one window" would be asserting
/// a property the design deliberately does not have.
/// </remarks>
public sealed class SendAccumulatorTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "accumulator-topic";

    // ------------------------------------------------------------------ timing / batching -----

    [Fact]
    public async Task SubThresholdBatch_IsReleasedByTheWindow()
    {
        // Well below the threshold, so the early wake never fires and only the free-running window
        // can release these records. The bound is generous on purpose: the delay is uniform over
        // 0..window, and the assertion that matters is "the timer releases it at all".
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, maxAccumulatedRecords: 1000, batchWindowMs: 20, batchChunk: 1100));

        Stopwatch elapsed = Stopwatch.StartNew();
        Task<RecordMetadata>[] sends = harness.Append(5);
        await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);
        elapsed.Stop();

        Assert.Equal(1, harness.Accumulator.SendBatchCallCount);
        Assert.Equal(5, harness.Accumulator.SendBatchRecordCount);
        Assert.True(
            elapsed.Elapsed < TimeSpan.FromSeconds(5),
            $"a sub-threshold batch took {elapsed.Elapsed} to be released by the {20} ms window");
    }

    [Fact]
    public async Task ThresholdBatch_IsReleasedByTheSignal_WellInsideTheWindow()
    {
        // A window far longer than the test's patience, so ONLY the threshold wake can release
        // these records. If the early signal (anchor :825-827) were missing, this would wait out
        // the 60 s window and blow the assertion below.
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 8, maxAccumulatedRecords: 1000, batchWindowMs: 60_000, batchChunk: 1100));

        Stopwatch elapsed = Stopwatch.StartNew();
        Task<RecordMetadata>[] sends = harness.Append(8);
        await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);
        elapsed.Stop();

        Assert.True(
            elapsed.Elapsed < TimeSpan.FromSeconds(10),
            $"an at-threshold batch took {elapsed.Elapsed} — the early wake did not fire, so the " +
            "60 s window released it instead");
    }

    // ------------------------------------------------------------------------- chunking (§3.4) -

    [Fact]
    public void DefaultChunk_OneNode_IsExactlyOneSendBatchCall()
    {
        // The default chunk IS node capacity, so ceil(count / chunk) is always 1 for a single node —
        // identical to the anchor, which issues one call per node. Asserted by CALL COUNT, not by
        // "the records arrived", which any batching whatsoever would satisfy.
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, maxAccumulatedRecords: 1000, batchWindowMs: 60_000, batchChunk: 1100));

        Task<RecordMetadata>[] sends = harness.Append(50);
        harness.DrainNow();

        Assert.Equal(1, harness.Accumulator.SendBatchCallCount);
        Assert.Equal(50, harness.Accumulator.SendBatchRecordCount);
        Assert.Equal(50, harness.Accumulator.LargestSendBatchCount);
        Assert.Equal(50, sends.Length);
    }

    [Fact]
    public void LoweredChunk_SplitsANodeIntoCeilCountOverChunkCalls()
    {
        // The ONLY axis that can reach the splitting branch: at the defaults the chunk equals node
        // capacity, so a node can never exceed it (§3.4 — "dead at the default BY CONSTRUCTION, and
        // load-bearing under an override"). 50 records at chunk 8 => ceil(50/8) == 7 calls, the last
        // one short.
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, maxAccumulatedRecords: 1000, batchWindowMs: 60_000, batchChunk: 8));

        _ = harness.Append(50);
        harness.DrainNow();

        Assert.Equal(7, harness.Accumulator.SendBatchCallCount);
        Assert.Equal(50, harness.Accumulator.SendBatchRecordCount);
        Assert.Equal(8, harness.Accumulator.LargestSendBatchCount);
    }

    [Fact]
    public async Task DeepChain_NeverExceedsOneChunkPerCall_AndSendsEveryRecord()
    {
        // A chain deep enough to span several nodes (capacity is threshold + 100 = 104 here) under
        // real batch-thread concurrency. Node formation is inherently load-dependent — a node can
        // only fill past the threshold if records arrive faster than the thread drains — so the
        // deterministic assertion is the INVARIANT §3.4 exists to guarantee: no single send_batch
        // ever carries more than one chunk, i.e. the loop is never "simplified" into a single
        // flattened call over the whole chain.
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 4, maxAccumulatedRecords: 100_000, batchWindowMs: 10, batchChunk: 104));

        Task<RecordMetadata>[] sends = harness.Append(500);
        await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);
        harness.DrainNow();

        Assert.Equal(500, harness.Accumulator.SendBatchRecordCount);
        Assert.True(
            harness.Accumulator.LargestSendBatchCount <= 104,
            $"a send_batch carried {harness.Accumulator.LargestSendBatchCount} records, above the " +
            "104-record chunk — the per-chunk loop is what bounds how long one call holds the core's " +
            "coarse producer mutex (§3.4)");
        Assert.True(harness.Accumulator.SendBatchCallCount >= 5, "500 records at chunk 104 need at least 5 calls");
    }

    [Fact]
    public void ChunkLargerThanANode_IsClampedToCapacity()
    {
        // A chunk cannot span two nodes, so an over-large override is indistinguishable from a full
        // node; clamping keeps the ceil(count / chunk) formula honest rather than letting the
        // override read as a different mode.
        SendAccumulatorSettings settings = new SendAccumulatorSettings(
            slotThreshold: 1000, maxAccumulatedRecords: 1000, batchWindowMs: 10, batchChunk: 999_999);

        Assert.Equal(1100, settings.SlotCapacity);
        Assert.Equal(1100, settings.BatchChunk);
    }

    // ------------------------------------------------------- pin lifetime across the deferral --

    [Fact]
    public async Task ForcedGcBetweenAppendAndDrain_DoesNotCorruptAnyRecord()
    {
        // The whole point of pinning: the record's buffers are borrowed across a REAL deferral now,
        // so a GC between Send and the drain must not move them. The window is long enough that the
        // collection provably lands inside it.
        using Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, maxAccumulatedRecords: 1000, batchWindowMs: 60_000, batchChunk: 1100));

        Task<RecordMetadata>[] sends = harness.Append(32);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        harness.DrainNow();
        await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);

        foreach (Task<RecordMetadata> send in sends)
        {
            RecordMetadata metadata = await send;
            Assert.Equal(Topic, metadata.Topic);
        }
    }

    // ---------------------------------------------------------------- settings (§3.2, test 4) --

    [Fact]
    public void Settings_Defaults_ArePythonsValues()
    {
        SendAccumulatorSettings settings = ReadSettingsWith(new Dictionary<string, string?>());

        Assert.Equal(1000, settings.SlotThreshold);      // PRODUCER_RECORD_SLOT_THRESHOLD
        Assert.Equal(1100, settings.SlotCapacity);       // (THRESHOLD + 100)
        Assert.Equal(1000, settings.MaxAccumulatedRecords); // = SLOT_THRESHOLD
        Assert.Equal(10, settings.BatchWindowMs);        // the bare 10 ms literal
        Assert.Equal(1100, settings.BatchChunk);         // Python's effective per-call maximum
    }

    [Fact]
    public void Settings_EnvironmentOverrides_TakeEffect()
    {
        SendAccumulatorSettings settings = ReadSettingsWith(new Dictionary<string, string?>
        {
            [SendAccumulatorSettings.ThresholdVariable] = "40",
            [SendAccumulatorSettings.MaxAccumulatedVariable] = "17",
            [SendAccumulatorSettings.WindowVariable] = "3",
            [SendAccumulatorSettings.ChunkVariable] = "9",
        });

        Assert.Equal(40, settings.SlotThreshold);
        Assert.Equal(140, settings.SlotCapacity);   // still threshold + 100
        Assert.Equal(17, settings.MaxAccumulatedRecords);
        Assert.Equal(3, settings.BatchWindowMs);
        Assert.Equal(9, settings.BatchChunk);
    }

    [Fact]
    public void Settings_BoundDefaultsToTheEffectiveThreshold_NotTheConstant()
    {
        // Python DEFINES the bound as the threshold, so lowering only the threshold must move the
        // bound with it — otherwise the two silently decouple.
        SendAccumulatorSettings settings = ReadSettingsWith(new Dictionary<string, string?>
        {
            [SendAccumulatorSettings.ThresholdVariable] = "25",
        });

        Assert.Equal(25, settings.SlotThreshold);
        Assert.Equal(25, settings.MaxAccumulatedRecords);
        Assert.Equal(125, settings.BatchChunk);   // and the chunk follows capacity
    }

    [Theory]
    [InlineData("not-a-number")]
    [InlineData("")]
    [InlineData("0")]
    [InlineData("-5")]
    public void Settings_InvalidOverride_FallsBackToTheDefault(string raw)
    {
        // An operator escape hatch must not be able to fail producer construction — and a ZERO
        // window in particular would turn the batch thread's wait loop into a spin loop.
        SendAccumulatorSettings settings = ReadSettingsWith(new Dictionary<string, string?>
        {
            [SendAccumulatorSettings.ThresholdVariable] = raw,
            [SendAccumulatorSettings.WindowVariable] = raw,
            [SendAccumulatorSettings.ChunkVariable] = raw,
        });

        Assert.Equal(1000, settings.SlotThreshold);
        Assert.Equal(10, settings.BatchWindowMs);
        Assert.Equal(1100, settings.BatchChunk);
    }

    [Fact]
    public void Settings_AreReadOnceAtConstruction_NotPerSend()
    {
        // "Read once at construction" is the recorded contract (§3.2): a later change to the
        // environment must not reach a producer that is already running.
        SendAccumulator accumulator;
        using Harness harness = new Harness(
            () => ReadSettingsWith(new Dictionary<string, string?>
            {
                [SendAccumulatorSettings.WindowVariable] = "37",
            }));

        accumulator = harness.Accumulator;
        Assert.Equal(37, accumulator.Settings.BatchWindowMs);

        string? previous = Environment.GetEnvironmentVariable(SendAccumulatorSettings.WindowVariable);
        Environment.SetEnvironmentVariable(SendAccumulatorSettings.WindowVariable, "12345");
        try
        {
            Assert.Equal(37, accumulator.Settings.BatchWindowMs);
        }
        finally
        {
            Environment.SetEnvironmentVariable(SendAccumulatorSettings.WindowVariable, previous);
        }
    }

    // ------------------------------------------------------------------- teardown (minimal) ----

    [Fact]
    public void Stop_DrainsTheAccumulatorBeforeExiting_NoRecordIsAbandoned()
    {
        // §3.8 steps 3–4: closing the accumulator must send what it already holds, not drop it. With
        // a 60 s window nothing can have been drained by the timer, so every one of these records is
        // still in the chain when Stop() runs — and every send must SETTLE (none left pending, which
        // is the shape that hangs an awaiting caller forever).
        Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, maxAccumulatedRecords: 1000, batchWindowMs: 60_000, batchChunk: 1100));

        Task<RecordMetadata>[] sends = harness.Append(20);
        Assert.Equal(0, harness.Accumulator.SendBatchCallCount);

        harness.Dispose();

        Assert.Equal(1, harness.Accumulator.SendBatchCallCount);
        Assert.Equal(20, harness.Accumulator.SendBatchRecordCount);
        foreach (Task<RecordMetadata> send in sends)
        {
            Assert.True(send.IsCompleted, "a record held by the accumulator at teardown was abandoned");
        }
    }

    [Fact]
    public void AppendAfterStop_ThrowsAndTransfersNoPin()
    {
        Harness harness = new Harness(new SendAccumulatorSettings(
            slotThreshold: 1000, maxAccumulatedRecords: 1000, batchWindowMs: 10, batchChunk: 1100));
        SendAccumulator accumulator = harness.Accumulator;
        harness.Dispose();

        MemoryHandle keyPin = default;
        MemoryHandle valuePin = ProducerSendBatchMarshal.PinIfNeeded(new byte[] { 1, 2, 3 });
        PinnedTopicCache.TopicPin topicPin = harness.Topics.Rent(Topic);
        try
        {
            Assert.Throws<ObjectDisposedException>(() => accumulator.Append(
                new SerializedProducerRecord(Topic, 0, null, null, new byte[] { 1, 2, 3 }),
                topicPin,
                keyPin,
                valuePin,
                new TaskCompletionSource<RecordMetadata>(TaskCreationOptions.RunContinuationsAsynchronously),
                delivery: null));
        }
        finally
        {
            // Still OURS to release — the accumulator took nothing, which is the contract the
            // caller's `appended` flag relies on.
            topicPin.Release();
            valuePin.Dispose();
            keyPin.Dispose();
        }
    }

    /// <summary>
    /// Reads <see cref="SendAccumulatorSettings.FromEnvironment"/> with the given variables set,
    /// restoring the previous values afterwards. The read itself is pure — no producer is
    /// constructed inside the window — so a concurrently-running test class cannot observe it.
    /// </summary>
    private static SendAccumulatorSettings ReadSettingsWith(Dictionary<string, string?> overrides)
    {
        string[] all =
        {
            SendAccumulatorSettings.ThresholdVariable,
            SendAccumulatorSettings.MaxAccumulatedVariable,
            SendAccumulatorSettings.WindowVariable,
            SendAccumulatorSettings.ChunkVariable,
        };

        Dictionary<string, string?> previous = new Dictionary<string, string?>();
        foreach (string variable in all)
        {
            previous[variable] = Environment.GetEnvironmentVariable(variable);
            overrides.TryGetValue(variable, out string? value);
            Environment.SetEnvironmentVariable(variable, value);
        }

        try
        {
            return SendAccumulatorSettings.FromEnvironment();
        }
        finally
        {
            foreach (string variable in all)
            {
                Environment.SetEnvironmentVariable(variable, previous[variable]);
            }
        }
    }

    /// <summary>
    /// A producer + pump + topic cache + accumulator wired exactly as
    /// <c>NativeProducer.EnsureAccumulator</c> wires them, torn down in the same §3.8 order
    /// (accumulator drain → pump gate → flush → pump join → producer destroy).
    /// </summary>
    private sealed class Harness : IDisposable
    {
        private readonly NativeProducer _producer;
        private readonly SendCompletionPump _pump;
        private bool _disposed;

        internal Harness(SendAccumulatorSettings settings)
            : this(() => settings)
        {
        }

        internal Harness(Func<SendAccumulatorSettings> settingsFactory)
        {
            _producer = NativeProducer.CreateMock(autoComplete: true);
            _pump = new SendCompletionPump();
            Topics = new PinnedTopicCache();
            Accumulator = new SendAccumulator(_producer.Handle, Topics, _pump, settingsFactory());
        }

        internal SendAccumulator Accumulator { get; }

        internal PinnedTopicCache Topics { get; }

        /// <summary>Appends <paramref name="count"/> records the way <c>SendViaPump</c> does.</summary>
        internal Task<RecordMetadata>[] Append(int count)
        {
            Task<RecordMetadata>[] sends = new Task<RecordMetadata>[count];
            for (int i = 0; i < count; i++)
            {
                byte[] value = new byte[] { (byte)i, 0xAA, 0xBB };
                SerializedProducerRecord record =
                    new SerializedProducerRecord(Topic, 0, null, null, value);

                MemoryHandle keyPin = default;
                MemoryHandle valuePin = ProducerSendBatchMarshal.PinIfNeeded(record.Value);
                PinnedTopicCache.TopicPin topicPin = Topics.Rent(Topic);
                TaskCompletionSource<RecordMetadata> completion =
                    new TaskCompletionSource<RecordMetadata>(TaskCreationOptions.RunContinuationsAsynchronously);

                bool appended = false;
                try
                {
                    Accumulator.Append(record, topicPin, keyPin, valuePin, completion, delivery: null);
                    appended = true;
                }
                finally
                {
                    if (!appended)
                    {
                        topicPin.Release();
                        valuePin.Dispose();
                        keyPin.Dispose();
                    }
                }

                sends[i] = completion.Task;
            }

            return sends;
        }

        /// <summary>Forces one drain and waits for it, the way the test drain hook does.</summary>
        internal void DrainNow() =>
            Assert.True(Accumulator.DrainPending(s_deadline), "the accumulator did not drain in time");

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;

            // The §3.8 ordering: drain the accumulator into a STILL-OPEN pump gate, then close the
            // gate, then flush so the pump's blocking get_all can return, then join it, then destroy.
            Accumulator.Stop();
            _pump.CloseGate();
            NativeMethods.ProducerFlush(_producer.Handle, out IntPtr flushError);
            _ = KafkaException.FromHandle(flushError);
            _pump.Stop();
            _producer.Dispose();
        }
    }
}
