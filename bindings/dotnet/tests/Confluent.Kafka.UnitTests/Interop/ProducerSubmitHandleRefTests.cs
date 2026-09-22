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

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The M11/P8 Major-4 regression guard: a producer async-submit helper whose
/// <c>DangerousAddRef</c> throws must still free the rooting <see cref="System.Runtime.InteropServices.GCHandle"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>The bug.</b> All four submit helpers took the span-the-op <c>DangerousAddRef</c>
/// <em>above</em> the <c>try</c> whose <c>catch</c> calls <c>AbandonBeforeSubmit()</c>.
/// <c>SafeHandle.DangerousAddRef</c> throws <see cref="ObjectDisposedException"/> when a concurrent
/// teardown closed the handle between <c>ThrowIfClosed()</c> and there — reachable on a REAL
/// producer, which is explicitly multi-writer (ffi §A1). The throw escaped above the <c>try</c>, so
/// cleanup never ran and the <c>GCHandle</c> rooted the completion context for the PROCESS
/// LIFETIME, silently, behind a perfectly plausible <see cref="ObjectDisposedException"/>. Severity
/// is a leak, not a crash — which is why nothing caught it.
/// </para>
/// <para>
/// <b>How this reproduces it deterministically.</b> Closing the <see cref="SafeProducerHandle"/>
/// directly leaves <c>NativeProducer</c>'s own <c>_closed</c> latch OPEN, so <c>ThrowIfClosed()</c>
/// passes and <c>DangerousAddRef</c> throws — exactly the state the race produces, without having to
/// win a race. Each rejected submit allocates a context; pre-fix each one is rooted forever, so
/// managed memory grows without bound across iterations.
/// </para>
/// <para>
/// <b>Runs in a non-parallel collection.</b> The retention signal is read with
/// <see cref="GC.GetTotalMemory(bool)"/>, which is process-global: tests allocating on other
/// threads at the same instant make the delta noisy enough to flip this test. The collection below
/// serializes it against the rest of the suite so the measurement sees only its own allocations.
/// </para>
/// <para>
/// The producer is deliberately NOT disposed at the end: its native handle is already released, and
/// <c>NativeProducer.Dispose</c> would pass the stale raw pointer to <c>Producer_close</c>.
/// </para>
/// </remarks>
[CollectionDefinition(SerialMemoryMeasurementCollection.Name, DisableParallelization = true)]
public sealed class SerialMemoryMeasurementCollection
{
    /// <summary>The collection name — see <see cref="ProducerSubmitHandleRefTests"/>.</summary>
    internal const string Name = "serial-memory-measurement";
}

/// <inheritdoc cref="SerialMemoryMeasurementCollection"/>
[Collection(SerialMemoryMeasurementCollection.Name)]
public sealed class ProducerSubmitHandleRefTests
{
    // Enough rejected submits that ~300 bytes of leaked context each would dwarf GC noise.
    private const int Iterations = 20_000;

    private const int Warmup = 1_000;

    // Pre-fix growth is ~6.7 MB (measured); post-fix it is GC noise around zero. The budget sits
    // well above the noise floor and well below the signal.
    private const long GrowthBudgetBytes = 2_000_000;

    [Fact]
    public void SubmitVoidOperation_WhenAddRefThrows_DoesNotRootTheCompletionContext()
    {
        NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);

        // Simulate the teardown that closed the handle underneath an in-flight submit.
        producer.Handle.Dispose();

        for (int i = 0; i < Warmup; i++)
        {
            AssertRejected(producer);
        }

        long baseline = GC.GetTotalMemory(forceFullCollection: true);

        for (int i = 0; i < Iterations; i++)
        {
            AssertRejected(producer);
        }

        long growth = GC.GetTotalMemory(forceFullCollection: true) - baseline;
        Assert.True(
            growth < GrowthBudgetBytes,
            $"Rejected submits rooted their completion contexts: managed memory grew {growth} bytes "
            + $"across {Iterations} iterations (budget {GrowthBudgetBytes}).");
    }

    private static void AssertRejected(NativeProducer producer)
    {
        // ThrowIfClosed passes (the latch is open); DangerousAddRef throws. The exception is the
        // documented outcome — the regression is what it leaves behind.
        // The submit helper is NOT an async method — SubmitVoidOperation throws synchronously
        // before any Task is produced — so the void-bodied lambda (Assert.Throws(Action)) is the
        // correct overload here, not ThrowsAsync.
        Assert.Throws<ObjectDisposedException>(() => { _ = producer.FlushWithCallback(); });
    }
}
