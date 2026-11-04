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
/// A non-parallel xUnit collection for tests that read the process-wide <see cref="GC.GetTotalMemory(bool)"/>,
/// which allocations on other threads would make noisy.
/// </summary>
[CollectionDefinition(SerialMemoryMeasurementCollection.Name, DisableParallelization = true)]
public sealed class SerialMemoryMeasurementCollection
{
    internal const string Name = "serial-memory-measurement";
}

/// <summary>
/// A rejected async submit, its handle released under it, must not root its completion context: managed memory
/// stays flat across many rejected flushes.
/// </summary>
[Collection(SerialMemoryMeasurementCollection.Name)]
public sealed class ProducerSubmitHandleRefTests
{
    // Enough rejected submits that ~300 bytes of leaked context each would dwarf GC noise.
    private const int Iterations = 20_000;

    private const int Warmup = 1_000;

    // The budget sits well above the GC noise and well below a rooted context per rejected submit.
    private const long GrowthBudgetBytes = 2_000_000;

    [Fact]
    public void SubmitVoidOperation_WhenAddRefThrows_DoesNotRootTheCompletionContext()
    {
        NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);

        // Releasing the handle directly leaves the producer's closed latch open, so the closed check passes and the
        // marshaller's DangerousAddRef throws, as in a race with teardown. The producer is not disposed afterwards.
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
        // FlushWithCallback is not async, so the throw leaves the call and Assert.Throws(Action) is the overload.
        Assert.Throws<ObjectDisposedException>(() => { _ = producer.FlushWithCallback(); });
    }
}
