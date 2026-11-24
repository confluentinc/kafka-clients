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
using Confluent.Kafka.Producer;
using Xunit;
using Xunit.Abstractions;

namespace Confluent.Kafka.UnitTests;

// GC.GetAllocatedBytesForCurrentThread has no net462 equivalent, so the class is net8.0+ only.
#if NET8_0_OR_GREATER

/// <summary>
/// The size of one future engine, <see cref="KafkaFutureImpl{T}"/> or <see cref="AsyncKafkaFutureImpl{T}"/>: a new
/// engine is one object, for a send's future and for a plain one, so a future costs one object, of
/// <see cref="EngineBytes"/> in a 64-bit process. Measured with the per-thread allocation counter around
/// <see cref="EngineCount"/> creations, best of N, and asserted exactly in a 64-bit process only.
/// </summary>
public sealed class KafkaFutureEngineSizeTests
{
    // The measured size of one engine in a 64-bit process (8 B pointers; net8.0, net10.0): its ten fields, 56 B, and
    // the object header, 16 B. One more reference field makes it 80 B. A 32-bit process (CI's Windows x86 job) has
    // smaller objects, and its size is not pinned: the test prints it there and asserts nothing.
    private const long EngineBytes = 72;

    private const int EngineCount = 64;

    private const int WarmupRounds = 3;

    // Best of N: an allocation outside the engines (a first use, a growth) raises one attempt, while a larger engine
    // raises every attempt, the minimum included.
    private const int MeasurementAttempts = 8;

    private readonly ITestOutputHelper _output;

    public KafkaFutureEngineSizeTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void KafkaFutureImpl_ForSend_IsOneObjectOfTheEngineSize() =>
        AssertEngineSize("KafkaFutureImpl<RecordMetadata>.ForSend()", static () => KafkaFutureImpl<RecordMetadata>.ForSend());

    [Fact]
    public void KafkaFutureImpl_Plain_IsOneObjectOfTheEngineSize() =>
        AssertEngineSize("new KafkaFutureImpl<RecordMetadata>()", static () => new KafkaFutureImpl<RecordMetadata>());

    [Fact]
    public void AsyncKafkaFutureImpl_ForSend_IsOneObjectOfTheEngineSize() =>
        AssertEngineSize(
            "AsyncKafkaFutureImpl<RecordMetadata>.ForSend()", static () => AsyncKafkaFutureImpl<RecordMetadata>.ForSend());

    [Fact]
    public void AsyncKafkaFutureImpl_Plain_IsOneObjectOfTheEngineSize() =>
        AssertEngineSize("new AsyncKafkaFutureImpl<RecordMetadata>()", static () => new AsyncKafkaFutureImpl<RecordMetadata>());

    private void AssertEngineSize(string engine, Func<object> create)
    {
        long bytes = Measure(create);

        _output.WriteLine(
            $"{engine}: {bytes / EngineCount} B per engine ({bytes} B over {EngineCount} engines, best of " +
            $"{MeasurementAttempts}, {IntPtr.Size * 8}-bit process).");

        if (Environment.Is64BitProcess)
        {
            Assert.True(
                bytes == EngineBytes * EngineCount,
                $"{engine} allocated {bytes} B for {EngineCount} engines, {(double)bytes / EngineCount} B each, where " +
                $"one engine is one object of {EngineBytes} B: a future must stay one object of that size.");
        }
    }

    private static long Measure(Func<object> create)
    {
        // Pre-allocated outside every measured window; it keeps each engine reachable until the window ends.
        object[] engines = new object[EngineCount];

        // Warm up so the measured runs are steady-state (JIT, the engine type's static constructor).
        for (int round = 0; round < WarmupRounds; round++)
        {
            Create(engines, create);
        }

        long bytes = long.MaxValue;
        for (int attempt = 0; attempt < MeasurementAttempts; attempt++)
        {
            Array.Clear(engines, 0, engines.Length);

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            long before = GC.GetAllocatedBytesForCurrentThread();
            Create(engines, create);
            long after = GC.GetAllocatedBytesForCurrentThread();

            bytes = Math.Min(bytes, after - before);
        }

        GC.KeepAlive(engines);
        return bytes;
    }

    private static void Create(object[] engines, Func<object> create)
    {
        for (int i = 0; i < engines.Length; i++)
        {
            engines[i] = create();
        }
    }
}

#endif
