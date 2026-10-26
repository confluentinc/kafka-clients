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
using System.Runtime.InteropServices;
using System.Threading.Tasks;

using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The async send's pins: the process-wide empty sentinel and the exactly-once unpin, asserted directly, since a
/// wrong pin lifetime works almost always and corrupts rarely.
/// </summary>
public sealed class ProducerSendPinLifetimeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "pin-lifetime-topic";

    [Fact]
    public void Pin_AbsentKeyAndValue_PassNoBytes()
    {
        SendPins pins = default;
        pins.Pin(null, null, out IntPtr keyBytes, out IntPtr valueBytes);
        try
        {
            Assert.Equal(IntPtr.Zero, keyBytes);
            Assert.Equal(IntPtr.Zero, valueBytes);
        }
        finally
        {
            pins.Release();
        }
    }

    [Fact]
    public void Pin_EmptyKeyAndValue_UseAStableProcessWideSentinel()
    {
        // The core reads an async send's bytes until the hand-over, after Send's frame is gone, so an
        // empty key or value points at one process-wide byte: the same address for the key and the
        // value, and across calls at different stack depths, which a stack sentinel cannot give.
        SendPins first = default;
        SendPins second = default;
        first.Pin(ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, out IntPtr firstKey, out IntPtr firstValue);
        try
        {
            IntPtr secondKey = PinAnEmptyKeyFromADeeperFrame(ref second);
            KafkaBytes key = Marshal.PtrToStructure<KafkaBytes>(firstKey);
            KafkaBytes value = Marshal.PtrToStructure<KafkaBytes>(firstValue);

            Assert.NotEqual(IntPtr.Zero, key.Data);
            Assert.Equal(0, key.Length);
            Assert.Equal(0, value.Length);
            Assert.Equal(key.Data, value.Data);
            Assert.Equal(key.Data, Marshal.PtrToStructure<KafkaBytes>(secondKey).Data);
        }
        finally
        {
            second.Release();
            first.Release();
        }
    }

    [Fact]
    public void Pin_PresentKeyAndValue_PointAtThePinnedBuffersAndSurviveGc()
    {
        byte[] key = new byte[] { 1, 2, 3, 4 };
        byte[] value = new byte[] { 9, 8, 7 };
        SendPins pins = default;
        pins.Pin(new ReadOnlyMemory<byte>(key), new ReadOnlyMemory<byte>(value), out IntPtr keyBytes, out IntPtr valueBytes);
        try
        {
            Collect();

            AssertPointsAt(key, Marshal.PtrToStructure<KafkaBytes>(keyBytes));
            AssertPointsAt(value, Marshal.PtrToStructure<KafkaBytes>(valueBytes));
        }
        finally
        {
            pins.Release();
        }
    }

    [Fact]
    public async Task Send_ManyRecords_LeavesNoBufferPinned()
    {
        // A MemoryHandle pin is a strong root, so a leaked pin keeps its buffer alive: each send gets a buffer nothing
        // else references, which must become collectable once the send has completed.
        WeakReference[] valueRefs;

        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using (TestTimeout.Disposal(producer))
        {
            valueRefs = await SendThrowawayBuffersAsync(producer, count: 64);
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        int alive = 0;
        foreach (WeakReference reference in valueRefs)
        {
            if (reference.IsAlive)
            {
                alive++;
            }
        }

        Assert.Equal(0, alive);
    }

    [Fact]
    public void Pin_Buffer_IsCollectableOnceThePinsAreReleased()
    {
        // The control for the test above: the same pin keeps its buffer alive until it is released,
        // so a "0 alive" result can be attributed to the release.
        SendPins pins = default;
        WeakReference reference = PinAndForget(ref pins);

        Collect();
        Assert.True(reference.IsAlive, "a live pin must keep its buffer alive");

        pins.Release();

        Collect();
        Assert.False(reference.IsAlive, "releasing the pins must let the buffer be collected");
    }

    private static void Collect()
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
    }

    private static void AssertPointsAt(byte[] expected, KafkaBytes bytes)
    {
        Assert.Equal(expected.Length, bytes.Length);
        for (int i = 0; i < expected.Length; i++)
        {
            Assert.Equal(expected[i], Marshal.ReadByte(bytes.Data, i));
        }
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static IntPtr PinAnEmptyKeyFromADeeperFrame(ref SendPins pins)
    {
        pins.Pin(ReadOnlyMemory<byte>.Empty, null, out IntPtr keyBytes, out _);
        return keyBytes;
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static WeakReference PinAndForget(ref SendPins pins)
    {
        byte[] buffer = new byte[4096];
        pins.Pin(null, new ReadOnlyMemory<byte>(buffer), out _, out _);
        return new WeakReference(buffer);
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static async Task<WeakReference[]> SendThrowawayBuffersAsync(
        AsyncMockProducer<byte[], byte[]> producer,
        int count)
    {
        WeakReference[] references = new WeakReference[count];
        Task<RecordMetadata>[] sends = new Task<RecordMetadata>[count];

        for (int i = 0; i < count; i++)
        {
            // 4 KiB so a surviving pin is unmistakable, and referenced ONLY by the record and the
            // weak reference — the record itself is dropped as soon as Send returns.
            byte[] value = new byte[4096];
            references[i] = new WeakReference(value);
            sends[i] = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, value, partition: 0)).Delivery();
        }

        await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);
        Array.Clear(sends, 0, sends.Length);
        return references;
    }
}
