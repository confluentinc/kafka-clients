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

using Confluent.Kafka.Common;
using Confluent.Kafka.Internal.Interop;
using Confluent.Kafka.Java.Lang;

using Xunit;

using JavaTimeoutException = Confluent.Kafka.Java.Util.Concurrent.TimeoutException;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Reading a core error built by the common error factories: its borrowed source, and the owned handle freed exactly
/// once. Which class each id reads into is in <see cref="FromFfiErrorClassTests"/>.
/// </summary>
public sealed class KafkaExceptionReadTests
{
    private const string NonAsciiMessage = "café-Ω-日本語-😀";

    /// <summary>A common error factory that builds an error from a UTF-8 message.</summary>
    private delegate IntPtr MessageFactory(IntPtr message, ref IntPtr outErr);

    [Fact]
    public void ABorrowedRead_LeavesTheHandleAlive_ForItsOwner()
    {
        IntPtr error = Build(NativeMethods.ErrorLocalIllegalArgument, NonAsciiMessage);
        try
        {
            Exception first = KafkaException.FromFfiError(error, owned: false)!;
            Exception second = KafkaException.FromFfiError(error, owned: false)!;

            Assert.Equal(NonAsciiMessage, first.Message);
            Assert.Equal(NonAsciiMessage, second.Message);
            Assert.IsType<IllegalArgumentException>(first);
            Assert.IsType<IllegalArgumentException>(second);
            Assert.NotSame(first, second);
        }
        finally
        {
            NativeMethods.ErrorDestroy(error);
        }
    }

    [Fact]
    public void ErrorWithoutSource_HasANullSource()
    {
        IntPtr error = Build(NativeMethods.ErrorLocalIllegalState, NonAsciiMessage);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            Assert.Equal(IntPtr.Zero, NativeMethods.ErrorSource(error, ref outErr));
            Assert.Equal(IntPtr.Zero, outErr);
        }
        finally
        {
            NativeMethods.ErrorDestroy(error);
        }
    }

    [Fact]
    public void AnOwnedRead_RepeatedManyTimes_FreesEachHandleOnce()
    {
        for (int i = 0; i < 1000; i++)
        {
            Exception read = KafkaException.FromFfiError(Build(NativeMethods.ErrorLocalTimeout, NonAsciiMessage), owned: true)!;
            Assert.IsType<JavaTimeoutException>(read);
        }
    }

    [Fact]
    public void NullHandles_AreNoOps()
    {
        IntPtr outErr = IntPtr.Zero;

        NativeMethods.ErrorDestroy(IntPtr.Zero, ref outErr);
        NativeMethods.StringDestroy(IntPtr.Zero, ref outErr);
        NativeMethods.MapDestroy(IntPtr.Zero, ref outErr);
        NativeMethods.KafkaFutureDestroy(IntPtr.Zero, ref outErr);

        Assert.Equal(IntPtr.Zero, outErr);
        Assert.Null(KafkaException.FromFfiError(IntPtr.Zero, owned: true));
        Assert.Null(KafkaException.FromFfiError(IntPtr.Zero, owned: false));
    }

    private static IntPtr Build(MessageFactory factory, string message)
    {
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(message);
        IntPtr outErr = IntPtr.Zero;
        IntPtr error = factory(pinned.Pointer, ref outErr);
        Assert.Equal(IntPtr.Zero, outErr);
        Assert.NotEqual(IntPtr.Zero, error);
        return error;
    }
}
