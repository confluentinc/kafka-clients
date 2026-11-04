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
using System.Collections.Generic;

using Confluent.Kafka.Common;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Reading a core error built by the common error factories: its code, its UTF-8 message, its retriable flag
/// and its borrowed source, with the owned handle freed exactly once.
/// </summary>
public sealed class KafkaExceptionReadTests
{
    private const string NonAsciiMessage = "café-Ω-日本語-😀";

    private const string UnknownServerErrorMessage =
        "The server experienced an unexpected error when processing the request.";

    /// <summary>A common error factory that builds an error from a UTF-8 message.</summary>
    private delegate IntPtr MessageFactory(IntPtr message, ref IntPtr outErr);

    private static readonly Dictionary<string, (MessageFactory Factory, int Code)> s_localFactories = new()
    {
        ["local_concurrent_modification"] = (NativeMethods.ErrorLocalConcurrentModification, -2),
        ["local_illegal_argument"] = (NativeMethods.ErrorLocalIllegalArgument, -3),
        ["local_illegal_state"] = (NativeMethods.ErrorLocalIllegalState, -4),
        ["local_timeout"] = (NativeMethods.ErrorLocalTimeout, -5),
    };

    [Theory]
    [InlineData("local_concurrent_modification")]
    [InlineData("local_illegal_argument")]
    [InlineData("local_illegal_state")]
    [InlineData("local_timeout")]
    public void LocalFactory_ReadsBackItsCode_AndItsNonAsciiMessage(string factory)
    {
        (MessageFactory build, int code) = s_localFactories[factory];

        KafkaException read = KafkaException.FromHandle(Build(build, NonAsciiMessage))!;

        Assert.Equal(code, read.Code);
        Assert.Equal(NonAsciiMessage, read.Message);
        Assert.False(read.IsRetriable);
        Assert.Null(read.InnerException);
    }

    [Fact]
    public void BareKafkaError_IsTheUnknownServerErrorCode_WithItsDefaultMessage_AndNoApiError()
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr error = NativeMethods.ErrorKafka(ref outErr);
        Assert.Equal(IntPtr.Zero, outErr);
        Assert.NotEqual(IntPtr.Zero, error);

        Assert.Equal(0, NativeMethods.ErrorIsApiError(error, ref outErr));
        Assert.Equal(IntPtr.Zero, outErr);

        KafkaException read = KafkaException.FromHandle(error)!;
        Assert.Equal(-1, read.Code);
        Assert.Equal(UnknownServerErrorMessage, read.Message);
        Assert.False(read.IsRetriable);
        Assert.Null(read.InnerException);
    }

    [Fact]
    public void FromBorrowedHandle_LeavesTheHandleAlive_ForItsOwner()
    {
        IntPtr error = Build(NativeMethods.ErrorLocalIllegalArgument, NonAsciiMessage);
        try
        {
            KafkaException first = KafkaException.FromBorrowedHandle(error)!;
            KafkaException second = KafkaException.FromBorrowedHandle(error)!;

            Assert.Equal(NonAsciiMessage, first.Message);
            Assert.Equal(NonAsciiMessage, second.Message);
            Assert.Equal(first.Code, second.Code);
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
    public void FromHandle_RepeatedManyTimes_FreesEachHandleOnce()
    {
        for (int i = 0; i < 1000; i++)
        {
            KafkaException read = KafkaException.FromHandle(Build(NativeMethods.ErrorLocalTimeout, NonAsciiMessage))!;
            Assert.Equal(-5, read.Code);
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
        Assert.Null(KafkaException.FromHandle(IntPtr.Zero));
        Assert.Null(KafkaException.FromBorrowedHandle(IntPtr.Zero));
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
