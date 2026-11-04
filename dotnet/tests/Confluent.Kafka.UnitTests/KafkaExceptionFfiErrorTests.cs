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
using System.Runtime.CompilerServices;

using Confluent.Kafka.Common;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Tests reading a core error into the exception its code maps to (a local code to its .NET built-in, a carried managed
/// exception to the same instance, any other code to a <see cref="KafkaException"/>), with its cause chain and its
/// handle ownership.
/// </summary>
public sealed class KafkaExceptionFfiErrorTests
{
    private const int UnknownServerErrorCode = -1;

    private const int RequestTimedOutCode = 7;

    private const int LocalCallbackCode = -30;

    private const string NonAsciiMessage = "café-Ω-日本語-😀";

    private const string UnknownServerErrorMessage =
        "The server experienced an unexpected error when processing the request.";

    /// <summary>A common error factory that builds an error from a UTF-8 message.</summary>
    private delegate IntPtr MessageFactory(IntPtr message, ref IntPtr outErr);

    private static readonly Dictionary<string, (MessageFactory Factory, int Code, Type Expected)> s_localRows = new()
    {
        ["local_concurrent_modification"] =
            (NativeMethods.ErrorLocalConcurrentModification, -2, typeof(InvalidOperationException)),
        ["local_illegal_argument"] = (NativeMethods.ErrorLocalIllegalArgument, -3, typeof(ArgumentException)),
        ["local_illegal_state"] = (NativeMethods.ErrorLocalIllegalState, -4, typeof(InvalidOperationException)),
        ["local_timeout"] = (NativeMethods.ErrorLocalTimeout, -5, typeof(TimeoutException)),
    };

    [Theory]
    [InlineData("local_concurrent_modification")]
    [InlineData("local_illegal_argument")]
    [InlineData("local_illegal_state")]
    [InlineData("local_timeout")]
    public void LocalCode_IsItsDotNetBuiltIn_WithTheCoreMessage(string row)
    {
        (MessageFactory factory, int code, Type expected) = s_localRows[row];
        IntPtr error = Build(factory, NonAsciiMessage);
        Assert.Equal(code, NativeMethods.Code(error));
        string? coreMessage = Utf8Marshal.PtrToString(NativeMethods.Message(error));

        Exception read = KafkaException.FromFfiError(error, owned: true)!;

        Assert.IsType(expected, read);
        Assert.Equal(NonAsciiMessage, coreMessage);
        Assert.Equal(coreMessage, read.Message);
        Assert.Null(read.InnerException);
    }

    [Fact]
    public void LocalCallback_IsTheCarriedKafkaException_TheSameInstance()
    {
        var carried = new KafkaException("listener failed: café");
        IntPtr error = ManagedExceptionCarrier.ToError(carried);
        Assert.Equal(LocalCallbackCode, NativeMethods.Code(error));
        Assert.Equal("listener failed: café", Utf8Marshal.PtrToString(NativeMethods.Message(error)));

        Assert.Same(carried, KafkaException.FromFfiError(error, owned: true));
    }

    [Fact]
    public void LocalCallback_IsTheCarriedInstance_ForAnExceptionThatIsNotAKafkaException()
    {
        var carried = new ListenerException("listener failed: Ω");
        IntPtr error = ManagedExceptionCarrier.ToError(carried);
        Assert.Equal(LocalCallbackCode, NativeMethods.Code(error));
        Assert.Equal("listener failed: Ω", Utf8Marshal.PtrToString(NativeMethods.Message(error)));

        Assert.Same(carried, KafkaException.FromFfiError(error, owned: true));
    }

    [Fact]
    public void LocalCallback_WithoutAnOpaque_IsAKafkaException_WithItsCode()
    {
        using Utf8Marshal.PinnedUtf8String message = Utf8Marshal.Pin("nothing carried");
        IntPtr outErr = IntPtr.Zero;
        IntPtr error = NativeMethods.ErrorLocalCallback(message.Pointer, IntPtr.Zero, null!, ref outErr);
        Assert.Equal(IntPtr.Zero, outErr);
        Assert.NotEqual(IntPtr.Zero, error);

        KafkaException read = Assert.IsType<KafkaException>(KafkaException.FromFfiError(error, owned: true));

        Assert.Equal(LocalCallbackCode, read.Code);
        Assert.Equal("nothing carried", read.Message);
        Assert.False(read.IsRetriable);
        Assert.Null(read.InnerException);
    }

    [Fact]
    public void AnyOtherCode_IsAKafkaException_WithItsCode_AndItsRetriableFlag()
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr error = NativeMethods.ErrorKafka(ref outErr);
        Assert.Equal(IntPtr.Zero, outErr);
        Assert.NotEqual(IntPtr.Zero, error);

        KafkaException read = Assert.IsType<KafkaException>(KafkaException.FromFfiError(error, owned: true));

        Assert.Equal(UnknownServerErrorCode, read.Code);
        Assert.Equal(UnknownServerErrorMessage, read.Message);
        Assert.False(read.IsRetriable);
        Assert.Null(read.InnerException);
    }

    [Fact]
    public void KafkaTimeout_IsARetriableKafkaException()
    {
        IntPtr error = Build(NativeMethods.ErrorTimeout, NonAsciiMessage);

        KafkaException read = Assert.IsType<KafkaException>(KafkaException.FromFfiError(error, owned: true));

        Assert.Equal(RequestTimedOutCode, read.Code);
        Assert.Equal(NonAsciiMessage, read.Message);
        Assert.True(read.IsRetriable);
        Assert.Null(read.InnerException);
    }

    [Fact]
    public void SourceChain_IsTheInnerException_ReadThroughTheTable_AtEveryLevel_AndEndsWithNull()
    {
        IntPtr root = Build(NativeMethods.ErrorLocalIllegalArgument, NonAsciiMessage);
        IntPtr middle = BuildWithSource("middle: café", root);
        IntPtr outer = BuildWithSource("outer", middle);

        KafkaException read = Assert.IsType<KafkaException>(KafkaException.FromFfiError(outer, owned: true));

        Assert.Equal(UnknownServerErrorCode, read.Code);
        Assert.Equal("outer", read.Message);
        Assert.False(read.IsRetriable);
        KafkaException cause = Assert.IsType<KafkaException>(read.InnerException);
        Assert.Equal(UnknownServerErrorCode, cause.Code);
        Assert.Equal("middle: café", cause.Message);
        Assert.False(cause.IsRetriable);
        ArgumentException rootCause = Assert.IsType<ArgumentException>(cause.InnerException);
        Assert.Equal(NonAsciiMessage, rootCause.Message);
        Assert.Null(rootCause.InnerException);
    }

    [Fact]
    public void SourceChain_ARetriableSource_KeepsItsOwnCodeAndFlag()
    {
        IntPtr outer = BuildWithSource("outer", Build(NativeMethods.ErrorTimeout, "timed out"));

        KafkaException read = Assert.IsType<KafkaException>(KafkaException.FromFfiError(outer, owned: true));

        Assert.Equal(UnknownServerErrorCode, read.Code);
        Assert.False(read.IsRetriable);
        KafkaException cause = Assert.IsType<KafkaException>(read.InnerException);
        Assert.Equal(RequestTimedOutCode, cause.Code);
        Assert.Equal("timed out", cause.Message);
        Assert.True(cause.IsRetriable);
        Assert.Null(cause.InnerException);
    }

    [Fact]
    public void SourceChain_ACarriedSource_IsTheSameInstance()
    {
        var carried = new ListenerException("listener failed");
        IntPtr outer = BuildWithSource("operation failed", ManagedExceptionCarrier.ToError(carried));

        KafkaException read = Assert.IsType<KafkaException>(KafkaException.FromFfiError(outer, owned: true));

        Assert.Equal(UnknownServerErrorCode, read.Code);
        Assert.Equal("operation failed", read.Message);
        Assert.Same(carried, read.InnerException);
    }

    [Fact]
    public void SourceChain_TheSourceIsBorrowed_AndReleasedOnlyWithItsOwner()
    {
        (WeakReference carried, IntPtr outer) = ReadAChainBorrowedAndDiscard();

        Collect();
        Assert.True(carried.IsAlive, "reading the chain must not destroy the borrowed source");

        NativeMethods.ErrorDestroy(outer);
        Collect();
        Assert.False(carried.IsAlive, "destroying the outer error must release its source");
    }

    [Fact]
    public void SourceChain_AnOwnedRead_ReleasesTheSourceWithTheOuterError()
    {
        WeakReference carried = ReadAChainOwnedAndDiscard();

        Collect();
        Assert.False(carried.IsAlive, "reading an owned error must destroy it and, with it, its source");
    }

    [Fact]
    public void Owned_IsDestroyedByTheRead_AlsoWhenItCarriesAnInstance()
    {
        WeakReference carried = ReadOwnedAndDiscard();

        Collect();
        Assert.False(carried.IsAlive, "reading an owned error must destroy it, releasing its GCHandle");
    }

    [Fact]
    public void Borrowed_IsLeftToItsOwner()
    {
        (WeakReference carried, IntPtr error) = ReadBorrowedAndDiscard();

        Collect();
        Assert.True(carried.IsAlive, "reading a borrowed error must not destroy it");

        NativeMethods.ErrorDestroy(error);
        Collect();
        Assert.False(carried.IsAlive, "the owner's destroy must release the GCHandle");
    }

    [Fact]
    public void NullHandle_IsNull_OwnedOrBorrowed()
    {
        Assert.Null(KafkaException.FromFfiError(IntPtr.Zero, owned: true));
        Assert.Null(KafkaException.FromFfiError(IntPtr.Zero, owned: false));
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference ReadOwnedAndDiscard()
    {
        var exception = new ListenerException("owned");
        IntPtr error = ManagedExceptionCarrier.ToError(exception);
        Assert.Same(exception, KafkaException.FromFfiError(error, owned: true));
        return new WeakReference(exception);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (WeakReference Carried, IntPtr Error) ReadBorrowedAndDiscard()
    {
        var exception = new ListenerException("borrowed");
        IntPtr error = ManagedExceptionCarrier.ToError(exception);
        Assert.Same(exception, KafkaException.FromFfiError(error, owned: false));
        Assert.Same(exception, KafkaException.FromFfiError(error, owned: false));
        return (new WeakReference(exception), error);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (WeakReference Carried, IntPtr Outer) ReadAChainBorrowedAndDiscard()
    {
        var exception = new ListenerException("borrowed source");
        IntPtr outer = BuildWithSource("outer", ManagedExceptionCarrier.ToError(exception));
        Assert.Same(exception, KafkaException.FromFfiError(outer, owned: false)!.InnerException);
        Assert.Same(exception, KafkaException.FromFfiError(outer, owned: false)!.InnerException);
        return (new WeakReference(exception), outer);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference ReadAChainOwnedAndDiscard()
    {
        var exception = new ListenerException("owned source");
        IntPtr outer = BuildWithSource("outer", ManagedExceptionCarrier.ToError(exception));
        Assert.Same(exception, KafkaException.FromFfiError(outer, owned: true)!.InnerException);
        return new WeakReference(exception);
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

    private static IntPtr BuildWithSource(string message, IntPtr source)
    {
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(message);
        IntPtr outErr = IntPtr.Zero;
        IntPtr error = NativeMethods.ErrorKafkaMessageSource(pinned.Pointer, source, ref outErr);
        Assert.Equal(IntPtr.Zero, outErr);
        Assert.NotEqual(IntPtr.Zero, error);
        return error;
    }

    private static void Collect()
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
    }

    /// <summary>An exception type that is not a <see cref="KafkaException"/>, carried through the core.</summary>
    private sealed class ListenerException : Exception
    {
        public ListenerException(string message)
            : base(message)
        {
        }
    }
}
