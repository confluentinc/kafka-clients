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
using System.Runtime.CompilerServices;

using Confluent.Kafka.Common;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// A managed exception crosses the core as a <c>local_callback</c> error and comes back as the same instance; the
/// core's release callback frees its <see cref="System.Runtime.InteropServices.GCHandle"/> when the error is destroyed.
/// </summary>
public sealed class ManagedExceptionCarrierTests
{
    private const int LocalCallbackCode = -30;

    private const string UnreadableMessage = "callback raised an exception";

    [Fact]
    public void ToError_CarriesTheSameInstance_UnderTheLocalCallbackCode_WithItsMessage()
    {
        var exception = new CarriedException("listener failed: café");
        IntPtr error = ManagedExceptionCarrier.ToError(exception);
        try
        {
            Assert.Same(exception, ManagedExceptionCarrier.Carried(error));
            Assert.Same(exception, ManagedExceptionCarrier.Carried(error));
            Assert.Equal(LocalCallbackCode, NativeMethods.Code(error));
            Assert.Equal("listener failed: café", Utf8Marshal.PtrToString(NativeMethods.Message(error)));
        }
        finally
        {
            NativeMethods.ErrorDestroy(error);
        }
    }

    [Fact]
    public void ToError_AnEmptyMessage_ReportsTheTypeName()
    {
        IntPtr error = ManagedExceptionCarrier.ToError(new InvalidOperationException(string.Empty));
        try
        {
            Assert.Equal(typeof(InvalidOperationException).FullName, Utf8Marshal.PtrToString(NativeMethods.Message(error)));
        }
        finally
        {
            NativeMethods.ErrorDestroy(error);
        }
    }

    [Fact]
    public void ToError_AMessageGetterThatThrows_ReportsTheFallbackText_AndCarriesTheSameInstance()
    {
        var exception = new ThrowingMessageException();
        IntPtr error = ManagedExceptionCarrier.ToError(exception);
        try
        {
            Assert.NotEqual(IntPtr.Zero, error);
            Assert.Equal(LocalCallbackCode, NativeMethods.Code(error));
            Assert.Equal(UnreadableMessage, Utf8Marshal.PtrToString(NativeMethods.Message(error)));
            Assert.Same(exception, ManagedExceptionCarrier.Carried(error));
        }
        finally
        {
            NativeMethods.ErrorDestroy(error);
        }
    }

    [Fact]
    public void ToError_AMessageGetterThatReturnsNull_ReportsTheTypeName_AndCarriesTheSameInstance()
    {
        var exception = new NullMessageException();
        IntPtr error = ManagedExceptionCarrier.ToError(exception);
        try
        {
            Assert.NotEqual(IntPtr.Zero, error);
            Assert.Equal(LocalCallbackCode, NativeMethods.Code(error));
            Assert.Equal(
                "Confluent.Kafka.UnitTests.Interop.ManagedExceptionCarrierTests+NullMessageException",
                Utf8Marshal.PtrToString(NativeMethods.Message(error)));
            Assert.Same(exception, ManagedExceptionCarrier.Carried(error));
        }
        finally
        {
            NativeMethods.ErrorDestroy(error);
        }
    }

    [Fact]
    public void Carried_AnotherErrorClass_CarriesNothing()
    {
        using Utf8Marshal.PinnedUtf8String message = Utf8Marshal.Pin("not a callback error");
        IntPtr outErr = IntPtr.Zero;
        IntPtr error = NativeMethods.ErrorLocalIllegalState(message.Pointer, ref outErr);
        Assert.Equal(IntPtr.Zero, outErr);
        try
        {
            Assert.Null(ManagedExceptionCarrier.Carried(error));
        }
        finally
        {
            NativeMethods.ErrorDestroy(error);
        }
    }

    [Fact]
    public void DestroyingTheError_ReleasesTheHandle_AndOnlyThen()
    {
        (WeakReference carried, IntPtr error) = CarryAnUnreferencedException();

        Collect();
        Assert.True(carried.IsAlive, "the error's GCHandle must keep the exception alive until the error is destroyed");

        NativeMethods.ErrorDestroy(error);
        Collect();
        Assert.False(carried.IsAlive, "destroying the error must release its GCHandle");
    }

    [Fact]
    public void ToErrorThenDestroy_RepeatedManyTimes_ReleasesEveryHandle()
    {
        WeakReference? last = null;
        for (int i = 0; i < 1000; i++)
        {
            (WeakReference carried, IntPtr error) = CarryAnUnreferencedException();
            NativeMethods.ErrorDestroy(error);
            last = carried;
        }

        Collect();
        Assert.False(last!.IsAlive);
    }

    [Fact]
    public void FromHandle_DestroysTheErrorItReads()
    {
        WeakReference carried = ReadAndDiscard();

        Collect();
        Assert.False(carried.IsAlive, "FromHandle must destroy the owned error, releasing its GCHandle");
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference ReadAndDiscard()
    {
        (WeakReference carried, IntPtr error) = CarryAnUnreferencedException();
        _ = KafkaException.FromHandle(error);
        return carried;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (WeakReference Carried, IntPtr Error) CarryAnUnreferencedException()
    {
        var exception = new CarriedException("unreferenced");
        return (new WeakReference(exception), ManagedExceptionCarrier.ToError(exception));
    }

    private static void Collect()
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
    }

    /// <summary>An exception type only these tests create, carried through the core with its message.</summary>
    private sealed class CarriedException : Exception
    {
        public CarriedException(string message)
            : base(message)
        {
        }
    }

    /// <summary>An exception whose <see cref="Exception.Message"/> getter throws.</summary>
    private sealed class ThrowingMessageException : Exception
    {
        public override string Message => throw new InvalidOperationException("the message getter failed");
    }

    /// <summary>An exception whose <see cref="Exception.Message"/> getter returns <see langword="null"/>.</summary>
    private sealed class NullMessageException : Exception
    {
        public override string Message => null!;
    }
}
