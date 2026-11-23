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
using System.Reflection;
using System.Runtime.InteropServices;

using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The native-future value source over client-less <c>kafka_common_KafkaFuture_completed_future</c> futures, whose
/// <c>get_cb</c> delivers inline: the delivery settles the managed future, then releases the handle, and a read of a
/// released handle answers from the settled state. Each derived class runs them on its flavor's engine.
/// </summary>
public abstract class NativeFutureSourceTestBase
{
    private static readonly IntPtr s_value = new IntPtr(42);

    [Fact]
    public void ACompletedFuture_IsSettledByItsDelivery_ThenItsHandleIsReleased()
    {
        SafeKafkaFutureHandle handle = SafeKafkaFutureHandle.Owned(Completed(s_value, IntPtr.Zero));

        IKafkaFutureImpl<long> future = NativeFutureSource<long>.Start(handle, value => value.ToInt64(), Flavour);

        Assert.IsAssignableFrom(Flavour == AdminFlavour.Sync ? typeof(KafkaFuture<long>) : typeof(AsyncKafkaFuture<long>), future);
        Assert.True(future.IsSettled);
        Assert.True(handle.IsClosed);
        Assert.Equal(42L, future.GetNowUnwrapped(-1L));
        Assert.Equal(42L, future.GetUnwrapped());
    }

    [Fact]
    public void AFailedCompletedFuture_FailsWithTheCoresError()
    {
        IntPtr error = Native.ErrorKafkaMessage("boom");
        SafeKafkaFutureHandle handle = SafeKafkaFutureHandle.Owned(Completed(IntPtr.Zero, error));

        IKafkaFutureImpl<long> future = NativeFutureSource<long>.Start(handle, value => value.ToInt64(), Flavour);

        Assert.True(handle.IsClosed);
        Assert.True(future.IsCompletedExceptionally());
        KafkaException failure = Assert.IsType<KafkaException>(future.Failure);
        Assert.Equal("boom", failure.Message);
    }

    [Fact]
    public void AConverterThatThrows_FailsTheFuture_AndTheHandleIsStillReleased()
    {
        InvalidOperationException thrown = new InvalidOperationException("converter");
        SafeKafkaFutureHandle handle = SafeKafkaFutureHandle.Owned(Completed(s_value, IntPtr.Zero));

        IKafkaFutureImpl<long> future = NativeFutureSource<long>.Start(handle, _ => throw thrown, Flavour);

        Assert.True(handle.IsClosed);
        Assert.Same(thrown, future.Failure);
    }

    [Fact]
    public void AReleasedHandle_ReadsAsDone_AndTryReadLeavesTheSettledStateToTheFuture()
    {
        SafeKafkaFutureHandle handle = SafeKafkaFutureHandle.Owned(Completed(s_value, IntPtr.Zero));
        IKafkaFutureImpl<long> future = NativeFutureSource<long>.Start(handle, value => value.ToInt64(), Flavour);
        IFutureValueSource<long> source = SourceOf(future);

        Assert.True(handle.IsClosed);
        Assert.True(source.IsDone);
        Assert.False(source.TryRead(out _, out Exception? error));
        Assert.Null(error);
        Assert.Equal(42L, future.GetNowUnwrapped(-1L));
    }

    [Fact]
    public void ABorrowedFuture_HoldsItsOwner_UntilItsDelivery_AndDoesNotDestroyTheFuture()
    {
        IntPtr outErr = IntPtr.Zero;
        SafeKafkaMapHandle owner = new SafeKafkaMapHandle(NativeMethods.MapNew(ref outErr));
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr raw = Completed(s_value, IntPtr.Zero);
        SafeKafkaFutureHandle borrowed = SafeKafkaFutureHandle.Borrowed(raw, owner);

        owner.Dispose();
        Assert.False(owner.IsClosed);

        IKafkaFutureImpl<long> future = NativeFutureSource<long>.Start(borrowed, value => value.ToInt64(), Flavour);

        Assert.Equal(42L, future.GetNowUnwrapped(-1L));
        Assert.True(borrowed.IsClosed);
        Assert.True(owner.IsClosed);

        using SafeKafkaFutureHandle owned = SafeKafkaFutureHandle.Owned(raw);
        Assert.Equal(1, NativeMethods.KafkaFutureIsDone(owned, ref outErr));
        KafkaException.ThrowIfFfiFailed(ref outErr);
    }

    private protected abstract AdminFlavour Flavour { get; }

    private static IFutureValueSource<long> SourceOf(IKafkaFutureImpl<long> future) =>
        (IFutureValueSource<long>)typeof(FutureState<long>)
            .GetField("_source", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(future.GetType()
                .GetField("_state", BindingFlags.Instance | BindingFlags.NonPublic)!
                .GetValue(future))!;

    private static IntPtr Completed(IntPtr value, IntPtr error)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr future = NativeMethods.KafkaFutureCompletedFuture(value, error, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return future;
    }

    /// <summary>The C functions only these tests call.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        internal static IntPtr ErrorKafkaMessage(string message)
        {
            using Utf8Marshal.PinnedUtf8String text = Utf8Marshal.Pin(message);
            IntPtr outErr = IntPtr.Zero;
            IntPtr error = ErrorKafkaMessageErr(text.Pointer, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return error;
        }

        [DllImport(DllName, EntryPoint = "kafka_common_Error_kafka_message_err", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr ErrorKafkaMessageErr(IntPtr message, ref IntPtr outErr);
    }
}

/// <summary>
/// The native-future value source tests on the sync admin client's engine.
/// </summary>
public sealed class NativeFutureSourceTests : NativeFutureSourceTestBase
{
    private protected override AdminFlavour Flavour => AdminFlavour.Sync;
}

/// <summary>
/// The native-future value source tests on the async admin client's engine.
/// </summary>
public sealed class AsyncNativeFutureSourceTests : NativeFutureSourceTestBase
{
    private protected override AdminFlavour Flavour => AdminFlavour.Async;
}
