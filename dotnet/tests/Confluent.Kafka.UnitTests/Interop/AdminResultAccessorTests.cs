// Copyright 2026 Confluent Inc.
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
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Tests <see cref="NativeAdminClient.CallAccessor(SafeAdminResultHandle, NativeAdminClient.ResultFuture)"/>, which
/// every admin Result accessor call goes through: the Result is held for the call, so a Dispose during it destroys the
/// Result only once the accessor has returned, and a disposed Result is never read. The handles wrap a fake pointer
/// whose destroy only counts, so no native Result is involved.
/// </summary>
public sealed class AdminResultAccessorTests
{
    private static readonly IntPtr s_result = new IntPtr(0x1000);
    private static readonly IntPtr s_key = new IntPtr(0x2000);
    private static readonly IntPtr s_value = new IntPtr(0x3000);

    [Fact]
    public void CallAccessor_ADisposeDuringTheCall_DestroysTheResultAfterTheAccessorReturns()
    {
        var destroys = new Destroys();
        SafeAdminResultHandle result = destroys.Handle();
        int destroysDuringTheCall = -1;

        IntPtr value = NativeAdminClient.CallAccessor(result, (IntPtr self, ref IntPtr outErr) =>
        {
            Assert.Equal(s_result, self);
            result.Dispose();
            destroysDuringTheCall = destroys.Count;
            return s_value;
        });

        Assert.Equal(s_value, value);
        Assert.Equal(0, destroysDuringTheCall);
        Assert.Equal(1, destroys.Count);
    }

    [Fact]
    public void CallAccessor_WithAKey_PassesTheKeyAndHoldsTheResult()
    {
        var destroys = new Destroys();
        SafeAdminResultHandle result = destroys.Handle();
        int destroysDuringTheCall = -1;

        IntPtr value = NativeAdminClient.CallAccessor(
            result,
            (IntPtr self, IntPtr key, ref IntPtr outErr) =>
            {
                Assert.Equal(s_result, self);
                Assert.Equal(s_key, key);
                result.Dispose();
                destroysDuringTheCall = destroys.Count;
                return s_value;
            },
            s_key);

        Assert.Equal(s_value, value);
        Assert.Equal(0, destroysDuringTheCall);
        Assert.Equal(1, destroys.Count);
    }

    [Fact]
    public void CallAccessor_ADisposedResult_ThrowsObjectDisposedWithoutCallingTheAccessor()
    {
        var destroys = new Destroys();
        SafeAdminResultHandle result = destroys.Handle();
        result.Dispose();
        bool called = false;

        Assert.Throws<ObjectDisposedException>(() => NativeAdminClient.CallAccessor(result, (IntPtr self, ref IntPtr outErr) =>
        {
            called = true;
            return s_value;
        }));
        Assert.Throws<ObjectDisposedException>(() => NativeAdminClient.CallAccessor(
            result,
            (IntPtr self, IntPtr key, ref IntPtr outErr) =>
            {
                called = true;
                return s_value;
            },
            s_key));

        Assert.False(called);
        Assert.Equal(1, destroys.Count);
    }

    [Fact]
    public void CallAccessor_AThrowingAccessor_ReleasesTheResult()
    {
        var destroys = new Destroys();
        SafeAdminResultHandle result = destroys.Handle();

        InvalidOperationException thrown = Assert.Throws<InvalidOperationException>(() => NativeAdminClient.CallAccessor(
            result, (IntPtr self, ref IntPtr outErr) => throw new InvalidOperationException("accessor failed")));
        Assert.Equal("accessor failed", thrown.Message);
        Assert.Equal(0, destroys.Count);

        result.Dispose();

        Assert.Equal(1, destroys.Count);
    }

    /// <summary>Counts the destroys of the fake Results it hands out.</summary>
    private sealed class Destroys
    {
        internal int Count { get; private set; }

        internal SafeAdminResultHandle Handle() =>
            new SafeAdminResultHandle(s_result, (IntPtr result, ref IntPtr outErr) => Count++);
    }
}
