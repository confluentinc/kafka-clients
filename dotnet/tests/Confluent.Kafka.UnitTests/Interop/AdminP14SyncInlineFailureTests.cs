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
using System.Threading;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// A sync Result over an RPC whose callbacks fire <b>inline</b>, inside the submit (M15/P14 S4): every future the
/// Result hands out is already done when the RPC returns, and its <c>Get()</c> throws the delivered error at once.
/// The RPC is the production core, driven through its own submit seam (DoD #12), and the Result is built by its
/// production <c>From</c> — the path <see cref="MockAdminClient"/> and <see cref="KafkaAdminClient"/> take.
/// </summary>
/// <remarks>
/// This is what makes the core's "latches before the submit" ordering load-bearing at the public boundary: a latch
/// created after the submit would miss the inline callback, and the future would never complete.
/// </remarks>
public sealed class AdminP14SyncInlineFailureTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>
    /// <see cref="CreateTopicsResult"/>: both topics fail inside the submit. On return every view is done — the erased
    /// <c>Values</c>, <c>All()</c> and a typed accessor — and each <c>Get()</c> throws the exact delivered error, the
    /// aggregate the first in key order.
    /// </summary>
    [Fact]
    public void CreateTopics_CallbacksFiredInsideTheSubmit_EveryFutureIsDoneOnReturn_AndGetThrowsAtOnce()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        CreateTopicsResult result = CreateTopicsResult.From(admin.CreateTopics(
            new[] { new NewTopic("inline-alpha", 1, 1), new NewTopic("inline-beta", 1, 1) },
            options: null,
            AdminFlavour.Sync,
            (nativeHandle, topics, count, timeoutMs, validateOnly, retryOnQuotaViolation, callback, userData) =>
            {
                // `topics` holds NewTopic_t handles, not names, so each key is pinned here.
                using Utf8Marshal.PinnedUtf8String alphaKey = Utf8Marshal.Pin("inline-alpha");
                using Utf8Marshal.PinnedUtf8String betaKey = Utf8Marshal.Pin("inline-beta");
                callback(betaKey.Pointer, IntPtr.Zero, MakeError(42, "beta failed inline"), userData);
                callback(alphaKey.Pointer, IntPtr.Zero, MakeError(41, "alpha failed inline"), userData);
            }));

        Assert.True(result.Values["inline-alpha"].IsDone, "the inline callback ran before the RPC returned");
        Assert.True(result.Values["inline-beta"].IsDone, "the inline callback ran before the RPC returned");
        Assert.True(result.All().IsDone, "every key was done before the RPC returned");
        Assert.True(result.NumPartitions("inline-beta").IsDone, "a projection is done when its key is");

        KafkaException alpha = AssertThrew(BlockingCall.RunWait(() => result.Values["inline-alpha"].Get()), 41, "alpha failed inline");
        AssertThrew(BlockingCall.RunWait(() => result.Values["inline-beta"].Get()), 42, "beta failed inline");
        AssertThrew(BlockingCall.RunGet(() => result.NumPartitions("inline-beta").Get()), 42, "beta failed inline");
        Assert.Same(alpha, AssertThrew(BlockingCall.RunWait(() => result.All().Get()), 41, "alpha failed inline"));

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(
            SpinWait.SpinUntil(() => handle.IsClosed, s_deadline),
            "the operation must have released its reference to the client");
    }

    private static KafkaException AssertThrew(object? outcome, int code, string message)
    {
        KafkaException failure = Assert.IsType<KafkaException>(outcome);
        Assert.Equal(code, failure.Code);
        Assert.Equal(message, failure.Message);
        return failure;
    }

    /// <summary>
    /// Builds an <b>owned</b> <c>kafka_common_KafkaError_t</c> standing in for the one native would hand the callback;
    /// the trampoline frees it via <see cref="KafkaException.FromHandle"/>.
    /// </summary>
    private static IntPtr MakeError(int code, string message)
    {
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(message);
        IntPtr error = NativeMethods.KafkaErrorNew(code, pinned.Pointer);
        Assert.NotEqual(IntPtr.Zero, error);
        return error;
    }
}
