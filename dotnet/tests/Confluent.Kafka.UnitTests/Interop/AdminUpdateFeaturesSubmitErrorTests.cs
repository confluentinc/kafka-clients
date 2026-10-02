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
using System.Runtime.InteropServices;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// M15/P13.3 F6 — <c>kafka_admin_AdminClient_update_features_async</c> returns an owned
/// <c>kafka_common_Error_t *</c> since PR #201 round 70. Non-NULL means nothing was submitted
/// and the callback will never fire, so <see cref="NativeAdminClient.UpdateFeatures(IReadOnlyDictionary{string, FeatureUpdate}, UpdateFeaturesOptions?)"/>
/// throws that error from the call itself — Java's synchronous
/// <c>IllegalArgumentException</c> — and releases the operation it had rooted.
/// </summary>
/// <remarks>
/// Against the old <c>void</c> declaration the error was leaked and every feature's
/// <see cref="Task"/> waited for a callback that never comes, holding the operation's
/// <c>GCHandle</c> and the span-the-op client reference for the process lifetime.
/// <see cref="SafeHandle.IsClosed"/> right after <c>Dispose</c> is the witness of the release:
/// nothing is in flight, so the destroy must not be deferred.
/// </remarks>
public sealed class AdminUpdateFeaturesSubmitErrorTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The header's <c>kafka_common_ErrorCode_LOCAL_ILLEGAL_ARGUMENT = -3</c> — the code the
    /// core's <c>Error::local_illegal_argument</c> carries.
    /// </summary>
    private const int LocalIllegalArgumentErrorCode = -3;

    /// <summary>The code Kafka assigns to <c>INVALID_REQUEST</c>.</summary>
    private const int InvalidRequestCode = 42;

    /// <summary>
    /// (a) Through the seam: a submit that returns an owned error makes
    /// <c>UpdateFeatures</c> throw that error — its code and message — and hand back nothing,
    /// and the operation is released: the <c>GCHandle</c> no longer roots it, and the client
    /// destroys on <c>Dispose</c> at once.
    /// </summary>
    [Fact]
    public void ARefusedSubmit_ThrowsTheReturnedError_AndReleasesTheOperation()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        (KafkaException thrown, WeakReference operation, int submits) = RefuseOnce(admin);

        Assert.Equal(1, submits);
        Assert.Equal(InvalidRequestCode, thrown.Code);
        Assert.Equal("the stand-in refused the request", thrown.Message);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        Assert.False(operation.IsAlive, "the GCHandle must be freed: no callback will ever free it");

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(handle.IsClosed, "the span-the-op reference must be released: nothing is in flight");
    }

    /// <summary>
    /// (b) Through the real ABI: two distinct <c>"f"</c> instances are two keys under the
    /// caller's reference comparer, and the core refuses the repeat synchronously with its own
    /// code and message. The call throws before any result is handed out.
    /// </summary>
    /// <remarks>
    /// This is the input the binding's own guards cannot see, and <c>UpdateFeatures</c>
    /// deliberately does not de-duplicate it (M15/P13.3 F4): the core's refusal is the answer.
    /// Against the old <c>void</c> declaration the call returned and <c>All()</c> never
    /// completed, so the whole body runs under <see cref="TestTimeout"/>: that regression fails
    /// at the deadline instead of hanging the run.
    /// </remarks>
    [Fact]
    public async Task ARepeatedFeatureUnderTheCallersComparer_IsRefusedByTheCore_BeforeAnyResult()
    {
        string first = Copy("f");
        string second = Copy("f");
        Assert.NotSame(first, second);
        Dictionary<string, FeatureUpdate> updates =
            new Dictionary<string, FeatureUpdate>(ReferenceComparer.Instance)
            {
                [first] = new FeatureUpdate(1, FeatureUpdate.UpgradeType.Upgrade),
                [second] = new FeatureUpdate(2, FeatureUpdate.UpgradeType.Upgrade),
            };
        Assert.Equal(2, updates.Count);

        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        bool returned = false;
        KafkaException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(async () =>
            {
                UpdateFeaturesResult result = admin.UpdateFeatures(updates, options: null);
                returned = true;
                await result.All().ConfigureAwait(false);
            }),
            s_deadline);

        Assert.False(returned, "the core refuses the call itself, so no result may be handed out");
        Assert.Equal(LocalIllegalArgumentErrorCode, thrown.Code);
        Assert.Equal("feature update at index 1 repeats feature `f`", thrown.Message);

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(handle.IsClosed, "a refused call must not keep the client alive");
    }

    /// <summary>
    /// Runs one refused <c>UpdateFeatures</c> in its own frame, so nothing on the test's stack
    /// can keep the operation alive after it returns.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (KafkaException Thrown, WeakReference Operation, int Submits) RefuseOnce(NativeAdminClient admin)
    {
        WeakReference? operation = null;
        int submits = 0;

        KafkaException thrown = Assert.Throws<KafkaException>(() => admin.UpdateFeatures(
            new Dictionary<string, FeatureUpdate>(StringComparer.Ordinal)
            {
                ["metadata.version"] = new FeatureUpdate(1, FeatureUpdate.UpgradeType.Upgrade),
            },
            options: null,
            (nativeHandle, features, maxVersionLevels, upgradeTypes, count, timeoutMs, validateOnly, callback,
                userData) =>
            {
                submits++;
                operation = new WeakReference(GCHandle.FromIntPtr(userData).Target);
                return MakeError(InvalidRequestCode, "the stand-in refused the request");
            }));

        return (thrown, operation!, submits);
    }

    /// <summary>
    /// An <b>owned</b> error, as the core returns one. <c>UpdateFeatures</c> frees it through
    /// <see cref="KafkaException.FromHandle"/>, so it is never freed here.
    /// </summary>
    private static IntPtr MakeError(int code, string message)
    {
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(message);
        IntPtr error = NativeMethods.KafkaErrorNew(code, pinned.Pointer);
        Assert.NotEqual(IntPtr.Zero, error);
        return error;
    }

    /// <summary>A fresh string instance equal to <paramref name="value"/> but never the same reference.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static string Copy(string value) => new string(value.ToCharArray());

    /// <summary>
    /// Reference equality over strings, hand-written because this project also compiles for
    /// net462, where <c>System.Collections.Generic.ReferenceEqualityComparer</c> (.NET 5+) does
    /// not exist.
    /// </summary>
    private sealed class ReferenceComparer : IEqualityComparer<string>
    {
        public static readonly ReferenceComparer Instance = new ReferenceComparer();

        public bool Equals(string? x, string? y) => ReferenceEquals(x, y);

        public int GetHashCode(string obj) => RuntimeHelpers.GetHashCode(obj);
    }
}
