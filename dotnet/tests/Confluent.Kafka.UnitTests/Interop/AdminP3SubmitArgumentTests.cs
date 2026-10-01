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

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// What M15/P3 Stage 1's options POCOs actually become at the P/Invoke — the P3 twin of
/// <see cref="AdminSubmitArgumentTests"/>.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>The timeout and <c>DescribeCluster</c>'s two booleans are invisible to a
/// behavioural test.</b> The Rust <c>MockAdminClient</c> ignores the <em>options</em> it is
/// handed — <c>fn describe_cluster(&amp;self, _options: DescribeClusterOptions)</c>, and
/// <c>_options</c> likewise on <c>list_config_resources</c> and
/// <c>list_client_metrics_resources</c> — so their values are read here, at the seam, where
/// they are facts rather than inferences. Measured on the un-fixed commit: swapping
/// <c>DescribeCluster</c>'s two booleans, and discarding the validated timeout on all
/// three RPCs, both left the whole suite green (M15/P3 round 1, finding 69.1).
/// </para>
/// <para>
/// <c>listConfigResources</c>' type filter is the exception, and
/// <see cref="ListConfigResources_TheTypeArrayAndCount_ReachTheSubmit"/> reads it here
/// anyway: it is a <em>parameter</em> rather than an option, so the mock honours it and
/// <c>PublicAdminClusterConfigResourcesTests.ListConfigResources_HonoursTheTypeFilter</c>
/// already observes its effect. Reading it at the seam pins the id encoding and the
/// <c>count</c> beside the timeout it travels with.
/// </para>
/// <para>
/// ⚠ <b>Why a swap is not cosmetic.</b> Asking the broker for fenced brokers when the
/// caller asked for authorized operations makes
/// <see cref="DescribeClusterResult.AuthorizedOperations"/> yield <see langword="null"/>
/// <em>correctly</em> — the ABI gate really is false — so the wrong answer is
/// indistinguishable from the documented "the broker did not report them" path, which is
/// the null-vs-empty distinction this stage exists to get right.
/// </para>
/// <para>
/// ⚠ <b>No <c>MarshalAs(I1)</c> claim is made here.</b> An injected submit never crosses
/// the P/Invoke, so this file cannot observe marshalling at all; <c>I1</c> is pinned
/// structurally by
/// <c>AdminNativeMethodsMarshallingTests.EveryAdminBoolParameter_IsMarshalledAsI1</c>,
/// which sweeps the whole admin surface.
/// </para>
/// </remarks>
public sealed class AdminP3SubmitArgumentTests
{
    /// <summary>
    /// A <see langword="null"/> timeout must become a <b>negative</b> <c>timeout_ms</c>,
    /// which the ABI reads as "unset, use the client default" — <b>not</b> <c>0</c>, which
    /// would mean "time out immediately". Both spellings of "no timeout" agree.
    /// </summary>
    [Theory]
    [InlineData(Rpc.DescribeCluster)]
    [InlineData(Rpc.ListConfigResources)]
    [InlineData(Rpc.ListClientMetricsResources)]
    public void NullTimeout_MapsToANegative_NotZero(Rpc rpc)
    {
        Assert.True(
            Capture(rpc, timeoutMs: null, useOptions: false).TimeoutMs < 0,
            "a null timeout must map to a NEGATIVE timeout_ms (unset), not 0");

        Assert.True(
            Capture(rpc, timeoutMs: null, useOptions: true).TimeoutMs < 0,
            "an explicit options object with a null timeout must map the same way");
    }

    /// <summary>
    /// An explicit timeout is forwarded verbatim, and <c>0</c> stays <c>0</c> — a real
    /// request ("do not wait"), distinct from <see langword="null"/>. Conflating the two is
    /// the mistake the negative-means-unset convention invites.
    /// </summary>
    [Theory]
    [InlineData(Rpc.DescribeCluster, 0)]
    [InlineData(Rpc.DescribeCluster, 12_345)]
    [InlineData(Rpc.ListConfigResources, 0)]
    [InlineData(Rpc.ListConfigResources, 23_456)]
    [InlineData(Rpc.ListClientMetricsResources, 0)]
    [InlineData(Rpc.ListClientMetricsResources, 34_567)]
    public void ExplicitTimeout_IsForwardedVerbatim(Rpc rpc, int timeoutMs) =>
        Assert.Equal(timeoutMs, Capture(rpc, timeoutMs, useOptions: true).TimeoutMs);

    /// <summary>
    /// <c>DescribeCluster</c>'s two booleans reach the submit <b>independently</b>, in
    /// their own argument slots — all four combinations, so a swap cannot hide behind a
    /// case where both happen to agree.
    /// </summary>
    /// <remarks>
    /// Adjacent same-typed arguments are where a transposition survives review, which is
    /// why all four combinations are driven rather than a sample. The P1 precedent for
    /// <c>createTopics</c>' pair is
    /// <see cref="AdminSubmitArgumentTests.BothBools_AreForwardedIndependently"/>.
    /// </remarks>
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void DescribeCluster_BothBools_AreForwardedIndependently(
        bool includeAuthorizedOperations, bool includeFencedBrokers)
    {
        Captured captured = CaptureDescribeCluster(new DescribeClusterOptions
        {
            IncludeAuthorizedOperations = includeAuthorizedOperations,
            IncludeFencedBrokers = includeFencedBrokers,
        });

        Assert.Equal(includeAuthorizedOperations, captured.IncludeAuthorizedOperations);
        Assert.Equal(includeFencedBrokers, captured.IncludeFencedBrokers);
    }

    /// <summary>
    /// <see langword="null"/> options send Java's defaults: both <c>DescribeCluster</c>
    /// booleans <see langword="false"/>, matching
    /// <c>DescribeClusterOptions.java:25, :27</c>.
    /// </summary>
    [Fact]
    public void DescribeCluster_NullOptions_SendJavasDefaults()
    {
        Captured captured = CaptureDescribeCluster(options: null);

        Assert.False(captured.IncludeAuthorizedOperations);
        Assert.False(captured.IncludeFencedBrokers);
        Assert.True(captured.TimeoutMs < 0);
    }

    /// <summary>
    /// The requested type ids reach the submit as their own array beside the timeout —
    /// Java's <c>Type.id()</c> codes, not enum ordinals, with <c>count</c> matching.
    /// </summary>
    [Fact]
    public void ListConfigResources_TheTypeArrayAndCount_ReachTheSubmit()
    {
        Captured captured = CaptureListConfigResources(options: null);

        Assert.Equal(1, captured.Count);
        Assert.Equal((int)ConfigResourceType.Topic, captured.FirstResourceType);
        Assert.Equal(2, captured.FirstResourceType);
    }

    /// <summary>Which RPC a parameterised case drives.</summary>
    public enum Rpc
    {
        /// <summary>Result shape 5 — two booleans plus the timeout.</summary>
        DescribeCluster,

        /// <summary>Result sub-shape 3b — a type-id array plus the timeout.</summary>
        ListConfigResources,

        /// <summary>Result sub-shape 3b — the timeout is the whole input.</summary>
        ListClientMetricsResources,
    }

    private static Captured Capture(Rpc rpc, int? timeoutMs, bool useOptions) => rpc switch
    {
        Rpc.DescribeCluster =>
            CaptureDescribeCluster(useOptions ? new DescribeClusterOptions { TimeoutMs = timeoutMs } : null),
        Rpc.ListConfigResources =>
            CaptureListConfigResources(useOptions ? new ListConfigResourcesOptions { TimeoutMs = timeoutMs } : null),
        _ => CaptureListClientMetricsResources(useOptions, timeoutMs),
    };

    /// <summary>
    /// Runs the production submit with a stand-in that records the arguments instead of
    /// calling native, then completes the operation through the <b>production</b>
    /// trampoline so the <c>GCHandle</c> and the span-the-op reference are released before
    /// the client is disposed.
    /// </summary>
    /// <remarks>
    /// No assertion is made on the resulting <see cref="System.Threading.Tasks.Task"/>
    /// here: <c>DescribeCluster</c>'s accessors are projections over a source built with
    /// <c>RunContinuationsAsynchronously</c>, so a synchronous read would be a race. That
    /// the trampoline faults every awaiter and frees the handle exactly once is what
    /// <see cref="AdminP3OperationLifetimeTests"/> proves; this file's business is the
    /// argument values.
    /// </remarks>
    private static Captured CaptureDescribeCluster(DescribeClusterOptions? options)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        Captured captured = new Captured();
        admin.DescribeCluster(
            options,
            (nativeHandle, timeoutMs, includeAuthorizedOperations, includeFencedBrokers, callback, userData) =>
            {
                captured.TimeoutMs = timeoutMs;
                captured.IncludeAuthorizedOperations = includeAuthorizedOperations;
                captured.IncludeFencedBrokers = includeFencedBrokers;
                captured.UserData = userData;
            });

        AdminCallbacks.DescribeCluster(IntPtr.Zero, CapturedError(), captured.UserData);
        return captured;
    }

    /// <inheritdoc cref="CaptureDescribeCluster"/>
    private static Captured CaptureListConfigResources(ListConfigResourcesOptions? options)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        Captured captured = new Captured();
        ListConfigResourcesResult result = admin.ListConfigResources(
            new[] { ConfigResourceType.Topic },
            options,
            (nativeHandle, resourceTypes, count, timeoutMs, callback, userData) =>
            {
                captured.TimeoutMs = timeoutMs;
                captured.Count = count;
                captured.FirstResourceType = count > 0 ? resourceTypes[0] : int.MinValue;
                captured.UserData = userData;
            });

        AdminCallbacks.ListConfigResources(IntPtr.Zero, CapturedError(), captured.UserData);

        // All() hands back the completion source's own task, so this read is synchronous
        // and race-free — the shape DescribeCluster's projections do not have.
        Assert.NotNull(result.All().Exception);
        return captured;
    }

#pragma warning disable CS0618 // Java deprecates this RPC and its options type; mirrored, not avoided.

    /// <inheritdoc cref="CaptureDescribeCluster"/>
    private static Captured CaptureListClientMetricsResources(bool useOptions, int? timeoutMs)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        Captured captured = new Captured();
        ListClientMetricsResourcesResult result = admin.ListClientMetricsResources(
            useOptions ? new ListClientMetricsResourcesOptions { TimeoutMs = timeoutMs } : null,
            (nativeHandle, submittedTimeoutMs, callback, userData) =>
            {
                captured.TimeoutMs = submittedTimeoutMs;
                captured.UserData = userData;
            });

        AdminCallbacks.ListClientMetricsResources(IntPtr.Zero, CapturedError(), captured.UserData);

        Assert.NotNull(result.All().Exception);
        return captured;
    }

#pragma warning restore CS0618

    /// <summary>
    /// An <b>owned</b> error for the trampoline to consume, standing in for the one native
    /// would hand the callback. The trampoline frees it via
    /// <see cref="KafkaException.FromHandle"/>, so it must never be freed here.
    /// </summary>
    private static IntPtr CapturedError()
    {
        using Utf8Marshal.PinnedUtf8String message = Utf8Marshal.Pin("captured");
        IntPtr error = NativeMethods.KafkaErrorNew(1, message.Pointer);
        Assert.NotEqual(IntPtr.Zero, error);
        return error;
    }

    private sealed class Captured
    {
        internal int TimeoutMs { get; set; }

        internal bool IncludeAuthorizedOperations { get; set; }

        internal bool IncludeFencedBrokers { get; set; }

        internal int Count { get; set; }

        internal int FirstResourceType { get; set; }

        internal IntPtr UserData { get; set; }
    }
}
