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
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// What M15/P3 Stage 2's options and inputs actually become at the P/Invoke — the
/// config-RPC twin of <see cref="AdminP3SubmitArgumentTests"/>, and the widest instance
/// of the standing obligation (PLAN §9.1 item 5 / decision D19) that every admin RPC's
/// options are asserted at the submit seam.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>The Rust <c>MockAdminClient</c> ignores <c>_options</c> for these RPCs too</b>
/// (<c>fn describe_configs(&amp;self, …, _options: DescribeConfigsOptions)</c>), so a
/// behavioural test cannot see the two <c>describeConfigs</c> booleans, the
/// <c>validate_only</c> flag, or any timeout. It also cannot see the <b>shape</b> of the
/// five-array <c>incrementalAlterConfigs</c> input — the row grouping and the
/// null-versus-empty config value are decided entirely on the way in.
/// </para>
/// <para>
/// ⚠ <b>The null config value is the sharpest case here.</b> A
/// <see cref="AlterConfigOpType.Delete"/> carries a <see langword="null"/> value, and the
/// header calls the null pointer out as the value <c>DELETE</c> uses. An empty string is a
/// <em>different request</em>, so both are asserted, distinctly.
/// </para>
/// </remarks>
public sealed class AdminConfigsSubmitArgumentTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly ConfigResource s_topicResource =
        new ConfigResource(ConfigResourceType.Topic, "cfg-topic");

    private static readonly ConfigResource s_brokerResource =
        new ConfigResource(ConfigResourceType.Broker, "0");

    /// <summary>
    /// A <see langword="null"/> timeout must become a <b>negative</b> <c>timeout_ms</c>
    /// ("unset"), never <c>0</c>; an explicit one is forwarded verbatim, and <c>0</c> stays
    /// <c>0</c>.
    /// </summary>
    [Fact]
    public void DescribeConfigs_TimeoutMapping()
    {
        Assert.True(CaptureDescribe(options: null).TimeoutMs < 0);
        Assert.True(CaptureDescribe(new DescribeConfigsOptions()).TimeoutMs < 0);
        Assert.Equal(0, CaptureDescribe(new DescribeConfigsOptions { TimeoutMs = 0 }).TimeoutMs);
        Assert.Equal(4_242, CaptureDescribe(new DescribeConfigsOptions { TimeoutMs = 4_242 }).TimeoutMs);
    }

    /// <inheritdoc cref="DescribeConfigs_TimeoutMapping"/>
    [Fact]
    public void IncrementalAlterConfigs_TimeoutMapping()
    {
        Assert.True(CaptureAlter(options: null).TimeoutMs < 0);
        Assert.True(CaptureAlter(new AlterConfigsOptions()).TimeoutMs < 0);
        Assert.Equal(0, CaptureAlter(new AlterConfigsOptions { TimeoutMs = 0 }).TimeoutMs);
        Assert.Equal(5_353, CaptureAlter(new AlterConfigsOptions { TimeoutMs = 5_353 }).TimeoutMs);
    }

    /// <summary>
    /// <c>describeConfigs</c>' two booleans reach the submit <b>independently</b>, in their
    /// own argument slots — all four combinations, so a transposition cannot hide behind a
    /// case where both agree.
    /// </summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void DescribeConfigs_BothBools_AreForwardedIndependently(bool synonyms, bool documentation)
    {
        Captured captured = CaptureDescribe(new DescribeConfigsOptions
        {
            IncludeSynonyms = synonyms,
            IncludeDocumentation = documentation,
        });

        Assert.Equal(synonyms, captured.IncludeSynonyms);
        Assert.Equal(documentation, captured.IncludeDocumentation);
    }

    /// <summary><c>validateOnly</c> reaches the submit, and defaults to false as Java does.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void IncrementalAlterConfigs_ValidateOnly_IsForwarded(bool validateOnly)
    {
        Assert.Equal(validateOnly, CaptureAlter(new AlterConfigsOptions { ValidateOnly = validateOnly }).ValidateOnly);
        Assert.False(CaptureAlter(options: null).ValidateOnly);
    }

    /// <summary>
    /// The resource type and name reach the submit as parallel arrays, with the type as
    /// Java's <c>Type.id()</c> code rather than an enum ordinal.
    /// </summary>
    [Fact]
    public void DescribeConfigs_ResourcesReachTheSubmitAsParallelArrays()
    {
        Captured captured = CaptureDescribe(options: null, s_topicResource, s_brokerResource);

        Assert.Equal(2, captured.Count);
        Assert.Equal(new[] { 2, 4 }, captured.ResourceTypes);
        Assert.Equal(new[] { "cfg-topic", "0" }, captured.ResourceNames);
    }

    /// <summary>
    /// ⚠ <b>Rows naming the same resource are emitted contiguously, in the caller's op
    /// order, and two resources never interleave</b> — the header requires it.
    /// </summary>
    /// <remarks>
    /// The assertion is on the flattened arrays themselves: three ops on one resource and
    /// two on another must produce five rows whose resource columns are grouped, with each
    /// group's config names in the order the caller listed them.
    /// </remarks>
    [Fact]
    public void IncrementalAlterConfigs_RowsAreGroupedByResource_InCallerOrder()
    {
        Captured captured = CaptureAlter(
            options: null,
            (s_topicResource, new[]
            {
                new AlterConfigOp(new ConfigEntry("a", "1"), AlterConfigOpType.Set),
                new AlterConfigOp(new ConfigEntry("b", "2"), AlterConfigOpType.Append),
                new AlterConfigOp(new ConfigEntry("c", null), AlterConfigOpType.Delete),
            }),
            (s_brokerResource, new[]
            {
                new AlterConfigOp(new ConfigEntry("d", "4"), AlterConfigOpType.Subtract),
                new AlterConfigOp(new ConfigEntry("e", "5"), AlterConfigOpType.Set),
            }));

        Assert.Equal(5, captured.Count);

        // Grouped: the first three rows are the topic's, the last two the broker's.
        Assert.Equal(new[] { "cfg-topic", "cfg-topic", "cfg-topic", "0", "0" }, captured.ResourceNames);
        Assert.Equal(new[] { 2, 2, 2, 4, 4 }, captured.ResourceTypes);

        // In the caller's op order within each group, with the op-type wire ids.
        Assert.Equal(new[] { "a", "b", "c", "d", "e" }, captured.ConfigNames);
        Assert.Equal(new[] { 0, 2, 1, 3, 0 }, captured.OpTypes);
    }

    /// <summary>
    /// ⚠ <b>A <see langword="null"/> config value reaches the ABI as a NULL pointer, and an
    /// empty string does not.</b> They are different requests: the null is the value
    /// <see cref="AlterConfigOpType.Delete"/> uses, and a <c>?? string.Empty</c> anywhere on
    /// this path would silently turn one into the other.
    /// </summary>
    [Fact]
    public void IncrementalAlterConfigs_NullConfigValue_ReachesTheAbiAsNull_AndEmptyDoesNot()
    {
        Captured captured = CaptureAlter(
            options: null,
            (s_topicResource, new[]
            {
                new AlterConfigOp(new ConfigEntry("deleted", null), AlterConfigOpType.Delete),
                new AlterConfigOp(new ConfigEntry("emptied", string.Empty), AlterConfigOpType.Set),
                new AlterConfigOp(new ConfigEntry("valued", "v"), AlterConfigOpType.Set),
            }));

        Assert.Equal(3, captured.Count);

        // Row 0 — the null: a NULL pointer, not a pointer to "".
        Assert.Equal(IntPtr.Zero, captured.ConfigValuePointers[0]);
        Assert.Null(captured.ConfigValues[0]);

        // Row 1 — the empty string: a NON-null pointer to a zero-length string.
        Assert.NotEqual(IntPtr.Zero, captured.ConfigValuePointers[1]);
        Assert.Equal(string.Empty, captured.ConfigValues[1]);

        Assert.Equal("v", captured.ConfigValues[2]);
    }

    /// <summary>
    /// ⚠ A resource with an <b>empty</b> operation collection contributes <b>exactly one
    /// sentinel row</b> — its type and name, a NULL config name, a NULL config value and op
    /// type <c>-1</c> — and its awaitable then waits for that resource's own callback
    /// (M15/P13.2, finding F1).
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>This asserts the request SHAPE and the countdown together.</b> The row is the
    /// header's "non-NULL resource name but a NULL config name" encoding; the NULL is
    /// asserted on the <em>pointer</em>, since a pointer to <c>""</c> would be a real
    /// operation on a config named <c>""</c>. The key is <b>not</b> completed when the submit
    /// returns — the local completion this test used to pin is gone — and firing the one
    /// callback the ABI owes for it is what resolves it.
    /// </para>
    /// <para>
    /// The <c>n == 0</c> submit-boundary assertion this test used to carry lives in
    /// <see cref="IncrementalAlterConfigs_AnEmptyMap_SettlesAtTheSubmitBoundary"/>: a
    /// zero-op resource is no longer an <c>n == 0</c> case.
    /// </para>
    /// </remarks>
    [Fact]
    public void IncrementalAlterConfigs_AResourceWithNoOps_ContributesOneSentinelRow()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        Captured captured = new Captured();
        AlterConfigsResult result = admin.IncrementalAlterConfigs(
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
            {
                [s_topicResource] = Array.Empty<AlterConfigOp>(),
            },
            options: null,
            captured.RecordAlter);

        Assert.Equal(1, captured.Count);
        Assert.Equal(new[] { 2 }, captured.ResourceTypes);
        Assert.Equal(new[] { "cfg-topic" }, captured.ResourceNames);
        Assert.Equal(new string?[] { null }, captured.ConfigNames);
        Assert.Equal(new[] { IntPtr.Zero }, captured.ConfigNamePointers);
        Assert.Equal(new[] { IntPtr.Zero }, captured.ConfigValuePointers);
        Assert.Equal(new[] { -1 }, captured.OpTypes);

        // Pending on its callback — not completed locally.
        Task pending = result.Values[s_topicResource];
        Assert.False(pending.IsCompleted, "a zero-op resource must wait for the callback the ABI owes it");

        FireAlter(s_topicResource, IntPtr.Zero, captured.UserData);

        Assert.True(pending.IsCompleted);
        Assert.Null(pending.Exception);
    }

    /// <summary>
    /// ⚠ <b>The equivalent-mutant guard for F1's countdown.</b> Resource A (two ops), B (no
    /// ops) and C (one op) flatten to rows <c>[A, A, B, C]</c>, and the operation waits for
    /// <b>three</b> callbacks: after two, the third key is still pending and the operation
    /// still holds the client past its <see cref="NativeAdminClient.Dispose"/>.
    /// </summary>
    /// <remarks>
    /// A countdown left at "keys minus the zero-op ones" (two here) is invisible to any test
    /// that fires every callback: it reaches zero on the last one either way. It is visible
    /// only here, where one callback is withheld — that countdown would reach zero after two,
    /// settle the withheld key without an answer, and release the client early (the M15/P13.1
    /// lesson, <c>ffi-marshalling.md</c> §B6). The withheld key is the zero-op one, so the
    /// same assertion also rules out completing it locally.
    /// </remarks>
    [Fact]
    public void IncrementalAlterConfigs_MixedOps_RowShapeAndCallbackCount()
    {
        ConfigResource a = new ConfigResource(ConfigResourceType.Topic, "cfg-a");
        ConfigResource b = new ConfigResource(ConfigResourceType.Topic, "cfg-b");
        ConfigResource c = new ConfigResource(ConfigResourceType.Broker, "0");

        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        Captured captured = new Captured();
        AlterConfigsResult result = admin.IncrementalAlterConfigs(
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
            {
                [a] = new[]
                {
                    new AlterConfigOp(new ConfigEntry("a1", "1"), AlterConfigOpType.Set),
                    new AlterConfigOp(new ConfigEntry("a2", null), AlterConfigOpType.Delete),
                },
                [b] = Array.Empty<AlterConfigOp>(),
                [c] = new[] { new AlterConfigOp(new ConfigEntry("c1", "3"), AlterConfigOpType.Append) },
            },
            options: null,
            captured.RecordAlter);

        // ---- The row shape: grouped, in caller order, with B's sentinel in its place. ----
        Assert.Equal(4, captured.Count);
        Assert.Equal(new[] { "cfg-a", "cfg-a", "cfg-b", "0" }, captured.ResourceNames);
        Assert.Equal(new[] { 2, 2, 2, 4 }, captured.ResourceTypes);
        Assert.Equal(new[] { "a1", "a2", null, "c1" }, captured.ConfigNames);
        Assert.Equal(IntPtr.Zero, captured.ConfigNamePointers[2]);
        Assert.Equal(IntPtr.Zero, captured.ConfigValuePointers[2]);
        Assert.Equal(new[] { 0, 1, -1, 2 }, captured.OpTypes);

        // ---- Two of the three callbacks: the third key is still pending. ----
        FireAlter(a, IntPtr.Zero, captured.UserData);
        FireAlter(c, IntPtr.Zero, captured.UserData);

        Assert.True(result.Values[a].IsCompleted);
        Assert.True(result.Values[c].IsCompleted);
        Assert.False(result.Values[b].IsCompleted, "the countdown must wait for the zero-op key's own callback");

        // …and the operation still holds the client: Dispose must defer the native destroy.
        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.False(handle.IsClosed, "the operation is still in flight, so the client must stay alive");

        // ---- The third callback resolves the last key and releases the client. ----
        FireAlter(b, IntPtr.Zero, captured.UserData);

        Assert.True(handle.IsClosed, "the last callback must run the deferred release");
        foreach (KeyValuePair<ConfigResource, Task> entry in result.Values)
        {
            Assert.True(entry.Value.IsCompleted);
            Assert.Null(entry.Value.Exception);
        }
    }

    /// <summary>
    /// A zero-op resource under <see cref="AlterConfigsOptions.ValidateOnly"/> — "validate
    /// this resource, change nothing" — is <b>sent</b>: the submit carries
    /// <c>validate_only</c> and the resource's sentinel row, and the resource is answered
    /// by its callback.
    /// </summary>
    /// <remarks>
    /// Only the request is asserted: the Rust mock ignores <c>validate_only</c>
    /// (<c>_options</c>), so what a broker would answer is not observable without one.
    /// </remarks>
    [Fact]
    public void IncrementalAlterConfigs_ZeroOpsWithValidateOnly_IsSentAndAnswered()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        Captured captured = new Captured();
        AlterConfigsResult result = admin.IncrementalAlterConfigs(
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
            {
                [s_topicResource] = Array.Empty<AlterConfigOp>(),
            },
            new AlterConfigsOptions { ValidateOnly = true },
            captured.RecordAlter);

        Assert.True(captured.ValidateOnly);
        Assert.Equal(1, captured.Count);
        Assert.Equal(new[] { "cfg-topic" }, captured.ResourceNames);
        Assert.Equal(new[] { IntPtr.Zero }, captured.ConfigNamePointers);
        Assert.Equal(new[] { -1 }, captured.OpTypes);
        Assert.False(result.Values[s_topicResource].IsCompleted);

        FireAlter(s_topicResource, CapturedError(), captured.UserData);

        KafkaException answer = Assert.IsType<KafkaException>(
            Assert.Single(result.Values[s_topicResource].Exception!.InnerExceptions));
        Assert.Equal(1, answer.Code);
        Assert.Equal("captured", answer.Message);
    }

    /// <summary>
    /// ⚠ <b>The <c>n == 0</c> submit boundary</b>: an EMPTY map submits no row, names no
    /// resource, and so settles inside the submit's caller — the submit token is the whole
    /// countdown, and the operation releases the client before the call returns.
    /// </summary>
    /// <remarks>
    /// Moved here from the zero-op test when F1 gave a zero-op resource its sentinel row: an
    /// empty map is now the only way to reach <c>n == 0</c>. No callback is (or may be)
    /// fired — the <c>GCHandle</c> is already freed.
    /// </remarks>
    [Fact]
    public void IncrementalAlterConfigs_AnEmptyMap_SettlesAtTheSubmitBoundary()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        Captured captured = new Captured();
        AlterConfigsResult result = admin.IncrementalAlterConfigs(
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>(),
            options: null,
            captured.RecordAlter);

        Assert.Equal(0, captured.Count);
        Assert.Empty(result.Values);
        Assert.True(result.All().IsCompleted);
        Assert.Null(result.All().Exception);

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(handle.IsClosed, "an n == 0 operation must have released the client at the submit boundary");
    }

    /// <summary>
    /// ⚠ <b>An undefined resource type and <see cref="ConfigResourceType.Unknown"/> name the
    /// same resource</b>, so <c>describeConfigs</c> submits it once, as <c>UNKNOWN</c>'s id
    /// (M15/P13.2, finding G2-1).
    /// </summary>
    /// <remarks>
    /// The ABI folds the undefined id to <c>UNKNOWN</c> and collapses duplicate resources,
    /// so it answers these two keys with <b>one</b> callback. Before G2-1 the binding sent
    /// both and armed two, so the operation never settled. <c>CaptureDescribe</c> fires the
    /// one callback and asserts the awaitable settled.
    /// </remarks>
    [Fact]
    public void DescribeConfigs_AnUndefinedTypeAndUnknown_SubmitOneResource()
    {
        Captured captured = CaptureDescribe(
            options: null,
            new ConfigResource((ConfigResourceType)64, "x"),
            new ConfigResource(ConfigResourceType.Unknown, "x"));

        Assert.Equal(1, captured.Count);
        Assert.Equal(new[] { 0 }, captured.ResourceTypes);
        Assert.Equal(new[] { "x" }, captured.ResourceNames);
    }

    /// <summary>A repeated resource is one entry, because Java's result is a map.</summary>
    [Fact]
    public void DescribeConfigs_DeduplicatesResources()
    {
        Captured captured = CaptureDescribe(
            options: null,
            s_topicResource,
            new ConfigResource(ConfigResourceType.Topic, "cfg-topic"),
            s_brokerResource);

        Assert.Equal(2, captured.Count);
        Assert.Equal(new[] { "cfg-topic", "0" }, captured.ResourceNames);
    }

    private static Captured CaptureDescribe(DescribeConfigsOptions? options, params ConfigResource[] resources)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        Captured captured = new Captured();
        DescribeConfigsResult result = admin.DescribeConfigs(
            resources.Length == 0 ? new[] { s_topicResource } : resources,
            options,
            captured.RecordDescribe);

        // Shape 4a: one callback per key, keyed by the composite (type id, name) pair.
        foreach (ConfigResource resource in result.Values.Keys)
        {
            using Utf8Marshal.PinnedUtf8String pinnedName = Utf8Marshal.Pin(resource.Name);
            AdminCallbacks.DescribeConfigs(
                (int)resource.Type, pinnedName.Pointer, IntPtr.Zero, CapturedError(), captured.UserData);
        }

        // Values[] hands back the bridge's own task, so this read is synchronous.
        foreach (KeyValuePair<ConfigResource, Task<Config>> entry in result.Values)
        {
            Assert.NotNull(entry.Value.Exception);
        }

        return captured;
    }

    private static Captured CaptureAlter(
        AlterConfigsOptions? options,
        params (ConfigResource Resource, AlterConfigOp[] Ops)[] rows)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> configs =
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>();
        if (rows.Length == 0)
        {
            configs[s_topicResource] = new[] { new AlterConfigOp(new ConfigEntry("k", "v"), AlterConfigOpType.Set) };
        }
        else
        {
            foreach ((ConfigResource resource, AlterConfigOp[] ops) in rows)
            {
                configs[resource] = ops;
            }
        }

        Captured captured = new Captured();
        AlterConfigsResult result = admin.IncrementalAlterConfigs(configs, options, captured.RecordAlter);

        // One callback per resource — a zero-op one included, since its sentinel row names
        // it (M15/P13.2, finding F1) — so every key is answered, and faulted, the same way.
        foreach (ConfigResource resource in configs.Keys)
        {
            FireAlter(resource, CapturedError(), captured.UserData);
        }

        foreach (KeyValuePair<ConfigResource, Task> entry in result.Values)
        {
            Assert.NotNull(entry.Value.Exception);
        }

        return captured;
    }

    /// <summary>
    /// Fires the <b>production</b> <c>incrementalAlterConfigs</c> trampoline for one
    /// resource, as native would: the resource's composite key and an owned (or null)
    /// error. The name is pinned only for the call, which is all the trampoline borrows it
    /// for.
    /// </summary>
    private static void FireAlter(ConfigResource resource, IntPtr error, IntPtr userData)
    {
        using Utf8Marshal.PinnedUtf8String name = Utf8Marshal.Pin(resource.Name);
        AdminCallbacks.IncrementalAlterConfigs((int)resource.Type, name.Pointer, error, userData);
    }

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

    /// <summary>
    /// Records the arguments a submit would have handed native. The two recording methods
    /// bind directly to the production submit delegates, so a signature change breaks the
    /// test at compile time rather than silently capturing the wrong slot.
    /// </summary>
    private sealed class Captured
    {
        internal int TimeoutMs { get; private set; }

        internal bool IncludeSynonyms { get; private set; }

        internal bool IncludeDocumentation { get; private set; }

        internal bool ValidateOnly { get; private set; }

        internal int Count { get; private set; }

        internal int[] ResourceTypes { get; private set; } = Array.Empty<int>();

        internal string?[] ResourceNames { get; private set; } = Array.Empty<string>();

        internal string?[] ConfigNames { get; private set; } = Array.Empty<string>();

        internal string?[] ConfigValues { get; private set; } = Array.Empty<string>();

        internal IntPtr[] ConfigNamePointers { get; private set; } = Array.Empty<IntPtr>();

        internal IntPtr[] ConfigValuePointers { get; private set; } = Array.Empty<IntPtr>();

        internal int[] OpTypes { get; private set; } = Array.Empty<int>();

        internal IntPtr UserData { get; private set; }

        internal void RecordDescribe(
            IntPtr admin,
            int[] resourceTypes,
            IntPtr[] resourceNames,
            int count,
            int timeoutMs,
            bool includeSynonyms,
            bool includeDocumentation,
            AdminCallbacks.DescribeConfigsCallback callback,
            IntPtr userData)
        {
            Count = count;
            ResourceTypes = Take(resourceTypes, count);
            ResourceNames = ReadStrings(resourceNames, count);
            TimeoutMs = timeoutMs;
            IncludeSynonyms = includeSynonyms;
            IncludeDocumentation = includeDocumentation;
            UserData = userData;
        }

        internal void RecordAlter(
            IntPtr admin,
            int[] resourceTypes,
            IntPtr[] resourceNames,
            IntPtr[] configNames,
            IntPtr[] configValues,
            int[] opTypes,
            int count,
            int timeoutMs,
            bool validateOnly,
            AdminCallbacks.IncrementalAlterConfigsCallback callback,
            IntPtr userData)
        {
            Count = count;
            ResourceTypes = Take(resourceTypes, count);
            ResourceNames = ReadStrings(resourceNames, count);
            ConfigNames = ReadStrings(configNames, count);
            ConfigNamePointers = TakePointers(configNames, count);
            ConfigValues = ReadStrings(configValues, count);
            ConfigValuePointers = TakePointers(configValues, count);
            OpTypes = Take(opTypes, count);
            TimeoutMs = timeoutMs;
            ValidateOnly = validateOnly;
            UserData = userData;
        }

        private static int[] Take(int[] source, int count)
        {
            int[] taken = new int[count];
            Array.Copy(source, taken, count);
            return taken;
        }

        private static IntPtr[] TakePointers(IntPtr[] source, int count)
        {
            IntPtr[] taken = new IntPtr[count];
            Array.Copy(source, taken, count);
            return taken;
        }

        /// <summary>
        /// Reads the pinned UTF-8 strings back <b>while the submit is still on the
        /// stack</b> — the pins are released in the submit's <c>finally</c>, so a later
        /// read would be a use-after-free.
        /// </summary>
        private static string?[] ReadStrings(IntPtr[] source, int count)
        {
            string?[] values = new string?[count];
            for (int index = 0; index < count; index++)
            {
                values[index] = Utf8Marshal.PtrToString(source[index]);
            }

            return values;
        }
    }
}
