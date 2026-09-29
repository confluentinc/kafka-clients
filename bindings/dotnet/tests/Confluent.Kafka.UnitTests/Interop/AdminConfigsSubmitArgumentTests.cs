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
    /// A resource with an <b>empty</b> operation collection contributes <b>no row</b> to the
    /// request, while still getting a per-key awaitable — Java keys the result on the map.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>This asserts the request SHAPE.</b> An earlier version completed the operation
    /// through the <em>submit-failure</em> path and then asserted the awaitable had faulted
    /// — which every awaitable did on that path, so it passed while the outcome was in fact
    /// wrong (M15/P3 round 3, finding 69.6). The end-to-end outcome has its own success-path
    /// test, <c>PublicAdminConfigsTests.IncrementalAlterConfigs_AResourceWithNoOps_CompletesSuccessfully</c>,
    /// driven through the mock with nothing injected.
    /// </para>
    /// <para>
    /// ⚠ <b>M15/P9 CP6 also makes this the <c>n == 0</c> submit-boundary test.</b> With no
    /// row there is no named resource, so the whole countdown is the submit token and the
    /// operation settles inside the injected submit's caller — the key resolving without any
    /// callback at all is now asserted here, and no completion is (or may be) fired.
    /// </para>
    /// </remarks>
    [Fact]
    public void IncrementalAlterConfigs_AResourceWithNoOps_ContributesNoRow()
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

        Assert.Equal(0, captured.Count);
        Assert.True(result.Values.ContainsKey(s_topicResource));

        // ⚠ M15/P9 CP6: no row means no NAMED resource, so n == 0 and the submit token is
        // the whole countdown — the operation settles, frees its GCHandle and releases its
        // span-the-op reference before this call returns. Firing a callback here would be a
        // use-after-free, and the key is already resolved without one.
        Assert.True(result.Values[s_topicResource].IsCompleted);
        Assert.Null(result.Values[s_topicResource].Exception);
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
        foreach (KeyValuePair<ConfigResource, System.Threading.Tasks.Task<Config>> entry in result.Values)
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

        // One callback per NAMED resource (M15/P9 CP6): a zero-op resource contributes no
        // row, so the per-key ABI never names it and it is completed locally instead.
        foreach (KeyValuePair<ConfigResource, IReadOnlyCollection<AlterConfigOp>> entry in configs)
        {
            if (entry.Value.Count == 0)
            {
                continue;
            }

            using Utf8Marshal.PinnedUtf8String name = Utf8Marshal.Pin(entry.Key.Name);
            AdminCallbacks.IncrementalAlterConfigs(
                (int)entry.Key.Type, name.Pointer, CapturedError(), captured.UserData);
        }

        foreach (KeyValuePair<ConfigResource, System.Threading.Tasks.Task> entry in result.Values)
        {
            if (configs[entry.Key].Count == 0)
            {
                Assert.True(entry.Value.IsCompleted);
                Assert.Null(entry.Value.Exception);
            }
            else
            {
                Assert.NotNull(entry.Value.Exception);
            }
        }

        return captured;
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
