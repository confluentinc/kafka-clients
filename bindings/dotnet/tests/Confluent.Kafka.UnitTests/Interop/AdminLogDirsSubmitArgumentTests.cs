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
/// What M15/P3 Stage 3's options and inputs actually become at the P/Invoke — the log-dir
/// twin of <see cref="AdminConfigsSubmitArgumentTests"/>, discharging the standing
/// obligation (PLAN §9.1 item 5 / decision D19) that every admin RPC's options are asserted
/// at the submit seam.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>The Rust <c>MockAdminClient</c> ignores <c>_options</c> for all three RPCs</b>
/// (<c>fn describe_log_dirs(&amp;self, …, _options: DescribeLogDirsOptions)</c> and its two
/// siblings), so no behavioural test can observe a timeout. It also cannot observe the
/// <b>shape</b> of the four- and three-array inputs — the column alignment between topics,
/// partitions, broker ids and log directories is decided entirely on the way in, and a
/// transposition there would send a well-formed request about the wrong replicas.
/// </para>
/// <para>
/// ⚠ <b>The silent-skip inputs are the sharpest cases here.</b> The header says an entry
/// with a NULL topic or NULL log dir "is skipped" — with no error — so the caller would be
/// left holding an awaitable for a replica the broker was never asked about. Both are
/// rejected at the C# boundary (ffi §B5) and asserted below with their parameter names.
/// </para>
/// </remarks>
public sealed class AdminLogDirsSubmitArgumentTests
{
    private static readonly TopicPartitionReplica s_replica = new TopicPartitionReplica("logdir-topic", 3, 7);

    private static readonly TopicPartitionReplica s_otherReplica =
        new TopicPartitionReplica("other-topic", 11, 2);

    /// <summary>One arbitrary move, so the timeout cases carry a non-empty request.</summary>
    private static readonly (TopicPartitionReplica Replica, string LogDir) s_move = (s_replica, "/data");

    /// <summary>
    /// A <see langword="null"/> timeout must become a <b>negative</b> <c>timeout_ms</c>
    /// ("unset"), never <c>0</c>; an explicit one is forwarded verbatim, and <c>0</c> stays
    /// <c>0</c>.
    /// </summary>
    [Fact]
    public void DescribeLogDirs_TimeoutMapping()
    {
        Assert.True(CaptureDescribeLogDirs(null, 0).TimeoutMs < 0);
        Assert.True(CaptureDescribeLogDirs(new DescribeLogDirsOptions(), 0).TimeoutMs < 0);
        Assert.Equal(0, CaptureDescribeLogDirs(new DescribeLogDirsOptions { TimeoutMs = 0 }, 0).TimeoutMs);
        Assert.Equal(6_161, CaptureDescribeLogDirs(new DescribeLogDirsOptions { TimeoutMs = 6_161 }, 0).TimeoutMs);
    }

    /// <inheritdoc cref="DescribeLogDirs_TimeoutMapping"/>
    [Fact]
    public void AlterReplicaLogDirs_TimeoutMapping()
    {
        Assert.True(CaptureAlter(null, s_move).TimeoutMs < 0);
        Assert.True(CaptureAlter(new AlterReplicaLogDirsOptions(), s_move).TimeoutMs < 0);
        Assert.Equal(0, CaptureAlter(new AlterReplicaLogDirsOptions { TimeoutMs = 0 }, s_move).TimeoutMs);
        Assert.Equal(7_272, CaptureAlter(new AlterReplicaLogDirsOptions { TimeoutMs = 7_272 }, s_move).TimeoutMs);
    }

    /// <inheritdoc cref="DescribeLogDirs_TimeoutMapping"/>
    [Fact]
    public void DescribeReplicaLogDirs_TimeoutMapping()
    {
        Assert.True(CaptureDescribeReplicas(null, s_replica).TimeoutMs < 0);
        Assert.True(CaptureDescribeReplicas(new DescribeReplicaLogDirsOptions(), s_replica).TimeoutMs < 0);
        Assert.Equal(0, CaptureDescribeReplicas(new DescribeReplicaLogDirsOptions { TimeoutMs = 0 }, s_replica).TimeoutMs);
        Assert.Equal(
            8_383,
            CaptureDescribeReplicas(new DescribeReplicaLogDirsOptions { TimeoutMs = 8_383 }, s_replica).TimeoutMs);
    }

    /// <summary>
    /// A negative timeout is a precondition failure, rejected before any pin or P/Invoke
    /// (ffi §B5), naming the options type so the caller can tell which argument was wrong.
    /// </summary>
    [Fact]
    public void ANegativeTimeout_IsRejectedBeforeTheCall()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.DescribeLogDirs(new[] { 0 }, new DescribeLogDirsOptions { TimeoutMs = -1 }));
        Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.AlterReplicaLogDirs(
                new Dictionary<TopicPartitionReplica, string> { [s_replica] = "/data" },
                new AlterReplicaLogDirsOptions { TimeoutMs = -1 }));
        Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.DescribeReplicaLogDirs(
                new[] { s_replica }, new DescribeReplicaLogDirsOptions { TimeoutMs = -1 }));
    }

    /// <summary>
    /// ⚠ <b>The broker ids reach the submit as a <b>bare scalar</b> array</b> — the first
    /// such input in M15, and the reason <c>describeLogDirs</c> needs no string pinning at
    /// all.
    /// </summary>
    [Fact]
    public void DescribeLogDirs_BrokersReachTheSubmitAsAScalarArray()
    {
        Captured captured = CaptureDescribeLogDirs(null, 0, 4, 2);

        Assert.Equal(3, captured.Count);
        Assert.Equal(new[] { 0, 4, 2 }, captured.Brokers);
    }

    /// <summary>A repeated broker is one entry, because Java's result is a map.</summary>
    [Fact]
    public void DescribeLogDirs_DeduplicatesBrokers_PreservingRequestOrder()
    {
        Captured captured = CaptureDescribeLogDirs(null, 5, 1, 5, 1, 9);

        Assert.Equal(3, captured.Count);
        Assert.Equal(new[] { 5, 1, 9 }, captured.Brokers);
    }

    /// <summary>
    /// ⚠ <b>The four <c>alterReplicaLogDirs</c> arrays are COLUMN-ALIGNED</b>: row
    /// <c>i</c> must carry the same replica's topic, partition, broker id and destination.
    /// Two replicas differing in every field make a transposition impossible to miss.
    /// </summary>
    [Fact]
    public void AlterReplicaLogDirs_TheFourArraysAreColumnAligned()
    {
        Captured captured = CaptureAlter(
            null,
            (s_replica, "/data/one"),
            (s_otherReplica, "/data/two"));

        Assert.Equal(2, captured.Count);
        Assert.Equal(new[] { "logdir-topic", "other-topic" }, captured.Topics);
        Assert.Equal(new[] { 3, 11 }, captured.Partitions);
        Assert.Equal(new[] { 7, 2 }, captured.BrokerIds);
        Assert.Equal(new[] { "/data/one", "/data/two" }, captured.LogDirs);
    }

    /// <summary>
    /// The three <c>describeReplicaLogDirs</c> arrays are column-aligned in the same way,
    /// with no fourth column — Java's input is a collection, not a map.
    /// </summary>
    [Fact]
    public void DescribeReplicaLogDirs_TheThreeArraysAreColumnAligned()
    {
        Captured captured = CaptureDescribeReplicas(null, s_replica, s_otherReplica);

        Assert.Equal(2, captured.Count);
        Assert.Equal(new[] { "logdir-topic", "other-topic" }, captured.Topics);
        Assert.Equal(new[] { 3, 11 }, captured.Partitions);
        Assert.Equal(new[] { 7, 2 }, captured.BrokerIds);
    }

    /// <summary>A repeated replica is one entry, because Java's result is a map.</summary>
    [Fact]
    public void DescribeReplicaLogDirs_DeduplicatesReplicas_ByValueEquality()
    {
        Captured captured = CaptureDescribeReplicas(
            null,
            s_replica,
            new TopicPartitionReplica("logdir-topic", 3, 7),
            s_otherReplica);

        // ⚠ The duplicate is a DIFFERENT instance with equal fields, so this also pins that
        // the de-duplication runs through TopicPartitionReplica's value equality rather than
        // reference identity.
        Assert.Equal(2, captured.Count);
        Assert.Equal(new[] { "logdir-topic", "other-topic" }, captured.Topics);
    }

    /// <summary>
    /// ⚠⚠ <b>A <see langword="null"/> log directory is rejected here, because the ABI
    /// SILENTLY SKIPS that row.</b> Left to the ABI the caller would hold an awaitable for a
    /// replica the broker was never asked about, faulting later with a message about a
    /// missing result rather than about the argument that caused it.
    /// </summary>
    [Fact]
    public void AlterReplicaLogDirs_ANullLogDirectory_IsRejectedBeforeTheCall()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        ArgumentException failure = Assert.Throws<ArgumentException>(
            () => admin.AlterReplicaLogDirs(
                new Dictionary<TopicPartitionReplica, string?> { [s_replica] = null }!,
                options: null));

        Assert.Equal("replicaAssignment", failure.ParamName);

        // The replica is named, so the caller can find the offending entry in a large map.
        Assert.Contains("logdir-topic-3-7", failure.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// ⚠ <b>The topic is guarded at the <see cref="TopicPartitionReplica"/> constructor,
    /// not restated at the submit</b> — Java's own <c>requireNonNull</c> sits there
    /// (<c>TopicPartitionReplica.java:34</c>), so the ABI's "an entry with a NULL topic is
    /// skipped" cannot be reached through this surface at all.
    /// </summary>
    [Fact]
    public void ANullTopic_CannotReachEitherSubmit()
    {
        ArgumentNullException failure =
            Assert.Throws<ArgumentNullException>(() => new TopicPartitionReplica(null!, 0, 0));
        Assert.Equal("topic", failure.ParamName);
    }

    /// <summary>
    /// A null collection or map is an <see cref="ArgumentNullException"/> naming its
    /// parameter, and a null element inside one is an <see cref="ArgumentException"/> — the
    /// two are different mistakes and are reported differently.
    /// </summary>
    [Fact]
    public void NullInputs_AreRejectedWithTheirParameterNames()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        Assert.Equal(
            "brokers",
            Assert.Throws<ArgumentNullException>(() => admin.DescribeLogDirs(null!, options: null)).ParamName);
        Assert.Equal(
            "replicaAssignment",
            Assert.Throws<ArgumentNullException>(() => admin.AlterReplicaLogDirs(null!, options: null)).ParamName);
        Assert.Equal(
            "replicas",
            Assert.Throws<ArgumentNullException>(
                () => admin.DescribeReplicaLogDirs(null!, options: null)).ParamName);

        ArgumentException nullElement = Assert.Throws<ArgumentException>(
            () => admin.DescribeReplicaLogDirs(new TopicPartitionReplica?[] { null }!, options: null));
        Assert.Equal("replicas", nullElement.ParamName);
    }

    /// <summary>
    /// An empty input submits an empty request rather than throwing — Java's
    /// <c>describeLogDirs(emptyList())</c> is legal and yields an empty result.
    /// </summary>
    [Fact]
    public void EmptyInputs_SubmitAnEmptyRequest()
    {
        Assert.Equal(0, CaptureDescribeLogDirs(null, Array.Empty<int>()).Count);
        Assert.Equal(0, CaptureDescribeReplicas(null, Array.Empty<TopicPartitionReplica>()).Count);
    }

    private static Captured CaptureDescribeLogDirs(DescribeLogDirsOptions? options, params int[] brokers)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        Captured captured = new Captured();
        DescribeLogDirsResult result = admin.DescribeLogDirs(brokers, options, captured.RecordDescribeLogDirs);

        AdminCallbacks.DescribeLogDirs(IntPtr.Zero, CapturedError(), captured.UserData);
        AssertEveryAwaitableFaulted(result.Descriptions);
        return captured;
    }

    private static Captured CaptureAlter(
        AlterReplicaLogDirsOptions? options,
        params (TopicPartitionReplica Replica, string LogDir)[] moves)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        Dictionary<TopicPartitionReplica, string> assignment = new Dictionary<TopicPartitionReplica, string>();
        foreach ((TopicPartitionReplica replica, string logDir) in moves)
        {
            assignment[replica] = logDir;
        }

        Captured captured = new Captured();
        AlterReplicaLogDirsResult result =
            admin.AlterReplicaLogDirs(assignment, options, captured.RecordAlterReplicaLogDirs);

        AdminCallbacks.AlterReplicaLogDirs(IntPtr.Zero, CapturedError(), captured.UserData);
        AssertEveryAwaitableFaulted(result.Values);
        return captured;
    }

    private static Captured CaptureDescribeReplicas(
        DescribeReplicaLogDirsOptions? options, params TopicPartitionReplica[] replicas)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        Captured captured = new Captured();
        DescribeReplicaLogDirsResult result =
            admin.DescribeReplicaLogDirs(replicas, options, captured.RecordDescribeReplicaLogDirs);

        AdminCallbacks.DescribeReplicaLogDirs(IntPtr.Zero, CapturedError(), captured.UserData);
        AssertEveryAwaitableFaulted(result.Values);
        return captured;
    }

    /// <summary>
    /// Observes every awaitable the injected submit left behind, so the operation's
    /// <c>GCHandle</c> and span-the-op reference are released before the client is disposed
    /// and no faulted task goes unobserved.
    /// </summary>
    private static void AssertEveryAwaitableFaulted<TKey, TTask>(IReadOnlyDictionary<TKey, TTask> tasks)
        where TTask : Task
    {
        foreach (KeyValuePair<TKey, TTask> entry in tasks)
        {
            Assert.NotNull(entry.Value.Exception);
        }
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
    /// Records the arguments a submit would have handed native. Each recording method binds
    /// directly to the production submit delegate, so a signature change breaks the test at
    /// compile time rather than silently capturing the wrong slot.
    /// </summary>
    private sealed class Captured
    {
        internal int TimeoutMs { get; private set; }

        internal int Count { get; private set; }

        internal int[] Brokers { get; private set; } = Array.Empty<int>();

        internal string?[] Topics { get; private set; } = Array.Empty<string>();

        internal int[] Partitions { get; private set; } = Array.Empty<int>();

        internal int[] BrokerIds { get; private set; } = Array.Empty<int>();

        internal string?[] LogDirs { get; private set; } = Array.Empty<string>();

        internal IntPtr UserData { get; private set; }

        internal void RecordDescribeLogDirs(
            IntPtr admin,
            int[] brokers,
            int count,
            int timeoutMs,
            AdminCallbacks.DescribeLogDirsCallback callback,
            IntPtr userData)
        {
            Count = count;
            Brokers = Take(brokers, count);
            TimeoutMs = timeoutMs;
            UserData = userData;
        }

        internal void RecordAlterReplicaLogDirs(
            IntPtr admin,
            IntPtr[] topics,
            int[] partitions,
            int[] brokerIds,
            IntPtr[] logDirs,
            int count,
            int timeoutMs,
            AdminCallbacks.AlterReplicaLogDirsCallback callback,
            IntPtr userData)
        {
            Count = count;
            Topics = ReadStrings(topics, count);
            Partitions = Take(partitions, count);
            BrokerIds = Take(brokerIds, count);
            LogDirs = ReadStrings(logDirs, count);
            TimeoutMs = timeoutMs;
            UserData = userData;
        }

        internal void RecordDescribeReplicaLogDirs(
            IntPtr admin,
            IntPtr[] topics,
            int[] partitions,
            int[] brokerIds,
            int count,
            int timeoutMs,
            AdminCallbacks.DescribeReplicaLogDirsCallback callback,
            IntPtr userData)
        {
            Count = count;
            Topics = ReadStrings(topics, count);
            Partitions = Take(partitions, count);
            BrokerIds = Take(brokerIds, count);
            TimeoutMs = timeoutMs;
            UserData = userData;
        }

        private static int[] Take(int[] source, int count)
        {
            int[] taken = new int[count];
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
