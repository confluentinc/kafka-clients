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
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Drives M15/P3 Stage 3's value trees over <b>real</b> native result roots — the
/// <c>describeLogDirs</c> tree (result → <c>LogDirDescriptionMap_t</c> →
/// <c>LogDirDescription_t</c> → six flattened replica accessors) and the flatter
/// <c>describeReplicaLogDirs</c> one (result → <c>ReplicaLogDirInfo_t</c>) — that a single
/// <c>*Result_destroy</c> invalidates wholesale.
/// </summary>
/// <remarks>
/// <para>
/// As in Stage 2, the production trampoline destroys the root in its <c>finally</c>, so
/// these tests submit the <c>*_async</c> entry point directly with a capturing callback,
/// keep the root alive, walk it with <em>production's</em> marshaller, destroy the root,
/// and only then read.
/// </para>
/// <para>
/// ⚠⚠ <b>Since M15/P9 the per-key <c>KafkaError</c> is OWNED and the nested one is still
/// borrowed.</b> The per-key one arrives as its own callback argument and is destroyed by
/// whoever reads it; the nested <c>LogDirDescription_error</c> is read by
/// <see cref="LogDirMarshal"/> inside the value tree, is <c>const</c> — borrowed, never
/// destroyed — and dies with the value root. Their <em>reachability</em> differs sharply
/// against the mock, which is measured rather than assumed and recorded on
/// <see cref="TheNestedDirectoryError_IsAFieldOnASuccessfulDescription"/>.
/// </para>
/// </remarks>
public sealed class AdminLogDirsMarshalTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "logdir-marshal-topic";

    /// <summary>
    /// The mock seeds every broker with Java's <c>MockAdminClient.DEFAULT_LOG_DIRS</c>
    /// (<c>mock_admin_client.rs:95</c>), so this path is the one a created topic lands in.
    /// </summary>
    private const string DefaultLogDir = "/tmp/kafka-logs";

    /// <summary>
    /// Rooted for the process lifetime, as every callback handed to native must be
    /// (ffi §B6 keep-alive) — even a test's.
    /// </summary>
    private static readonly AdminCallbacks.DescribeLogDirsCallback s_captureLogDirs = OnCaptureLogDirs;

    /// <inheritdoc cref="s_captureLogDirs"/>
    private static readonly AdminCallbacks.DescribeReplicaLogDirsCallback s_captureReplicas =
        OnCaptureReplica;

    /// <inheritdoc cref="s_captureLogDirs"/>
    private static readonly AdminCallbacks.AlterReplicaLogDirsCallback s_captureAlterReplicas =
        OnCaptureAlterReplica;

    /// <summary>
    /// ⚠ <b>The test that catches a lazily-held borrowed pointer in the deepest Stage-3
    /// tree.</b> A copy-out defect here is invisible to any test that reads the tree while
    /// the root is still alive, which is why the read order below is inverted: the whole
    /// per-broker map — paths, descriptions,
    /// volume sizes and every replica — is read <em>after</em>
    /// <c>LogDirDescriptionMap_destroy</c> has invalidated every pointer it came from.
    /// </summary>
    [Fact]
    public async Task TheLogDirTree_IsCopiedOut_AndSurvivesTheRootsDestroy()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic(Topic, 2, 1) }, options: null).All(), s_deadline);

        LogDirsCaptureState capture = SubmitLogDirsAndCapture(admin, 0);

        IReadOnlyDictionary<string, LogDirDescription> directories;
        try
        {
            // The key crossed as a plain int32_t broker id, and there is no per-key error,
            // so the value is present.
            Assert.Equal(0, capture.Broker);
            Assert.Equal(IntPtr.Zero, capture.Error);

            directories = AdminCallbacks.LogDirDescriptionsPerKeyValue(capture.Value);
        }
        finally
        {
            NativeMethods.LogDirDescriptionMapDestroy(capture.Value);
        }

        // The value is gone. Everything below reads only owned managed state.
        LogDirDescription description = Assert.Contains(DefaultLogDir, directories);
        Assert.Null(description.Error);

        // ⚠ The mock reports UNKNOWN_VOLUME_BYTES for both, which is Java's empty
        // OptionalLong — so both must be null here, NOT -1.
        Assert.Null(description.TotalBytes);
        Assert.Null(description.UsableBytes);

        Assert.Equal(2, description.ReplicaInfos.Count);
        foreach (int partition in new[] { 0, 1 })
        {
            ReplicaInfo replica = Assert.Contains(new TopicPartition(Topic, partition), description.ReplicaInfos);
            Assert.Equal(0, replica.Size);
            Assert.Equal(0, replica.OffsetLag);
            Assert.False(replica.IsFuture);
        }
    }

    /// <summary>
    /// The <c>describeReplicaLogDirs</c> value is copied out before it dies too, with the
    /// two genuinely-nullable directory strings preserved as <see langword="null"/> rather
    /// than <c>""</c>.
    /// </summary>
    [Fact]
    public async Task TheReplicaInfo_IsCopiedOut_AndSurvivesTheRootsDestroy()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }, options: null).All(), s_deadline);

        TopicPartitionReplica replica = new TopicPartitionReplica(Topic, 0, 0);
        ReplicaCaptureState capture = SubmitReplicaLogDirsAndCapture(admin, replica);

        DescribeReplicaLogDirsResult.ReplicaLogDirInfo info;
        try
        {
            // The 3-part key crossed as three scalar arguments, copied out in the callback
            // because the topic pointer is borrowed for the call only. No per-key error, so
            // the value is present.
            Assert.Equal(replica, new TopicPartitionReplica(capture.Topic!, capture.Partition, capture.BrokerId));
            Assert.Equal(IntPtr.Zero, capture.Error);

            info = AdminCallbacks.ReplicaLogDirInfoPerKeyValue(capture.Value);
        }
        finally
        {
            NativeMethods.ReplicaLogDirInfoDestroy(capture.Value);
        }

        Assert.Equal(DefaultLogDir, info.GetCurrentReplicaLogDir());
        Assert.Equal(0, info.GetCurrentReplicaOffsetLag());

        // ⚠ A replica at rest is not being moved, so Java's getFutureReplicaLogDir() is
        // null — the ordinary case, and it must not become "".
        Assert.Null(info.GetFutureReplicaLogDir());
    }

    /// <summary>
    /// ⚠⚠ <b>M15/P9 CP6 inverted this claim: the per-replica error is now OWNED.</b> The
    /// per-key ABI hands each replica its own <c>kafka_common_KafkaError_t</c> and no result
    /// root, so the callback is the only free site and
    /// <see cref="KafkaException.FromHandle"/> — which destroys — is the only correct reader.
    /// </summary>
    /// <remarks>
    /// Before CP6 this same error was a borrowed <c>const</c> field inside an aggregate
    /// <c>AlterReplicaLogDirsResult_t</c>, read twice here to prove reading did not consume
    /// it, with the root destroy as the single free. There is no root any more: reading the
    /// handle twice would be a use-after-free, and a <c>FromBorrowedHandle</c> read would
    /// leak it.
    /// <para>
    /// ⚠ <b><c>alterReplicaLogDirs</c> is the RPC used here because the mock completes its
    /// per-replica futures <em>exceptionally</em></b> (<c>mock_admin_client.rs:1300-1305</c>
    /// and <c>:1327-1331</c>), which is what puts a non-null error into the callback at all.
    /// Where a mock completes every key successfully the callback still fires, but with a
    /// null pointer — so an injection routed through such an RPC would measure nothing.
    /// </para>
    /// </remarks>
    [Fact]
    public async Task PerReplicaError_IsOwned_AndIsConsumedByTheCallback()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }, options: null).All(), s_deadline);

        TopicPartitionReplica replica = new TopicPartitionReplica(Topic, 0, 0);
        ReplicaCaptureState capture =
            SubmitAlterReplicaLogDirsAndCapture(admin, replica, "/not-a-configured-dir");

        // The key crossed as its own three scalars, not as an index into a root.
        Assert.Equal(Topic, capture.Topic);
        Assert.Equal(0, capture.Partition);
        Assert.Equal(0, capture.BrokerId);

        Assert.NotEqual(IntPtr.Zero, capture.Error);

        // FromHandle destroys the handle in its own finally — the single free.
        KafkaException failure = Assert.IsType<KafkaException>(KafkaException.FromHandle(capture.Error));

        // The mock's exact message (mock_admin_client.rs:1304).
        Assert.Equal("Log directory /not-a-configured-dir is offline", failure.Message);
    }

    /// <summary>
    /// ⚠⚠ <b>The nested <see cref="LogDirDescription.Error"/> is a FIELD on a description
    /// the broker returned successfully — it does NOT fault the broker's awaitable.</b> The
    /// header says so outright, and the two errors mean opposite things.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>MEASURED COVERAGE GAP, recorded rather than papered over.</b> No broker-free
    /// path produces a <em>non-null</em> nested error: the Rust mock builds every
    /// description with <c>LogDirDescription::new(None, …)</c> and then carries
    /// <c>existing.error().cloned()</c> forward, which is <c>None</c> on every iteration
    /// (<c>mock_admin_client.rs:1251-1262</c>), and a <c>LogDirDescription_t</c> can only be
    /// reached through a <c>describeLogDirs</c> result root — the ABI exposes no constructor
    /// for one. So the nested read in
    /// <see cref="LogDirMarshal.CopyOutDescription"/> always sees a NULL pointer here, and a
    /// double-free injection <em>at that site</em> is a no-op: it cannot be distinguished
    /// from a correct implementation by any unit test.
    /// </para>
    /// <para>
    /// ⚠ <b>That was MEASURED, in three runs, because "the injection passed" is otherwise
    /// indistinguishable from "the injection was never reached".</b> (1) A destroy injected
    /// immediately after the accessor left the suite <b>green</b>. (2) The same injection at
    /// the per-key site — <see cref="PerReplicaError_IsBorrowed_AndSurvivesTheWalk"/> —
    /// <b>aborted the test host</b>, so the technique does detect a double free. (3) The
    /// decisive one: a control-positive throwing <em>when the pointer is null</em> at this
    /// very site turned exactly the two <c>describeLogDirs</c> tests <b>red</b>, which is
    /// what proves the site is entered on every walk and that the pointer is null every time
    /// it is. Without (3), (1)'s green would be evidence of nothing.
    /// </para>
    /// <para>
    /// What is therefore asserted here is the <b>shape</b>: a description carrying a
    /// non-null error is an ordinary value, constructible and readable, with nothing about
    /// it that a walker would treat as a failure. The behavioural half — that such a
    /// description arrives on a <em>succeeding</em> task — is unreachable without a broker
    /// and is stated, not faked.
    /// </para>
    /// </remarks>
    [Fact]
    public void TheNestedDirectoryError_IsAFieldOnASuccessfulDescription()
    {
        KafkaException offline = new KafkaException("The log directory is offline.");
        LogDirDescription description = new LogDirDescription(
            offline,
            new Dictionary<TopicPartition, ReplicaInfo>(),
            totalBytes: null,
            usableBytes: null);

        Assert.Same(offline, description.Error);
        Assert.Empty(description.ReplicaInfos);

        // The rendering carries the message rather than swallowing it, so a directory-level
        // failure is visible in diagnostics even though it did not fault anything.
        Assert.Contains("The log directory is offline.", description.ToString(), StringComparison.Ordinal);
    }

    /// <summary>
    /// ⚠ <b><c>-1</c> IS a sentinel for the volume sizes — and <c>0</c> is NOT.</b> Java's
    /// constructor maps <c>UNKNOWN_VOLUME_BYTES</c> to an empty <c>OptionalLong</c>
    /// (<c>LogDirDescription.java:49</c>); every other value, <c>0</c> included, is a real
    /// size.
    /// </summary>
    /// <remarks>
    /// ⚠ Asserted against the mapping directly, because the mock reports <c>-1</c> for both
    /// sizes on every description it builds — it never supplies a real one, so a
    /// result-driven test can only ever exercise the sentinel arm.
    /// <para>
    /// ⚠ <b>This is the opposite convention to the sibling in the same stage</b>:
    /// <c>ReplicaLogDirInfo.GetFutureReplicaOffsetLag()</c>'s <c>-1</c> is a
    /// <em>value</em> Java returns, not an absence marker. Two conventions, one stage — the
    /// header and Java's javadoc decide per accessor.
    /// </para>
    /// </remarks>
    [Fact]
    public void VolumeBytes_MapsOnlyTheSentinel_AndKeepsZero()
    {
        // The sentinel — Java's empty OptionalLong.
        Assert.Null(LogDirMarshal.VolumeBytes(-1L));

        // ⚠ Zero is a real size and must survive. A "negative or falsy means absent" test
        // would pass the case above and silently destroy this one.
        Assert.Equal(0L, LogDirMarshal.VolumeBytes(0L));

        Assert.Equal(1L, LogDirMarshal.VolumeBytes(1L));
        Assert.Equal(1_099_511_627_776L, LogDirMarshal.VolumeBytes(1_099_511_627_776L));
        Assert.Equal(long.MaxValue, LogDirMarshal.VolumeBytes(long.MaxValue));

        // ⚠ Only -1 is the sentinel: another negative is not "absent", it is that value.
        // The ABI documents exactly one, so mapping a range would invent a contract.
        Assert.Equal(-2L, LogDirMarshal.VolumeBytes(-2L));
        Assert.Equal(long.MinValue, LogDirMarshal.VolumeBytes(long.MinValue));
    }

    /// <summary>
    /// ⚠ <b>Nothing native-backed survives on the copied-out types</b> — no
    /// <see cref="IntPtr"/>, no <see cref="SafeHandle"/>, anywhere in the Stage-3 value
    /// tree.
    /// </summary>
    /// <remarks>
    /// The <b>structural</b> half of the copy-out guarantee. It covers what the behavioural
    /// tests cannot reach — a description carrying a nested error, and a
    /// <see cref="ReplicaInfo"/> the mock never varies — because a field holding a borrowed
    /// pointer is caught here regardless of whether any test can reach it.
    /// </remarks>
    [Fact]
    public void TheCopiedOutTypes_HoldNoNativeState()
    {
        foreach (Type type in new[]
                 {
                     typeof(LogDirDescription),
                     typeof(ReplicaInfo),
                     typeof(DescribeReplicaLogDirsResult.ReplicaLogDirInfo),
                     typeof(TopicPartitionReplica),
                 })
        {
            FieldInfo[] fields = type.GetFields(
                BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static);

            Assert.DoesNotContain(fields, field => field.FieldType == typeof(IntPtr));
            Assert.DoesNotContain(fields, field => typeof(SafeHandle).IsAssignableFrom(field.FieldType));
            Assert.DoesNotContain(fields, field => field.FieldType == typeof(UIntPtr));
        }
    }

    private static LogDirsCaptureState SubmitLogDirsAndCapture(NativeAdminClient admin, params int[] brokers)
    {
        LogDirsCaptureState capture = new LogDirsCaptureState();
        GCHandle gcHandle = GCHandle.Alloc(capture, GCHandleType.Normal);
        try
        {
            NativeMethods.AdminClientDescribeLogDirsAsync(
                admin.Handle.DangerousGetHandle(),
                brokers,
                brokers.Length,
                -1,
                s_captureLogDirs,
                GCHandle.ToIntPtr(gcHandle));

            Assert.True(capture.Done.Wait(s_deadline), "the describeLogDirs callback never fired");
        }
        finally
        {
            gcHandle.Free();
        }

        Assert.NotEqual(IntPtr.Zero, capture.Value);
        return capture;
    }

    private static ReplicaCaptureState SubmitReplicaLogDirsAndCapture(
        NativeAdminClient admin, params TopicPartitionReplica[] replicas)
    {
        ReplicaCaptureState capture = new ReplicaCaptureState();
        GCHandle gcHandle = GCHandle.Alloc(capture, GCHandleType.Normal);
        List<Utf8Marshal.PinnedUtf8String> pinned = new List<Utf8Marshal.PinnedUtf8String>(replicas.Length);
        try
        {
            IntPtr[] topics = new IntPtr[replicas.Length];
            int[] partitions = new int[replicas.Length];
            int[] brokerIds = new int[replicas.Length];
            for (int index = 0; index < replicas.Length; index++)
            {
                Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(replicas[index].Topic);
                pinned.Add(topic);
                topics[index] = topic.Pointer;
                partitions[index] = replicas[index].Partition;
                brokerIds[index] = replicas[index].BrokerId;
            }

            NativeMethods.AdminClientDescribeReplicaLogDirsAsync(
                admin.Handle.DangerousGetHandle(),
                topics,
                partitions,
                brokerIds,
                replicas.Length,
                -1,
                s_captureReplicas,
                GCHandle.ToIntPtr(gcHandle));

            Assert.True(capture.Done.Wait(s_deadline), "the describeReplicaLogDirs callback never fired");
        }
        finally
        {
            foreach (Utf8Marshal.PinnedUtf8String topic in pinned)
            {
                topic.Dispose();
            }

            gcHandle.Free();
        }

        Assert.NotEqual(IntPtr.Zero, capture.Value);
        return capture;
    }

    /// <summary>
    /// Submits one replica move through the <b>real</b> entry point and hands the caller what
    /// that replica's own callback delivered, so the owned per-replica error comes from
    /// native rather than from a hand-built handle.
    /// </summary>
    private static ReplicaCaptureState SubmitAlterReplicaLogDirsAndCapture(
        NativeAdminClient admin, TopicPartitionReplica replica, string logDir)
    {
        ReplicaCaptureState capture = new ReplicaCaptureState();
        GCHandle gcHandle = GCHandle.Alloc(capture, GCHandleType.Normal);
        try
        {
            using Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(replica.Topic);
            using Utf8Marshal.PinnedUtf8String directory = Utf8Marshal.Pin(logDir);

            NativeMethods.AdminClientAlterReplicaLogDirsAsync(
                admin.Handle.DangerousGetHandle(),
                new[] { topic.Pointer },
                new[] { replica.Partition },
                new[] { replica.BrokerId },
                new[] { directory.Pointer },
                1,
                -1,
                s_captureAlterReplicas,
                GCHandle.ToIntPtr(gcHandle));

            Assert.True(capture.Done.Wait(s_deadline), "the alterReplicaLogDirs callback never fired");
        }
        finally
        {
            gcHandle.Free();
        }

        return capture;
    }

    /// <summary>
    /// The shape-4a twin of <see cref="OnCaptureAlterReplica"/>: <c>describeLogDirs</c> delivers one
    /// callback per broker id, carrying that key's own <b>owned</b> value and error.
    /// </summary>
    private static void OnCaptureLogDirs(int broker, IntPtr value, IntPtr error, IntPtr userData)
    {
        // A callback entered from native is a no-throw boundary even in a test.
        try
        {
            LogDirsCaptureState capture = (LogDirsCaptureState)GCHandle.FromIntPtr(userData).Target!;
            capture.Broker = broker;
            capture.Value = value;
            capture.Error = error;
            capture.Done.Set();
        }
        catch (Exception)
        {
            // Swallow: an escaping exception would unwind into Rust. The Wait above then
            // times out and fails the test with a clear message.
        }
    }

    /// <summary>
    /// The shape-4a twin of <see cref="OnCaptureAlterReplica"/> for <c>describeReplicaLogDirs</c>: one
    /// callback per replica, carrying that key's three scalars plus its own <b>owned</b>
    /// value and error.
    /// </summary>
    private static void OnCaptureReplica(
        IntPtr topic, int partition, int brokerId, IntPtr value, IntPtr error, IntPtr userData)
    {
        // A callback entered from native is a no-throw boundary even in a test.
        try
        {
            ReplicaCaptureState capture = (ReplicaCaptureState)GCHandle.FromIntPtr(userData).Target!;

            // ⚠ Copied HERE: the topic is a borrowed const char*, valid for the call only.
            capture.Topic = Utf8Marshal.PtrToString(topic);
            capture.Partition = partition;
            capture.BrokerId = brokerId;
            capture.Value = value;
            capture.Error = error;
            capture.Done.Set();
        }
        catch (Exception)
        {
            // Swallow: an escaping exception would unwind into Rust. The Wait above then
            // times out and fails the test with a clear message.
        }
    }

    /// <summary>
    /// The shape-4b form: <c>alterReplicaLogDirs</c> delivers one callback per replica,
    /// carrying that key's three scalars and its own <b>owned</b> error — a null error being
    /// that replica's success.
    /// </summary>
    private static void OnCaptureAlterReplica(
        IntPtr topic, int partition, int brokerId, IntPtr error, IntPtr userData)
    {
        // A callback entered from native is a no-throw boundary even in a test.
        try
        {
            ReplicaCaptureState capture = (ReplicaCaptureState)GCHandle.FromIntPtr(userData).Target!;

            // ⚠ Copied HERE: the topic is a borrowed const char*, valid for the call only.
            capture.Topic = Utf8Marshal.PtrToString(topic);
            capture.Partition = partition;
            capture.BrokerId = brokerId;
            capture.Error = error;
            capture.Done.Set();
        }
        catch (Exception)
        {
            // Swallow: an escaping exception would unwind into Rust. The Wait above then
            // times out and fails the test with a clear message.
        }
    }

    /// <summary>
    /// What one shape-4a <c>describeReplicaLogDirs</c> callback delivered.
    /// </summary>
    private sealed class ReplicaCaptureState
    {
        internal string? Topic;

        internal int Partition = int.MinValue;

        internal int BrokerId = int.MinValue;

        internal IntPtr Value;

        internal IntPtr Error;

        internal ManualResetEventSlim Done { get; } = new ManualResetEventSlim(false);
    }

    /// <summary>
    /// What one shape-4a <c>describeLogDirs</c> callback delivered. Its own type: the key is
    /// a scalar broker id, so there is no result root to share with the two shape-1
    /// neighbours.
    /// </summary>
    private sealed class LogDirsCaptureState
    {
        internal int Broker = int.MinValue;

        internal IntPtr Value;

        internal IntPtr Error;

        internal ManualResetEventSlim Done { get; } = new ManualResetEventSlim(false);
    }
}
