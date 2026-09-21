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
using System.Runtime.InteropServices;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The span-the-op reference contract and the result-shape edges for M15/P3 Stage 3's three
/// RPCs — the log-dir twin of <see cref="AdminConfigsLifetimeTests"/>.
/// <c>kafka_admin_AdminClient_destroy</c> is <b>not</b> ref-counted and does <b>not</b>
/// drain, so every new submit needs its own proof rather than inheriting an earlier one's.
/// </summary>
public sealed class AdminLogDirsLifetimeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "logdir-lifetime-topic";

    private static readonly TopicPartitionReplica s_replica = new TopicPartitionReplica(Topic, 0, 0);

    /// <summary>
    /// The three-way differential for all three new submits. A single-case assertion cannot
    /// tell a working reference count from a permanently unbalanced one — both read "not
    /// released" — so each RPC gets all three cases.
    /// </summary>
    [Theory]
    [InlineData(Rpc.DescribeLogDirs)]
    [InlineData(Rpc.AlterReplicaLogDirs)]
    [InlineData(Rpc.DescribeReplicaLogDirs)]
    public async Task DisposeRacingAnInFlightOperation_DefersTheNativeDestroy(Rpc rpc)
    {
        // ---- (1) Nothing in flight: Dispose releases immediately. ----
        NativeAdminClient baseline = NativeAdminClient.CreateMock(1);
        SafeAdminHandle baselineHandle = baseline.Handle;
        TestTimeout.Run(baseline.Dispose, s_deadline);
        Assert.True(baselineHandle.IsClosed, "with no operation in flight the native release must be immediate");

        // ---- (2) One in flight: Dispose must NOT release. ----
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        IntPtr capturedUserData = IntPtr.Zero;
        Func<Task> outcome = SubmitCapturing(admin, rpc, userData => capturedUserData = userData);
        Assert.NotEqual(IntPtr.Zero, capturedUserData);

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.False(
            handle.IsClosed,
            "an in-flight operation must defer AdminClient_destroy — the ABI does not protect this itself");

        // ---- (3) Completing the operation releases it. ----
        Complete(rpc, capturedUserData, MakeError(42, "submit failed"));
        Assert.True(handle.IsClosed, "completing the in-flight operation must run the deferred release");

        await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(outcome), s_deadline);
    }

    /// <summary>
    /// Many operations of all three kinds leave the reference count <b>balanced</b>.
    /// </summary>
    [Fact]
    public async Task ManyOperationsOfEveryKind_LeaveTheReferenceCountBalanced()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        for (int i = 0; i < 25; i++)
        {
            foreach (Rpc rpc in new[] { Rpc.DescribeLogDirs, Rpc.AlterReplicaLogDirs, Rpc.DescribeReplicaLogDirs })
            {
                IntPtr userData = IntPtr.Zero;
                Func<Task> outcome = SubmitCapturing(admin, rpc, captured => userData = captured);
                Complete(rpc, userData, MakeError(9, "balance probe"));
                await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(outcome), s_deadline);
            }

            Assert.False(handle.IsClosed, "the client is still alive between operations");
        }

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(handle.IsClosed, "75 operations must leave the reference count balanced");
    }

    /// <summary>
    /// A submit that throws before native ran must not root the operation forever.
    /// </summary>
    [Theory]
    [InlineData(Rpc.DescribeLogDirs)]
    [InlineData(Rpc.AlterReplicaLogDirs)]
    [InlineData(Rpc.DescribeReplicaLogDirs)]
    public void SubmitThatThrows_AbandonsTheOperationAndLeavesTheHandleReleasable(Rpc rpc)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        Assert.Throws<InvalidOperationException>(() => SubmitThrowing(admin, rpc));

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(
            handle.IsClosed,
            "a submit that never reached native must not leave the operation's reference held");
    }

    /// <summary>
    /// The callback's <c>error</c> parameter is <b>OWNED</b> — the mirror image of the
    /// borrowed per-key errors inside a result — so the trampoline frees it via
    /// <see cref="KafkaException.FromHandle"/> and it must never be freed again.
    /// </summary>
    [Theory]
    [InlineData(Rpc.DescribeLogDirs)]
    [InlineData(Rpc.AlterReplicaLogDirs)]
    [InlineData(Rpc.DescribeReplicaLogDirs)]
    public async Task TheCallbacksErrorParameter_IsOwned_AndConsumedExactlyOnce(Rpc rpc)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        for (int i = 0; i < 50; i++)
        {
            string message = $"owned error {i} — não ascii";
            Func<Task> outcome = SubmitCapturing(
                admin, rpc, userData => Complete(rpc, userData, MakeError(70 + i, message)));

            KafkaException failure =
                await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(outcome), s_deadline);
            Assert.Equal(70 + i, failure.Code);
            Assert.Equal(message, failure.Message);
        }
    }

    /// <summary>
    /// All three new trampolines are <b>total no-throw boundaries</b>: a managed exception
    /// raised inside one is absorbed rather than unwinding into Rust.
    /// </summary>
    [Theory]
    [InlineData(Rpc.DescribeLogDirs)]
    [InlineData(Rpc.AlterReplicaLogDirs)]
    [InlineData(Rpc.DescribeReplicaLogDirs)]
    public void EveryTrampoline_IsATotalNoThrowBoundary(Rpc rpc)
    {
        GCHandle wrongType = GCHandle.Alloc("not an admin operation", GCHandleType.Normal);
        try
        {
            Complete(rpc, GCHandle.ToIntPtr(wrongType), MakeError(5, "absorbed"));
        }
        finally
        {
            wrongType.Free();
        }
    }

    /// <summary>
    /// ⚠⚠ <b><c>describeReplicaLogDirs</c>' result can omit a requested key, and the honest
    /// outcome is a FAULT naming that key.</b>
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>Reachable through the MOCK ONLY, and that is a contract the header states</b>:
    /// Java's real client seeds one future per requested replica
    /// (<c>KafkaAdminClient.java:3066-3068</c>) and completes every one
    /// (<c>:3141-3145</c>), the Rust core does the same, and the header says so in
    /// <c>kafka_admin_DescribeReplicaLogDirsResult_count</c>'s own docs — "absence from the
    /// result is not the signal for an unknown topic; a null current log dir is". Only
    /// <c>MockAdminClient</c> diverges, skipping replicas of topics it does not know
    /// (<c>mock_admin_client.rs:1352-1355</c>), which is what makes this path testable
    /// without a broker at all. Its two Stage-3 siblings insert an entry for every input
    /// they are given (<c>:1227-1229</c>, <c>:1290-1291</c>), so neither reaches it.
    /// </para>
    /// <para>
    /// ⚠ <b>The fault is deliberate, not a gap.</b> Completing the key locally with a
    /// default <c>ReplicaLogDirInfo</c> would report a current log directory of
    /// <see langword="null"/> — indistinguishable from the real client's answer for a
    /// replica the broker genuinely does not host — so the binding would be inventing data
    /// it does not have. The same reasoning keeps <see cref="LogDirDescription"/> free of a
    /// faked <c>IsCordoned</c>. What is asserted is therefore that the message
    /// <em>names the key</em>, so the caller can tell which replica went missing.
    /// </para>
    /// </remarks>
    [Fact]
    public async Task AReplicaTheMockOmits_FaultsThatKeyWithAMessageNamingIt()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }, options: null).All(), s_deadline);

        TopicPartitionReplica known = new TopicPartitionReplica(Topic, 0, 0);
        TopicPartitionReplica unknown = new TopicPartitionReplica("logdir-absent-topic", 0, 0);

        DescribeReplicaLogDirsResult result =
            admin.DescribeReplicaLogDirs(new[] { known, unknown }, options: null);

        // The known replica still succeeds: one missing key does not fail the call.
        DescribeReplicaLogDirsResult.ReplicaLogDirInfo info =
            await TestTimeout.Run(() => result.Values[known], s_deadline);
        Assert.NotNull(info.GetCurrentReplicaLogDir());

        KafkaException missing = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.Values[unknown]), s_deadline);
        Assert.Equal(
            "The describeReplicaLogDirs result contained no entry for 'logdir-absent-topic-0-0'.",
            missing.Message);

        // …and All() therefore faults as well.
        await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);
    }

    /// <summary>
    /// ⚠ <b>A per-key failure is not a call failure</b>: <c>alterReplicaLogDirs</c> faults
    /// only the rejected replica's awaitable while its sibling succeeds — result shape 2,
    /// where a null per-key error <em>is</em> the success value.
    /// </summary>
    [Fact]
    public async Task AlterReplicaLogDirs_FaultsOnlyTheRejectedReplica()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic(Topic, 2, 1) }, options: null).All(), s_deadline);

        TopicPartitionReplica accepted = new TopicPartitionReplica(Topic, 0, 0);
        TopicPartitionReplica rejected = new TopicPartitionReplica(Topic, 1, 0);

        AlterReplicaLogDirsResult result = admin.AlterReplicaLogDirs(
            new Dictionary<TopicPartitionReplica, string>
            {
                // The dir the mock seeded for every broker.
                [accepted] = "/tmp/kafka-logs",

                // A directory the broker does not have.
                [rejected] = "/no/such/dir",
            },
            options: null);

        await TestTimeout.Run(() => result.Values[accepted], s_deadline);

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.Values[rejected]), s_deadline);
        Assert.Equal("Log directory /no/such/dir is offline", failure.Message);

        await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);
    }

    /// <summary>Which RPC a parameterised case drives.</summary>
    public enum Rpc
    {
        /// <summary>Result shape 1 — a bare scalar key with a per-broker map value.</summary>
        DescribeLogDirs,

        /// <summary>Result shape 2 — a 3-part composite key with a void per-replica future.</summary>
        AlterReplicaLogDirs,

        /// <summary>Result shape 1 — a 3-part composite key with a per-replica value.</summary>
        DescribeReplicaLogDirs,
    }

    private static Func<Task> SubmitCapturing(NativeAdminClient admin, Rpc rpc, Action<IntPtr> onSubmit)
    {
        switch (rpc)
        {
            case Rpc.DescribeLogDirs:
                {
                    DescribeLogDirsResult result = admin.DescribeLogDirs(
                        new[] { 0 },
                        options: null,
                        (nativeHandle, brokers, count, timeoutMs, callback, userData) => onSubmit(userData));
                    return () => result.Descriptions[0];
                }

            case Rpc.AlterReplicaLogDirs:
                {
                    AlterReplicaLogDirsResult result = admin.AlterReplicaLogDirs(
                        new Dictionary<TopicPartitionReplica, string> { [s_replica] = "/data" },
                        options: null,
                        (nativeHandle, topics, partitions, brokerIds, logDirs, count, timeoutMs, callback, userData) =>
                            onSubmit(userData));
                    return () => result.Values[s_replica];
                }

            default:
                {
                    DescribeReplicaLogDirsResult result = admin.DescribeReplicaLogDirs(
                        new[] { s_replica },
                        options: null,
                        (nativeHandle, topics, partitions, brokerIds, count, timeoutMs, callback, userData) =>
                            onSubmit(userData));
                    return () => result.Values[s_replica];
                }
        }
    }

    private static void SubmitThrowing(NativeAdminClient admin, Rpc rpc)
    {
        switch (rpc)
        {
            case Rpc.DescribeLogDirs:
                admin.DescribeLogDirs(
                    new[] { 0 },
                    options: null,
                    (nativeHandle, brokers, count, timeoutMs, callback, userData) =>
                        throw new InvalidOperationException("submit failed"));
                return;

            case Rpc.AlterReplicaLogDirs:
                admin.AlterReplicaLogDirs(
                    new Dictionary<TopicPartitionReplica, string> { [s_replica] = "/data" },
                    options: null,
                    (nativeHandle, topics, partitions, brokerIds, logDirs, count, timeoutMs, callback, userData) =>
                        throw new InvalidOperationException("submit failed"));
                return;

            default:
                admin.DescribeReplicaLogDirs(
                    new[] { s_replica },
                    options: null,
                    (nativeHandle, topics, partitions, brokerIds, count, timeoutMs, callback, userData) =>
                        throw new InvalidOperationException("submit failed"));
                return;
        }
    }

    /// <summary>
    /// Drives the <b>production</b> trampoline with a top-level submit failure — the path
    /// where there is no result table, so every requested key fails with that one error.
    /// </summary>
    private static void Complete(Rpc rpc, IntPtr userData, IntPtr error)
    {
        switch (rpc)
        {
            case Rpc.DescribeLogDirs:
                AdminCallbacks.DescribeLogDirs(IntPtr.Zero, error, userData);
                return;

            case Rpc.AlterReplicaLogDirs:
                AdminCallbacks.AlterReplicaLogDirs(IntPtr.Zero, error, userData);
                return;

            default:
                AdminCallbacks.DescribeReplicaLogDirs(IntPtr.Zero, error, userData);
                return;
        }
    }

    /// <summary>
    /// Builds an <b>owned</b> <c>kafka_common_KafkaError_t</c> to stand in for the one
    /// native would hand the callback. The trampoline frees it via
    /// <see cref="KafkaException.FromHandle"/>, so it must never be freed here.
    /// </summary>
    private static IntPtr MakeError(int code, string message)
    {
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(message);
        IntPtr error = NativeMethods.KafkaErrorNew(code, pinned.Pointer);
        Assert.NotEqual(IntPtr.Zero, error);
        return error;
    }
}
