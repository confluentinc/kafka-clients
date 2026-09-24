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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Drives <see cref="KeyedResultMarshal.CompleteKey{TKey, TValue}"/> over <b>real</b>
/// native per-key payloads — the highest-risk surface in the migration, because under
/// result shape 4 both the per-key <c>value</c> and the per-key <c>error</c> are
/// <b>owned</b> by the callback invocation: consuming one twice is a double free (a
/// process abort no managed assertion can catch), and not consuming it leaks.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>M15/P9 CP2 inverted this file's central claim.</b> Before the per-key ABI these
/// tests proved the opposite ownership: the per-key error was <em>borrowed</em> from a
/// <c>CreateTopicsResult_t</c> root, had to survive the walk, and died with the root. The
/// root no longer exists — <c>create_topics_async</c> hands each key its own owned value
/// and error — so <see cref="KafkaException.FromHandle"/> is now the correct read and a
/// surviving <see cref="KafkaException.FromBorrowedHandle"/> would leak. The borrowed-error
/// walk keeps its own coverage in <c>AdminP2bResultMarshalTests</c>,
/// <c>AdminP4ResultMarshalTests</c>, <c>AdminP5ResultMarshalTests</c> and
/// <c>AdminP6ResultMarshalTests</c> for as long as it has live callers.
/// </para>
/// <para>
/// The two tests that could only be expressed against a result table are gone with it: the
/// shape-2 table walk (covered by <c>AdminP4ResultMarshalTests</c>, over a real
/// <c>listOffsets</c> table) and the missing-key sweep, whose firing point moved to
/// countdown zero and is asserted there by <c>AdminP9CountdownTests</c>.
/// </para>
/// <para>
/// The key reader and value marshaller come from production —
/// <see cref="KeyedResultMarshal.ReadStringKey"/> /
/// <see cref="AdminCallbacks.TopicMetadataAndConfigPerKeyValue"/> — so a test-local copy
/// cannot keep passing after production changes what it points at
/// (<c>definition-of-done.md</c> §12). Only <c>destroyValue</c> is wrapped, to count it.
/// </para>
/// </remarks>
public sealed class AdminKeyedResultMarshalTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>The C code Kafka assigns to <c>INVALID_REPLICATION_FACTOR</c>.</summary>
    private const int InvalidReplicationFactorCode = 38;

    /// <summary>The C code Kafka assigns to <c>TOPIC_ALREADY_EXISTS</c>.</summary>
    private const int TopicAlreadyExistsCode = 36;

    private const string GoodTopic = "keyed-marshal-good";

    private const string BadTopic = "keyed-marshal-bad";

    /// <summary>
    /// Rooted for the process lifetime, as every callback handed to native must be
    /// (ffi §B6 keep-alive) — even a test's.
    /// </summary>
    private static readonly AdminCallbacks.CreateTopicsCallback s_perKey = OnPerKey;

    /// <summary>
    /// The phase's central memory-safety claim, over real native memory: each key resolves
    /// from its own callback, and that callback consumes its owned value exactly once — on
    /// the success path <em>and</em> on the failure path, where the value is NULL.
    /// </summary>
    [Fact]
    public async Task PerKeyPayload_IsOwned_AndConsumedExactlyOnce()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        // One topic the 1-broker mock can create, one it must reject (replication factor
        // 5 > 1 broker) — so the same submit carries both outcomes.
        Drive drive = DrivePerKey(
            admin,
            new[] { GoodTopic, BadTopic },
            new NewTopic(GoodTopic, 1, 1),
            new NewTopic(BadTopic, 1, 5));

        // Each key carries its OWN outcome — the discriminator against an implementation
        // that lets the last callback to arrive decide every key's fate.
        await TestTimeout.Run(() => drive.Operation.Tasks[GoodTopic], s_deadline);
        Assert.True(drive.Operation.Tasks[BadTopic].IsFaulted);

        KafkaException failure = Assert.IsType<KafkaException>(
            drive.Operation.Tasks[BadTopic].Exception!.InnerException);
        Assert.Equal(InvalidReplicationFactorCode, failure.Code);
        Assert.Equal("Replication factor: 5 is larger than brokers: 1", failure.Message);

        // ⚠ THE OWNERSHIP ASSERTION. The destroy runs once per callback — including the
        // failed key, whose value is NULL — and never twice, which would abort the host.
        Assert.Equal(2, drive.DestroyCalls);
        Assert.Equal(1, drive.NonNullValues);
    }

    /// <summary>
    /// The copied-out per-key value stays valid after the owned native payload it was read
    /// from is destroyed. Nothing native-backed may outlive its owner (ffi §B4).
    /// </summary>
    [Fact]
    public async Task PerKeyValue_IsCopiedOut_AndOutlivesTheNativePayload()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        NewTopic topic = new NewTopic(GoodTopic, 3, 1)
        {
            Configs = new Dictionary<string, string> { ["cleanup.policy"] = "compact" },
        };

        Drive drive = DrivePerKey(admin, new[] { GoodTopic }, topic);
        Assert.Equal(1, drive.DestroyCalls);

        // Read only AFTER the payload is gone: every string and flag below must already be
        // owned managed state, not a pointer into freed native memory.
        TopicMetadataAndConfig value = await TestTimeout.Run(
            () => drive.Operation.Tasks[GoodTopic], s_deadline);
        Assert.True(value.HasMetadata);
        Assert.Equal(3, value.NumPartitions());
        Assert.Equal(1, value.ReplicationFactor());
        Assert.NotEqual(Uuid.Zero, value.TopicId());

        ConfigEntry entry = Assert.IsType<ConfigEntry>(value.Config().Get("cleanup.policy"));
        Assert.Equal("compact", entry.Value);
    }

    /// <summary>
    /// A per-key error message with non-ASCII content round-trips through the borrowed
    /// NUL-terminated read — the <c>LPStr</c> guard (ffi §B3). The mock echoes the topic
    /// name into its "exists already" message, so a non-ASCII topic name proves both
    /// directions of the UTF-8 marshalling in one call: the key arrives as the callback's
    /// own <c>const char*</c>, and a mis-read key would miss the lookup below entirely.
    /// </summary>
    [Fact]
    public void NonAsciiTopicName_RoundTripsThroughKeyAndErrorMessage()
    {
        const string Topic = "témas-日本語-🎉";

        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        // First call creates it; the second must fail with TOPIC_ALREADY_EXISTS, whose
        // message quotes the topic name back at us.
        DrivePerKey(admin, new[] { Topic }, new NewTopic(Topic, 1, 1));

        Drive drive = DrivePerKey(admin, new[] { Topic }, new NewTopic(Topic, 1, 1));

        Assert.True(drive.Operation.Tasks[Topic].IsFaulted);
        KafkaException failure = Assert.IsType<KafkaException>(
            drive.Operation.Tasks[Topic].Exception!.InnerException);
        Assert.Equal(TopicAlreadyExistsCode, failure.Code);
        Assert.Equal($"Topic {Topic} exists already.", failure.Message);
    }

    /// <summary>
    /// Submits <c>create_topics_async</c> with a callback that runs the <em>production</em>
    /// per-key marshaller, and returns once every key's callback has fired. The payload
    /// pointers never escape the callback, because under shape 4 they are owned by it.
    /// </summary>
    private static Drive DrivePerKey(
        NativeAdminClient admin, string[] keys, params NewTopic[] topics)
    {
        IntPtr[] handles = new IntPtr[topics.Length];
        Drive drive = new Drive(keys, topics.Length);
        GCHandle gcHandle = GCHandle.Alloc(drive, GCHandleType.Normal);
        try
        {
            for (int i = 0; i < topics.Length; i++)
            {
                handles[i] = NewTopicMarshal.Build(topics[i]);
            }

            NativeMethods.AdminClientCreateTopicsAsync(
                admin.Handle.DangerousGetHandle(),
                handles,
                handles.Length,
                -1,
                false,
                true,
                s_perKey,
                GCHandle.ToIntPtr(gcHandle));

            Assert.True(
                drive.Done.Wait(s_deadline),
                $"only {drive.Done.InitialCount - drive.Done.CurrentCount} of "
                    + $"{drive.Done.InitialCount} per-key callbacks fired");
        }
        finally
        {
            // The ABI copies out; the caller retains ownership of the input entries.
            foreach (IntPtr handle in handles)
            {
                NativeMethods.NewTopicDestroy(handle);
            }

            gcHandle.Free();
        }

        return drive;
    }

    private static void OnPerKey(IntPtr key, IntPtr value, IntPtr error, IntPtr userData)
    {
        // A callback entered from native is a no-throw boundary even in a test.
        Drive? drive = null;
        try
        {
            drive = (Drive)GCHandle.FromIntPtr(userData).Target!;
            if (value != IntPtr.Zero)
            {
                Interlocked.Increment(ref drive.NonNullValues);
            }

            KeyedResultMarshal.CompleteKey(
                drive.Operation,
                KeyedResultMarshal.ReadStringKey(key),
                value,
                error,
                AdminCallbacks.TopicMetadataAndConfigPerKeyValue,
                drive.DestroyValue);
        }
        catch (Exception)
        {
            // Swallow: an escaping exception would unwind into Rust. The Wait above then
            // times out and fails the test with a clear message.
        }
        finally
        {
            drive?.Done.Signal();
        }
    }

    private sealed class Drive
    {
        internal int DestroyCalls;

        internal int NonNullValues;

        internal Drive(string[] keys, int expectedCallbacks)
        {
            Operation = new KeyedAdminOperation<string, TopicMetadataAndConfig>(
                "createTopics", keys, StringComparer.Ordinal);
            Done = new CountdownEvent(expectedCallbacks);
            DestroyValue = handle =>
            {
                Interlocked.Increment(ref DestroyCalls);
                NativeMethods.TopicMetadataAndConfigDestroy(handle);
            };
        }

        internal KeyedAdminOperation<string, TopicMetadataAndConfig> Operation { get; }

        internal CountdownEvent Done { get; }

        /// <summary>
        /// Production's destroy, counted. Allocated once so the count is not confused by a
        /// fresh delegate per callback.
        /// </summary>
        internal Action<IntPtr> DestroyValue { get; }
    }
}
