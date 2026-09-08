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
/// Drives <see cref="KeyedResultMarshal"/> over a <b>real</b> native
/// <c>CreateTopicsResult_t</c> — the highest-risk surface in M15/P1, because a per-key
/// error is <b>borrowed</b> from the result root and destroying it is a double free:
/// a process abort no managed assertion can catch.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why these tests own the result handle instead of going through
/// <c>MockAdminClient</c>.</b> The production trampoline destroys the result root in its
/// <c>finally</c> — correctly — so it never lets a caller inspect the borrowed pointers
/// afterwards. Here the test submits <c>create_topics_async</c> directly with its own
/// capturing callback, so the result stays alive and the test can (a) walk it with the
/// <em>production</em> accessors and marshaller, (b) prove the borrowed error is still
/// readable <em>after</em> the walk, and (c) destroy the root exactly once. Under the
/// injection this rule guards against — teaching
/// <see cref="KafkaException.FromBorrowedHandle"/> to destroy — step (b) becomes a
/// use-after-free read and step (c) becomes a double free, so the test goes red rather
/// than passing while silently proving nothing.
/// </para>
/// <para>
/// The accessor set and value marshaller come from
/// <see cref="AdminCallbacks.CreateTopicsAccessors"/> /
/// <see cref="AdminCallbacks.TopicMetadataAndConfigValue"/> — production's own
/// (<c>definition-of-done.md</c> §12) — so a test-local copy cannot keep passing after
/// production changes what it points at.
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
    private static readonly AdminCallbacks.CreateTopicsCallback s_capture = OnCapture;

    /// <summary>
    /// The phase's central memory-safety claim, over real native memory: walking a
    /// result with a per-key failure reads the borrowed error and leaves it
    /// <b>alive</b>, so the single root destroy that follows is the only free.
    /// </summary>
    [Fact]
    public void PerKeyError_IsBorrowed_AndSurvivesTheWalk()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        // One topic the 1-broker mock can create, one it must reject (replication factor
        // 5 > 1 broker) — so the same result carries both outcomes.
        IntPtr result = SubmitAndCaptureResult(
            admin,
            new NewTopic(GoodTopic, 1, 1),
            new NewTopic(BadTopic, 1, 5));

        try
        {
            KeyedAdminOperation<TopicMetadataAndConfig> operation =
                new KeyedAdminOperation<TopicMetadataAndConfig>(
                    "createTopics", new[] { GoodTopic, BadTopic });

            KeyedResultMarshal.Complete(
                result,
                AdminCallbacks.CreateTopicsAccessors,
                operation,
                AdminCallbacks.TopicMetadataAndConfigValue);

            // Each key carries its OWN outcome — the discriminator against an
            // implementation that faults everything as soon as any key fails.
            Assert.Equal(TaskStatus.RanToCompletion, operation.Tasks[GoodTopic].Status);
            Assert.True(operation.Tasks[BadTopic].IsFaulted);

            KafkaException failure = Assert.IsType<KafkaException>(
                operation.Tasks[BadTopic].Exception!.InnerException);
            Assert.Equal(InvalidReplicationFactorCode, failure.Code);
            Assert.Equal("Replication factor: 5 is larger than brokers: 1", failure.Message);

            // ⚠ THE INJECTION POINT. If FromBorrowedHandle destroyed what it read, this
            // re-read would be a use-after-free and the destroy below a double free. The
            // borrowed pointer must still resolve to the same values.
            int badIndex = IndexOf(result, BadTopic);
            IntPtr borrowedError = NativeMethods.CreateTopicsResultGetError(result, badIndex);
            Assert.NotEqual(IntPtr.Zero, borrowedError);
            Assert.Equal(InvalidReplicationFactorCode, NativeMethods.Code(borrowedError));
            Assert.Equal(
                "Replication factor: 5 is larger than brokers: 1",
                Utf8Marshal.PtrToString(NativeMethods.Message(borrowedError)));
        }
        finally
        {
            // The ONLY free of the borrowed error, via its owning root — exactly once.
            NativeMethods.CreateTopicsResultDestroy(result);
        }
    }

    /// <summary>
    /// The copied-out per-key value stays valid after the root — and everything it
    /// carries — is destroyed. Nothing native-backed may survive the root (ffi §B4).
    /// </summary>
    [Fact]
    public async Task PerKeyValue_IsCopiedOut_AndOutlivesTheResultRoot()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        NewTopic topic = new NewTopic(GoodTopic, 3, 1)
        {
            Configs = new Dictionary<string, string> { ["cleanup.policy"] = "compact" },
        };

        IntPtr result = SubmitAndCaptureResult(admin, topic);

        KeyedAdminOperation<TopicMetadataAndConfig> operation =
            new KeyedAdminOperation<TopicMetadataAndConfig>("createTopics", new[] { GoodTopic });
        try
        {
            KeyedResultMarshal.Complete(
                result,
                AdminCallbacks.CreateTopicsAccessors,
                operation,
                AdminCallbacks.TopicMetadataAndConfigValue);
        }
        finally
        {
            NativeMethods.CreateTopicsResultDestroy(result);
        }

        // Read only AFTER the root is gone: every string and flag below must already be
        // owned managed state, not a pointer into freed native memory.
        TopicMetadataAndConfig value = await operation.Tasks[GoodTopic];
        Assert.True(value.HasMetadata);
        Assert.Equal(3, value.NumPartitions());
        Assert.Equal((short)1, value.ReplicationFactor());
        Assert.NotEqual(Uuid.Zero, value.TopicId());

        ConfigEntry entry = Assert.IsType<ConfigEntry>(value.Config().Get("cleanup.policy"));
        Assert.Equal("compact", entry.Value);
    }

    /// <summary>
    /// <b>Result shape 2</b> — the per-key <c>KafkaFuture&lt;Void&gt;</c> form, where the
    /// ABI exposes no <c>_get_value</c> at all and a null error <em>is</em> the success
    /// value. P1 ships no shape-2 RPC (<c>deleteTopics</c> is M15/P2), so the shape is
    /// exercised the only way it can be without inventing one: the production walker is
    /// handed a value-less accessor set over a real result table, driving exactly the
    /// branch a shape-2 RPC will take — including the borrowed per-key error.
    /// </summary>
    [Fact]
    public async Task Shape2_TreatsANullPerKeyErrorAsTheSuccessValue()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        IntPtr result = SubmitAndCaptureResult(
            admin,
            new NewTopic(GoodTopic, 1, 1),
            new NewTopic(BadTopic, 1, 5));

        try
        {
            KeyedResultMarshal.Accessors voidShape = new KeyedResultMarshal.Accessors(
                NativeMethods.CreateTopicsResultCount,
                NativeMethods.CreateTopicsResultGetKey,
                NativeMethods.CreateTopicsResultGetError,
                getValue: null);

            VoidKeyedAdminOperation operation =
                new VoidKeyedAdminOperation("createTopics", new[] { GoodTopic, BadTopic });

            KeyedResultMarshal.Complete<bool>(result, voidShape, operation, marshalValue: null);

            // Success carries no value: the Task simply completes.
            Assert.Equal(TaskStatus.RanToCompletion, operation.Tasks[GoodTopic].Status);
            Assert.True(await operation.Tasks[GoodTopic]);

            // …and a per-key error still faults only its own key, borrowed as ever.
            Assert.True(operation.Tasks[BadTopic].IsFaulted);
            KafkaException failure = Assert.IsType<KafkaException>(
                operation.Tasks[BadTopic].Exception!.InnerException);
            Assert.Equal(InvalidReplicationFactorCode, failure.Code);
        }
        finally
        {
            NativeMethods.CreateTopicsResultDestroy(result);
        }
    }

    /// <summary>
    /// A per-key error message with non-ASCII content round-trips through the borrowed
    /// NUL-terminated read — the <c>LPStr</c> guard (ffi §B3). The mock echoes the topic
    /// name into its "exists already" message, so a non-ASCII topic name proves both
    /// directions of the UTF-8 marshalling in one call.
    /// </summary>
    [Fact]
    public void NonAsciiTopicName_RoundTripsThroughKeyAndErrorMessage()
    {
        const string Topic = "témas-日本語-🎉";

        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        // First call creates it; the second must fail with TOPIC_ALREADY_EXISTS, whose
        // message quotes the topic name back at us.
        IntPtr created = SubmitAndCaptureResult(admin, new NewTopic(Topic, 1, 1));
        NativeMethods.CreateTopicsResultDestroy(created);

        IntPtr result = SubmitAndCaptureResult(admin, new NewTopic(Topic, 1, 1));
        try
        {
            KeyedAdminOperation<TopicMetadataAndConfig> operation =
                new KeyedAdminOperation<TopicMetadataAndConfig>("createTopics", new[] { Topic });

            KeyedResultMarshal.Complete(
                result,
                AdminCallbacks.CreateTopicsAccessors,
                operation,
                AdminCallbacks.TopicMetadataAndConfigValue);

            // The key itself round-tripped, or TryGetValue below would have missed.
            Assert.True(operation.Tasks[Topic].IsFaulted);
            KafkaException failure = Assert.IsType<KafkaException>(
                operation.Tasks[Topic].Exception!.InnerException);
            Assert.Equal(TopicAlreadyExistsCode, failure.Code);
            Assert.Equal($"Topic {Topic} exists already.", failure.Message);
        }
        finally
        {
            NativeMethods.CreateTopicsResultDestroy(result);
        }
    }

    /// <summary>
    /// A requested key the result never mentions must <b>fault</b>, not hang. The walker
    /// leaves it untouched and <c>FailUncompleted</c> — which the production trampoline
    /// calls in its <c>finally</c> — is what makes "no <c>Task</c> can ever hang" true.
    /// </summary>
    [Fact]
    public void AKeyMissingFromTheResult_IsFaulted_NotLeftHanging()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        IntPtr result = SubmitAndCaptureResult(admin, new NewTopic(GoodTopic, 1, 1));
        try
        {
            KeyedAdminOperation<TopicMetadataAndConfig> operation =
                new KeyedAdminOperation<TopicMetadataAndConfig>(
                    "createTopics", new[] { GoodTopic, "never-requested-of-the-core" });

            KeyedResultMarshal.Complete(
                result,
                AdminCallbacks.CreateTopicsAccessors,
                operation,
                AdminCallbacks.TopicMetadataAndConfigValue);

            Assert.False(operation.Tasks["never-requested-of-the-core"].IsCompleted);

            operation.FailUncompleted();

            Assert.True(operation.Tasks["never-requested-of-the-core"].IsFaulted);
            KafkaException failure = Assert.IsType<KafkaException>(
                operation.Tasks["never-requested-of-the-core"].Exception!.InnerException);
            Assert.Equal(
                "The createTopics result contained no entry for 'never-requested-of-the-core'.",
                failure.Message);

            // The key that WAS in the result is untouched by the sweep.
            Assert.Equal(TaskStatus.RanToCompletion, operation.Tasks[GoodTopic].Status);
        }
        finally
        {
            NativeMethods.CreateTopicsResultDestroy(result);
        }
    }

    /// <summary>
    /// Submits <c>create_topics_async</c> directly and hands the caller the resulting
    /// <b>owned</b> result root, which the callback deliberately does not destroy — the
    /// callback owns it, and here that owner is the test.
    /// </summary>
    private static IntPtr SubmitAndCaptureResult(NativeAdminClient admin, params NewTopic[] topics)
    {
        IntPtr[] handles = new IntPtr[topics.Length];
        Capture capture = new Capture();
        GCHandle gcHandle = GCHandle.Alloc(capture, GCHandleType.Normal);
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
                s_capture,
                GCHandle.ToIntPtr(gcHandle));

            Assert.True(capture.Done.Wait(s_deadline), "the createTopics callback never fired");
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

        // A top-level submit failure would mean there is nothing to walk; surface it
        // rather than dereferencing a null result.
        KafkaException? submitFailure = KafkaException.FromHandle(capture.Error);
        if (submitFailure is not null)
        {
            throw submitFailure;
        }

        Assert.NotEqual(IntPtr.Zero, capture.Result);
        return capture.Result;
    }

    private static int IndexOf(IntPtr result, string key)
    {
        int count = NativeMethods.CreateTopicsResultCount(result);
        for (int i = 0; i < count; i++)
        {
            if (string.Equals(Utf8Marshal.PtrToString(NativeMethods.CreateTopicsResultGetKey(result, i)), key, StringComparison.Ordinal))
            {
                return i;
            }
        }

        throw new InvalidOperationException($"The result has no entry for '{key}'.");
    }

    private static void OnCapture(IntPtr result, IntPtr error, IntPtr userData)
    {
        // A callback entered from native is a no-throw boundary even in a test.
        try
        {
            Capture capture = (Capture)GCHandle.FromIntPtr(userData).Target!;
            capture.Result = result;
            capture.Error = error;
            capture.Done.Set();
        }
        catch (Exception)
        {
            // Swallow: an escaping exception would unwind into Rust. The Wait above then
            // times out and fails the test with a clear message.
        }
    }

    private sealed class Capture
    {
        internal IntPtr Result;

        internal IntPtr Error;

        internal ManualResetEventSlim Done { get; } = new ManualResetEventSlim(false);
    }
}
