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
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The native core behind both admin flavours and their mocks: it owns the client handle and its drain thread,
/// makes each RPC's C call, hands the native futures to the <c>*Result</c> factories, and runs the teardown.
/// </summary>
internal sealed partial class NativeAdminClient : IDisposable
{
    private static readonly SeedOffsetsFunction s_mockAdminClientUpdateBeginningOffsets =
        NativeMethods.MockAdminClientUpdateBeginningOffsets;

    private static readonly SeedOffsetsFunction s_mockAdminClientUpdateConsumerGroupOffsets =
        NativeMethods.MockAdminClientUpdateConsumerGroupOffsets;

    private static readonly SeedOffsetsFunction s_mockAdminClientUpdateEndOffsets =
        NativeMethods.MockAdminClientUpdateEndOffsets;

    private const int UnsetTimeoutMs = -1;

    private const string SyncDrainThreadName = "kafka-admin-client-thread";

    private const string AsyncDrainThreadName = "confluent-kafka-admin-pump";

    private const string CloseNegativeTimeoutMessage = "The timeout cannot be negative.";

    // One comparer for every ConfigResource-keyed map the submits and the Results build, so they agree on a key.
    private static readonly IEqualityComparer<ConfigResource> s_configResourceComparer =
        EqualityComparer<ConfigResource>.Default;

    private static readonly NativeMethods.AdminVoidCallback s_closeCompletion = OnCloseCompletion;

    private readonly SafeAdminClientHandle _handle;

    private readonly CompletionThread _completion;

    private readonly HandleUses _uses = new HandleUses();

    private readonly bool _isMock;
    private int _closed;

    private NativeAdminClient(SafeAdminClientHandle handle, bool isMock, AdminFlavour flavour)
    {
        _handle = handle;
        _isMock = isMock;
        _completion = new CompletionThread(
            flavour == AdminFlavour.Sync ? SyncDrainThreadName : AsyncDrainThreadName, DrainOf(handle, _uses));
        handle.ReleaseAfterDestroy(_completion.ReleaseNotifyOpaque);
    }

    internal static NativeAdminClient Create(
        IReadOnlyDictionary<string, string> config, AdminFlavour flavour = AdminFlavour.Sync)
    {
        if (config is null)
        {
            throw new ArgumentNullException(nameof(config));
        }

        foreach (KeyValuePair<string, string> entry in config)
        {
            if (entry.Value is null)
            {
                throw new ArgumentException(
                    $"Configuration value for key '{entry.Key}' must not be null.",
                    nameof(config));
            }
        }

        return Start(new SafeKafkaAdminClientHandle(NewAdminClient(config)), isMock: false, flavour);
    }

    internal static NativeAdminClient CreateMock(int numBrokers, AdminFlavour flavour = AdminFlavour.Sync)
    {
        MockAdminClientBuilderCore builder = MockAdminClientBuilderCore.Create();
        builder.NumBrokers(numBrokers);
        return StartMock(builder.Build(), flavour);
    }

    internal static NativeAdminClient StartMock(IntPtr mock, AdminFlavour flavour)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr view = NativeMethods.MockAdminClientAsAdmin(mock, ref outErr);
        if (outErr != IntPtr.Zero)
        {
            IntPtr destroyErr = IntPtr.Zero;
            NativeMethods.MockAdminClientDestroy(mock, ref destroyErr);
            FreeError(destroyErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }

        return Start(new SafeMockAdminClientHandle(view, mock), isMock: true, flavour);
    }

    private static IntPtr NewAdminClient(IReadOnlyDictionary<string, string> config)
    {
        List<Utf8Marshal.PinnedUtf8String> pins = new List<Utf8Marshal.PinnedUtf8String>(config.Count * 2);
        IntPtr map = IntPtr.Zero;
        IntPtr adminConfig = IntPtr.Zero;
        IntPtr outErr = IntPtr.Zero;
        try
        {
            map = NativeMethods.MapNew(ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            foreach (KeyValuePair<string, string> entry in config)
            {
                Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin(entry.Key);
                pins.Add(key);
                Utf8Marshal.PinnedUtf8String value = Utf8Marshal.Pin(entry.Value);
                pins.Add(value);
                NativeMethods.MapPut(map, key.Pointer, value.Pointer, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }

            adminConfig = NativeMethods.AdminClientConfigNew(map, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr admin = NativeMethods.AdminClientCreate(adminConfig, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return admin;
        }
        finally
        {
            IntPtr destroyErr = IntPtr.Zero;
            if (adminConfig != IntPtr.Zero)
            {
                NativeMethods.AdminClientConfigDestroy(adminConfig, ref destroyErr);
                FreeError(destroyErr);
                destroyErr = IntPtr.Zero;
            }

            foreach (Utf8Marshal.PinnedUtf8String pin in pins)
            {
                pin.Dispose();
            }

            if (map != IntPtr.Zero)
            {
                NativeMethods.MapDestroy(map, ref destroyErr);
                FreeError(destroyErr);
            }
        }
    }

    private static NativeAdminClient Start(SafeAdminClientHandle handle, bool isMock, AdminFlavour flavour)
    {
        NativeAdminClient client = new NativeAdminClient(handle, isMock, flavour);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.AdminSetCallbacksNotify(
                handle, CompletionThread.NotifyHook, client._completion.NotifyOpaque, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
        catch
        {
            Interlocked.Exchange(ref client._closed, 1);
            client.TearDown();
            throw;
        }

        return client;
    }

    // The drain holds the handle weakly so that a client never closed can still reach its finalizer and be leaked
    // with a warning.
    private static Func<int> DrainOf(SafeAdminClientHandle handle, HandleUses uses)
    {
        WeakReference<SafeAdminClientHandle> weak = new WeakReference<SafeAdminClientHandle>(handle);
        return () =>
        {
            if (!weak.TryGetTarget(out SafeAdminClientHandle? target) || !uses.TryEnterUnlessDestroyed())
            {
                return 0;
            }

            try
            {
                IntPtr outErr = IntPtr.Zero;
                int ran = NativeMethods.AdminExecuteCallbacks(target, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                return ran;
            }
            finally
            {
                if (uses.Exit())
                {
                    uses.Wake();
                }
            }
        };
    }

    private static void FreeError(IntPtr error)
    {
        if (error != IntPtr.Zero)
        {
            NativeMethods.ErrorDestroy(error);
        }
    }

    private static List<TopicPartition> DistinctPartitions(
        IReadOnlyCollection<TopicPartition>? partitions, string parameterName)
    {
        List<TopicPartition> selection = new List<TopicPartition>(partitions?.Count ?? 0);
        if (partitions is null)
        {
            return selection;
        }

        HashSet<TopicPartition> seen = new HashSet<TopicPartition>();
        foreach (TopicPartition partition in partitions)
        {
            if (partition.Topic is null)
            {
                throw new ArgumentException(
                    "The partitions must not contain a topic partition with a null topic.", parameterName);
            }

            AdminStrings.Validate(partition.Topic, parameterName);

            if (seen.Add(partition))
            {
                selection.Add(partition);
            }
        }

        return selection;
    }

    internal void Close() => CloseWithin(null);

    internal void CloseSync(TimeSpan timeout) => CloseWithin(CloseTimeoutMs(timeout));

    internal Task Close(CancellationToken cancellationToken) => CloseWithinAsync(null, cancellationToken);

    internal Task Close(TimeSpan timeout, CancellationToken cancellationToken) =>
        CloseWithinAsync(CloseTimeoutMs(timeout), cancellationToken);

    public void Dispose()
    {
        if (!TryBeginClose())
        {
            return;
        }

        try
        {
            if (InCallback)
            {
                CloseFromCallback(null);
            }
            else
            {
                CloseAndTearDown(null);
            }
        }
        catch (Exception)
        {
            // Dispose does not throw.
        }
    }

    internal async ValueTask DisposeAsync()
    {
        if (!TryBeginClose())
        {
            return;
        }

        try
        {
            if (InCallback)
            {
                CloseFromCallback(null);
            }
            else
            {
                await CloseAndTearDownAsync(null, CancellationToken.None).ConfigureAwait(false);
            }
        }
        catch (Exception)
        {
            // DisposeAsync does not throw.
        }
    }

    private static void OnCloseCompletion(IntPtr opaque)
    {
        try
        {
            GCHandle gcHandle = GCHandle.FromIntPtr(opaque);
            Action completion = (Action)gcHandle.Target!;
            gcHandle.Free();
            completion();
        }
        catch (Exception)
        {
            // Never unwind into native.
        }
    }

    private long CloseTimeoutMs(TimeSpan timeout)
    {
        if (timeout < TimeSpan.Zero && !_isMock)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), CloseNegativeTimeoutMessage);
        }

        return (long)timeout.TotalMilliseconds;
    }

    private void CloseWithin(long? timeoutMs)
    {
        if (!TryBeginClose())
        {
            return;
        }

        if (InCallback)
        {
            CloseFromCallback(timeoutMs);
            return;
        }

        CloseAndTearDown(timeoutMs);
    }

    private Task CloseWithinAsync(long? timeoutMs, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!TryBeginClose())
        {
            return Task.CompletedTask;
        }

        if (InCallback)
        {
            CloseFromCallback(timeoutMs);
            return Task.CompletedTask;
        }

        return CloseAndTearDownAsync(timeoutMs, cancellationToken);
    }

    // A mock's close does nothing, as Java's mock: only its teardown destroys it.
    private void SubmitClose(long? timeoutMs, Action onClosed)
    {
        if (_isMock)
        {
            onClosed();
            return;
        }

        GCHandle gcHandle = GCHandle.Alloc(onClosed);
        try
        {
            if (timeoutMs is long ms)
            {
                NativeMethods.AdminCloseWithTimeoutCb(_handle, ms, s_closeCompletion, GCHandle.ToIntPtr(gcHandle));
            }
            else
            {
                NativeMethods.AdminCloseCb(_handle, s_closeCompletion, GCHandle.ToIntPtr(gcHandle));
            }
        }
        catch
        {
            gcHandle.Free();
            throw;
        }
    }

    // On the drain thread the close cannot be waited for: the teardown runs after the drain, and the destroy waits for
    // the close and fires its completion.
    private void CloseFromCallback(long? timeoutMs)
    {
        SubmitClose(timeoutMs, () => { });
        _completion.RequestTeardown(TearDown);
    }

    private void CloseAndTearDown(long? timeoutMs)
    {
        Completion<bool> closed = new Completion<bool>(_ => { });
        try
        {
            SubmitClose(timeoutMs, () => closed.Complete(true));
            _ = closed.Wait(Timeout.InfiniteTimeSpan, out _);
        }
        finally
        {
            TearDown();
        }
    }

    // The token ends only the caller's wait: the close goes on in the core and the teardown runs when it ends.
    private async Task CloseAndTearDownAsync(long? timeoutMs, CancellationToken cancellationToken)
    {
        TaskCompletionSource<bool> closed =
            new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        try
        {
            SubmitClose(timeoutMs, () => closed.TrySetResult(true));
        }
        catch
        {
            TearDown();
            throw;
        }

        if (cancellationToken.CanBeCanceled)
        {
            TaskCompletionSource<bool> abandoned =
                new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            using (cancellationToken.Register(() => abandoned.TrySetResult(true)))
            {
                if (await Task.WhenAny(closed.Task, abandoned.Task).ConfigureAwait(false) != closed.Task)
                {
                    _ = closed.Task.ContinueWith(
                        _ => TearDownAfterAbandonedClose(),
                        CancellationToken.None,
                        TaskContinuationOptions.None,
                        TaskScheduler.Default);
                    throw new OperationCanceledException(cancellationToken);
                }
            }
        }

        await closed.Task.ConfigureAwait(false);
        TearDown();
    }

    private void TearDownAfterAbandonedClose()
    {
        try
        {
            TearDown();
        }
        catch (Exception)
        {
            // The caller stopped waiting; there is no one to report to.
        }
    }

    private void TearDown()
    {
        _completion.Dispose();
        _uses.WaitForNoneThenMarkDestroyed();
        _handle.CloseExplicitly();
    }

    // A negative timeout is sent as 0, as Java's calcDeadlineMs clamps it: the ABI reads a negative as unset.
    private static int ToNativeTimeoutMs(int? timeoutMs) =>
        timeoutMs is null ? UnsetTimeoutMs : Math.Max(0, timeoutMs.Value);

    private static List<string> DistinctNames(
        IReadOnlyCollection<string> names, string elementNoun, string parameterName)
    {
        List<string> keys = new List<string>(names.Count);
        HashSet<string> seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (string name in names)
        {
            if (name is null)
            {
                throw new ArgumentException(
                    $"The {elementNoun} must not contain a null element.", parameterName);
            }

            AdminStrings.Validate(name, parameterName);
            if (seen.Add(name))
            {
                keys.Add(name);
            }
        }

        return keys;
    }

    internal void Controller(Node controller)
    {
        ThrowIfClosed();
        if (controller is null)
        {
            throw new ArgumentException("The controller node must be in the list of brokers", nameof(controller));
        }

        ValidateNode(controller, nameof(controller));
        CallMock((mock, arguments) =>
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.MockAdminClientSetController(mock, arguments.Node(controller), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        });
    }

    internal void AddTopic(
        bool @internal,
        string name,
        IReadOnlyList<TopicPartitionInfo> partitions,
        IReadOnlyDictionary<string, string>? configs)
    {
        ThrowIfClosed();
        if (name is null)
        {
            throw new ArgumentNullException(nameof(name));
        }

        if (partitions is null)
        {
            throw new ArgumentNullException(nameof(partitions));
        }

        AdminStrings.Validate(name, nameof(name));
        TopicPartitionInfo[] partitionSnapshot = new TopicPartitionInfo[partitions.Count];
        for (int i = 0; i < partitionSnapshot.Length; i++)
        {
            TopicPartitionInfo partition = partitions[i]
                ?? throw new ArgumentException("The partitions must not contain a null element.", nameof(partitions));
            if (partition.Leader is not null)
            {
                ValidateNode(partition.Leader, nameof(partitions));
            }

            ValidateNodes(partition.Replicas, nameof(partitions));
            ValidateNodes(partition.InSyncReplicas, nameof(partitions));
            ValidateNodes(partition.Elr, nameof(partitions));
            ValidateNodes(partition.LastKnownElr, nameof(partitions));
            partitionSnapshot[i] = partition;
        }

        List<KeyValuePair<string, string>>? configSnapshot = null;
        if (configs is not null)
        {
            configSnapshot = new List<KeyValuePair<string, string>>(configs.Count);
            foreach (KeyValuePair<string, string> entry in configs)
            {
                if (entry.Key is null || entry.Value is null)
                {
                    throw new ArgumentException(
                        "The configs must not contain a null key or a null value.", nameof(configs));
                }

                AdminStrings.Validate(entry.Key, nameof(configs));
                AdminStrings.Validate(entry.Value, nameof(configs));
                configSnapshot.Add(entry);
            }
        }

        CallMock((mock, arguments) =>
        {
            IntPtr partitionList = arguments.List();
            foreach (TopicPartitionInfo partition in partitionSnapshot)
            {
                arguments.Add(partitionList, arguments.TopicPartitionInfo(partition));
            }

            IntPtr outErr = IntPtr.Zero;
            NativeMethods.MockAdminClientAddTopic(
                mock,
                @internal ? (sbyte)1 : (sbyte)0,
                arguments.Utf8(name),
                partitionList,
                configSnapshot is null ? IntPtr.Zero : arguments.StringMap(configSnapshot),
                ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        });
    }

    internal void MarkTopicForDeletion(string name)
    {
        ThrowIfClosed();
        if (name is null)
        {
            throw new ArgumentNullException(nameof(name));
        }

        AdminStrings.Validate(name, nameof(name));
        CallMock((mock, arguments) =>
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.MockAdminClientMarkTopicForDeletion(mock, arguments.Utf8(name), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        });
    }

    internal void TimeoutNextRequest(int numberOfRequest) =>
        CallMock((mock, arguments) =>
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.MockAdminClientTimeoutNextRequest(mock, numberOfRequest, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        });

    internal void UpdateBeginningOffsets(IReadOnlyDictionary<TopicPartition, long> newOffsets) =>
        SeedOffsets(newOffsets, s_mockAdminClientUpdateBeginningOffsets);

    internal void UpdateEndOffsets(IReadOnlyDictionary<TopicPartition, long> newOffsets) =>
        SeedOffsets(newOffsets, s_mockAdminClientUpdateEndOffsets);

    internal void UpdateConsumerGroupOffsets(IReadOnlyDictionary<TopicPartition, long> newOffsets) =>
        SeedOffsets(newOffsets, s_mockAdminClientUpdateConsumerGroupOffsets);

    internal void SetFetchesRemainingUntilVisible(string topicName, int fetchesRemainingUntilVisible)
    {
        ThrowIfClosed();
        if (topicName is null)
        {
            throw new ArgumentNullException(nameof(topicName));
        }

        AdminStrings.Validate(topicName, nameof(topicName));
        CallMock((mock, arguments) =>
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.MockAdminClientSetFetchesRemainingUntilVisible(
                mock, arguments.Utf8(topicName), fetchesRemainingUntilVisible, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        });
    }

    private delegate void SeedOffsetsFunction(IntPtr mock, IntPtr newOffsets, ref IntPtr outErr);

    private static void ValidateNode(Node node, string parameterName)
    {
        AdminStrings.Validate(node.Host, parameterName);
        AdminStrings.Validate(node.Rack, parameterName);
    }

    private static void ValidateNodes(IReadOnlyList<Node>? nodes, string parameterName)
    {
        if (nodes is null)
        {
            return;
        }

        foreach (Node node in nodes)
        {
            if (node is null)
            {
                throw new ArgumentException("The partitions must not contain a null node.", parameterName);
            }

            ValidateNode(node, parameterName);
        }
    }

    private void SeedOffsets(IReadOnlyDictionary<TopicPartition, long> newOffsets, SeedOffsetsFunction seed)
    {
        ThrowIfClosed();
        if (newOffsets is null)
        {
            throw new ArgumentNullException(nameof(newOffsets));
        }

        int count = newOffsets.Count;
        string[] topics = new string[count];
        int[] partitions = new int[count];
        long[] values = new long[count];
        int index = 0;
        foreach (KeyValuePair<TopicPartition, long> entry in newOffsets)
        {
            if (entry.Key.Topic is null)
            {
                throw new ArgumentException(
                    "The offsets map must not contain a topic partition with a null topic.", nameof(newOffsets));
            }

            AdminStrings.Validate(entry.Key.Topic, nameof(newOffsets));
            topics[index] = entry.Key.Topic;
            partitions[index] = entry.Key.Partition;
            values[index] = entry.Value;
            index++;
        }

        CallMock((mock, arguments) =>
        {
            IntPtr outErr = IntPtr.Zero;
            seed(mock, arguments.LongMap(topics, partitions, values, index), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        });
    }

    private void CallMock(Action<IntPtr, NativeCallArguments> call)
    {
        bool added = false;
        EnterUse();
        try
        {
            using NativeCallArguments arguments = new NativeCallArguments();
            call(_handle.DangerousAddRefClass(ref added), arguments);
        }
        finally
        {
            if (added)
            {
                _handle.DangerousRelease();
            }

            ExitUse();
        }
    }

    private bool InCallback => _completion.IsCurrentThread;

    private bool TryBeginClose() => Interlocked.CompareExchange(ref _closed, 1, 0) == 0;

    private void EnterUse()
    {
        _uses.Enter();
        if (Volatile.Read(ref _closed) != 0)
        {
            ExitUse();
            ThrowIfClosed();
        }
    }

    private void ExitUse()
    {
        if (_uses.Exit() && Volatile.Read(ref _closed) != 0)
        {
            _uses.Wake();
        }
    }

    private void ThrowIfClosed()
    {
        if (Volatile.Read(ref _closed) != 0)
        {
            throw new ObjectDisposedException(_isMock ? nameof(MockAdminClient) : nameof(KafkaAdminClient));
        }
    }
}
