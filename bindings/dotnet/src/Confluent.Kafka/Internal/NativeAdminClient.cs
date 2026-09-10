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
using System.Globalization;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The shared native surface behind <see cref="KafkaAdminClient"/> and
/// <see cref="MockAdminClient"/>: it owns the <see cref="SafeAdminHandle"/>, submits
/// every RPC, and orchestrates teardown. The two public clients differ only in how the
/// handle is constructed — the ABI hands back the <em>same</em>
/// <c>kafka_admin_AdminClient_t*</c> for both — so keeping the RPC surface here is what
/// stops it being written twice, exactly as <c>NativeConsumer</c> / <c>NativeProducer</c>
/// do for their pairs.
/// </summary>
/// <remarks>
/// <para>
/// <b>Every RPC drives the <c>_async</c> ABI entry point, even though the C# method is
/// synchronous.</b> That reads backwards at first glance and is deliberate: "synchronous"
/// describes the C# method's own return behaviour — it hands back a <c>*Result</c>
/// without waiting, as Java's non-blocking <c>Admin</c> methods do — and the
/// <c>_async</c> ABI is what makes that possible. The sync ABI twin blocks until every
/// per-key future resolves, so calling it would invent the blocking Java does not have,
/// and wrapping it in <c>Task.Run</c> would be sync-over-async (ffi §B7). The 46
/// synchronous ABI entry points are simply unused by this binding.
/// </para>
/// <para>
/// <b>No access guard, and none is wanted.</b> Unlike the consumer, the admin ABI
/// permits concurrent operations, so there is no single-op-in-flight assumption here and
/// no managed mirror of one. What each in-flight operation <em>does</em> hold is a
/// span-the-op reference on the client handle — see <see cref="AdminOperation"/>.
/// </para>
/// </remarks>
internal sealed class NativeAdminClient : IDisposable
{
    /// <summary>
    /// A negative <c>timeout_ms</c> means <b>unset</b> — the client default applies —
    /// and for <c>close</c> it means Java's no-argument <c>close()</c> (wait
    /// indefinitely). It does <b>not</b> mean a zero timeout, which is why a null
    /// <c>TimeSpan</c>/<c>int</c> maps here rather than to 0.
    /// </summary>
    private const int UnsetTimeoutMs = -1;

    /// <summary>
    /// Java's own <c>DescribeTopicsOptions.partitionSizeLimitPerResponse</c> default
    /// (<c>DescribeTopicsOptions.java:28</c>), used when the caller passes no options at
    /// all so that <c>options: null</c> behaves exactly like a fresh instance.
    /// </summary>
    private const int DefaultPartitionSizeLimitPerResponse = 2000;

    /// <summary>
    /// The comparer every <see cref="ConfigResource"/>-keyed bridge and result view is
    /// built with, so a lookup in a per-key map and a lookup in an aggregate can never
    /// disagree about a key.
    /// </summary>
    /// <remarks>
    /// <see cref="EqualityComparer{T}.Default"/> dispatches to
    /// <see cref="ConfigResource.Equals(object)"/> — type plus an ordinal name — which is
    /// exactly Java's <c>equals</c>. Naming it once here is what lets the submit, the
    /// bridge and the public result all be handed the <em>same</em> instance rather than
    /// each reaching for a default that could later diverge.
    /// </remarks>
    private static readonly IEqualityComparer<ConfigResource> s_configResourceComparer =
        EqualityComparer<ConfigResource>.Default;

    /// <summary>
    /// The comparer every <see cref="TopicPartitionReplica"/>-keyed bridge and result view
    /// is built with, for the same reason as <see cref="s_configResourceComparer"/>: it is a
    /// reference type with custom value equality, so a per-key map and an aggregate built
    /// with different comparers could disagree about whether a key is present.
    /// </summary>
    private static readonly IEqualityComparer<TopicPartitionReplica> s_replicaComparer =
        EqualityComparer<TopicPartitionReplica>.Default;

    private readonly SafeAdminHandle _handle;
    private int _closed;

    private NativeAdminClient(SafeAdminHandle handle)
    {
        _handle = handle;
    }

    /// <summary>
    /// The ABI shape of <c>create_topics_async</c>. A method-group reference to
    /// <see cref="NativeMethods.AdminClientCreateTopicsAsync"/> binds to it directly, so
    /// production passes the real P/Invoke while a test can pass a stand-in that
    /// captures <c>user_data</c> and drives the <em>production</em> trampoline at a
    /// moment of its choosing — the only way to make "an operation is in flight"
    /// deterministic without a broker or a sleep.
    /// </summary>
    internal delegate void NativeCreateTopicsSubmit(
        IntPtr admin,
        IntPtr[] topics,
        int count,
        int timeoutMs,
        bool validateOnly,
        bool retryOnQuotaViolation,
        AdminCallbacks.CreateTopicsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The owned client handle. Exposed for the interop tests, which read
    /// <see cref="SafeHandle.IsClosed"/> to observe when the native release actually
    /// happened; the public clients never expose it.
    /// </summary>
    /// <summary>
    /// The <c>delete_topics[_by_ids]_async</c> submit shape, injectable for the same
    /// reason as <see cref="NativeCreateTopicsSubmit"/>: "an operation is in flight" has
    /// to be a fact a test controls, not a race it hopes to win.
    /// </summary>
    internal delegate void NativeDeleteTopicsSubmit(
        IntPtr admin,
        IntPtr[] keys,
        int count,
        int timeoutMs,
        bool retryOnQuotaViolation,
        AdminCallbacks.DeleteTopicsCallback callback,
        IntPtr userData);

    /// <inheritdoc cref="NativeDeleteTopicsSubmit"/>
    internal delegate void NativeDescribeTopicsSubmit(
        IntPtr admin,
        IntPtr[] keys,
        int count,
        int timeoutMs,
        bool includeAuthorizedOperations,
        int partitionSizeLimitPerResponse,
        AdminCallbacks.DescribeTopicsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>list_topics_async</c> submit shape, injectable for the same reason as
    /// <see cref="NativeCreateTopicsSubmit"/>. ⚠ Note the absence of a key array — the
    /// keys are discovered from the response, which is why this RPC uses
    /// <see cref="SingleAdminOperation{TValue}"/> rather than the per-key bridge.
    /// </summary>
    internal delegate void NativeListTopicsSubmit(
        IntPtr admin,
        int timeoutMs,
        bool listInternal,
        AdminCallbacks.ListTopicsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>create_partitions_async</c> submit shape — <b>two</b> parallel arrays,
    /// injectable for the same reason as <see cref="NativeCreateTopicsSubmit"/>.
    /// </summary>
    internal delegate void NativeCreatePartitionsSubmit(
        IntPtr admin,
        IntPtr[] topics,
        IntPtr[] newPartitions,
        int count,
        int timeoutMs,
        bool validateOnly,
        bool retryOnQuotaViolation,
        AdminCallbacks.CreatePartitionsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>delete_records_async</c> submit shape — <b>three</b> parallel arrays,
    /// injectable for the same reason as <see cref="NativeCreateTopicsSubmit"/>.
    /// </summary>
    internal delegate void NativeDeleteRecordsSubmit(
        IntPtr admin,
        IntPtr[] topics,
        int[] partitions,
        long[] beforeOffsets,
        int count,
        int timeoutMs,
        AdminCallbacks.DeleteRecordsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>describe_cluster_async</c> submit shape, injectable for the same reason as
    /// <see cref="NativeCreateTopicsSubmit"/>. ⚠ No key array and no key type at all — the
    /// result is four attributes of one cluster (result shape 5), so this RPC uses
    /// <see cref="SingleAdminOperation{TValue}"/>.
    /// </summary>
    internal delegate void NativeDescribeClusterSubmit(
        IntPtr admin,
        int timeoutMs,
        bool includeAuthorizedOperations,
        bool includeFencedBrokers,
        AdminCallbacks.DescribeClusterCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>list_config_resources_async</c> submit shape, injectable for the same reason
    /// as <see cref="NativeCreateTopicsSubmit"/>. The <c>int[]</c> carries
    /// <c>ConfigResource.Type.id()</c> codes; a <c>count</c> of 0 is the legitimate
    /// "every supported type" request, not an error.
    /// </summary>
    internal delegate void NativeListConfigResourcesSubmit(
        IntPtr admin,
        int[] resourceTypes,
        int count,
        int timeoutMs,
        AdminCallbacks.ListConfigResourcesCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>list_client_metrics_resources_async</c> submit shape, injectable for the same
    /// reason as <see cref="NativeCreateTopicsSubmit"/>. No arrays at all.
    /// </summary>
    internal delegate void NativeListClientMetricsResourcesSubmit(
        IntPtr admin,
        int timeoutMs,
        AdminCallbacks.ListClientMetricsResourcesCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>describe_log_dirs_async</c> submit shape — <b>one</b> array of broker ids,
    /// injectable for the same reason as <see cref="NativeCreateTopicsSubmit"/>.
    /// </summary>
    internal delegate void NativeDescribeLogDirsSubmit(
        IntPtr admin,
        int[] brokers,
        int count,
        int timeoutMs,
        AdminCallbacks.DescribeLogDirsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>alter_replica_log_dirs_async</c> submit shape — <b>four</b> parallel arrays,
    /// injectable for the same reason as <see cref="NativeCreateTopicsSubmit"/>.
    /// </summary>
    internal delegate void NativeAlterReplicaLogDirsSubmit(
        IntPtr admin,
        IntPtr[] topics,
        int[] partitions,
        int[] brokerIds,
        IntPtr[] logDirs,
        int count,
        int timeoutMs,
        AdminCallbacks.AlterReplicaLogDirsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>describe_replica_log_dirs_async</c> submit shape — <b>three</b> parallel
    /// arrays, injectable for the same reason as <see cref="NativeCreateTopicsSubmit"/>.
    /// </summary>
    internal delegate void NativeDescribeReplicaLogDirsSubmit(
        IntPtr admin,
        IntPtr[] topics,
        int[] partitions,
        int[] brokerIds,
        int count,
        int timeoutMs,
        AdminCallbacks.DescribeReplicaLogDirsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>describe_configs_async</c> submit shape — <b>two</b> parallel arrays plus two
    /// booleans, injectable for the same reason as <see cref="NativeCreateTopicsSubmit"/>.
    /// </summary>
    internal delegate void NativeDescribeConfigsSubmit(
        IntPtr admin,
        int[] resourceTypes,
        IntPtr[] resourceNames,
        int count,
        int timeoutMs,
        bool includeSynonyms,
        bool includeDocumentation,
        AdminCallbacks.DescribeConfigsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>incremental_alter_configs_async</c> submit shape — <b>five</b> parallel
    /// arrays, one row per operation, injectable for the same reason as
    /// <see cref="NativeCreateTopicsSubmit"/>.
    /// </summary>
    internal delegate void NativeIncrementalAlterConfigsSubmit(
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
        IntPtr userData);

    internal SafeAdminHandle Handle => _handle;

    /// <summary>
    /// Creates a real admin client from a config map: each entry becomes an
    /// <c>AdminClientProperties_put</c> (keys are the Java dotted names), then
    /// <c>AdminClient_new</c> reads the properties. A construction failure surfaces as a
    /// <see cref="KafkaException"/>.
    /// </summary>
    /// <param name="config">Config keyed by Java dotted names; values are strings.</param>
    /// <exception cref="ArgumentNullException"><paramref name="config"/> is null.</exception>
    /// <exception cref="ArgumentException">A config value is null.</exception>
    /// <exception cref="KafkaException">The core rejected the configuration.</exception>
    internal static NativeAdminClient Create(IReadOnlyDictionary<string, string> config)
    {
        // Preconditions BEFORE any pin/marshal/P-Invoke (ffi §B5): the ABI does not
        // validate them and panics on violation (UB across FFI).
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

        SafeAdminHandle handle;
        IntPtr error;

        SafeAdminPropertiesHandle props = SafeAdminPropertiesHandle.Create();
        try
        {
            foreach (KeyValuePair<string, string> entry in config)
            {
                using Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin(entry.Key);
                using Utf8Marshal.PinnedUtf8String value = Utf8Marshal.Pin(entry.Value);
                NativeMethods.AdminClientPropertiesPut(props.DangerousGetHandle(), key.Pointer, value.Pointer);
            }

            // props is passed as the SafeHandle so the marshaller keeps it alive across
            // the call; the ABI does not consume it (freed below). The client handle
            // arrives ALREADY WRAPPED — the marshaller invokes SafeAdminHandle's private
            // ctor and sets the pointer atomically, closing the allocation-gap window.
            handle = NativeMethods.AdminClientNew(props, out error);
        }
        finally
        {
            // Header: the caller retains props ownership → free it after the call.
            props.Dispose();
        }

        KafkaException? failure = KafkaException.FromHandle(error);
        if (failure is not null)
        {
            // On the null native return the marshaller handed back an IsInvalid handle;
            // disposing it skips ReleaseHandle, so there is no spurious destroy.
            handle.Dispose();
            throw failure;
        }

        if (handle.IsInvalid)
        {
            // (null handle, null error) would be a core contract violation. Never store an
            // IsInvalid handle — every later call would hand native a null pointer.
            handle.Dispose();
            throw new KafkaException("kafka_admin_AdminClient_new returned a null handle without an error.");
        }

        return new NativeAdminClient(handle);
    }

    /// <summary>
    /// Creates a broker-less mock admin client. The ABI returns the same handle type as
    /// the real constructor, so the whole RPC surface works against it unchanged.
    /// </summary>
    /// <param name="numBrokers">The number of brokers to simulate; at least 1.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="numBrokers"/> is less than 1.</exception>
    /// <exception cref="KafkaException">The core could not create the mock.</exception>
    internal static NativeAdminClient CreateMock(int numBrokers)
    {
        // Validate BEFORE the call (ffi §B5). The ABI returns null for num_brokers < 1 —
        // Java's MockAdminClient.Builder.build() throw expressed in the FFI idiom — and a
        // null must be mapped, never dereferenced. Rejecting it here gives the caller the
        // .NET exception the mistake deserves instead of an opaque core error.
        if (numBrokers < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(numBrokers), numBrokers, "A mock admin client requires at least one broker.");
        }

        SafeAdminHandle handle = NativeMethods.MockAdminClientNew(numBrokers);
        if (handle.IsInvalid)
        {
            // Reachable only if the core could not create its tokio runtime — the guard
            // above already excluded the num_brokers case. There is no out_error on this
            // entry point, so the null return is all the ABI gives us.
            handle.Dispose();
            throw new KafkaException("kafka_admin_MockAdminClient_new returned a null handle.");
        }

        return new NativeAdminClient(handle);
    }

    /// <summary>
    /// Submits <c>createTopics</c> and returns immediately with one awaitable per topic
    /// (Java's non-blocking <c>createTopics</c>).
    /// </summary>
    /// <param name="newTopics">The topics to create.</param>
    /// <param name="options">Request options, or <see langword="null"/> for Java's defaults.</param>
    internal CreateTopicsResult CreateTopics(IEnumerable<NewTopic> newTopics, CreateTopicsOptions? options) =>
        CreateTopics(newTopics, options, NativeMethods.AdminClientCreateTopicsAsync);

    /// <summary>
    /// The <c>createTopics</c> submit, with the native call injectable. Production calls
    /// the overload above, which supplies the real P/Invoke; the interop tests supply a
    /// stand-in so the in-flight window is deterministic. Everything else — validation,
    /// the per-key sources, the <c>GCHandle</c>, the span-the-op reference, the input
    /// handles' lifetime — is the one production path either way, so a test cannot
    /// accidentally prove a property of its own fixture.
    /// </summary>
    internal CreateTopicsResult CreateTopics(
        IEnumerable<NewTopic> newTopics,
        CreateTopicsOptions? options,
        NativeCreateTopicsSubmit submit)
    {
        ThrowIfClosed();

        // ---- Preconditions, BEFORE any pin / marshal / P-Invoke (ffi §B5) ----
        if (newTopics is null)
        {
            throw new ArgumentNullException(nameof(newTopics));
        }

        int timeoutMs = UnsetTimeoutMs;
        bool validateOnly = false;
        bool retryOnQuotaViolation = true;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(CreateTopicsOptions));
            validateOnly = options.ValidateOnly;
            retryOnQuotaViolation = options.RetryOnQuotaViolation;
        }

        // Java keys its result on a Map and skips a repeated name (KafkaAdminClient
        // populates `topicFutures` only `if (!topicFutures.containsKey(...))`), so a
        // duplicate is one entry here too — and the request array is de-duplicated with
        // it, so the two sides cannot disagree about how many topics were asked for.
        List<NewTopic> requested = new List<NewTopic>();
        List<string> keys = new List<string>();
        HashSet<string> seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (NewTopic topic in newTopics)
        {
            if (topic is null)
            {
                throw new ArgumentException("The topics to create must not contain a null element.", nameof(newTopics));
            }

            if (topic.Configs is not null)
            {
                foreach (KeyValuePair<string, string> entry in topic.Configs)
                {
                    if (entry.Value is null)
                    {
                        throw new ArgumentException(
                            $"Configuration value for key '{entry.Key}' on topic '{topic.Name}' must not be null.",
                            nameof(newTopics));
                    }
                }
            }

            if (seen.Add(topic.Name))
            {
                requested.Add(topic);
                keys.Add(topic.Name);
            }
        }

        // ---- Publish everything the callback needs BEFORE the call ----
        // The header requires it, and the inline-callback path makes it real: the callback
        // can run on this very thread before the entry point returns.
        KeyedAdminOperation<string, TopicMetadataAndConfig> operation =
            new KeyedAdminOperation<string, TopicMetadataAndConfig>(
                "createTopics", keys, StringComparer.Ordinal);
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        // Deliberately EMPTY here and allocated inside the try. Everything between the
        // GCHandle allocation above and the try is a window in which a throw would root
        // the operation for the process lifetime, because neither the catch nor the
        // finally covers it — so the window is kept to nothing at all.
        // Array.Empty allocates nothing.
        IntPtr[] handles = Array.Empty<IntPtr>();
        try
        {
            handles = new IntPtr[requested.Count];

            // Span-the-op reference, INSIDE the try so a DangerousAddRef throw routes
            // through AbandonBeforeSubmit rather than rooting the GCHandle forever. It
            // keeps the native client alive from here until the completion callback
            // releases it — the binding's whole defence against the ABI's unguarded
            // AdminClient_destroy (see AdminOperation / SafeAdminHandle).
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            for (int i = 0; i < requested.Count; i++)
            {
                handles[i] = NewTopicMarshal.Build(requested[i]);
            }

            submit(
                _handle.DangerousGetHandle(),
                handles,
                handles.Length,
                timeoutMs,
                validateOnly,
                retryOnQuotaViolation,
                AdminCallbacks.CreateTopics,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            // Native never ran → the callback can never fire → we own the cleanup.
            // Idempotent, so it is harmless even in the (unreachable) case where an
            // inline callback already ran before the throw.
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            // The ABI copies out during the submit and "the caller retains ownership" of
            // the input entries, so they are destroyed here — after the call, on every
            // path, including a partially built array. Null-safe.
            foreach (IntPtr handle in handles)
            {
                NativeMethods.NewTopicDestroy(handle);
            }
        }

        return new CreateTopicsResult(operation.Tasks);
    }

    internal DeleteTopicsResult DeleteTopics(TopicCollection topics, DeleteTopicsOptions? options) =>
        DeleteTopics(
            topics,
            options,
            NativeMethods.AdminClientDeleteTopicsAsync,
            NativeMethods.AdminClientDeleteTopicsByIdsAsync);

    /// <summary>
    /// <b>Entry-point selection is the whole point of <see cref="TopicCollection"/>.</b>
    /// The ABI gives the two forms separate functions rather than a tagged input struct,
    /// so "names xor ids" cannot be violated; this switch is where the C# type's two
    /// inhabitants are mapped onto them.
    /// </summary>
    internal DeleteTopicsResult DeleteTopics(
        TopicCollection topics,
        DeleteTopicsOptions? options,
        NativeDeleteTopicsSubmit submitByName,
        NativeDeleteTopicsSubmit submitByIds)
    {
        ThrowIfClosed();

        if (topics is null)
        {
            throw new ArgumentNullException(nameof(topics));
        }

        int timeoutMs = UnsetTimeoutMs;
        bool retryOnQuotaViolation = true;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(DeleteTopicsOptions));
            retryOnQuotaViolation = options.RetryOnQuotaViolation;
        }

        switch (topics)
        {
            case TopicCollection.TopicNameCollection names:
                {
                    List<string> keys = DistinctNames(names.TopicNames(), nameof(topics));
                    VoidKeyedAdminOperation<string> operation = new VoidKeyedAdminOperation<string>(
                        "deleteTopics", keys, StringComparer.Ordinal);

                    Submit(
                        operation,
                        keys,
                        (admin, pinned, count, callbackUserData) => submitByName(
                            admin,
                            pinned,
                            count,
                            timeoutMs,
                            retryOnQuotaViolation,
                            AdminCallbacks.DeleteTopicsByName,
                            callbackUserData));

                    return DeleteTopicsResult.OfTopicNames(operation.Tasks, operation.KeyComparer);
                }

            case TopicCollection.TopicIdCollection ids:
                {
                    List<Uuid> keys = DistinctIds(ids.TopicIds());
                    VoidKeyedAdminOperation<Uuid> operation = new VoidKeyedAdminOperation<Uuid>(
                        "deleteTopics", keys, EqualityComparer<Uuid>.Default);

                    Submit(
                        operation,
                        ToBase64(keys),
                        (admin, pinned, count, callbackUserData) => submitByIds(
                            admin,
                            pinned,
                            count,
                            timeoutMs,
                            retryOnQuotaViolation,
                            AdminCallbacks.DeleteTopicsById,
                            callbackUserData));

                    return DeleteTopicsResult.OfTopicIds(operation.Tasks, operation.KeyComparer);
                }

            default:
                throw UnreachableCollection(nameof(topics));
        }
    }

    internal DescribeTopicsResult DescribeTopics(TopicCollection topics, DescribeTopicsOptions? options) =>
        DescribeTopics(
            topics,
            options,
            NativeMethods.AdminClientDescribeTopicsAsync,
            NativeMethods.AdminClientDescribeTopicsByIdsAsync);

    /// <inheritdoc cref="DeleteTopics(TopicCollection, DeleteTopicsOptions, NativeDeleteTopicsSubmit, NativeDeleteTopicsSubmit)"/>
    internal DescribeTopicsResult DescribeTopics(
        TopicCollection topics,
        DescribeTopicsOptions? options,
        NativeDescribeTopicsSubmit submitByName,
        NativeDescribeTopicsSubmit submitByIds)
    {
        ThrowIfClosed();

        if (topics is null)
        {
            throw new ArgumentNullException(nameof(topics));
        }

        int timeoutMs = UnsetTimeoutMs;
        bool includeAuthorizedOperations = false;
        int partitionSizeLimitPerResponse = DefaultPartitionSizeLimitPerResponse;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(DescribeTopicsOptions));

            if (options.PartitionSizeLimitPerResponse < 0)
            {
                // The ABI reads a negative as "keep Java's 2000 default"
                // (src/ffi/admin.rs:3712), so a negative would be silently reinterpreted
                // rather than honoured — the same reasoning as the timeout guard.
                throw new ArgumentOutOfRangeException(
                    nameof(options),
                    options.PartitionSizeLimitPerResponse,
                    "DescribeTopicsOptions.PartitionSizeLimitPerResponse must not be negative.");
            }

            includeAuthorizedOperations = options.IncludeAuthorizedOperations;
            partitionSizeLimitPerResponse = options.PartitionSizeLimitPerResponse;
        }

        switch (topics)
        {
            case TopicCollection.TopicNameCollection names:
                {
                    List<string> keys = DistinctNames(names.TopicNames(), nameof(topics));
                    KeyedAdminOperation<string, TopicDescription> operation =
                        new KeyedAdminOperation<string, TopicDescription>(
                            "describeTopics", keys, StringComparer.Ordinal);

                    Submit(
                        operation,
                        keys,
                        (admin, pinned, count, callbackUserData) => submitByName(
                            admin,
                            pinned,
                            count,
                            timeoutMs,
                            includeAuthorizedOperations,
                            partitionSizeLimitPerResponse,
                            AdminCallbacks.DescribeTopicsByName,
                            callbackUserData));

                    return DescribeTopicsResult.OfTopicNames(operation.Tasks, operation.KeyComparer);
                }

            case TopicCollection.TopicIdCollection ids:
                {
                    List<Uuid> keys = DistinctIds(ids.TopicIds());
                    KeyedAdminOperation<Uuid, TopicDescription> operation =
                        new KeyedAdminOperation<Uuid, TopicDescription>(
                            "describeTopics", keys, EqualityComparer<Uuid>.Default);

                    Submit(
                        operation,
                        ToBase64(keys),
                        (admin, pinned, count, callbackUserData) => submitByIds(
                            admin,
                            pinned,
                            count,
                            timeoutMs,
                            includeAuthorizedOperations,
                            partitionSizeLimitPerResponse,
                            AdminCallbacks.DescribeTopicsById,
                            callbackUserData));

                    return DescribeTopicsResult.OfTopicIds(operation.Tasks, operation.KeyComparer);
                }

            default:
                throw UnreachableCollection(nameof(topics));
        }
    }

    internal ListTopicsResult ListTopics(ListTopicsOptions? options) =>
        ListTopics(options, NativeMethods.AdminClientListTopicsAsync);

    /// <summary>
    /// Submits <c>listTopics</c> and returns immediately with the <b>single</b> awaitable
    /// Java's <c>ListTopicsResult</c> wraps (result shape 3).
    /// </summary>
    /// <remarks>
    /// ⚠ <b>No keys are pre-registered, because there are none to register.</b> The ABI
    /// entry point takes no key array — the topics are discovered from the response — so
    /// the per-key bridge, which builds one source per requested key before the submit,
    /// cannot express this RPC. <see cref="SingleAdminOperation{TValue}"/> reuses every
    /// rooting invariant and differs only in the completion payload.
    /// </remarks>
    internal ListTopicsResult ListTopics(ListTopicsOptions? options, NativeListTopicsSubmit submit)
    {
        ThrowIfClosed();

        int timeoutMs = UnsetTimeoutMs;
        bool listInternal = false;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(ListTopicsOptions));
            listInternal = options.ListInternal;
        }

        // ---- Publish everything the callback needs BEFORE the call ----
        SingleAdminOperation<IReadOnlyDictionary<string, TopicListing>> operation =
            new SingleAdminOperation<IReadOnlyDictionary<string, TopicListing>>("listTopics");
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);
        try
        {
            // Span-the-op reference, INSIDE the try so a DangerousAddRef throw routes
            // through AbandonBeforeSubmit rather than rooting the GCHandle forever.
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            submit(
                _handle.DangerousGetHandle(),
                timeoutMs,
                listInternal,
                AdminCallbacks.ListTopics,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            // Native never ran → the callback can never fire → we own the cleanup.
            operation.AbandonBeforeSubmit();
            throw;
        }

        return new ListTopicsResult(operation.Task);
    }

    internal CreatePartitionsResult CreatePartitions(
        IReadOnlyDictionary<string, NewPartitions> newPartitions, CreatePartitionsOptions? options) =>
        CreatePartitions(newPartitions, options, NativeMethods.AdminClientCreatePartitionsAsync);

    /// <summary>
    /// Submits <c>createPartitions</c> and returns immediately with one awaitable per
    /// topic. Java's <c>Map&lt;String, NewPartitions&gt;</c> becomes the ABI's two
    /// parallel arrays.
    /// </summary>
    /// <remarks>
    /// No de-duplication step: the input <em>is</em> a map, so its keys are already
    /// distinct — under the caller's own comparer, and therefore under the finer
    /// <see cref="StringComparer.Ordinal"/> the bridge keys by.
    /// </remarks>
    internal CreatePartitionsResult CreatePartitions(
        IReadOnlyDictionary<string, NewPartitions> newPartitions,
        CreatePartitionsOptions? options,
        NativeCreatePartitionsSubmit submit)
    {
        ThrowIfClosed();

        // ---- Preconditions, BEFORE any pin / marshal / P-Invoke (ffi §B5) ----
        if (newPartitions is null)
        {
            throw new ArgumentNullException(nameof(newPartitions));
        }

        int timeoutMs = UnsetTimeoutMs;
        bool validateOnly = false;
        bool retryOnQuotaViolation = true;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(CreatePartitionsOptions));
            validateOnly = options.ValidateOnly;
            retryOnQuotaViolation = options.RetryOnQuotaViolation;
        }

        List<string> keys = new List<string>(newPartitions.Count);
        List<NewPartitions> requested = new List<NewPartitions>(newPartitions.Count);
        foreach (KeyValuePair<string, NewPartitions> entry in newPartitions)
        {
            // The header requires `count` valid C strings and `count` valid entries, and
            // the ABI does not validate its own preconditions (ffi §B5). A null on either
            // side makes the ABI *skip that pair*, which would silently drop a topic the
            // caller asked for and leave its awaiter to FailUncompleted.
            if (entry.Key is null)
            {
                throw new ArgumentException(
                    "The new-partitions map must not contain a null topic name.", nameof(newPartitions));
            }

            if (entry.Value is null)
            {
                throw new ArgumentException(
                    $"The new-partitions entry for topic '{entry.Key}' must not be null.", nameof(newPartitions));
            }

            keys.Add(entry.Key);
            requested.Add(entry.Value);
        }

        VoidKeyedAdminOperation<string> operation =
            new VoidKeyedAdminOperation<string>("createPartitions", keys, StringComparer.Ordinal);
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        // Both deliberately EMPTY/null here and allocated inside the try, as CreateTopics
        // does: everything between the GCHandle allocation above and the try is a window
        // in which a throw would root the operation for the process lifetime, because
        // neither the catch nor the finally covers it.
        IntPtr[] handles = Array.Empty<IntPtr>();
        List<Utf8Marshal.PinnedUtf8String>? pinned = null;
        try
        {
            handles = new IntPtr[requested.Count];
            pinned = new List<Utf8Marshal.PinnedUtf8String>(keys.Count);

            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            IntPtr[] topics = new IntPtr[keys.Count];
            for (int i = 0; i < keys.Count; i++)
            {
                Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin(keys[i]);
                pinned.Add(key);
                topics[i] = key.Pointer;
            }

            for (int i = 0; i < requested.Count; i++)
            {
                handles[i] = NewPartitionsMarshal.Build(requested[i]);
            }

            submit(
                _handle.DangerousGetHandle(),
                topics,
                handles,
                handles.Length,
                timeoutMs,
                validateOnly,
                retryOnQuotaViolation,
                AdminCallbacks.CreatePartitions,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            // The ABI copies out during the submit and "the caller retains ownership" of
            // the input entries, so they are destroyed here — after the call, on every
            // path, including a partially built array. Null-safe.
            foreach (IntPtr handle in handles)
            {
                NativeMethods.NewPartitionsDestroy(handle);
            }

            // The key strings are pinned only for the call (ffi §A4's call-scoped rule):
            // the ABI copies them out during the submit.
            if (pinned is not null)
            {
                foreach (Utf8Marshal.PinnedUtf8String key in pinned)
                {
                    key.Dispose();
                }
            }
        }

        return new CreatePartitionsResult(operation.Tasks, operation.KeyComparer);
    }

    internal DeleteRecordsResult DeleteRecords(
        IReadOnlyDictionary<TopicPartition, RecordsToDelete> recordsToDelete,
        DeleteRecordsOptions? options) =>
        DeleteRecords(recordsToDelete, options, NativeMethods.AdminClientDeleteRecordsAsync);

    /// <summary>
    /// Submits <c>deleteRecords</c> and returns immediately with one awaitable per topic
    /// partition. Java's <c>Map&lt;TopicPartition, RecordsToDelete&gt;</c> becomes the
    /// ABI's three parallel arrays — <c>topics</c>, <c>partitions</c>,
    /// <c>before_offsets</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <c>RecordsToDelete</c> carries only an offset, so — unlike <c>NewPartitions</c> —
    /// it needs no input handle and nothing here has to be destroyed afterwards.
    /// </para>
    /// <para>
    /// An offset of <c>-1</c> is passed through unchanged: it is Java's documented
    /// "truncate to the high watermark", a <em>value</em> rather than an unset sentinel,
    /// so the negative-value guard that applies to timeouts deliberately does not apply
    /// here.
    /// </para>
    /// </remarks>
    internal DeleteRecordsResult DeleteRecords(
        IReadOnlyDictionary<TopicPartition, RecordsToDelete> recordsToDelete,
        DeleteRecordsOptions? options,
        NativeDeleteRecordsSubmit submit)
    {
        ThrowIfClosed();

        if (recordsToDelete is null)
        {
            throw new ArgumentNullException(nameof(recordsToDelete));
        }

        int timeoutMs = UnsetTimeoutMs;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(DeleteRecordsOptions));
        }

        List<TopicPartition> keys = new List<TopicPartition>(recordsToDelete.Count);
        int[] partitions = new int[recordsToDelete.Count];
        long[] beforeOffsets = new long[recordsToDelete.Count];
        int next = 0;
        foreach (KeyValuePair<TopicPartition, RecordsToDelete> entry in recordsToDelete)
        {
            // A `default(TopicPartition)` has a null Topic, and the header's "an entry
            // with a NULL topic is skipped" would silently drop it (ffi §B5).
            if (entry.Key.Topic is null)
            {
                throw new ArgumentException(
                    "The records-to-delete map must not contain a topic partition with a null topic.",
                    nameof(recordsToDelete));
            }

            if (entry.Value is null)
            {
                throw new ArgumentException(
                    $"The records-to-delete entry for '{entry.Key}' must not be null.",
                    nameof(recordsToDelete));
            }

            keys.Add(entry.Key);
            partitions[next] = entry.Key.Partition;
            beforeOffsets[next] = entry.Value.BeforeOffset();
            next++;
        }

        // EqualityComparer<TopicPartition>.Default dispatches to the struct's own
        // IEquatable implementation (ordinal on the topic), so it neither boxes nor
        // disagrees with the public DeleteRecordsResult view — the same reasoning as the
        // Uuid-keyed deleteTopics path.
        KeyedAdminOperation<TopicPartition, DeletedRecords> operation =
            new KeyedAdminOperation<TopicPartition, DeletedRecords>(
                "deleteRecords", keys, EqualityComparer<TopicPartition>.Default);
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        List<Utf8Marshal.PinnedUtf8String>? pinned = null;
        try
        {
            pinned = new List<Utf8Marshal.PinnedUtf8String>(keys.Count);

            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            IntPtr[] topics = new IntPtr[keys.Count];
            for (int i = 0; i < keys.Count; i++)
            {
                Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(keys[i].Topic);
                pinned.Add(topic);
                topics[i] = topic.Pointer;
            }

            // The blittable int[] / long[] are pinned by the interop marshaller for the
            // duration of the call; the ABI copies out during it (ffi §A4 call-scoped).
            submit(
                _handle.DangerousGetHandle(),
                topics,
                partitions,
                beforeOffsets,
                keys.Count,
                timeoutMs,
                AdminCallbacks.DeleteRecords,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            if (pinned is not null)
            {
                foreach (Utf8Marshal.PinnedUtf8String topic in pinned)
                {
                    topic.Dispose();
                }
            }
        }

        return new DeleteRecordsResult(operation.Tasks);
    }

    internal DescribeClusterResult DescribeCluster(DescribeClusterOptions? options) =>
        DescribeCluster(options, NativeMethods.AdminClientDescribeClusterAsync);

    /// <summary>
    /// Submits <c>describeCluster</c> and returns immediately with the four awaitables
    /// Java's <c>DescribeClusterResult</c> exposes (result shape 5).
    /// </summary>
    /// <remarks>
    /// ⚠ <b>One completion, four projections.</b> Java holds four independent
    /// <c>KafkaFuture</c> fields; the ABI settles the whole result together and has no
    /// <c>KafkaFuture</c> type with which to express independent timing, so the four public
    /// tasks derive from this single <see cref="SingleAdminOperation{TValue}"/> over an
    /// internal snapshot. The deviation is recorded on
    /// <see cref="DescribeClusterResult"/> (M15/P3 decision D12 — there is deliberately no
    /// public aggregate type).
    /// </remarks>
    internal DescribeClusterResult DescribeCluster(
        DescribeClusterOptions? options, NativeDescribeClusterSubmit submit)
    {
        ThrowIfClosed();

        int timeoutMs = UnsetTimeoutMs;
        bool includeAuthorizedOperations = false;
        bool includeFencedBrokers = false;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(DescribeClusterOptions));
            includeAuthorizedOperations = options.IncludeAuthorizedOperations;
            includeFencedBrokers = options.IncludeFencedBrokers;
        }

        // ---- Publish everything the callback needs BEFORE the call ----
        SingleAdminOperation<DescribeClusterSnapshot> operation =
            new SingleAdminOperation<DescribeClusterSnapshot>("describeCluster");
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);
        try
        {
            // Span-the-op reference, INSIDE the try so a DangerousAddRef throw routes
            // through AbandonBeforeSubmit rather than rooting the GCHandle forever.
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            submit(
                _handle.DangerousGetHandle(),
                timeoutMs,
                includeAuthorizedOperations,
                includeFencedBrokers,
                AdminCallbacks.DescribeCluster,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            // Native never ran → the callback can never fire → we own the cleanup.
            operation.AbandonBeforeSubmit();
            throw;
        }

        return new DescribeClusterResult(operation.Task);
    }

    internal ListConfigResourcesResult ListConfigResources(
        IReadOnlyCollection<ConfigResourceType>? configResourceTypes, ListConfigResourcesOptions? options) =>
        ListConfigResources(configResourceTypes, options, NativeMethods.AdminClientListConfigResourcesAsync);

    /// <summary>
    /// Submits <c>listConfigResources</c> and returns immediately with the <b>single</b>
    /// awaitable Java's <c>ListConfigResourcesResult</c> wraps (result sub-shape 3b).
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>An empty or absent type filter is the "every supported type" request and must
    /// NOT be rejected.</b> The header: "pass NULL or <c>count == 0</c> for Java's empty
    /// set, which means 'every supported type'", matching Java's no-argument
    /// <c>listConfigResources()</c>, which delegates with <c>Set.of()</c>
    /// (<c>Admin.java:1812</c>). So there is deliberately no emptiness guard and no
    /// <c>?? throw</c> here — either would turn Java's most common call into an error.
    /// </para>
    /// <para>
    /// The types are de-duplicated because Java's parameter is a <c>Set</c>. Request order
    /// is preserved among the survivors; it does not reach the result, whose entries the
    /// ABI sorts by <c>(type id, name)</c>.
    /// </para>
    /// </remarks>
    internal ListConfigResourcesResult ListConfigResources(
        IReadOnlyCollection<ConfigResourceType>? configResourceTypes,
        ListConfigResourcesOptions? options,
        NativeListConfigResourcesSubmit submit)
    {
        ThrowIfClosed();

        int timeoutMs = UnsetTimeoutMs;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(ListConfigResourcesOptions));
        }

        int[] resourceTypes = DistinctTypeIds(configResourceTypes);

        SingleAdminOperation<IReadOnlyCollection<ConfigResource>> operation =
            new SingleAdminOperation<IReadOnlyCollection<ConfigResource>>("listConfigResources");
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);
        try
        {
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            // The blittable int[] is pinned by the interop marshaller for the duration of
            // the call; the ABI copies out during it (ffi §A4 call-scoped).
            submit(
                _handle.DangerousGetHandle(),
                resourceTypes,
                resourceTypes.Length,
                timeoutMs,
                AdminCallbacks.ListConfigResources,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }

        return new ListConfigResourcesResult(operation.Task);
    }

#pragma warning disable CS0618 // Java deprecates this RPC and its three types; mirrored, not avoided.

    internal ListClientMetricsResourcesResult ListClientMetricsResources(
        ListClientMetricsResourcesOptions? options) =>
        ListClientMetricsResources(options, NativeMethods.AdminClientListClientMetricsResourcesAsync);

    /// <summary>
    /// Submits <c>listClientMetricsResources</c> and returns immediately with the
    /// <b>single</b> awaitable Java's <c>ListClientMetricsResourcesResult</c> wraps (result
    /// sub-shape 3b).
    /// </summary>
    /// <remarks>
    /// Java deprecates this RPC in favour of <c>listConfigResources</c> filtered to
    /// <c>CLIENT_METRICS</c> (<c>Admin.java:1821-1824</c>); it is bound for parity, and the
    /// deprecation is carried onto the public surface rather than dropped.
    /// </remarks>
    internal ListClientMetricsResourcesResult ListClientMetricsResources(
        ListClientMetricsResourcesOptions? options,
        NativeListClientMetricsResourcesSubmit submit)
    {
        ThrowIfClosed();

        int timeoutMs = UnsetTimeoutMs;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(ListClientMetricsResourcesOptions));
        }

        SingleAdminOperation<IReadOnlyCollection<ClientMetricsResourceListing>> operation =
            new SingleAdminOperation<IReadOnlyCollection<ClientMetricsResourceListing>>(
                "listClientMetricsResources");
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);
        try
        {
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            submit(
                _handle.DangerousGetHandle(),
                timeoutMs,
                AdminCallbacks.ListClientMetricsResources,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }

        return new ListClientMetricsResourcesResult(operation.Task);
    }

#pragma warning restore CS0618

    internal DescribeConfigsResult DescribeConfigs(
        IReadOnlyCollection<ConfigResource> resources, DescribeConfigsOptions? options) =>
        DescribeConfigs(resources, options, NativeMethods.AdminClientDescribeConfigsAsync);

    /// <summary>
    /// Submits <c>describeConfigs</c> and returns immediately with one awaitable per
    /// resource. Java's <c>Collection&lt;ConfigResource&gt;</c> becomes the ABI's two
    /// parallel arrays — <c>resource_types</c> (Java's <c>Type.id()</c> codes) and
    /// <c>resource_names</c>.
    /// </summary>
    /// <remarks>
    /// De-duplication mirrors Java, whose result is a <c>Map</c>, so a repeated resource is
    /// one entry — the same reasoning as <c>createTopics</c>. The null-element check is
    /// mandatory rather than defensive: the header says "an entry with a NULL name is
    /// skipped", silently, which would drop a resource whose <see cref="Task"/> the caller
    /// is holding (ffi §B5).
    /// </remarks>
    internal DescribeConfigsResult DescribeConfigs(
        IReadOnlyCollection<ConfigResource> resources,
        DescribeConfigsOptions? options,
        NativeDescribeConfigsSubmit submit)
    {
        ThrowIfClosed();

        // ---- Preconditions, BEFORE any pin / marshal / P-Invoke (ffi §B5) ----
        if (resources is null)
        {
            throw new ArgumentNullException(nameof(resources));
        }

        int timeoutMs = UnsetTimeoutMs;
        bool includeSynonyms = false;
        bool includeDocumentation = false;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(DescribeConfigsOptions));
            includeSynonyms = options.IncludeSynonyms;
            includeDocumentation = options.IncludeDocumentation;
        }

        List<ConfigResource> keys = DistinctResources(resources, nameof(resources));

        int[] resourceTypes = new int[keys.Count];
        for (int i = 0; i < keys.Count; i++)
        {
            resourceTypes[i] = (int)keys[i].Type;
        }

        KeyedAdminOperation<ConfigResource, Config> operation =
            new KeyedAdminOperation<ConfigResource, Config>(
                "describeConfigs", keys, s_configResourceComparer);
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        List<Utf8Marshal.PinnedUtf8String>? pinned = null;
        try
        {
            pinned = new List<Utf8Marshal.PinnedUtf8String>(keys.Count);

            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            IntPtr[] resourceNames = new IntPtr[keys.Count];
            for (int i = 0; i < keys.Count; i++)
            {
                Utf8Marshal.PinnedUtf8String name = Utf8Marshal.Pin(keys[i].Name);
                pinned.Add(name);
                resourceNames[i] = name.Pointer;
            }

            submit(
                _handle.DangerousGetHandle(),
                resourceTypes,
                resourceNames,
                keys.Count,
                timeoutMs,
                includeSynonyms,
                includeDocumentation,
                AdminCallbacks.DescribeConfigs,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            if (pinned is not null)
            {
                foreach (Utf8Marshal.PinnedUtf8String name in pinned)
                {
                    name.Dispose();
                }
            }
        }

        return new DescribeConfigsResult(operation.Tasks, operation.KeyComparer);
    }

    internal AlterConfigsResult IncrementalAlterConfigs(
        IReadOnlyDictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> configs,
        AlterConfigsOptions? options) =>
        IncrementalAlterConfigs(configs, options, NativeMethods.AdminClientIncrementalAlterConfigsAsync);

    /// <summary>
    /// Submits <c>incrementalAlterConfigs</c> and returns immediately with one awaitable
    /// per resource. Java's <c>Map&lt;ConfigResource, Collection&lt;AlterConfigOp&gt;&gt;</c>
    /// becomes the ABI's <b>five parallel arrays, one row per operation</b>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>Rows for one resource are emitted contiguously, in the caller's op order</b> —
    /// the header requires it ("rows naming the same resource are grouped in order"), and
    /// the flattening below walks the map resource-by-resource so two resources can never
    /// interleave.
    /// </para>
    /// <para>
    /// ⚠ <b>A null config value is passed through as a null pointer.</b> It is the value
    /// <see cref="AlterConfigOpType.Delete"/> uses, and the header names it. There is
    /// deliberately no <c>?? string.Empty</c> anywhere on this path: an empty value and an
    /// absent value are different requests.
    /// </para>
    /// <para>
    /// ⚠ <b>A null resource name or config name is rejected here</b>, because the ABI
    /// <em>silently skips</em> such a row — the caller would be left holding a
    /// <see cref="Task"/> for a resource the broker was never asked about
    /// (<c>FailUncompleted</c> would fault it, but with a far less useful message).
    /// </para>
    /// <para>
    /// ⚠⚠ <b>A resource mapped to an EMPTY operation collection completes successfully
    /// LOCALLY, and that is a recorded divergence — not a full fix</b>
    /// (<c>definition-of-done.md</c> §7; M15/P3 round 3, finding 69.6). Java keys its
    /// futures on the <em>resource collection</em>, which it sends alongside the ops map
    /// (<c>KafkaAdminClient.java:2889-2896</c>, <c>:2902</c>), so the broker <b>does</b> hear
    /// about a zero-op resource and answers for it. The ABI request is
    /// <b>row-flattened</b> — one row per operation — so a zero-op resource contributes no
    /// row and is <b>absent from the request entirely</b>
    /// (<c>src/ffi/admin.rs:4233-4260</c> builds the resource map from rows alone). There is
    /// no encoding for it: a row with a null config name is <em>skipped</em> by the ABI, and
    /// any non-null config name would be a real operation.
    /// </para>
    /// <para>
    /// <b>The root of the divergence is single and stated once: the resource is never
    /// sent, so any answer the broker would have given for it is lost.</b> Local completion
    /// therefore reproduces Java's outcome for a resource that exists and is authorized.
    /// Three instances where it does not, each independently checkable — this is a list of
    /// what was found, not a claim that nothing else follows from the root:
    /// </para>
    /// <list type="number">
    /// <item>
    /// <b>The resource does not exist.</b> Java's future fails; here it succeeds. Evidence
    /// that this is a real answer rather than a hypothetical: the Rust core checks the
    /// resource <em>before</em> applying any operation — <c>mock_admin_client.rs:630-636</c>
    /// resolves the topic and returns <c>UnknownTopicOrPartition</c> "No such topic as {name}"
    /// on the way to a no-op <c>apply_alter_ops</c> — so with a zero-op list the core would
    /// still fail it, exactly as Java does. Only the FFI encoding loses it.
    /// </item>
    /// <item>
    /// <b>Authorization fails for the resource.</b> Java sends it and surfaces the broker's
    /// per-resource authorization error; here nothing is asked, so it succeeds.
    /// </item>
    /// <item>
    /// <b><see cref="AlterConfigsOptions.ValidateOnly"/> is set.</b> This is the case a
    /// caller most plausibly reaches with an empty collection — "validate this resource,
    /// change nothing" — and it is the case local completion answers without validating
    /// anything.
    /// </item>
    /// </list>
    /// <para>
    /// Closing the root needs a way to express a zero-operation resource in the request,
    /// which is a <b>Rust-core (Mode-B) dependency</b> and is escalated as such rather than worked around further. The core
    /// already behaves correctly; only the row encoding cannot carry it. Faulting the
    /// awaitable instead was the shipped behaviour and was worse — it reported a defect for
    /// a call Java accepts — and rejecting the input outright is not open, because Java
    /// accepts it too.
    /// </para>
    /// </remarks>
    internal AlterConfigsResult IncrementalAlterConfigs(
        IReadOnlyDictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> configs,
        AlterConfigsOptions? options,
        NativeIncrementalAlterConfigsSubmit submit)
    {
        ThrowIfClosed();

        if (configs is null)
        {
            throw new ArgumentNullException(nameof(configs));
        }

        int timeoutMs = UnsetTimeoutMs;
        bool validateOnly = false;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(AlterConfigsOptions));
            validateOnly = options.ValidateOnly;
        }

        // ---- Flatten the map to one row per operation, grouped by resource ----
        List<ConfigResource> keys = new List<ConfigResource>(configs.Count);
        List<ConfigResource> rowResources = new List<ConfigResource>();
        List<AlterConfigOp> rowOps = new List<AlterConfigOp>();

        // ⚠ Resources the ABI request cannot carry. `configs` is a map, so each key appears
        // once, and every operation becomes exactly one row — so "the collection is empty"
        // IS "contributes no row". See the divergence note on this method.
        List<ConfigResource> keysWithNoRequest = new List<ConfigResource>();
        foreach (KeyValuePair<ConfigResource, IReadOnlyCollection<AlterConfigOp>> entry in configs)
        {
            if (entry.Key is null)
            {
                throw new ArgumentException(
                    "The configs map must not contain a null resource.", nameof(configs));
            }

            if (entry.Value is null)
            {
                throw new ArgumentException(
                    $"The operations for '{entry.Key}' must not be null.", nameof(configs));
            }

            // The header skips a row whose resource name or config name is NULL. Neither
            // can be null here: ConfigResource's and ConfigEntry's constructors both reject
            // a null name, so the guard lives there rather than being restated per row —
            // a second check would only shadow the one that actually runs.

            keys.Add(entry.Key);
            if (entry.Value.Count == 0)
            {
                keysWithNoRequest.Add(entry.Key);
            }

            foreach (AlterConfigOp op in entry.Value)
            {
                if (op is null)
                {
                    throw new ArgumentException(
                        $"The operations for '{entry.Key}' must not contain a null element.", nameof(configs));
                }

                rowResources.Add(entry.Key);
                rowOps.Add(op);
            }
        }

        int rowCount = rowOps.Count;
        int[] resourceTypes = new int[rowCount];
        int[] opTypes = new int[rowCount];
        for (int i = 0; i < rowCount; i++)
        {
            resourceTypes[i] = (int)rowResources[i].Type;
            opTypes[i] = (int)rowOps[i].OpType;
        }

        VoidKeyedAdminOperation<ConfigResource> operation =
            new VoidKeyedAdminOperation<ConfigResource>(
                "incrementalAlterConfigs", keys, s_configResourceComparer);

        // Registering these makes the completion resolve them successfully instead of
        // letting FailUncompleted fault them — see the divergence note above this method.
        operation.SetKeysWithNoRequest(keysWithNoRequest);

        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        List<Utf8Marshal.PinnedUtf8String>? pinned = null;
        try
        {
            pinned = new List<Utf8Marshal.PinnedUtf8String>(rowCount * 3);

            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            IntPtr[] resourceNames = new IntPtr[rowCount];
            IntPtr[] configNames = new IntPtr[rowCount];
            IntPtr[] configValues = new IntPtr[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                Utf8Marshal.PinnedUtf8String resourceName = Utf8Marshal.Pin(rowResources[i].Name);
                pinned.Add(resourceName);
                resourceNames[i] = resourceName.Pointer;

                Utf8Marshal.PinnedUtf8String configName = Utf8Marshal.Pin(rowOps[i].ConfigEntry.Name);
                pinned.Add(configName);
                configNames[i] = configName.Pointer;

                // ⚠ A null value stays a NULL POINTER — it is DELETE's null value, and the
                // ABI documents it as such. No `?? string.Empty` here, ever.
                string? value = rowOps[i].ConfigEntry.Value;
                if (value is null)
                {
                    configValues[i] = IntPtr.Zero;
                }
                else
                {
                    Utf8Marshal.PinnedUtf8String configValue = Utf8Marshal.Pin(value);
                    pinned.Add(configValue);
                    configValues[i] = configValue.Pointer;
                }
            }

            submit(
                _handle.DangerousGetHandle(),
                resourceTypes,
                resourceNames,
                configNames,
                configValues,
                opTypes,
                rowCount,
                timeoutMs,
                validateOnly,
                AdminCallbacks.IncrementalAlterConfigs,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            if (pinned is not null)
            {
                foreach (Utf8Marshal.PinnedUtf8String value in pinned)
                {
                    value.Dispose();
                }
            }
        }

        return new AlterConfigsResult(operation.Tasks, operation.KeyComparer);
    }

    internal DescribeLogDirsResult DescribeLogDirs(
        IReadOnlyCollection<int> brokers, DescribeLogDirsOptions? options) =>
        DescribeLogDirs(brokers, options, NativeMethods.AdminClientDescribeLogDirsAsync);

    /// <summary>
    /// Submits <c>describeLogDirs</c> and returns immediately with one awaitable per broker.
    /// Java's <c>Collection&lt;Integer&gt;</c> becomes the ABI's single broker-id array.
    /// </summary>
    /// <remarks>
    /// De-duplication mirrors Java, whose result is a <c>Map</c> keyed by broker id, so a
    /// repeated broker is one entry. There is no null-element check because the element type
    /// is <see cref="int"/> — there is no null to reject, which is also why this is the one
    /// Stage-3 input with no silent-skip hazard.
    /// </remarks>
    internal DescribeLogDirsResult DescribeLogDirs(
        IReadOnlyCollection<int> brokers,
        DescribeLogDirsOptions? options,
        NativeDescribeLogDirsSubmit submit)
    {
        ThrowIfClosed();

        if (brokers is null)
        {
            throw new ArgumentNullException(nameof(brokers));
        }

        int timeoutMs = UnsetTimeoutMs;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(DescribeLogDirsOptions));
        }

        List<int> keys = new List<int>(brokers.Count);
        HashSet<int> seen = new HashSet<int>();
        foreach (int broker in brokers)
        {
            if (seen.Add(broker))
            {
                keys.Add(broker);
            }
        }

        KeyedAdminOperation<int, IReadOnlyDictionary<string, LogDirDescription>> operation =
            new KeyedAdminOperation<int, IReadOnlyDictionary<string, LogDirDescription>>(
                "describeLogDirs", keys, EqualityComparer<int>.Default);
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);
        try
        {
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            // The blittable int[] is pinned by the interop marshaller for the duration of
            // the call; the ABI copies out during it (ffi §A4 call-scoped).
            submit(
                _handle.DangerousGetHandle(),
                keys.ToArray(),
                keys.Count,
                timeoutMs,
                AdminCallbacks.DescribeLogDirs,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }

        return new DescribeLogDirsResult(operation.Tasks);
    }

    internal AlterReplicaLogDirsResult AlterReplicaLogDirs(
        IReadOnlyDictionary<TopicPartitionReplica, string> replicaAssignment,
        AlterReplicaLogDirsOptions? options) =>
        AlterReplicaLogDirs(
            replicaAssignment, options, NativeMethods.AdminClientAlterReplicaLogDirsAsync);

    /// <summary>
    /// Submits <c>alterReplicaLogDirs</c> and returns immediately with one awaitable per
    /// replica. Java's <c>Map&lt;TopicPartitionReplica, String&gt;</c> becomes the ABI's four
    /// parallel arrays.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>§9.1 item 9 (KEY-SET vs ROW-SET) was checked here BEFORE the RPC was written,
    /// and the zero-row shape CANNOT arise.</b> The finding it exists for (69.6) needed a
    /// <c>Map&lt;K, Collection&lt;V&gt;&gt;</c>, where a key can map to an empty collection
    /// and so flatten to no rows. This map is <c>K → V</c>: <b>every key carries exactly one
    /// value and therefore produces exactly one row</b>, so <c>count</c> always equals the
    /// key count and no key can vanish from the request. Nothing is completed locally here,
    /// and no divergence arises.
    /// </para>
    /// <para>
    /// ⚠ <b>A null topic or null log directory is rejected here</b>, because the ABI
    /// <em>silently skips</em> such a row — the caller would be left holding a
    /// <see cref="Task"/> for a replica the broker was never asked about. That is the one
    /// way a key could still lose its row, and it is turned into an
    /// <see cref="ArgumentException"/> naming the entry rather than a late
    /// <c>FailUncompleted</c> message (ffi §B5).
    /// </para>
    /// </remarks>
    internal AlterReplicaLogDirsResult AlterReplicaLogDirs(
        IReadOnlyDictionary<TopicPartitionReplica, string> replicaAssignment,
        AlterReplicaLogDirsOptions? options,
        NativeAlterReplicaLogDirsSubmit submit)
    {
        ThrowIfClosed();

        if (replicaAssignment is null)
        {
            throw new ArgumentNullException(nameof(replicaAssignment));
        }

        int timeoutMs = UnsetTimeoutMs;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(AlterReplicaLogDirsOptions));
        }

        List<TopicPartitionReplica> keys = new List<TopicPartitionReplica>(replicaAssignment.Count);
        List<string> logDirs = new List<string>(replicaAssignment.Count);
        foreach (KeyValuePair<TopicPartitionReplica, string> entry in replicaAssignment)
        {
            if (entry.Key is null)
            {
                throw new ArgumentException(
                    "The replica assignment must not contain a null replica.", nameof(replicaAssignment));
            }

            // The ABI skips a row whose log dir is NULL, which would silently drop this
            // replica. The topic cannot be null — TopicPartitionReplica's constructor
            // rejects that — so the guard lives there and is not restated here.
            if (entry.Value is null)
            {
                throw new ArgumentException(
                    $"The log directory for '{entry.Key}' must not be null.", nameof(replicaAssignment));
            }

            keys.Add(entry.Key);
            logDirs.Add(entry.Value);
        }

        VoidKeyedAdminOperation<TopicPartitionReplica> operation =
            new VoidKeyedAdminOperation<TopicPartitionReplica>(
                "alterReplicaLogDirs", keys, s_replicaComparer);
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        List<Utf8Marshal.PinnedUtf8String>? pinned = null;
        try
        {
            pinned = new List<Utf8Marshal.PinnedUtf8String>(keys.Count * 2);

            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            IntPtr[] topics = new IntPtr[keys.Count];
            int[] partitions = new int[keys.Count];
            int[] brokerIds = new int[keys.Count];
            IntPtr[] directories = new IntPtr[keys.Count];
            for (int i = 0; i < keys.Count; i++)
            {
                Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(keys[i].Topic);
                pinned.Add(topic);
                topics[i] = topic.Pointer;
                partitions[i] = keys[i].Partition;
                brokerIds[i] = keys[i].BrokerId;

                Utf8Marshal.PinnedUtf8String directory = Utf8Marshal.Pin(logDirs[i]);
                pinned.Add(directory);
                directories[i] = directory.Pointer;
            }

            submit(
                _handle.DangerousGetHandle(),
                topics,
                partitions,
                brokerIds,
                directories,
                keys.Count,
                timeoutMs,
                AdminCallbacks.AlterReplicaLogDirs,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            if (pinned is not null)
            {
                foreach (Utf8Marshal.PinnedUtf8String value in pinned)
                {
                    value.Dispose();
                }
            }
        }

        return new AlterReplicaLogDirsResult(operation.Tasks, operation.KeyComparer);
    }

    internal DescribeReplicaLogDirsResult DescribeReplicaLogDirs(
        IReadOnlyCollection<TopicPartitionReplica> replicas, DescribeReplicaLogDirsOptions? options) =>
        DescribeReplicaLogDirs(
            replicas, options, NativeMethods.AdminClientDescribeReplicaLogDirsAsync);

    /// <summary>
    /// Submits <c>describeReplicaLogDirs</c> and returns immediately with one awaitable per
    /// replica. Java's <c>Collection&lt;TopicPartitionReplica&gt;</c> becomes the ABI's three
    /// parallel arrays.
    /// </summary>
    /// <remarks>
    /// <para>
    /// De-duplication mirrors Java, whose result is a <c>Map</c>, so a repeated replica is
    /// one entry. The topic cannot be null (<see cref="TopicPartitionReplica"/>'s
    /// constructor rejects it), so the ABI's "an entry with a NULL topic is skipped" cannot
    /// be reached through this surface.
    /// </para>
    /// <para>
    /// ⚠ <b>The result count is NOT guaranteed to equal the request count, and the honest
    /// outcome for a missing key is a FAULT.</b> Java's real client pre-registers a future
    /// per requested replica and completes each one — defaulting to an empty
    /// <c>ReplicaLogDirInfo</c> when the broker said nothing about it
    /// (<c>KafkaAdminClient.java:3103-3106</c> seeds <c>replicaDirInfoByPartition</c>, and
    /// <c>:3155-3160</c> completes every entry) — and the <b>Rust core does the same</b>
    /// (<c>src/admin/kafka_admin_client.rs:3705-3708</c> inserts a future for every
    /// requested replica). So against a real client every requested key gets an entry and
    /// <c>FailUncompleted</c> never fires.
    /// </para>
    /// <para>
    /// The <b>mock</b> is the exception: it omits replicas of unknown topics outright
    /// (<c>src/admin/mock_admin_client.rs:1352-1355</c>), so a broker-less test can reach
    /// the missing-key path. There <c>FailUncompleted</c> faults that key with a message
    /// naming it. That is deliberately <b>not</b> smoothed over by completing locally with a
    /// default: unlike the Stage-2 zero-op case, the key here is genuinely sent and the
    /// answer genuinely absent, so fabricating an "empty" description would report data the
    /// binding does not have — the same reasoning that keeps
    /// <see cref="LogDirDescription"/> free of a faked <c>IsCordoned</c>.
    /// </para>
    /// </remarks>
    internal DescribeReplicaLogDirsResult DescribeReplicaLogDirs(
        IReadOnlyCollection<TopicPartitionReplica> replicas,
        DescribeReplicaLogDirsOptions? options,
        NativeDescribeReplicaLogDirsSubmit submit)
    {
        ThrowIfClosed();

        if (replicas is null)
        {
            throw new ArgumentNullException(nameof(replicas));
        }

        int timeoutMs = UnsetTimeoutMs;
        if (options is not null)
        {
            timeoutMs = ValidateTimeoutMs(options.TimeoutMs, nameof(DescribeReplicaLogDirsOptions));
        }

        List<TopicPartitionReplica> keys = new List<TopicPartitionReplica>(replicas.Count);
        HashSet<TopicPartitionReplica> seen = new HashSet<TopicPartitionReplica>(s_replicaComparer);
        foreach (TopicPartitionReplica replica in replicas)
        {
            if (replica is null)
            {
                throw new ArgumentException(
                    "The replicas must not contain a null element.", nameof(replicas));
            }

            if (seen.Add(replica))
            {
                keys.Add(replica);
            }
        }

        KeyedAdminOperation<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo> operation =
            new KeyedAdminOperation<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo>(
                "describeReplicaLogDirs", keys, s_replicaComparer);
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        List<Utf8Marshal.PinnedUtf8String>? pinned = null;
        try
        {
            pinned = new List<Utf8Marshal.PinnedUtf8String>(keys.Count);

            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            IntPtr[] topics = new IntPtr[keys.Count];
            int[] partitions = new int[keys.Count];
            int[] brokerIds = new int[keys.Count];
            for (int i = 0; i < keys.Count; i++)
            {
                Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(keys[i].Topic);
                pinned.Add(topic);
                topics[i] = topic.Pointer;
                partitions[i] = keys[i].Partition;
                brokerIds[i] = keys[i].BrokerId;
            }

            submit(
                _handle.DangerousGetHandle(),
                topics,
                partitions,
                brokerIds,
                keys.Count,
                timeoutMs,
                AdminCallbacks.DescribeReplicaLogDirs,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            if (pinned is not null)
            {
                foreach (Utf8Marshal.PinnedUtf8String topic in pinned)
                {
                    topic.Dispose();
                }
            }
        }

        return new DescribeReplicaLogDirsResult(operation.Tasks, operation.KeyComparer);
    }

    /// <summary>
    /// De-duplicates the requested config resources, preserving request order, and rejects
    /// a null element before it can reach the ABI.
    /// </summary>
    /// <remarks>
    /// De-duplication mirrors Java, whose result is a <c>Map</c>. The null check is
    /// mandatory: the header skips an entry with a NULL name silently.
    /// </remarks>
    private static List<ConfigResource> DistinctResources(
        IReadOnlyCollection<ConfigResource> resources, string parameterName)
    {
        List<ConfigResource> keys = new List<ConfigResource>(resources.Count);
        HashSet<ConfigResource> seen = new HashSet<ConfigResource>(s_configResourceComparer);
        foreach (ConfigResource resource in resources)
        {
            if (resource is null)
            {
                throw new ArgumentException("The resources must not contain a null element.", parameterName);
            }

            if (seen.Add(resource))
            {
                keys.Add(resource);
            }
        }

        return keys;
    }

    /// <summary>
    /// De-duplicates the requested config-resource types into the ABI's
    /// <c>ConfigResource.Type.id()</c> array, preserving request order.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>A null or empty input yields an empty array, and that is a valid request</b> —
    /// Java's <c>Set.of()</c>, which the header maps to "every supported type". De-dup
    /// mirrors Java's <c>Set</c> parameter. There is no null-element check because the
    /// element type is an enum, which has no null.
    /// </remarks>
    private static int[] DistinctTypeIds(IReadOnlyCollection<ConfigResourceType>? types)
    {
        if (types is null || types.Count == 0)
        {
            return Array.Empty<int>();
        }

        List<int> ids = new List<int>(types.Count);
        HashSet<ConfigResourceType> seen = new HashSet<ConfigResourceType>();
        foreach (ConfigResourceType type in types)
        {
            if (seen.Add(type))
            {
                ids.Add((int)type);
            }
        }

        return ids.ToArray();
    }

    /// <summary>
    /// Validates and converts Java's <c>close(Duration)</c> timeout, then closes. Shared
    /// by both public clients so the guard and the millisecond conversion exist once.
    /// </summary>
    /// <param name="timeout">
    /// How long to wait for the background task. <see cref="TimeSpan.Zero"/> is valid.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is negative.</exception>
    internal Task Close(TimeSpan timeout)
    {
        // Validate BEFORE the native call (ffi §B5). A negative timeout must not simply
        // be forwarded: the ABI reads a negative timeout_ms as "wait indefinitely", so
        // passing one through would turn a caller mistake into an unbounded wait.
        if (timeout < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), timeout, "Timeout must not be negative.");
        }

        return Close((long)timeout.TotalMilliseconds);
    }

    /// <summary>
    /// Closes the client, awaiting the background task for up to
    /// <paramref name="timeoutMs"/> (negative = wait indefinitely, Java's no-argument
    /// <c>close()</c>), then releases the handle. Idempotent: a second call is a no-op.
    /// </summary>
    /// <exception cref="KafkaException">The core reported a close failure.</exception>
    internal async Task Close(long timeoutMs)
    {
        if (!TryBeginClose())
        {
            // A prior teardown already won the latch — closing again would double-close.
            return;
        }

        try
        {
            await CloseInternal(timeoutMs).ConfigureAwait(false);
        }
        finally
        {
            // Requests ReleaseHandle → AdminClient_destroy. It runs when the reference
            // count reaches zero, which is immediately when nothing is in flight and
            // deferred to the last in-flight operation's callback otherwise.
            _handle.Dispose();
        }
    }

    /// <summary>
    /// The graceful asynchronous teardown: <c>close_async</c> (which joins the
    /// background task) then the handle release. Unlike <see cref="Close(long)"/> a close
    /// failure is swallowed — <c>DisposeAsync</c> must not throw out of a
    /// <c>using</c> block during unwinding.
    /// </summary>
    internal async ValueTask DisposeAsync()
    {
        if (!TryBeginClose())
        {
            return;
        }

        try
        {
            await CloseInternal(UnsetTimeoutMs).ConfigureAwait(false);
        }
        catch (KafkaException)
        {
            // Teardown: surfacing a close failure from DisposeAsync would replace whatever
            // exception is already unwinding. Close(TimeSpan) is the surface that reports it.
        }
        finally
        {
            _handle.Dispose();
        }
    }

    /// <summary>
    /// The blocking teardown fallback: the <b>synchronous</b> <c>AdminClient_close</c>
    /// with Java's no-argument <c>close()</c> semantics, then the handle release.
    /// </summary>
    /// <remarks>
    /// The sync ABI is called directly — no <c>Task.Run</c>, no
    /// <c>GetAwaiter().GetResult()</c>. The wait happens inside the core's own
    /// multi-thread runtime, so the calling thread simply parks; that is the shipped
    /// sync-op precedent, not the sync-over-async this binding forbids. The handle is
    /// passed as the <see cref="SafeHandle"/> so the marshaller holds a call-scoped
    /// reference for the whole blocking call.
    /// </remarks>
    public void Dispose()
    {
        if (!TryBeginClose())
        {
            return;
        }

        try
        {
            NativeMethods.AdminClientClose(_handle, UnsetTimeoutMs);
        }
        finally
        {
            _handle.Dispose();
        }
    }

    /// <summary>
    /// Bridges <c>close_async</c> to a <see cref="Task"/> via the shared void completion
    /// bridge (admin's one genuinely single-awaiter operation, so it reuses
    /// <see cref="OperationCompletionSource"/> rather than the per-key
    /// <see cref="KeyedAdminOperation{TKey, TValue}"/>).
    /// </summary>
    private Task CloseInternal(long timeoutMs)
    {
        OperationCompletionSource context = new OperationCompletionSource();
        GCHandle gcHandle = GCHandle.Alloc(context, GCHandleType.Normal);
        context.SetGcHandle(gcHandle);
        try
        {
            // Span-the-op reference, inside the try for the same reason as in
            // CreateTopics: an AddRef throw must route through AbandonBeforeSubmit.
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                context.SetHandleRef(_handle);
            }

            NativeMethods.AdminClientCloseAsync(
                _handle.DangerousGetHandle(), timeoutMs, AdminCallbacks.Close, GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            context.AbandonBeforeSubmit();
            throw;
        }

        return context.Task;
    }

    /// <summary>
    /// Wins the one-shot teardown latch, so exactly one of
    /// <see cref="Dispose"/> / <see cref="DisposeAsync"/> / <see cref="Close(long)"/>
    /// performs the close and the handle release.
    /// </summary>
    /// <summary>
    /// Rejects a negative <c>TimeoutMs</c> before any native call (ffi §B5) and maps
    /// <see langword="null"/> onto the ABI's "unset" sentinel.
    /// </summary>
    /// <remarks>
    /// The ABI reads a negative <c>timeout_ms</c> as <em>unset</em>, so a negative would
    /// silently mean "use the client default" rather than the timeout asked for — the
    /// caller would never learn their value was discarded. Shared by every RPC so the
    /// three options types cannot drift apart on the rule or on its message.
    /// </remarks>
    private static int ValidateTimeoutMs(int? timeoutMs, string optionsTypeName)
    {
        if (timeoutMs is < 0)
        {
            throw new ArgumentOutOfRangeException(
                "options",
                timeoutMs,
                string.Format(
                    CultureInfo.InvariantCulture,
                    "{0}.TimeoutMs must not be negative; leave it null to use the client default.",
                    optionsTypeName));
        }

        return timeoutMs ?? UnsetTimeoutMs;
    }

    /// <summary>
    /// De-duplicates the requested topic names, preserving request order, and rejects a
    /// null element before it can reach the ABI.
    /// </summary>
    /// <remarks>
    /// De-duplication mirrors Java, whose result is a <c>Map</c>, so a repeated key is one
    /// entry — the same reasoning as <c>CreateTopics</c>. The null-element check is
    /// mandatory rather than defensive: the header requires "<c>count</c> valid C
    /// strings", and the ABI does not validate its own preconditions (ffi §B5). Java's
    /// <c>TopicCollection.ofTopicNames</c> accepts a null element and fails later, so the
    /// check lives at the submit rather than in the collection's factory.
    /// </remarks>
    private static List<string> DistinctNames(IReadOnlyCollection<string> names, string parameterName)
    {
        List<string> keys = new List<string>(names.Count);
        HashSet<string> seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (string name in names)
        {
            if (name is null)
            {
                throw new ArgumentException("The topic names must not contain a null element.", parameterName);
            }

            if (seen.Add(name))
            {
                keys.Add(name);
            }
        }

        return keys;
    }

    /// <inheritdoc cref="DistinctNames"/>
    private static List<Uuid> DistinctIds(IReadOnlyCollection<Uuid> ids)
    {
        List<Uuid> keys = new List<Uuid>(ids.Count);
        HashSet<Uuid> seen = new HashSet<Uuid>();
        foreach (Uuid id in ids)
        {
            // No null check: Uuid is a value type, so there is no null element to reject.
            if (seen.Add(id))
            {
                keys.Add(id);
            }
        }

        return keys;
    }

    /// <summary>
    /// The out-half of the base64 topic-id round trip: the by-id entry points take
    /// <c>const char *const *</c> base64 strings (Java's <c>Uuid.toString()</c> form), not
    /// binary UUIDs, and the header states "result keys are the same base64" — which is
    /// what lets the completion parse them straight back into <see cref="Uuid"/> keys.
    /// </summary>
    private static List<string> ToBase64(List<Uuid> ids)
    {
        List<string> text = new List<string>(ids.Count);
        foreach (Uuid id in ids)
        {
            text.Add(id.ToString());
        }

        return text;
    }

    /// <summary>
    /// The shared submit sequence for a keyed admin RPC: root the operation, take the
    /// span-the-op client reference, pin the keys, call native, unpin.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The order is load-bearing. The <c>GCHandle</c> and the
    /// <see cref="System.Runtime.InteropServices.SafeHandle.DangerousAddRef(ref bool)"/>
    /// are both published <b>before</b> the P/Invoke because the callback can fire
    /// <em>inside</em> it — the header requires everything the callback needs to be
    /// published before the call, not after. The reference is released by the completion
    /// (<c>AdminOperation.FreeGcHandle</c>), which is what makes a <c>Dispose</c> racing
    /// an in-flight operation defer <c>AdminClient_destroy</c> instead of freeing the
    /// client under it.
    /// </para>
    /// <para>
    /// The key strings are pinned only for the call (ffi §A4's call-scoped rule): the ABI
    /// copies them out during the submit, so nothing native holds them afterwards. The
    /// <c>finally</c> unpins on every path, including the inline-callback one — which has
    /// already run to completion by the time the P/Invoke returns.
    /// </para>
    /// </remarks>
    private void Submit(
        AdminOperation operation,
        List<string> keys,
        Action<IntPtr, IntPtr[], int, IntPtr> submit)
    {
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        // Allocated INSIDE the try, for the same reason CreateTopics allocates its handle
        // array there: everything between the GCHandle allocation above and the try is a
        // window in which a throw would root the operation for the process lifetime,
        // because neither the catch nor the finally covers it — so the window is kept to
        // nothing at all. Declaring the local null allocates nothing; the finally
        // null-checks precisely because the allocation itself is now inside the try.
        List<Utf8Marshal.PinnedUtf8String>? pinned = null;
        try
        {
            pinned = new List<Utf8Marshal.PinnedUtf8String>(keys.Count);

            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            IntPtr[] pointers = new IntPtr[keys.Count];
            for (int i = 0; i < keys.Count; i++)
            {
                Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin(keys[i]);
                pinned.Add(key);
                pointers[i] = key.Pointer;
            }

            submit(_handle.DangerousGetHandle(), pointers, pointers.Length, GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            if (pinned is not null)
            {
                foreach (Utf8Marshal.PinnedUtf8String key in pinned)
                {
                    key.Dispose();
                }
            }
        }
    }

    /// <summary>
    /// The exhaustiveness arm for a <see cref="TopicCollection"/> switch. Unreachable by
    /// construction — the outer constructor is private and both subclasses are sealed, so
    /// there is no third inhabitant — but C# cannot see that, so the arm names the
    /// invariant rather than being a bare <c>default</c>.
    /// </summary>
    private static ArgumentException UnreachableCollection(string parameterName) =>
        new ArgumentException(
            "The topic collection must come from TopicCollection.OfTopicNames or TopicCollection.OfTopicIds.",
            parameterName);

    private bool TryBeginClose() => Interlocked.Exchange(ref _closed, 1) == 0;

    /// <summary>The use-after-dispose guard for every RPC.</summary>
    private void ThrowIfClosed()
    {
        if (Volatile.Read(ref _closed) != 0)
        {
            throw new ObjectDisposedException(nameof(NativeAdminClient));
        }
    }
}
