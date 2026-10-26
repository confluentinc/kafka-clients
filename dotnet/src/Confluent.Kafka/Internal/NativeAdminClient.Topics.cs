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

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

internal sealed partial class NativeAdminClient
{
    private static readonly RpcWithOptions s_adminDescribeClusterWithOptions =
        NativeMethods.AdminDescribeClusterWithOptions;

    private static readonly RpcWithOptions s_adminListTopicsWithOptions = NativeMethods.AdminListTopicsWithOptions;

    private static readonly ResultFuture s_createTopicsValues = NativeMethods.CreateTopicsResultValues;

    private static readonly ResultFuture s_deleteTopicsTopicIdValues = NativeMethods.DeleteTopicsResultTopicIdValues;

    private static readonly ResultFuture s_deleteTopicsTopicNameValues =
        NativeMethods.DeleteTopicsResultTopicNameValues;

    private static readonly ResultFuture s_describeClusterAuthorizedOperations =
        NativeMethods.DescribeClusterResultAuthorizedOperations;

    private static readonly ResultFuture s_describeClusterClusterId = NativeMethods.DescribeClusterResultClusterId;

    private static readonly ResultFuture s_describeClusterController = NativeMethods.DescribeClusterResultController;

    private static readonly ResultFuture s_describeClusterNodes = NativeMethods.DescribeClusterResultNodes;

    private static readonly ResultFuture s_describeTopicsTopicIdValues =
        NativeMethods.DescribeTopicsResultTopicIdValues;

    private static readonly ResultFuture s_describeTopicsTopicNameValues =
        NativeMethods.DescribeTopicsResultTopicNameValues;

    private static readonly ResultFuture s_listTopicsNamesToListings = NativeMethods.ListTopicsResultNamesToListings;

    private static readonly AdminOptionsMarshal.New s_newListTopicsOptions = NativeMethods.ListTopicsOptionsNew;

    private static readonly AdminOptionsMarshal.Set<ListTopicsOptions> s_setListTopicsOption = SetListTopicsOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyListTopicsOptions =
        NativeMethods.ListTopicsOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyListTopicsResult =
        NativeMethods.ListTopicsResultDestroy;

    private static readonly Func<IntPtr, IReadOnlyDictionary<string, TopicListing>> s_copyOutTopicListings =
        TopicListingMarshal.CopyOutMap;

    private static readonly AdminOptionsMarshal.New s_newCreateTopicsOptions = NativeMethods.CreateTopicsOptionsNew;

    private static readonly AdminOptionsMarshal.Set<CreateTopicsOptions> s_setCreateTopicsOption = SetCreateTopicsOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyCreateTopicsOptions =
        NativeMethods.CreateTopicsOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyCreateTopicsResult =
        NativeMethods.CreateTopicsResultDestroy;

    private static readonly AdminOptionsMarshal.New s_newDeleteTopicsOptions = NativeMethods.DeleteTopicsOptionsNew;

    private static readonly AdminOptionsMarshal.Set<DeleteTopicsOptions> s_setDeleteTopicsOption = SetDeleteTopicsOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyDeleteTopicsOptions =
        NativeMethods.DeleteTopicsOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyDeleteTopicsResult =
        NativeMethods.DeleteTopicsResultDestroy;

    private static readonly AdminOptionsMarshal.New s_newDescribeTopicsOptions = NativeMethods.DescribeTopicsOptionsNew;

    private static readonly AdminOptionsMarshal.Set<DescribeTopicsOptions> s_setDescribeTopicsOption =
        SetDescribeTopicsOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyDescribeTopicsOptions =
        NativeMethods.DescribeTopicsOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyDescribeTopicsResult =
        NativeMethods.DescribeTopicsResultDestroy;

    private static readonly Func<IntPtr, TopicDescription> s_copyOutTopicDescription = TopicDescriptionMarshal.CopyOut;

    private static readonly Func<IntPtr, string> s_stringKey = static key => Utf8Marshal.PtrToString(key) ?? string.Empty;

    private static readonly Func<IntPtr, Uuid> s_uuidKey = UuidMarshal.CopyOut;

    private static readonly Func<IntPtr, object?> s_noValue = static _ => null;

    private static readonly AdminOptionsMarshal.New s_newDescribeClusterOptions = NativeMethods.DescribeClusterOptionsNew;

    private static readonly AdminOptionsMarshal.Set<DescribeClusterOptions> s_setDescribeClusterOption =
        SetDescribeClusterOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyDescribeClusterOptions =
        NativeMethods.DescribeClusterOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyDescribeClusterResult =
        NativeMethods.DescribeClusterResultDestroy;

    private static readonly Func<IntPtr, IReadOnlyCollection<Node>> s_copyOutNodes = DescribeClusterMarshal.CopyOutNodes;

    private static readonly Func<IntPtr, Node?> s_copyOutController = DescribeClusterMarshal.CopyOutController;

    private static readonly Func<IntPtr, string> s_copyOutClusterId = DescribeClusterMarshal.CopyOutClusterId;

    private static readonly Func<IntPtr, IReadOnlyCollection<AclOperation>?> s_copyOutAuthorizedOperations =
        DescribeClusterMarshal.CopyOutAuthorizedOperations;

    private delegate IntPtr RpcWithOptions(SafeAdminClientHandle admin, IntPtr options, ref IntPtr outErr);

    private delegate IntPtr ResultFuture(IntPtr result, ref IntPtr outErr);

    private delegate IntPtr KeyedResultFuture(IntPtr result, IntPtr key, ref IntPtr outErr);

    internal CreateTopicsRefinements CreateTopics(IEnumerable<NewTopic> newTopics, CreateTopicsOptions? options)
    {
        EnterUse();
        try
        {
            List<NewTopic> topics = RequestedTopics(newTopics);
            IntPtr[] natives = new IntPtr[topics.Count];
            IntPtr list = IntPtr.Zero;
            SafeAdminResultHandle result;
            try
            {
                for (int i = 0; i < natives.Length; i++)
                {
                    natives[i] = NewNativeTopic(topics[i]);
                }

                list = NewList(natives);
                result = CallWithOptions(
                    NewCreateTopicsOptions(options),
                    s_destroyCreateTopicsOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminCreateTopicsWithOptions(admin, list, nativeOptions, ref outErr),
                    s_destroyCreateTopicsResult);
            }
            finally
            {
                DestroyList(list);
                foreach (IntPtr native in natives)
                {
                    DestroyNewTopic(native);
                }
            }

            return new CreateTopicsRefinements(
                result,
                StartMapFutures(
                    result, s_createTopicsValues, s_stringKey, StringComparer.Ordinal, s_noValue));
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewCreateTopicsOptions(CreateTopicsOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newCreateTopicsOptions, options, options?.Setters, s_setCreateTopicsOption, s_destroyCreateTopicsOptions);

    internal static void FreeCreateTopicsOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyCreateTopicsOptions);

    private static List<NewTopic> RequestedTopics(IEnumerable<NewTopic> newTopics)
    {
        if (newTopics is null)
        {
            throw new ArgumentNullException(nameof(newTopics));
        }

        List<NewTopic> topics = new List<NewTopic>();
        foreach (NewTopic topic in newTopics)
        {
            if (topic is null)
            {
                throw new ArgumentException("The topics to create must not contain a null element.", nameof(newTopics));
            }

            AdminStrings.Validate(topic.Name, nameof(newTopics));
            if (topic.Configs is not null)
            {
                foreach (KeyValuePair<string, string> config in topic.Configs)
                {
                    AdminStrings.Validate(config.Key, nameof(newTopics));
                    if (config.Value is null)
                    {
                        throw new ArgumentException(
                            "The topics to create must not contain a null configuration value.", nameof(newTopics));
                    }

                    AdminStrings.Validate(config.Value, nameof(newTopics));
                }
            }

            topics.Add(topic);
        }

        return topics;
    }

    internal static IntPtr NewNativeTopic(NewTopic topic)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr native;
        using (Utf8Marshal.PinnedUtf8String name = Utf8Marshal.Pin(topic.Name))
        {
            native = topic.ReplicasAssignments is null
                ? NativeMethods.NewTopicWithNumPartitionsReplicationFactor(
                    name.Pointer, topic.NumPartitions ?? -1, topic.ReplicationFactor ?? (short)-1, ref outErr)
                : NewAssignedTopic(name.Pointer, topic.ReplicasAssignments, ref outErr);
        }

        KafkaException.ThrowIfFfiFailed(ref outErr);
        try
        {
            if (topic.Configs is not null)
            {
                SetConfigs(native, topic.Configs);
            }
        }
        catch
        {
            DestroyNewTopic(native);
            throw;
        }

        return native;
    }

    private static IntPtr NewAssignedTopic(
        IntPtr name, IReadOnlyDictionary<int, IReadOnlyList<int>> assignments, ref IntPtr outErr)
    {
        int count = assignments.Count;
        foreach (KeyValuePair<int, IReadOnlyList<int>> assignment in assignments)
        {
            count += assignment.Value.Count;
        }

        int[] ids = new int[count];
        GCHandle pinned = GCHandle.Alloc(ids, GCHandleType.Pinned);
        List<IntPtr> lists = new List<IntPtr>(assignments.Count);
        IntPtr map = IntPtr.Zero;
        try
        {
            IntPtr first = pinned.AddrOfPinnedObject();
            IntPtr buildErr = IntPtr.Zero;
            map = NativeMethods.MapNew(ref buildErr);
            KafkaException.ThrowIfFfiFailed(ref buildErr);
            int next = 0;
            foreach (KeyValuePair<int, IReadOnlyList<int>> assignment in assignments)
            {
                int partition = next++;
                ids[partition] = assignment.Key;
                IntPtr brokers = NativeMethods.ListNew(ref buildErr);
                KafkaException.ThrowIfFfiFailed(ref buildErr);
                lists.Add(brokers);
                foreach (int broker in assignment.Value)
                {
                    ids[next] = broker;
                    NativeMethods.ListAdd(brokers, IntPtr.Add(first, next * sizeof(int)), ref buildErr);
                    KafkaException.ThrowIfFfiFailed(ref buildErr);
                    next++;
                }

                NativeMethods.MapPut(map, IntPtr.Add(first, partition * sizeof(int)), brokers, ref buildErr);
                KafkaException.ThrowIfFfiFailed(ref buildErr);
            }

            return NativeMethods.NewTopicWithReplicasAssignments(name, map, ref outErr);
        }
        finally
        {
            DestroyMap(map);
            foreach (IntPtr brokers in lists)
            {
                DestroyList(brokers);
            }

            pinned.Free();
        }
    }

    private static void SetConfigs(IntPtr native, IReadOnlyDictionary<string, string> configs)
    {
        List<Utf8Marshal.PinnedUtf8String> pins = new List<Utf8Marshal.PinnedUtf8String>(configs.Count * 2);
        IntPtr map = IntPtr.Zero;
        try
        {
            IntPtr outErr = IntPtr.Zero;
            map = NativeMethods.MapNew(ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            foreach (KeyValuePair<string, string> config in configs)
            {
                Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin(config.Key);
                pins.Add(key);
                Utf8Marshal.PinnedUtf8String value = Utf8Marshal.Pin(config.Value);
                pins.Add(value);
                NativeMethods.MapPut(map, key.Pointer, value.Pointer, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }

            NativeMethods.NewTopicSetConfigs(native, map, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
        finally
        {
            DestroyMap(map);
            foreach (Utf8Marshal.PinnedUtf8String pin in pins)
            {
                pin.Dispose();
            }
        }
    }

    private static void SetCreateTopicsOption(IntPtr native, CreateTopicsOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(CreateTopicsOptions.TimeoutMs):
                NativeMethods.CreateTopicsOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            case nameof(CreateTopicsOptions.ValidateOnly):
                NativeMethods.CreateTopicsOptionsSetValidateOnly(native, options.ValidateOnly ? (sbyte)1 : (sbyte)0, ref outErr);
                break;
            case nameof(CreateTopicsOptions.RetryOnQuotaViolation):
                NativeMethods.CreateTopicsOptionsSetRetryOnQuotaViolation(
                    native, options.RetryOnQuotaViolation ? (sbyte)1 : (sbyte)0, ref outErr);
                break;
            default:
                throw new InvalidOperationException("CreateTopicsOptions has no C setter for " + setter + ".");
        }
    }

    private static Dictionary<TKey, KafkaFutureImpl<TValue>> StartMapFutures<TKey, TValue>(
        SafeAdminResultHandle result,
        ResultFuture accessor,
        Func<IntPtr, TKey> readKey,
        IEqualityComparer<TKey> comparer,
        Func<IntPtr, TValue> convert)
        where TKey : notnull
    {
        try
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr raw = accessor(result.DangerousGetHandle(), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            using SafeKafkaMapHandle map = new SafeKafkaMapHandle(raw);
            int count = NativeMethods.MapSize(map.DangerousGetHandle(), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            Dictionary<TKey, KafkaFutureImpl<TValue>> futures = new Dictionary<TKey, KafkaFutureImpl<TValue>>(count, comparer);
            for (int i = 0; i < count; i++)
            {
                IntPtr key = NativeMethods.MapKey(map.DangerousGetHandle(), i, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                IntPtr future = NativeMethods.MapValue(map.DangerousGetHandle(), i, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                futures[readKey(key)] = NativeFutureSource<TValue>.Start(SafeKafkaFutureHandle.Borrowed(future, map), convert);
            }

            return futures;
        }
        catch
        {
            result.Dispose();
            throw;
        }
    }

    private static IntPtr NewList(IntPtr[] elements)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr list = NativeMethods.ListNew(ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        try
        {
            foreach (IntPtr element in elements)
            {
                NativeMethods.ListAdd(list, element, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }
        }
        catch
        {
            DestroyList(list);
            throw;
        }

        return list;
    }

    private static void DestroyList(IntPtr list)
    {
        if (list == IntPtr.Zero)
        {
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ListDestroy(list, ref outErr);
        ReleaseError(outErr);
    }

    private static void DestroyMap(IntPtr map)
    {
        if (map == IntPtr.Zero)
        {
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        NativeMethods.MapDestroy(map, ref outErr);
        ReleaseError(outErr);
    }

    internal static void DestroyNewTopic(IntPtr topic)
    {
        if (topic == IntPtr.Zero)
        {
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        NativeMethods.NewTopicDestroy(topic, ref outErr);
        ReleaseError(outErr);
    }

    private static void ReleaseError(IntPtr error)
    {
        if (error != IntPtr.Zero)
        {
            NativeMethods.ErrorDestroy(error);
        }
    }

    internal (KafkaFutureImpl<IReadOnlyDictionary<string, TopicListing>> NamesToListings, SafeAdminResultHandle Result)
        ListTopics(ListTopicsOptions? options)
    {
        EnterUse();
        try
        {
            SafeAdminResultHandle result = CallWithOptions(
                NewListTopicsOptions(options),
                s_destroyListTopicsOptions,
                s_adminListTopicsWithOptions,
                s_destroyListTopicsResult);
            return (StartOwnedFuture(result, s_listTopicsNamesToListings, s_copyOutTopicListings), result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewListTopicsOptions(ListTopicsOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newListTopicsOptions, options, options?.Setters, s_setListTopicsOption, s_destroyListTopicsOptions);

    internal static void FreeListTopicsOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyListTopicsOptions);

    private static void SetListTopicsOption(IntPtr native, ListTopicsOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(ListTopicsOptions.TimeoutMs):
                NativeMethods.ListTopicsOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            case nameof(ListTopicsOptions.ListInternal):
                NativeMethods.ListTopicsOptionsSetListInternal(native, options.ListInternal ? (sbyte)1 : (sbyte)0, ref outErr);
                break;
            default:
                throw new InvalidOperationException("ListTopicsOptions has no C setter for " + setter + ".");
        }
    }

    internal (
        KafkaFutureImpl<IReadOnlyCollection<Node>> Nodes,
        KafkaFutureImpl<Node?> Controller,
        KafkaFutureImpl<string> ClusterId,
        KafkaFutureImpl<IReadOnlyCollection<AclOperation>?> AuthorizedOperations,
        SafeAdminResultHandle Result) DescribeCluster(DescribeClusterOptions? options)
    {
        EnterUse();
        try
        {
            SafeAdminResultHandle result = CallWithOptions(
                NewDescribeClusterOptions(options),
                s_destroyDescribeClusterOptions,
                s_adminDescribeClusterWithOptions,
                s_destroyDescribeClusterResult);
            KafkaFutureImpl<IReadOnlyCollection<Node>> nodes =
                StartOwnedFuture(result, s_describeClusterNodes, s_copyOutNodes);
            KafkaFutureImpl<Node?> controller =
                StartOwnedFuture(result, s_describeClusterController, s_copyOutController);
            KafkaFutureImpl<string> clusterId =
                StartOwnedFuture(result, s_describeClusterClusterId, s_copyOutClusterId);
            KafkaFutureImpl<IReadOnlyCollection<AclOperation>?> authorizedOperations = StartOwnedFuture(
                result, s_describeClusterAuthorizedOperations, s_copyOutAuthorizedOperations);
            return (nodes, controller, clusterId, authorizedOperations, result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewDescribeClusterOptions(DescribeClusterOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newDescribeClusterOptions,
            options,
            options?.Setters,
            s_setDescribeClusterOption,
            s_destroyDescribeClusterOptions);

    internal static void FreeDescribeClusterOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyDescribeClusterOptions);

    private static void SetDescribeClusterOption(
        IntPtr native, DescribeClusterOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(DescribeClusterOptions.TimeoutMs):
                NativeMethods.DescribeClusterOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            case nameof(DescribeClusterOptions.IncludeAuthorizedOperations):
                NativeMethods.DescribeClusterOptionsSetIncludeAuthorizedOperations(
                    native, options.IncludeAuthorizedOperations ? (sbyte)1 : (sbyte)0, ref outErr);
                break;
            case nameof(DescribeClusterOptions.IncludeFencedBrokers):
                NativeMethods.DescribeClusterOptionsSetIncludeFencedBrokers(
                    native, options.IncludeFencedBrokers ? (sbyte)1 : (sbyte)0, ref outErr);
                break;
            default:
                throw new InvalidOperationException("DescribeClusterOptions has no C setter for " + setter + ".");
        }
    }

    internal (
        IReadOnlyDictionary<string, KafkaFutureImpl<object?>>? ByName,
        IReadOnlyDictionary<Uuid, KafkaFutureImpl<object?>>? ById,
        SafeAdminResultHandle Result) DeleteTopics(TopicCollection? topics, DeleteTopicsOptions? options)
    {
        EnterUse();
        try
        {
            IntPtr collection = NewNativeTopicCollection(topics, "deleteTopics");
            SafeAdminResultHandle result;
            try
            {
                result = CallWithOptions(
                    NewDeleteTopicsOptions(options),
                    s_destroyDeleteTopicsOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminDeleteTopicsWithOptions(admin, collection, nativeOptions, ref outErr),
                    s_destroyDeleteTopicsResult);
            }
            finally
            {
                DestroyTopicCollection(collection);
            }

            return topics is TopicCollection.TopicIdCollection
                ? (null,
                    StartMapFutures(
                        result,
                        s_deleteTopicsTopicIdValues,
                        s_uuidKey,
                        EqualityComparer<Uuid>.Default,
                        s_noValue),
                    result)
                : (StartMapFutures(
                        result, s_deleteTopicsTopicNameValues, s_stringKey, StringComparer.Ordinal, s_noValue),
                    null,
                    result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewDeleteTopicsOptions(DeleteTopicsOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newDeleteTopicsOptions, options, options?.Setters, s_setDeleteTopicsOption, s_destroyDeleteTopicsOptions);

    internal static void FreeDeleteTopicsOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyDeleteTopicsOptions);

    private static void SetDeleteTopicsOption(IntPtr native, DeleteTopicsOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(DeleteTopicsOptions.TimeoutMs):
                NativeMethods.DeleteTopicsOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            case nameof(DeleteTopicsOptions.RetryOnQuotaViolation):
                NativeMethods.DeleteTopicsOptionsSetRetryOnQuotaViolation(
                    native, options.RetryOnQuotaViolation ? (sbyte)1 : (sbyte)0, ref outErr);
                break;
            default:
                throw new InvalidOperationException("DeleteTopicsOptions has no C setter for " + setter + ".");
        }
    }

    internal (
        IReadOnlyDictionary<string, KafkaFutureImpl<TopicDescription>>? ByName,
        IReadOnlyDictionary<Uuid, KafkaFutureImpl<TopicDescription>>? ById,
        SafeAdminResultHandle Result) DescribeTopics(TopicCollection? topics, DescribeTopicsOptions? options)
    {
        EnterUse();
        try
        {
            IntPtr collection = NewNativeTopicCollection(topics, "describeTopics");
            SafeAdminResultHandle result;
            try
            {
                result = CallWithOptions(
                    NewDescribeTopicsOptions(options),
                    s_destroyDescribeTopicsOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminDescribeTopicsWithTopicsOptions(admin, collection, nativeOptions, ref outErr),
                    s_destroyDescribeTopicsResult);
            }
            finally
            {
                DestroyTopicCollection(collection);
            }

            return StartDescribeTopicsFutures(result, topics is TopicCollection.TopicIdCollection);
        }
        finally
        {
            ExitUse();
        }
    }

    internal (
        IReadOnlyDictionary<string, KafkaFutureImpl<TopicDescription>>? ByName,
        IReadOnlyDictionary<Uuid, KafkaFutureImpl<TopicDescription>>? ById,
        SafeAdminResultHandle Result) DescribeTopics(IReadOnlyCollection<string> topicNames, DescribeTopicsOptions? options)
    {
        EnterUse();
        try
        {
            List<Utf8Marshal.PinnedUtf8String> pins = new List<Utf8Marshal.PinnedUtf8String>(topicNames.Count);
            IntPtr list = IntPtr.Zero;
            SafeAdminResultHandle result;
            try
            {
                list = NewTopicNameList(topicNames, nameof(topicNames), pins);
                result = CallWithOptions(
                    NewDescribeTopicsOptions(options),
                    s_destroyDescribeTopicsOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminDescribeTopicsWithTopicNamesOptions(admin, list, nativeOptions, ref outErr),
                    s_destroyDescribeTopicsResult);
            }
            finally
            {
                DestroyList(list);
                ReleasePins(pins);
            }

            return StartDescribeTopicsFutures(result, byId: false);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewDescribeTopicsOptions(DescribeTopicsOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newDescribeTopicsOptions, options, options?.Setters, s_setDescribeTopicsOption, s_destroyDescribeTopicsOptions);

    internal static void FreeDescribeTopicsOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyDescribeTopicsOptions);

    private static void SetDescribeTopicsOption(
        IntPtr native, DescribeTopicsOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(DescribeTopicsOptions.TimeoutMs):
                NativeMethods.DescribeTopicsOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            case nameof(DescribeTopicsOptions.IncludeAuthorizedOperations):
                NativeMethods.DescribeTopicsOptionsSetIncludeAuthorizedOperations(
                    native, options.IncludeAuthorizedOperations ? (sbyte)1 : (sbyte)0, ref outErr);
                break;
            case nameof(DescribeTopicsOptions.PartitionSizeLimitPerResponse):
                NativeMethods.DescribeTopicsOptionsSetPartitionSizeLimitPerResponse(
                    native, options.PartitionSizeLimitPerResponse, ref outErr);
                break;
            default:
                throw new InvalidOperationException("DescribeTopicsOptions has no C setter for " + setter + ".");
        }
    }

    private static (
        IReadOnlyDictionary<string, KafkaFutureImpl<TopicDescription>>? ByName,
        IReadOnlyDictionary<Uuid, KafkaFutureImpl<TopicDescription>>? ById,
        SafeAdminResultHandle Result) StartDescribeTopicsFutures(SafeAdminResultHandle result, bool byId) =>
        byId
            ? (null,
                StartMapFutures(
                    result,
                    s_describeTopicsTopicIdValues,
                    s_uuidKey,
                    EqualityComparer<Uuid>.Default,
                    s_copyOutTopicDescription),
                result)
            : (StartMapFutures(
                    result,
                    s_describeTopicsTopicNameValues,
                    s_stringKey,
                    StringComparer.Ordinal,
                    s_copyOutTopicDescription),
                null,
                result);

    private static IntPtr NewNativeTopicCollection(TopicCollection? topics, string operation)
    {
        switch (topics)
        {
            case TopicCollection.TopicNameCollection names:
                {
                    List<Utf8Marshal.PinnedUtf8String> pins = new List<Utf8Marshal.PinnedUtf8String>();
                    IntPtr list = IntPtr.Zero;
                    try
                    {
                        list = NewTopicNameList(names.TopicNames(), nameof(topics), pins);
                        IntPtr outErr = IntPtr.Zero;
                        IntPtr collection = NativeMethods.TopicCollectionOfTopicNames(list, ref outErr);
                        KafkaException.ThrowIfFfiFailed(ref outErr);
                        return collection;
                    }
                    finally
                    {
                        DestroyList(list);
                        ReleasePins(pins);
                    }
                }

            case TopicCollection.TopicIdCollection ids:
                {
                    IntPtr[] natives = new IntPtr[ids.TopicIds().Count];
                    IntPtr list = IntPtr.Zero;
                    try
                    {
                        int next = 0;
                        foreach (Uuid id in ids.TopicIds())
                        {
                            IntPtr outErr = IntPtr.Zero;
                            natives[next++] = NativeMethods.UuidNew(id.MostSignificantBits, id.LeastSignificantBits, ref outErr);
                            KafkaException.ThrowIfFfiFailed(ref outErr);
                        }

                        list = NewList(natives);
                        IntPtr collectionErr = IntPtr.Zero;
                        IntPtr collection = NativeMethods.TopicCollectionOfTopicIds(list, ref collectionErr);
                        KafkaException.ThrowIfFfiFailed(ref collectionErr);
                        return collection;
                    }
                    finally
                    {
                        DestroyList(list);
                        foreach (IntPtr native in natives)
                        {
                            DestroyUuid(native);
                        }
                    }
                }

            default:
                throw new ArgumentException(
                    "The TopicCollection: null provided did not match any supported classes for " + operation + ".");
        }
    }

    private static IntPtr NewTopicNameList(
        IReadOnlyCollection<string> topicNames, string parameterName, List<Utf8Marshal.PinnedUtf8String> pins)
    {
        List<string> names = DistinctNames(topicNames, "topic names", parameterName);
        IntPtr[] elements = new IntPtr[names.Count];
        for (int i = 0; i < elements.Length; i++)
        {
            Utf8Marshal.PinnedUtf8String pin = Utf8Marshal.Pin(names[i]);
            pins.Add(pin);
            elements[i] = pin.Pointer;
        }

        return NewList(elements);
    }

    private static void ReleasePins(List<Utf8Marshal.PinnedUtf8String> pins)
    {
        foreach (Utf8Marshal.PinnedUtf8String pin in pins)
        {
            pin.Dispose();
        }
    }

    private static void DestroyTopicCollection(IntPtr collection)
    {
        if (collection == IntPtr.Zero)
        {
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        NativeMethods.TopicCollectionDestroy(collection, ref outErr);
        ReleaseError(outErr);
    }

    private static void DestroyUuid(IntPtr uuid)
    {
        if (uuid == IntPtr.Zero)
        {
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        NativeMethods.UuidDestroy(uuid, ref outErr);
        ReleaseError(outErr);
    }

    private static KafkaFutureImpl<T> StartOwnedFuture<T>(
        SafeAdminResultHandle result, ResultFuture accessor, Func<IntPtr, T> convert)
    {
        try
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr future = accessor(result.DangerousGetHandle(), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return NativeFutureSource<T>.Start(SafeKafkaFutureHandle.Owned(future), convert);
        }
        catch
        {
            result.Dispose();
            throw;
        }
    }

    private static KafkaFutureImpl<T> StartBorrowedFuture<T>(
        SafeAdminResultHandle result, ResultFuture accessor, Func<IntPtr, T> convert)
    {
        try
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr future = accessor(result.DangerousGetHandle(), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return NativeFutureSource<T>.Start(SafeKafkaFutureHandle.Borrowed(future, result), convert);
        }
        catch
        {
            result.Dispose();
            throw;
        }
    }

    private SafeAdminResultHandle CallWithOptions(
        IntPtr options, AdminOptionsMarshal.Destroy destroyOptions, RpcWithOptions call, SafeAdminResultHandle.Destroy destroyResult)
    {
        IntPtr result;
        try
        {
            IntPtr outErr = IntPtr.Zero;
            result = call(_handle, options, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
        finally
        {
            AdminOptionsMarshal.Free(options, destroyOptions);
        }

        return new SafeAdminResultHandle(result, destroyResult);
    }
}
