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
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

internal sealed partial class NativeAdminClient
{
    private static readonly ResultFuture s_alterConfigsValues = NativeMethods.AlterConfigsResultValues;

    private static readonly ResultFuture s_describeConfigsValues = NativeMethods.DescribeConfigsResultValues;

    private static readonly ResultFuture s_listConfigResourcesAll = NativeMethods.ListConfigResourcesResultAll;

    private static readonly AdminOptionsMarshal.New s_newDescribeConfigsOptions = NativeMethods.DescribeConfigsOptionsNew;

    private static readonly AdminOptionsMarshal.Set<DescribeConfigsOptions> s_setDescribeConfigsOption =
        SetDescribeConfigsOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyDescribeConfigsOptions =
        NativeMethods.DescribeConfigsOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyDescribeConfigsResult =
        NativeMethods.DescribeConfigsResultDestroy;

    private static readonly AdminOptionsMarshal.New s_newAlterConfigsOptions = NativeMethods.AlterConfigsOptionsNew;

    private static readonly AdminOptionsMarshal.Set<AlterConfigsOptions> s_setAlterConfigsOption = SetAlterConfigsOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyAlterConfigsOptions =
        NativeMethods.AlterConfigsOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyAlterConfigsResult =
        NativeMethods.AlterConfigsResultDestroy;

    private static readonly AdminOptionsMarshal.New s_newListConfigResourcesOptions =
        NativeMethods.ListConfigResourcesOptionsNew;

    private static readonly AdminOptionsMarshal.Set<ListConfigResourcesOptions> s_setListConfigResourcesOption =
        SetListConfigResourcesOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyListConfigResourcesOptions =
        NativeMethods.ListConfigResourcesOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyListConfigResourcesResult =
        NativeMethods.ListConfigResourcesResultDestroy;

    private static readonly Func<IntPtr, ConfigResource> s_configResourceKey = ConfigResourceMarshal.CopyOut;

    private static readonly Func<IntPtr, Config> s_copyOutConfig = ConfigEntriesMarshal.CopyOut;

    private static readonly Func<IntPtr, IReadOnlyCollection<ConfigResource>> s_copyOutConfigResources =
        CopyOutConfigResources;

    internal (IReadOnlyDictionary<ConfigResource, KafkaFutureImpl<Config>> Values, SafeAdminResultHandle Result)
        DescribeConfigs(IReadOnlyCollection<ConfigResource> resources, DescribeConfigsOptions? options)
    {
        EnterUse();
        try
        {
            if (resources is null)
            {
                throw new ArgumentNullException(nameof(resources));
            }

            List<ConfigResource> keys = DistinctResources(resources, nameof(resources));
            IntPtr[] natives = new IntPtr[keys.Count];
            IntPtr list = IntPtr.Zero;
            SafeAdminResultHandle result;
            try
            {
                for (int i = 0; i < natives.Length; i++)
                {
                    natives[i] = ConfigResourceMarshal.Build(keys[i]);
                }

                list = NewList(natives);
                result = CallWithOptions(
                    NewDescribeConfigsOptions(options),
                    s_destroyDescribeConfigsOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminDescribeConfigsWithOptions(admin, list, nativeOptions, ref outErr),
                    s_destroyDescribeConfigsResult);
            }
            finally
            {
                DestroyList(list);
                foreach (IntPtr native in natives)
                {
                    ConfigResourceMarshal.Destroy(native);
                }
            }

            return (
                StartMapFutures(
                    result,
                    s_describeConfigsValues,
                    s_configResourceKey,
                    s_configResourceComparer,
                    s_copyOutConfig),
                result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewDescribeConfigsOptions(DescribeConfigsOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newDescribeConfigsOptions,
            options,
            options?.Setters,
            s_setDescribeConfigsOption,
            s_destroyDescribeConfigsOptions);

    internal static void FreeDescribeConfigsOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyDescribeConfigsOptions);

    private static void SetDescribeConfigsOption(
        IntPtr native, DescribeConfigsOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(DescribeConfigsOptions.TimeoutMs):
                NativeMethods.DescribeConfigsOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            case nameof(DescribeConfigsOptions.IncludeSynonyms):
                NativeMethods.DescribeConfigsOptionsSetIncludeSynonyms(
                    native, options.IncludeSynonyms ? (sbyte)1 : (sbyte)0, ref outErr);
                break;
            case nameof(DescribeConfigsOptions.IncludeDocumentation):
                NativeMethods.DescribeConfigsOptionsSetIncludeDocumentation(
                    native, options.IncludeDocumentation ? (sbyte)1 : (sbyte)0, ref outErr);
                break;
            default:
                throw new InvalidOperationException("DescribeConfigsOptions has no C setter for " + setter + ".");
        }
    }

    internal (IReadOnlyDictionary<ConfigResource, KafkaFutureImpl<object?>> Values, SafeAdminResultHandle Result)
        IncrementalAlterConfigs(
            IReadOnlyDictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> configs, AlterConfigsOptions? options)
    {
        EnterUse();
        try
        {
            if (configs is null)
            {
                throw new ArgumentNullException(nameof(configs));
            }

            List<ConfigResource> keys = new List<ConfigResource>(configs.Count);
            Dictionary<ConfigResource, List<AlterConfigOp>> merged =
                new Dictionary<ConfigResource, List<AlterConfigOp>>(configs.Count, s_configResourceComparer);
            foreach (KeyValuePair<ConfigResource, IReadOnlyCollection<AlterConfigOp>> entry in configs)
            {
                if (entry.Key is null)
                {
                    throw new ArgumentException("The configs map must not contain a null resource.", nameof(configs));
                }

                AdminStrings.Validate(entry.Key.Name, nameof(configs));
                if (entry.Value is null)
                {
                    throw new ArgumentException($"The operations for '{entry.Key}' must not be null.", nameof(configs));
                }

                if (!merged.TryGetValue(entry.Key, out List<AlterConfigOp>? ops))
                {
                    ops = new List<AlterConfigOp>(entry.Value.Count);
                    merged.Add(entry.Key, ops);
                    keys.Add(entry.Key);
                }

                foreach (AlterConfigOp op in entry.Value)
                {
                    if (op is null)
                    {
                        throw new ArgumentException(
                            $"The operations for '{entry.Key}' must not contain a null element.", nameof(configs));
                    }

                    AdminStrings.Validate(op.ConfigEntry.Name, nameof(configs));
                    AdminStrings.Validate(op.ConfigEntry.Value, nameof(configs));
                    ops.Add(op);
                }
            }

            List<Utf8Marshal.PinnedUtf8String> pins = new List<Utf8Marshal.PinnedUtf8String>();
            List<IntPtr> entries = new List<IntPtr>();
            List<IntPtr> nativeOps = new List<IntPtr>();
            IntPtr[] resources = new IntPtr[keys.Count];
            IntPtr[] lists = new IntPtr[keys.Count];
            IntPtr map = IntPtr.Zero;
            SafeAdminResultHandle result;
            try
            {
                map = NewMap();
                for (int i = 0; i < keys.Count; i++)
                {
                    resources[i] = ConfigResourceMarshal.Build(keys[i]);
                    lists[i] = NewList(Array.Empty<IntPtr>());
                    foreach (AlterConfigOp op in merged[keys[i]])
                    {
                        IntPtr native = NewAlterConfigOp(op, pins, entries);
                        nativeOps.Add(native);
                        ListAdd(lists[i], native);
                    }

                    MapPut(map, resources[i], lists[i]);
                }

                result = CallWithOptions(
                    NewAlterConfigsOptions(options),
                    s_destroyAlterConfigsOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminIncrementalAlterConfigsWithOptions(admin, map, nativeOptions, ref outErr),
                    s_destroyAlterConfigsResult);
            }
            finally
            {
                DestroyMap(map);
                for (int i = 0; i < keys.Count; i++)
                {
                    DestroyList(lists[i]);
                    ConfigResourceMarshal.Destroy(resources[i]);
                }

                foreach (IntPtr native in nativeOps)
                {
                    DestroyAlterConfigOp(native);
                }

                foreach (IntPtr entry in entries)
                {
                    DestroyConfigEntry(entry);
                }

                ReleasePins(pins);
            }

            return (
                StartMapFutures(
                    result,
                    s_alterConfigsValues,
                    s_configResourceKey,
                    s_configResourceComparer,
                    s_noValue),
                result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewAlterConfigsOptions(AlterConfigsOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newAlterConfigsOptions, options, options?.Setters, s_setAlterConfigsOption, s_destroyAlterConfigsOptions);

    internal static void FreeAlterConfigsOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyAlterConfigsOptions);

    private static void SetAlterConfigsOption(IntPtr native, AlterConfigsOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(AlterConfigsOptions.TimeoutMs):
                NativeMethods.AlterConfigsOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            case nameof(AlterConfigsOptions.ValidateOnly):
                NativeMethods.AlterConfigsOptionsSetValidateOnly(native, options.ValidateOnly ? (sbyte)1 : (sbyte)0, ref outErr);
                break;
            default:
                throw new InvalidOperationException("AlterConfigsOptions has no C setter for " + setter + ".");
        }
    }

    internal (KafkaFutureImpl<IReadOnlyCollection<ConfigResource>> All, SafeAdminResultHandle Result) ListConfigResources(
        IReadOnlyCollection<ConfigResourceType>? configResourceTypes, ListConfigResourcesOptions? options)
    {
        EnterUse();
        try
        {
            List<ConfigResourceType> types = DistinctTypes(configResourceTypes);
            IntPtr[] natives = new IntPtr[types.Count];
            for (int i = 0; i < natives.Length; i++)
            {
                natives[i] = ConfigResourceMarshal.NativeType(types[i]);
            }

            IntPtr list = NewList(natives);
            SafeAdminResultHandle result;
            try
            {
                result = CallWithOptions(
                    NewListConfigResourcesOptions(options),
                    s_destroyListConfigResourcesOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminListConfigResourcesWithOptions(admin, list, nativeOptions, ref outErr),
                    s_destroyListConfigResourcesResult);
            }
            finally
            {
                DestroyList(list);
            }

            return (
                StartOwnedFuture(result, s_listConfigResourcesAll, s_copyOutConfigResources),
                result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewListConfigResourcesOptions(ListConfigResourcesOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newListConfigResourcesOptions,
            options,
            options?.Setters,
            s_setListConfigResourcesOption,
            s_destroyListConfigResourcesOptions);

    internal static void FreeListConfigResourcesOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyListConfigResourcesOptions);

    private static void SetListConfigResourcesOption(
        IntPtr native, ListConfigResourcesOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(ListConfigResourcesOptions.TimeoutMs):
                NativeMethods.ListConfigResourcesOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            default:
                throw new InvalidOperationException("ListConfigResourcesOptions has no C setter for " + setter + ".");
        }
    }

    internal static IReadOnlyCollection<ConfigResource> CopyOutConfigResources(IntPtr list)
    {
        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.ListSize(list, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        List<ConfigResource> resources = new List<ConfigResource>(count);
        for (int i = 0; i < count; i++)
        {
            IntPtr resource = NativeMethods.ListGet(list, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            resources.Add(ConfigResourceMarshal.CopyOut(resource));
        }

        return resources;
    }

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

            AdminStrings.Validate(resource.Name, parameterName);
            if (seen.Add(resource))
            {
                keys.Add(resource);
            }
        }

        return keys;
    }

    private static List<ConfigResourceType> DistinctTypes(IReadOnlyCollection<ConfigResourceType>? types)
    {
        List<ConfigResourceType> distinct = new List<ConfigResourceType>(types?.Count ?? 0);
        if (types is null)
        {
            return distinct;
        }

        HashSet<ConfigResourceType> seen = new HashSet<ConfigResourceType>();
        foreach (ConfigResourceType type in types)
        {
            ConfigResourceType known = ConfigResourceMarshal.TypeFromId((int)type);
            if (seen.Add(known))
            {
                distinct.Add(known);
            }
        }

        return distinct;
    }

    internal static IntPtr NewAlterConfigOp(
        AlterConfigOp op, List<Utf8Marshal.PinnedUtf8String> pins, List<IntPtr> entries)
    {
        Utf8Marshal.PinnedUtf8String name = Utf8Marshal.Pin(op.ConfigEntry.Name);
        pins.Add(name);
        IntPtr value = IntPtr.Zero;
        if (op.ConfigEntry.Value is not null)
        {
            Utf8Marshal.PinnedUtf8String pinnedValue = Utf8Marshal.Pin(op.ConfigEntry.Value);
            pins.Add(pinnedValue);
            value = pinnedValue.Pointer;
        }

        IntPtr outErr = IntPtr.Zero;
        IntPtr entry = NativeMethods.ConfigEntryNew(name.Pointer, value, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        entries.Add(entry);
        IntPtr opType = op.OpType switch
        {
            AlterConfigOpType.Delete => NativeMethods.AlterConfigOpTypeDelete(ref outErr),
            AlterConfigOpType.Append => NativeMethods.AlterConfigOpTypeAppend(ref outErr),
            AlterConfigOpType.Subtract => NativeMethods.AlterConfigOpTypeSubtract(ref outErr),
            _ => NativeMethods.AlterConfigOpTypeSet(ref outErr),
        };
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr native = NativeMethods.AlterConfigOpNew(entry, opType, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return native;
    }

    private static void ListAdd(IntPtr list, IntPtr element)
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ListAdd(list, element, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
    }

    private static void DestroyAlterConfigOp(IntPtr op)
    {
        if (op == IntPtr.Zero)
        {
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        NativeMethods.AlterConfigOpDestroy(op, ref outErr);
        ReleaseError(outErr);
    }

    private static void DestroyConfigEntry(IntPtr entry)
    {
        if (entry == IntPtr.Zero)
        {
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ConfigEntryDestroy(entry, ref outErr);
        ReleaseError(outErr);
    }
}
