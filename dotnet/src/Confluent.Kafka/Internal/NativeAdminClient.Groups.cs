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
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

internal sealed partial class NativeAdminClient
{
    private static readonly GroupIdsRpc s_adminDescribeClassicGroupsWithOptions =
        NativeMethods.AdminDescribeClassicGroupsWithOptions;

    private static readonly GroupIdsRpc s_adminDescribeConsumerGroupsWithOptions =
        NativeMethods.AdminDescribeConsumerGroupsWithOptions;

    private static readonly ResultFuture s_describeClassicGroupsDescribedGroups =
        NativeMethods.DescribeClassicGroupsResultDescribedGroups;

    private static readonly ResultFuture s_describeConsumerGroupsDescribedGroups =
        NativeMethods.DescribeConsumerGroupsResultDescribedGroups;

    private static readonly ListSetter s_listGroupsOptionsInGroupStates = NativeMethods.ListGroupsOptionsInGroupStates;

    private static readonly ListSetter s_listGroupsOptionsWithTypes = NativeMethods.ListGroupsOptionsWithTypes;

    private static readonly ResultFuture s_listGroupsAll = NativeMethods.ListGroupsResultAll;

    private static readonly ResultFuture s_listGroupsErrors = NativeMethods.ListGroupsResultErrors;

    private static readonly ResultFuture s_listGroupsValid = NativeMethods.ListGroupsResultValid;

    private static readonly AdminOptionsMarshal.New s_newListGroupsOptions = NativeMethods.ListGroupsOptionsNew;

    private static readonly AdminOptionsMarshal.Set<ListGroupsOptions> s_setListGroupsOption = SetListGroupsOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyListGroupsOptions = NativeMethods.ListGroupsOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyListGroupsResult = NativeMethods.ListGroupsResultDestroy;

    private static readonly AdminOptionsMarshal.New s_newDescribeConsumerGroupsOptions =
        NativeMethods.DescribeConsumerGroupsOptionsNew;

    private static readonly AdminOptionsMarshal.Set<DescribeConsumerGroupsOptions> s_setDescribeConsumerGroupsOption =
        SetDescribeConsumerGroupsOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyDescribeConsumerGroupsOptions =
        NativeMethods.DescribeConsumerGroupsOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyDescribeConsumerGroupsResult =
        NativeMethods.DescribeConsumerGroupsResultDestroy;

    private static readonly AdminOptionsMarshal.New s_newDescribeClassicGroupsOptions =
        NativeMethods.DescribeClassicGroupsOptionsNew;

    private static readonly AdminOptionsMarshal.Set<DescribeClassicGroupsOptions> s_setDescribeClassicGroupsOption =
        SetDescribeClassicGroupsOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyDescribeClassicGroupsOptions =
        NativeMethods.DescribeClassicGroupsOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyDescribeClassicGroupsResult =
        NativeMethods.DescribeClassicGroupsResultDestroy;

    private static readonly Func<IntPtr, IReadOnlyCollection<GroupListing>> s_copyOutGroupListings =
        GroupDescriptionMarshal.CopyOutListings;

    private static readonly Func<IntPtr, IReadOnlyCollection<KafkaException>> s_copyOutGroupErrors =
        GroupDescriptionMarshal.CopyOutErrors;

    private static readonly Func<IntPtr, ConsumerGroupDescription> s_copyOutConsumerGroupDescription =
        GroupDescriptionMarshal.CopyOutConsumerGroupDescription;

    private static readonly Func<IntPtr, ClassicGroupDescription> s_copyOutClassicGroupDescription =
        GroupDescriptionMarshal.CopyOutClassicGroupDescription;

    internal (
        KafkaFutureImpl<IReadOnlyCollection<GroupListing>> All,
        KafkaFutureImpl<IReadOnlyCollection<GroupListing>> Valid,
        KafkaFutureImpl<IReadOnlyCollection<KafkaException>> Errors,
        SafeAdminResultHandle Result) ListGroups(ListGroupsOptions? options)
    {
        EnterUse();
        try
        {
            if (options is not null)
            {
                foreach (string protocolType in options.ProtocolTypes)
                {
                    AdminStrings.Validate(protocolType, nameof(options));
                }

                RequireDefinedFilter(options.GroupStates, GroupMarshal.NameFromState, nameof(ListGroupsOptions.GroupStates));
                RequireDefinedFilter(options.Types, GroupMarshal.NameFromType, nameof(ListGroupsOptions.Types));
            }

            SafeAdminResultHandle result = CallWithOptions(
                NewListGroupsOptions(options),
                s_destroyListGroupsOptions,
                (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                    NativeMethods.AdminListGroupsWithOptions(admin, nativeOptions, ref outErr),
                s_destroyListGroupsResult);
            KafkaFutureImpl<IReadOnlyCollection<GroupListing>> all =
                StartOwnedFuture(result, s_listGroupsAll, s_copyOutGroupListings);
            KafkaFutureImpl<IReadOnlyCollection<GroupListing>> valid =
                StartOwnedFuture(result, s_listGroupsValid, s_copyOutGroupListings);
            KafkaFutureImpl<IReadOnlyCollection<KafkaException>> errors =
                StartOwnedFuture(result, s_listGroupsErrors, s_copyOutGroupErrors);
            return (all, valid, errors, result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewListGroupsOptions(ListGroupsOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newListGroupsOptions, options, options?.Setters, s_setListGroupsOption, s_destroyListGroupsOptions);

    internal static void FreeListGroupsOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyListGroupsOptions);

    private static void SetListGroupsOption(IntPtr native, ListGroupsOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(ListGroupsOptions.TimeoutMs):
                NativeMethods.ListGroupsOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            case nameof(ListGroupsOptions.GroupStates):
                SetSingletonList(native, options.GroupStates, GroupMarshal.StateSingleton, s_listGroupsOptionsInGroupStates, ref outErr);
                break;
            case nameof(ListGroupsOptions.Types):
                SetSingletonList(native, options.Types, GroupMarshal.TypeSingleton, s_listGroupsOptionsWithTypes, ref outErr);
                break;
            case nameof(ListGroupsOptions.ProtocolTypes):
                SetProtocolTypes(native, options.ProtocolTypes, ref outErr);
                break;
            default:
                throw new InvalidOperationException("ListGroupsOptions has no C setter for " + setter + ".");
        }
    }

    private delegate void ListSetter(IntPtr native, IntPtr list, ref IntPtr outErr);

    private static void SetSingletonList<T>(
        IntPtr native, IReadOnlyCollection<T> values, Func<T, IntPtr> singleton, ListSetter set, ref IntPtr outErr)
    {
        IntPtr[] elements = new IntPtr[values.Count];
        int next = 0;
        foreach (T value in values)
        {
            elements[next++] = singleton(value);
        }

        IntPtr list = NewList(elements);
        try
        {
            set(native, list, ref outErr);
        }
        finally
        {
            DestroyList(list);
        }
    }

    private static void SetProtocolTypes(IntPtr native, IReadOnlyCollection<string> protocolTypes, ref IntPtr outErr)
    {
        List<Utf8Marshal.PinnedUtf8String> pins = new List<Utf8Marshal.PinnedUtf8String>(protocolTypes.Count);
        IntPtr list = IntPtr.Zero;
        try
        {
            IntPtr[] elements = new IntPtr[protocolTypes.Count];
            int next = 0;
            foreach (string protocolType in protocolTypes)
            {
                Utf8Marshal.PinnedUtf8String pin = Utf8Marshal.Pin(protocolType);
                pins.Add(pin);
                elements[next++] = pin.Pointer;
            }

            list = NewList(elements);
            NativeMethods.ListGroupsOptionsWithProtocolTypes(native, list, ref outErr);
        }
        finally
        {
            DestroyList(list);
            ReleasePins(pins);
        }
    }

    private static void RequireDefinedFilter<T>(
        IReadOnlyCollection<T> values, Func<T, string?> name, string propertyName)
        where T : struct
    {
        foreach (T value in values)
        {
            if (name(value) is null)
            {
                throw new ArgumentOutOfRangeException(
                    "options",
                    value,
                    $"{nameof(ListGroupsOptions)}.{propertyName} must contain only defined {typeof(T).Name} members.");
            }
        }
    }

    internal (IReadOnlyDictionary<string, KafkaFutureImpl<ConsumerGroupDescription>> DescribedGroups, SafeAdminResultHandle Result)
        DescribeConsumerGroups(IReadOnlyCollection<string> groupIds, DescribeConsumerGroupsOptions? options)
    {
        EnterUse();
        try
        {
            if (groupIds is null)
            {
                throw new ArgumentNullException(nameof(groupIds));
            }

            List<string> keys = DistinctNames(groupIds, "group ids", nameof(groupIds));
            SafeAdminResultHandle result = CallWithGroupIds(
                keys,
                () => NewDescribeConsumerGroupsOptions(options),
                s_destroyDescribeConsumerGroupsOptions,
                s_adminDescribeConsumerGroupsWithOptions,
                s_destroyDescribeConsumerGroupsResult);
            return (
                StartMapFutures(
                    result,
                    s_describeConsumerGroupsDescribedGroups,
                    s_stringKey,
                    StringComparer.Ordinal,
                    s_copyOutConsumerGroupDescription),
                result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewDescribeConsumerGroupsOptions(DescribeConsumerGroupsOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newDescribeConsumerGroupsOptions,
            options,
            options?.Setters,
            s_setDescribeConsumerGroupsOption,
            s_destroyDescribeConsumerGroupsOptions);

    internal static void FreeDescribeConsumerGroupsOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyDescribeConsumerGroupsOptions);

    private static void SetDescribeConsumerGroupsOption(
        IntPtr native, DescribeConsumerGroupsOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(DescribeConsumerGroupsOptions.TimeoutMs):
                NativeMethods.DescribeConsumerGroupsOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            case nameof(DescribeConsumerGroupsOptions.IncludeAuthorizedOperations):
                NativeMethods.DescribeConsumerGroupsOptionsSetIncludeAuthorizedOperations(
                    native, options.IncludeAuthorizedOperations ? (sbyte)1 : (sbyte)0, ref outErr);
                break;
            default:
                throw new InvalidOperationException("DescribeConsumerGroupsOptions has no C setter for " + setter + ".");
        }
    }

    internal (IReadOnlyDictionary<string, KafkaFutureImpl<ClassicGroupDescription>> DescribedGroups, SafeAdminResultHandle Result)
        DescribeClassicGroups(IReadOnlyCollection<string> groupIds, DescribeClassicGroupsOptions? options)
    {
        EnterUse();
        try
        {
            if (groupIds is null)
            {
                throw new ArgumentNullException(nameof(groupIds));
            }

            List<string> keys = DistinctNames(groupIds, "group ids", nameof(groupIds));
            SafeAdminResultHandle result = CallWithGroupIds(
                keys,
                () => NewDescribeClassicGroupsOptions(options),
                s_destroyDescribeClassicGroupsOptions,
                s_adminDescribeClassicGroupsWithOptions,
                s_destroyDescribeClassicGroupsResult);
            return (
                StartMapFutures(
                    result,
                    s_describeClassicGroupsDescribedGroups,
                    s_stringKey,
                    StringComparer.Ordinal,
                    s_copyOutClassicGroupDescription),
                result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewDescribeClassicGroupsOptions(DescribeClassicGroupsOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newDescribeClassicGroupsOptions,
            options,
            options?.Setters,
            s_setDescribeClassicGroupsOption,
            s_destroyDescribeClassicGroupsOptions);

    internal static void FreeDescribeClassicGroupsOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyDescribeClassicGroupsOptions);

    private static void SetDescribeClassicGroupsOption(
        IntPtr native, DescribeClassicGroupsOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(DescribeClassicGroupsOptions.TimeoutMs):
                NativeMethods.DescribeClassicGroupsOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            case nameof(DescribeClassicGroupsOptions.IncludeAuthorizedOperations):
                NativeMethods.DescribeClassicGroupsOptionsSetIncludeAuthorizedOperations(
                    native, options.IncludeAuthorizedOperations ? (sbyte)1 : (sbyte)0, ref outErr);
                break;
            default:
                throw new InvalidOperationException("DescribeClassicGroupsOptions has no C setter for " + setter + ".");
        }
    }

    private delegate IntPtr GroupIdsRpc(SafeAdminClientHandle admin, IntPtr groupIds, IntPtr options, ref IntPtr outErr);

    private SafeAdminResultHandle CallWithGroupIds(
        List<string> groupIds,
        Func<IntPtr> newOptions,
        AdminOptionsMarshal.Destroy destroyOptions,
        GroupIdsRpc rpc,
        SafeAdminResultHandle.Destroy destroyResult)
    {
        List<Utf8Marshal.PinnedUtf8String> pins = new List<Utf8Marshal.PinnedUtf8String>(groupIds.Count);
        IntPtr list = IntPtr.Zero;
        try
        {
            IntPtr[] elements = new IntPtr[groupIds.Count];
            for (int i = 0; i < elements.Length; i++)
            {
                Utf8Marshal.PinnedUtf8String pin = Utf8Marshal.Pin(groupIds[i]);
                pins.Add(pin);
                elements[i] = pin.Pointer;
            }

            list = NewList(elements);
            return CallWithOptions(
                newOptions(),
                destroyOptions,
                (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                    rpc(admin, list, nativeOptions, ref outErr),
                destroyResult);
        }
        finally
        {
            DestroyList(list);
            ReleasePins(pins);
        }
    }
}
