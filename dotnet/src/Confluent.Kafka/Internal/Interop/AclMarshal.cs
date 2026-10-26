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

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Builds the owned <c>kafka_common_acl_AclBinding_t</c> / <c>AclBindingFilter_t</c> handles an ACL RPC takes, and
/// copies the borrowed bindings, filters and filter results its futures deliver into managed values.
/// </summary>
internal static class AclMarshal
{
    private delegate void NativeDestroy(IntPtr native, ref IntPtr outErr);

    internal static IntPtr NewBinding(AclBinding binding)
    {
        IntPtr pattern = NewPattern(binding.Pattern);
        try
        {
            IntPtr entry = NewEntry(binding.Entry);
            try
            {
                IntPtr outErr = IntPtr.Zero;
                IntPtr native = NativeMethods.AclBindingNew(pattern, entry, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                return native;
            }
            finally
            {
                Destroy(entry, NativeMethods.AccessControlEntryDestroy);
            }
        }
        finally
        {
            Destroy(pattern, NativeMethods.ResourcePatternDestroy);
        }
    }

    internal static IntPtr NewFilter(AclBindingFilter filter)
    {
        IntPtr pattern = NewPatternFilter(filter.PatternFilter);
        try
        {
            IntPtr entry = NewEntryFilter(filter.EntryFilter);
            try
            {
                IntPtr outErr = IntPtr.Zero;
                IntPtr native = NativeMethods.AclBindingFilterNew(pattern, entry, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                return native;
            }
            finally
            {
                Destroy(entry, NativeMethods.AccessControlEntryFilterDestroy);
            }
        }
        finally
        {
            Destroy(pattern, NativeMethods.ResourcePatternFilterDestroy);
        }
    }

    internal static void DestroyBinding(IntPtr binding) => Destroy(binding, NativeMethods.AclBindingDestroy);

    internal static void DestroyFilter(IntPtr filter) => Destroy(filter, NativeMethods.AclBindingFilterDestroy);

    internal static AclBinding CopyOutBinding(IntPtr binding)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr pattern = NativeMethods.AclBindingPattern(binding, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr entry = NativeMethods.AclBindingEntry(binding, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);

        ResourcePattern copiedPattern = new ResourcePattern(
            ReadResourceType(NativeMethods.ResourcePatternResourceType(pattern, ref outErr), ref outErr),
            ReadString(NativeMethods.ResourcePatternName(pattern, ref outErr), ref outErr) ?? string.Empty,
            ReadPatternType(NativeMethods.ResourcePatternPatternType(pattern, ref outErr), ref outErr));
        AccessControlEntry copiedEntry = new AccessControlEntry(
            ReadString(NativeMethods.AccessControlEntryPrincipal(entry, ref outErr), ref outErr) ?? string.Empty,
            ReadString(NativeMethods.AccessControlEntryHost(entry, ref outErr), ref outErr) ?? string.Empty,
            ReadOperation(NativeMethods.AccessControlEntryOperation(entry, ref outErr), ref outErr),
            ReadPermissionType(NativeMethods.AccessControlEntryPermissionType(entry, ref outErr), ref outErr));
        return new AclBinding(copiedPattern, copiedEntry);
    }

    internal static AclBindingFilter CopyOutFilter(IntPtr filter)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr pattern = NativeMethods.AclBindingFilterPatternFilter(filter, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr entry = NativeMethods.AclBindingFilterEntryFilter(filter, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);

        ResourcePatternFilter copiedPattern = new ResourcePatternFilter(
            ReadResourceType(NativeMethods.ResourcePatternFilterResourceType(pattern, ref outErr), ref outErr),
            ReadString(NativeMethods.ResourcePatternFilterName(pattern, ref outErr), ref outErr),
            ReadPatternType(NativeMethods.ResourcePatternFilterPatternType(pattern, ref outErr), ref outErr));
        AccessControlEntryFilter copiedEntry = new AccessControlEntryFilter(
            ReadString(NativeMethods.AccessControlEntryFilterPrincipal(entry, ref outErr), ref outErr),
            ReadString(NativeMethods.AccessControlEntryFilterHost(entry, ref outErr), ref outErr),
            ReadOperation(NativeMethods.AccessControlEntryFilterOperation(entry, ref outErr), ref outErr),
            ReadPermissionType(NativeMethods.AccessControlEntryFilterPermissionType(entry, ref outErr), ref outErr));
        return new AclBindingFilter(copiedPattern, copiedEntry);
    }

    internal static IReadOnlyCollection<AclBinding> CopyOutBindings(IntPtr bindings)
    {
        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.ListSize(bindings, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        List<AclBinding> copied = new List<AclBinding>(count);
        for (int i = 0; i < count; i++)
        {
            IntPtr binding = NativeMethods.ListGet(bindings, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            copied.Add(CopyOutBinding(binding));
        }

        return copied;
    }

    internal static DeleteAclsResult.FilterResults CopyOutFilterResults(IntPtr filterResults)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr values = NativeMethods.DeleteAclsFilterResultsValues(filterResults, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        try
        {
            int count = NativeMethods.ListSize(values, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            List<DeleteAclsResult.FilterResult> copied = new List<DeleteAclsResult.FilterResult>(count);
            for (int i = 0; i < count; i++)
            {
                IntPtr result = NativeMethods.ListGet(values, i, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                IntPtr binding = NativeMethods.DeleteAclsFilterResultBinding(result, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                IntPtr borrowedError = NativeMethods.DeleteAclsFilterResultError(result, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);

                // A FilterResult holds a KafkaException, so a deletion error of another type fails the filter's future.
                Exception? error = KafkaException.FromFfiError(borrowedError, owned: false);
                if (error is not null and not KafkaException)
                {
                    throw error;
                }

                copied.Add(new DeleteAclsResult.FilterResult(
                    binding == IntPtr.Zero ? null : CopyOutBinding(binding), (KafkaException?)error));
            }

            return new DeleteAclsResult.FilterResults(copied);
        }
        finally
        {
            outErr = IntPtr.Zero;
            NativeMethods.ListDestroy(values, ref outErr);
            if (outErr != IntPtr.Zero)
            {
                NativeMethods.ErrorDestroy(outErr);
            }
        }
    }

    private static IntPtr NewPattern(ResourcePattern pattern)
    {
        IntPtr resourceType = ResourceTypeSingleton(pattern.ResourceType);
        IntPtr patternType = PatternTypeSingleton(pattern.PatternType);
        using Utf8Marshal.PinnedUtf8String name = Utf8Marshal.Pin(pattern.Name);
        IntPtr outErr = IntPtr.Zero;
        IntPtr native = NativeMethods.ResourcePatternNew(resourceType, name.Pointer, patternType, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return native;
    }

    private static IntPtr NewEntry(AccessControlEntry entry)
    {
        IntPtr operation = OperationSingleton(entry.Operation);
        IntPtr permissionType = PermissionTypeSingleton(entry.PermissionType);
        using Utf8Marshal.PinnedUtf8String principal = Utf8Marshal.Pin(entry.Principal);
        using Utf8Marshal.PinnedUtf8String host = Utf8Marshal.Pin(entry.Host);
        IntPtr outErr = IntPtr.Zero;
        IntPtr native = NativeMethods.AccessControlEntryNew(
            principal.Pointer, host.Pointer, operation, permissionType, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return native;
    }

    private static IntPtr NewPatternFilter(ResourcePatternFilter pattern)
    {
        IntPtr resourceType = ResourceTypeSingleton(pattern.ResourceType);
        IntPtr patternType = PatternTypeSingleton(pattern.PatternType);
        using Utf8Marshal.PinnedUtf8String? name = PinOrNull(pattern.Name);
        IntPtr outErr = IntPtr.Zero;
        IntPtr native = NativeMethods.ResourcePatternFilterNew(
            resourceType, name?.Pointer ?? IntPtr.Zero, patternType, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return native;
    }

    private static IntPtr NewEntryFilter(AccessControlEntryFilter entry)
    {
        IntPtr operation = OperationSingleton(entry.Operation);
        IntPtr permissionType = PermissionTypeSingleton(entry.PermissionType);
        using Utf8Marshal.PinnedUtf8String? principal = PinOrNull(entry.Principal);
        using Utf8Marshal.PinnedUtf8String? host = PinOrNull(entry.Host);
        IntPtr outErr = IntPtr.Zero;
        IntPtr native = NativeMethods.AccessControlEntryFilterNew(
            principal?.Pointer ?? IntPtr.Zero, host?.Pointer ?? IntPtr.Zero, operation, permissionType, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return native;
    }

    private static Utf8Marshal.PinnedUtf8String? PinOrNull(string? value) =>
        value is null ? null : Utf8Marshal.Pin(value);

    private static IntPtr ResourceTypeSingleton(ResourceType value)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr singleton = NativeMethods.ResourceTypeFromCode((sbyte)AclEnumCodes.DefinedOrUnknown(value), ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return singleton;
    }

    private static IntPtr PatternTypeSingleton(PatternType value)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr singleton = NativeMethods.PatternTypeFromCode((sbyte)AclEnumCodes.DefinedOrUnknown(value), ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return singleton;
    }

    private static IntPtr OperationSingleton(AclOperation value)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr singleton = NativeMethods.AclOperationFromCode((sbyte)AclEnumCodes.DefinedOrUnknown(value), ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return singleton;
    }

    private static IntPtr PermissionTypeSingleton(AclPermissionType value)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr singleton = NativeMethods.AclPermissionTypeFromCode(
            (sbyte)AclEnumCodes.DefinedOrUnknown(value), ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return singleton;
    }

    private static ResourceType ReadResourceType(IntPtr singleton, ref IntPtr outErr)
    {
        KafkaException.ThrowIfFfiFailed(ref outErr);
        sbyte code = NativeMethods.ResourceTypeCode(singleton, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return AclEnumCodes.DefinedOrUnknown((ResourceType)code);
    }

    private static PatternType ReadPatternType(IntPtr singleton, ref IntPtr outErr)
    {
        KafkaException.ThrowIfFfiFailed(ref outErr);
        sbyte code = NativeMethods.PatternTypeCode(singleton, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return AclEnumCodes.DefinedOrUnknown((PatternType)code);
    }

    private static AclOperation ReadOperation(IntPtr singleton, ref IntPtr outErr)
    {
        KafkaException.ThrowIfFfiFailed(ref outErr);
        sbyte code = NativeMethods.AclOperationCode(singleton, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return AclEnumCodes.DefinedOrUnknown((AclOperation)code);
    }

    private static AclPermissionType ReadPermissionType(IntPtr singleton, ref IntPtr outErr)
    {
        KafkaException.ThrowIfFfiFailed(ref outErr);
        sbyte code = NativeMethods.AclPermissionTypeCode(singleton, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return AclEnumCodes.DefinedOrUnknown((AclPermissionType)code);
    }

    private static string? ReadString(IntPtr value, ref IntPtr outErr)
    {
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return Utf8Marshal.PtrToString(value);
    }

    private static void Destroy(IntPtr native, NativeDestroy destroy)
    {
        if (native == IntPtr.Zero)
        {
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        destroy(native, ref outErr);
        if (outErr != IntPtr.Zero)
        {
            NativeMethods.ErrorDestroy(outErr);
        }
    }
}
