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
    private static readonly ResultFuture s_createAclsValues = NativeMethods.CreateAclsResultValues;

    private static readonly ResultFuture s_deleteAclsValues = NativeMethods.DeleteAclsResultValues;

    private static readonly ResultFuture s_describeAclsValues = NativeMethods.DescribeAclsResultValues;

    private static readonly AdminOptionsMarshal.New s_newCreateAclsOptions = NativeMethods.CreateAclsOptionsNew;

    private static readonly AdminOptionsMarshal.Set<CreateAclsOptions> s_setCreateAclsOption = SetCreateAclsOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyCreateAclsOptions = NativeMethods.CreateAclsOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyCreateAclsResult = NativeMethods.CreateAclsResultDestroy;

    private static readonly AdminOptionsMarshal.New s_newDescribeAclsOptions = NativeMethods.DescribeAclsOptionsNew;

    private static readonly AdminOptionsMarshal.Set<DescribeAclsOptions> s_setDescribeAclsOption = SetDescribeAclsOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyDescribeAclsOptions =
        NativeMethods.DescribeAclsOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyDescribeAclsResult =
        NativeMethods.DescribeAclsResultDestroy;

    private static readonly AdminOptionsMarshal.New s_newDeleteAclsOptions = NativeMethods.DeleteAclsOptionsNew;

    private static readonly AdminOptionsMarshal.Set<DeleteAclsOptions> s_setDeleteAclsOption = SetDeleteAclsOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyDeleteAclsOptions = NativeMethods.DeleteAclsOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyDeleteAclsResult = NativeMethods.DeleteAclsResultDestroy;

    private static readonly Func<IntPtr, AclBinding> s_aclBindingKey = AclMarshal.CopyOutBinding;

    private static readonly Func<IntPtr, AclBindingFilter> s_aclBindingFilterKey = AclMarshal.CopyOutFilter;

    private static readonly Func<IntPtr, IReadOnlyCollection<AclBinding>> s_copyOutAclBindings =
        AclMarshal.CopyOutBindings;

    private static readonly Func<IntPtr, DeleteAclsResult.FilterResults> s_copyOutFilterResults =
        AclMarshal.CopyOutFilterResults;

    internal (IReadOnlyDictionary<AclBinding, KafkaFutureImpl<object?>> Values, SafeAdminResultHandle Result) CreateAcls(
        IEnumerable<AclBinding> acls, CreateAclsOptions? options)
    {
        EnterUse();
        try
        {
            if (acls is null)
            {
                throw new ArgumentNullException(nameof(acls));
            }

            List<AclBinding> keys = DistinctBindings(acls, nameof(acls));
            IntPtr[] natives = new IntPtr[keys.Count];
            IntPtr list = IntPtr.Zero;
            SafeAdminResultHandle result;
            try
            {
                for (int i = 0; i < natives.Length; i++)
                {
                    natives[i] = AclMarshal.NewBinding(keys[i]);
                }

                list = NewList(natives);
                result = CallWithOptions(
                    NewCreateAclsOptions(options),
                    s_destroyCreateAclsOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminCreateAclsWithOptions(admin, list, nativeOptions, ref outErr),
                    s_destroyCreateAclsResult);
            }
            finally
            {
                DestroyList(list);
                foreach (IntPtr native in natives)
                {
                    AclMarshal.DestroyBinding(native);
                }
            }

            return (
                StartMapFutures(
                    result,
                    s_createAclsValues,
                    s_aclBindingKey,
                    EqualityComparer<AclBinding>.Default,
                    s_noValue),
                result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewCreateAclsOptions(CreateAclsOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newCreateAclsOptions, options, options?.Setters, s_setCreateAclsOption, s_destroyCreateAclsOptions);

    internal static void FreeCreateAclsOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyCreateAclsOptions);

    private static void SetCreateAclsOption(IntPtr native, CreateAclsOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(CreateAclsOptions.TimeoutMs):
                NativeMethods.CreateAclsOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            default:
                throw new InvalidOperationException("CreateAclsOptions has no C setter for " + setter + ".");
        }
    }

    internal (KafkaFutureImpl<IReadOnlyCollection<AclBinding>> Values, SafeAdminResultHandle Result) DescribeAcls(
        AclBindingFilter filter, DescribeAclsOptions? options)
    {
        EnterUse();
        try
        {
            if (filter is null)
            {
                throw new ArgumentNullException(nameof(filter));
            }

            AdminStrings.Validate(filter.PatternFilter.Name, nameof(filter));
            AdminStrings.Validate(filter.EntryFilter.Principal, nameof(filter));
            AdminStrings.Validate(filter.EntryFilter.Host, nameof(filter));

            IntPtr native = IntPtr.Zero;
            SafeAdminResultHandle result;
            try
            {
                native = AclMarshal.NewFilter(filter);
                result = CallWithOptions(
                    NewDescribeAclsOptions(options),
                    s_destroyDescribeAclsOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminDescribeAclsWithOptions(admin, native, nativeOptions, ref outErr),
                    s_destroyDescribeAclsResult);
            }
            finally
            {
                AclMarshal.DestroyFilter(native);
            }

            return (StartBorrowedFuture(result, s_describeAclsValues, s_copyOutAclBindings), result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewDescribeAclsOptions(DescribeAclsOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newDescribeAclsOptions, options, options?.Setters, s_setDescribeAclsOption, s_destroyDescribeAclsOptions);

    internal static void FreeDescribeAclsOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyDescribeAclsOptions);

    private static void SetDescribeAclsOption(IntPtr native, DescribeAclsOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(DescribeAclsOptions.TimeoutMs):
                NativeMethods.DescribeAclsOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            default:
                throw new InvalidOperationException("DescribeAclsOptions has no C setter for " + setter + ".");
        }
    }

    internal (
        IReadOnlyDictionary<AclBindingFilter, KafkaFutureImpl<DeleteAclsResult.FilterResults>> Values,
        SafeAdminResultHandle Result) DeleteAcls(IEnumerable<AclBindingFilter> filters, DeleteAclsOptions? options)
    {
        EnterUse();
        try
        {
            if (filters is null)
            {
                throw new ArgumentNullException(nameof(filters));
            }

            List<AclBindingFilter> keys = DistinctFilters(filters, nameof(filters));
            IntPtr[] natives = new IntPtr[keys.Count];
            IntPtr list = IntPtr.Zero;
            SafeAdminResultHandle result;
            try
            {
                for (int i = 0; i < natives.Length; i++)
                {
                    natives[i] = AclMarshal.NewFilter(keys[i]);
                }

                list = NewList(natives);
                result = CallWithOptions(
                    NewDeleteAclsOptions(options),
                    s_destroyDeleteAclsOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminDeleteAclsWithOptions(admin, list, nativeOptions, ref outErr),
                    s_destroyDeleteAclsResult);
            }
            finally
            {
                DestroyList(list);
                foreach (IntPtr native in natives)
                {
                    AclMarshal.DestroyFilter(native);
                }
            }

            return (
                StartMapFutures(
                    result,
                    s_deleteAclsValues,
                    s_aclBindingFilterKey,
                    EqualityComparer<AclBindingFilter>.Default,
                    s_copyOutFilterResults),
                result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewDeleteAclsOptions(DeleteAclsOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newDeleteAclsOptions, options, options?.Setters, s_setDeleteAclsOption, s_destroyDeleteAclsOptions);

    internal static void FreeDeleteAclsOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyDeleteAclsOptions);

    private static void SetDeleteAclsOption(IntPtr native, DeleteAclsOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(DeleteAclsOptions.TimeoutMs):
                NativeMethods.DeleteAclsOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            default:
                throw new InvalidOperationException("DeleteAclsOptions has no C setter for " + setter + ".");
        }
    }

    private static List<AclBinding> DistinctBindings(IEnumerable<AclBinding> acls, string parameterName)
    {
        List<AclBinding> keys = new List<AclBinding>();
        HashSet<AclBinding> seen = new HashSet<AclBinding>();
        foreach (AclBinding acl in acls)
        {
            if (acl is null)
            {
                throw new ArgumentException("The ACL bindings must not contain a null element.", parameterName);
            }

            AdminStrings.Validate(acl.Pattern.Name, parameterName);
            AdminStrings.Validate(acl.Entry.Principal, parameterName);
            AdminStrings.Validate(acl.Entry.Host, parameterName);
            if (seen.Add(acl))
            {
                keys.Add(acl);
            }
        }

        return keys;
    }

    private static List<AclBindingFilter> DistinctFilters(IEnumerable<AclBindingFilter> filters, string parameterName)
    {
        List<AclBindingFilter> keys = new List<AclBindingFilter>();
        HashSet<AclBindingFilter> seen = new HashSet<AclBindingFilter>();
        foreach (AclBindingFilter filter in filters)
        {
            if (filter is null)
            {
                throw new ArgumentException("The ACL filters must not contain a null element.", parameterName);
            }

            AdminStrings.Validate(filter.PatternFilter.Name, parameterName);
            AdminStrings.Validate(filter.EntryFilter.Principal, parameterName);
            AdminStrings.Validate(filter.EntryFilter.Host, parameterName);
            if (seen.Add(filter))
            {
                keys.Add(filter);
            }
        }

        return keys;
    }
}
