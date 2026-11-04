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
using Confluent.Kafka.Common.Quota;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

internal sealed partial class NativeAdminClient
{
    private static readonly ResultFuture s_alterClientQuotasValues = NativeMethods.AlterClientQuotasResultValues;

    private static readonly ResultFuture s_describeClientQuotasEntities =
        NativeMethods.DescribeClientQuotasResultEntities;

    private static readonly AdminOptionsMarshal.New s_newDescribeClientQuotasOptions =
        NativeMethods.DescribeClientQuotasOptionsNew;

    private static readonly AdminOptionsMarshal.Set<DescribeClientQuotasOptions> s_setDescribeClientQuotasOption =
        SetDescribeClientQuotasOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyDescribeClientQuotasOptions =
        NativeMethods.DescribeClientQuotasOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyDescribeClientQuotasResult =
        NativeMethods.DescribeClientQuotasResultDestroy;

    private static readonly AdminOptionsMarshal.New s_newAlterClientQuotasOptions =
        NativeMethods.AlterClientQuotasOptionsNew;

    private static readonly AdminOptionsMarshal.Set<AlterClientQuotasOptions> s_setAlterClientQuotasOption =
        SetAlterClientQuotasOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyAlterClientQuotasOptions =
        NativeMethods.AlterClientQuotasOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyAlterClientQuotasResult =
        NativeMethods.AlterClientQuotasResultDestroy;

    private static readonly Func<IntPtr, ClientQuotaEntity> s_quotaEntityKey = ClientQuotaMarshal.CopyOutEntity;

    private static readonly Func<IntPtr, IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>>
        s_copyOutQuotaEntities = ClientQuotaMarshal.CopyOutEntities;

    internal (
        KafkaFutureImpl<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>> Entities,
        SafeAdminResultHandle Result) DescribeClientQuotas(
        ClientQuotaFilter filter, DescribeClientQuotasOptions? options)
    {
        EnterUse();
        try
        {
            if (filter is null)
            {
                throw new ArgumentNullException(nameof(filter));
            }

            foreach (ClientQuotaFilterComponent component in filter.Components)
            {
                AdminStrings.Validate(component.EntityType, nameof(filter));
                AdminStrings.Validate(component.MatchName, nameof(filter));
            }

            IntPtr native = IntPtr.Zero;
            SafeAdminResultHandle result;
            try
            {
                native = ClientQuotaMarshal.NewFilter(filter);
                result = CallWithOptions(
                    NewDescribeClientQuotasOptions(options),
                    s_destroyDescribeClientQuotasOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminDescribeClientQuotasWithOptions(admin, native, nativeOptions, ref outErr),
                    s_destroyDescribeClientQuotasResult);
            }
            finally
            {
                ClientQuotaMarshal.DestroyFilter(native);
            }

            return (
                StartBorrowedFuture(result, s_describeClientQuotasEntities, s_copyOutQuotaEntities),
                result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewDescribeClientQuotasOptions(DescribeClientQuotasOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newDescribeClientQuotasOptions,
            options,
            options?.Setters,
            s_setDescribeClientQuotasOption,
            s_destroyDescribeClientQuotasOptions);

    internal static void FreeDescribeClientQuotasOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyDescribeClientQuotasOptions);

    private static void SetDescribeClientQuotasOption(
        IntPtr native, DescribeClientQuotasOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(DescribeClientQuotasOptions.TimeoutMs):
                NativeMethods.DescribeClientQuotasOptionsSetTimeoutMs(
                    native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            default:
                throw new InvalidOperationException("DescribeClientQuotasOptions has no C setter for " + setter + ".");
        }
    }

    internal (IReadOnlyDictionary<ClientQuotaEntity, KafkaFutureImpl<object?>> Values, SafeAdminResultHandle Result)
        AlterClientQuotas(IEnumerable<ClientQuotaAlteration> entries, AlterClientQuotasOptions? options)
    {
        EnterUse();
        try
        {
            if (entries is null)
            {
                throw new ArgumentNullException(nameof(entries));
            }

            List<ClientQuotaAlteration> alterations = RequestedAlterations(entries, nameof(entries));
            IntPtr[] natives = new IntPtr[alterations.Count];
            IntPtr list = IntPtr.Zero;
            SafeAdminResultHandle result;
            try
            {
                for (int i = 0; i < natives.Length; i++)
                {
                    natives[i] = ClientQuotaMarshal.NewAlteration(alterations[i]);
                }

                list = NewList(natives);
                result = CallWithOptions(
                    NewAlterClientQuotasOptions(options),
                    s_destroyAlterClientQuotasOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminAlterClientQuotasWithOptions(admin, list, nativeOptions, ref outErr),
                    s_destroyAlterClientQuotasResult);
            }
            finally
            {
                DestroyList(list);
                foreach (IntPtr native in natives)
                {
                    ClientQuotaMarshal.DestroyAlteration(native);
                }
            }

            return (
                StartMapFutures(
                    result,
                    s_alterClientQuotasValues,
                    s_quotaEntityKey,
                    EqualityComparer<ClientQuotaEntity>.Default,
                    s_noValue),
                result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewAlterClientQuotasOptions(AlterClientQuotasOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newAlterClientQuotasOptions,
            options,
            options?.Setters,
            s_setAlterClientQuotasOption,
            s_destroyAlterClientQuotasOptions);

    internal static void FreeAlterClientQuotasOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyAlterClientQuotasOptions);

    private static void SetAlterClientQuotasOption(
        IntPtr native, AlterClientQuotasOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(AlterClientQuotasOptions.TimeoutMs):
                NativeMethods.AlterClientQuotasOptionsSetTimeoutMs(
                    native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            case nameof(AlterClientQuotasOptions.ValidateOnly):
                NativeMethods.AlterClientQuotasOptionsSetValidateOnly(
                    native, (sbyte)(options.ValidateOnly ? 1 : 0), ref outErr);
                break;
            default:
                throw new InvalidOperationException("AlterClientQuotasOptions has no C setter for " + setter + ".");
        }
    }

    internal static List<ClientQuotaAlteration> RequestedAlterations(
        IEnumerable<ClientQuotaAlteration> entries, string parameterName)
    {
        // Every alteration is sent, a repeated entity included; the core answers each entity once.
        List<ClientQuotaAlteration> alterations = new List<ClientQuotaAlteration>();
        foreach (ClientQuotaAlteration alteration in entries)
        {
            if (alteration is null)
            {
                throw new ArgumentException(
                    "The client quota alterations must not contain a null element.", parameterName);
            }

            foreach (KeyValuePair<string, string?> part in alteration.Entity.Entries)
            {
                AdminStrings.Validate(part.Key, parameterName);
                AdminStrings.Validate(part.Value, parameterName);
            }

            foreach (ClientQuotaAlteration.Op op in alteration.Ops)
            {
                AdminStrings.Validate(op.Key, parameterName);
            }

            alterations.Add(alteration);
        }

        return alterations;
    }
}
