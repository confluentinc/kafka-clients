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
using System.Runtime.InteropServices;

namespace Confluent.Kafka.Internal.Interop;

internal static partial class NativeMethods
{
    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_describe_client_quotas_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminDescribeClientQuotasWithOptions(
        SafeAdminClientHandle self, IntPtr filter, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_alter_client_quotas_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminAlterClientQuotasWithOptions(
        SafeAdminClientHandle self, IntPtr entries, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClientQuotasResult_entities_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeClientQuotasResultEntities(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClientQuotasResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeClientQuotasResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterClientQuotasResult_values_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AlterClientQuotasResultValues(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterClientQuotasResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AlterClientQuotasResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClientQuotasOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeClientQuotasOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClientQuotasOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeClientQuotasOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClientQuotasOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeClientQuotasOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterClientQuotasOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AlterClientQuotasOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterClientQuotasOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AlterClientQuotasOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterClientQuotasOptions_set_validate_only_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AlterClientQuotasOptionsSetValidateOnly(IntPtr self, sbyte validateOnly, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterClientQuotasOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AlterClientQuotasOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_quota_ClientQuotaAlteration_Op_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ClientQuotaAlterationOpNew(IntPtr key, double value, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_quota_ClientQuotaAlteration_Op_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ClientQuotaAlterationOpDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_quota_ClientQuotaAlteration_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ClientQuotaAlterationNew(IntPtr entity, IntPtr ops, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_quota_ClientQuotaAlteration_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ClientQuotaAlterationDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_quota_ClientQuotaEntity_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ClientQuotaEntityNew(IntPtr entries, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_quota_ClientQuotaEntity_entries_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ClientQuotaEntityEntries(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_quota_ClientQuotaEntity_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ClientQuotaEntityDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_quota_ClientQuotaFilter_contains_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ClientQuotaFilterContains(IntPtr components, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_quota_ClientQuotaFilter_contains_only_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ClientQuotaFilterContainsOnly(IntPtr components, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_quota_ClientQuotaFilter_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ClientQuotaFilterDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_quota_ClientQuotaFilterComponent_of_entity_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ClientQuotaFilterComponentOfEntity(IntPtr entityType, IntPtr entityName, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_quota_ClientQuotaFilterComponent_of_default_entity_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ClientQuotaFilterComponentOfDefaultEntity(IntPtr entityType, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_quota_ClientQuotaFilterComponent_of_entity_type_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ClientQuotaFilterComponentOfEntityType(IntPtr entityType, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_quota_ClientQuotaFilterComponent_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ClientQuotaFilterComponentDestroy(IntPtr self, ref IntPtr outErr);
}
