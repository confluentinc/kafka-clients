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
    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_describe_configs_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminDescribeConfigsWithOptions(
        SafeAdminClientHandle self, IntPtr configResources, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeConfigsResult_values_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeConfigsResultValues(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeConfigsResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeConfigsResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeConfigsOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeConfigsOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeConfigsOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeConfigsOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeConfigsOptions_set_include_synonyms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeConfigsOptionsSetIncludeSynonyms(IntPtr self, sbyte includeSynonyms, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeConfigsOptions_set_include_documentation_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeConfigsOptionsSetIncludeDocumentation(
        IntPtr self, sbyte includeDocumentation, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeConfigsOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeConfigsOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_incremental_alter_configs_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminIncrementalAlterConfigsWithOptions(
        SafeAdminClientHandle self, IntPtr configs, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterConfigsResult_values_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AlterConfigsResultValues(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterConfigsResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AlterConfigsResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterConfigsOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AlterConfigsOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterConfigsOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AlterConfigsOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterConfigsOptions_set_validate_only_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AlterConfigsOptionsSetValidateOnly(IntPtr self, sbyte validateOnly, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterConfigsOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AlterConfigsOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterConfigOp_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AlterConfigOpNew(IntPtr configEntry, IntPtr operationType, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterConfigOp_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AlterConfigOpDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterConfigOp_OpType_set_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AlterConfigOpTypeSet(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterConfigOp_OpType_delete_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AlterConfigOpTypeDelete(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterConfigOp_OpType_append_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AlterConfigOpTypeAppend(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterConfigOp_OpType_subtract_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AlterConfigOpTypeSubtract(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConfigEntryNew(IntPtr name, IntPtr value, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConfigEntryDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_list_config_resources_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminListConfigResourcesWithOptions(
        SafeAdminClientHandle self, IntPtr configResourceTypes, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListConfigResourcesResult_all_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListConfigResourcesResultAll(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListConfigResourcesResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListConfigResourcesResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListConfigResourcesOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListConfigResourcesOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListConfigResourcesOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListConfigResourcesOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListConfigResourcesOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListConfigResourcesOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_config_ConfigResource_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConfigResourceNew(IntPtr resourceType, IntPtr name, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_config_ConfigResource_type_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConfigResourceGetType(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_config_ConfigResource_name_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConfigResourceName(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_config_ConfigResource_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConfigResourceDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_config_ConfigResource_Type_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern sbyte ConfigResourceTypeId(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_config_ConfigResource_Type_for_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConfigResourceTypeForId(sbyte id, ref IntPtr outErr);
}
