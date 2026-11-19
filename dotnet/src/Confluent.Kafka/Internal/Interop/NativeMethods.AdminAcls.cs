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
    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_create_acls_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminCreateAclsWithOptions(
        SafeAdminClientHandle self, IntPtr acls, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_describe_acls_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminDescribeAclsWithOptions(
        SafeAdminClientHandle self, IntPtr filter, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_delete_acls_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminDeleteAclsWithOptions(
        SafeAdminClientHandle self, IntPtr filters, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreateAclsResult_values_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr CreateAclsResultValues(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreateAclsResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void CreateAclsResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeAclsResult_values_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeAclsResultValues(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeAclsResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeAclsResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteAclsResult_values_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DeleteAclsResultValues(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteAclsResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DeleteAclsResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteAclsResult_FilterResults_values_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DeleteAclsFilterResultsValues(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteAclsResult_FilterResult_binding_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DeleteAclsFilterResultBinding(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteAclsResult_FilterResult_error_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DeleteAclsFilterResultError(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreateAclsOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr CreateAclsOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreateAclsOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void CreateAclsOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreateAclsOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void CreateAclsOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeAclsOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeAclsOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeAclsOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeAclsOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeAclsOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeAclsOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteAclsOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DeleteAclsOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteAclsOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DeleteAclsOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteAclsOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DeleteAclsOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_resource_ResourceType_from_code_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ResourceTypeFromCode(sbyte code, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_resource_ResourceType_code_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern sbyte ResourceTypeCode(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_resource_PatternType_from_code_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr PatternTypeFromCode(sbyte code, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_resource_PatternType_code_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern sbyte PatternTypeCode(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_acl_AclOperation_from_code_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AclOperationFromCode(sbyte code, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_acl_AclPermissionType_from_code_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AclPermissionTypeFromCode(sbyte code, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_acl_AclPermissionType_code_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern sbyte AclPermissionTypeCode(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_resource_ResourcePattern_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ResourcePatternNew(IntPtr resourceType, IntPtr name, IntPtr patternType, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_resource_ResourcePattern_resource_type_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ResourcePatternResourceType(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_resource_ResourcePattern_name_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ResourcePatternName(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_resource_ResourcePattern_pattern_type_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ResourcePatternPatternType(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_resource_ResourcePattern_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ResourcePatternDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_resource_ResourcePatternFilter_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ResourcePatternFilterNew(
        IntPtr resourceType, IntPtr name, IntPtr patternType, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_resource_ResourcePatternFilter_resource_type_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ResourcePatternFilterResourceType(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_resource_ResourcePatternFilter_name_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ResourcePatternFilterName(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_resource_ResourcePatternFilter_pattern_type_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ResourcePatternFilterPatternType(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_resource_ResourcePatternFilter_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ResourcePatternFilterDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_acl_AccessControlEntry_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AccessControlEntryNew(
        IntPtr principal, IntPtr host, IntPtr operation, IntPtr permissionType, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_acl_AccessControlEntry_principal_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AccessControlEntryPrincipal(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_acl_AccessControlEntry_host_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AccessControlEntryHost(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_acl_AccessControlEntry_operation_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AccessControlEntryOperation(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_acl_AccessControlEntry_permission_type_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AccessControlEntryPermissionType(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_acl_AccessControlEntry_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AccessControlEntryDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_acl_AccessControlEntryFilter_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AccessControlEntryFilterNew(
        IntPtr principal, IntPtr host, IntPtr operation, IntPtr permissionType, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_acl_AccessControlEntryFilter_principal_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AccessControlEntryFilterPrincipal(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_acl_AccessControlEntryFilter_host_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AccessControlEntryFilterHost(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_acl_AccessControlEntryFilter_operation_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AccessControlEntryFilterOperation(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_acl_AccessControlEntryFilter_permission_type_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AccessControlEntryFilterPermissionType(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_acl_AccessControlEntryFilter_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AccessControlEntryFilterDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_acl_AclBinding_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AclBindingNew(IntPtr pattern, IntPtr entry, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_acl_AclBinding_pattern_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AclBindingPattern(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_acl_AclBinding_entry_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AclBindingEntry(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_acl_AclBinding_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AclBindingDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_acl_AclBindingFilter_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AclBindingFilterNew(IntPtr patternFilter, IntPtr entryFilter, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_acl_AclBindingFilter_pattern_filter_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AclBindingFilterPatternFilter(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_acl_AclBindingFilter_entry_filter_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AclBindingFilterEntryFilter(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_acl_AclBindingFilter_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AclBindingFilterDestroy(IntPtr self, ref IntPtr outErr);
}
