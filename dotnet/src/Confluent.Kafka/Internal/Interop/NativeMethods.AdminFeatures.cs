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
    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_describe_features_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminDescribeFeaturesWithOptions(
        SafeAdminClientHandle self, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_update_features_with_options", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminUpdateFeaturesWithOptions(
        SafeAdminClientHandle self, IntPtr featureUpdates, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeFeaturesResult_feature_metadata_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeFeaturesResultFeatureMetadata(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeFeaturesResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeFeaturesResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_UpdateFeaturesResult_values_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr UpdateFeaturesResultValues(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_UpdateFeaturesResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void UpdateFeaturesResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_FeatureMetadata_finalized_features_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr FeatureMetadataFinalizedFeatures(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_FeatureMetadata_finalized_features_epoch_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long FeatureMetadataFinalizedFeaturesEpoch(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_FeatureMetadata_supported_features_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr FeatureMetadataSupportedFeatures(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_FinalizedVersionRange_min_version_level_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern short FinalizedVersionRangeMinVersionLevel(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_FinalizedVersionRange_max_version_level_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern short FinalizedVersionRangeMaxVersionLevel(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_SupportedVersionRange_min_version_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern short SupportedVersionRangeMinVersion(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_SupportedVersionRange_max_version_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern short SupportedVersionRangeMaxVersion(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_FeatureUpdate_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr FeatureUpdateNew(short maxVersionLevel, IntPtr upgradeType, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_FeatureUpdate_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void FeatureUpdateDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_FeatureUpdate_UpgradeType_from_code_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr UpgradeTypeFromCode(int code, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeFeaturesOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeFeaturesOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeFeaturesOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeFeaturesOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeFeaturesOptions_set_node_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeFeaturesOptionsSetNodeId(IntPtr self, int nodeId, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeFeaturesOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeFeaturesOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_UpdateFeaturesOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr UpdateFeaturesOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_UpdateFeaturesOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void UpdateFeaturesOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_UpdateFeaturesOptions_set_validate_only_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void UpdateFeaturesOptionsSetValidateOnly(IntPtr self, sbyte validateOnly, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_UpdateFeaturesOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void UpdateFeaturesOptionsDestroy(IntPtr self, ref IntPtr outErr);
}
