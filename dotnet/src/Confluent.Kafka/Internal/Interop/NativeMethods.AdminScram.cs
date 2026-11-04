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
    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_describe_user_scram_credentials_with_users_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminDescribeUserScramCredentialsWithUsersOptions(
        SafeAdminClientHandle self, IntPtr users, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_alter_user_scram_credentials_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminAlterUserScramCredentialsWithOptions(
        SafeAdminClientHandle self, IntPtr alterations, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeUserScramCredentialsResult_all_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeUserScramCredentialsResultAll(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeUserScramCredentialsResult_users_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeUserScramCredentialsResultUsers(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeUserScramCredentialsResult_description_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeUserScramCredentialsResultDescription(
        IntPtr self, IntPtr userName, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeUserScramCredentialsResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeUserScramCredentialsResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterUserScramCredentialsResult_values_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AlterUserScramCredentialsResultValues(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterUserScramCredentialsResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AlterUserScramCredentialsResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeUserScramCredentialsOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeUserScramCredentialsOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeUserScramCredentialsOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeUserScramCredentialsOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeUserScramCredentialsOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeUserScramCredentialsOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterUserScramCredentialsOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AlterUserScramCredentialsOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterUserScramCredentialsOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AlterUserScramCredentialsOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterUserScramCredentialsOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AlterUserScramCredentialsOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_UserScramCredentialsDescription_name_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr UserScramCredentialsDescriptionName(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_UserScramCredentialsDescription_credential_infos_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr UserScramCredentialsDescriptionCredentialInfos(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ScramCredentialInfo_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ScramCredentialInfoNew(IntPtr mechanism, int iterations, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ScramCredentialInfo_mechanism_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ScramCredentialInfoMechanism(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ScramCredentialInfo_iterations_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ScramCredentialInfoIterations(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ScramCredentialInfo_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ScramCredentialInfoDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ScramMechanism_from_type_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ScramMechanismFromType(sbyte type, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ScramMechanism_type_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern sbyte ScramMechanismType(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_UserScramCredentialDeletion_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr UserScramCredentialDeletionNew(IntPtr user, IntPtr mechanism, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_UserScramCredentialDeletion_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void UserScramCredentialDeletionDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_UserScramCredentialUpsertion_with_bytes_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr UserScramCredentialUpsertionWithBytes(
        IntPtr user, IntPtr credentialInfo, KafkaBytes password, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_UserScramCredentialUpsertion_with_salt_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr UserScramCredentialUpsertionWithSalt(
        IntPtr user, IntPtr credentialInfo, KafkaBytes password, KafkaBytes salt, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_UserScramCredentialUpsertion_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void UserScramCredentialUpsertionDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_UserScramCredentialAlteration_upsertion_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr UserScramCredentialAlterationUpsertion(IntPtr value, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_UserScramCredentialAlteration_deletion_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr UserScramCredentialAlterationDeletion(IntPtr value, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_UserScramCredentialAlteration_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void UserScramCredentialAlterationDestroy(IntPtr self, ref IntPtr outErr);
}
