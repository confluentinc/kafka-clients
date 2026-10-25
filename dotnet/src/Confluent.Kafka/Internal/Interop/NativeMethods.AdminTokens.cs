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
    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_create_delegation_token_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminCreateDelegationTokenWithOptions(
        SafeAdminClientHandle self, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_renew_delegation_token_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminRenewDelegationTokenWithOptions(
        SafeAdminClientHandle self, KafkaBytes hmac, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_expire_delegation_token_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminExpireDelegationTokenWithOptions(
        SafeAdminClientHandle self, KafkaBytes hmac, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_describe_delegation_token_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminDescribeDelegationTokenWithOptions(
        SafeAdminClientHandle self, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreateDelegationTokenResult_delegation_token_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr CreateDelegationTokenResultDelegationToken(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreateDelegationTokenResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void CreateDelegationTokenResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_RenewDelegationTokenResult_expiry_timestamp_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr RenewDelegationTokenResultExpiryTimestamp(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_RenewDelegationTokenResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void RenewDelegationTokenResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ExpireDelegationTokenResult_expiry_timestamp_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ExpireDelegationTokenResultExpiryTimestamp(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ExpireDelegationTokenResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ExpireDelegationTokenResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeDelegationTokenResult_delegation_tokens_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeDelegationTokenResultDelegationTokens(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeDelegationTokenResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeDelegationTokenResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreateDelegationTokenOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr CreateDelegationTokenOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreateDelegationTokenOptions_set_renewers_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void CreateDelegationTokenOptionsSetRenewers(IntPtr self, IntPtr renewers, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreateDelegationTokenOptions_set_owner_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void CreateDelegationTokenOptionsSetOwner(IntPtr self, IntPtr owner, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreateDelegationTokenOptions_set_max_lifetime_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void CreateDelegationTokenOptionsSetMaxLifetimeMs(IntPtr self, long maxLifetimeMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreateDelegationTokenOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void CreateDelegationTokenOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreateDelegationTokenOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void CreateDelegationTokenOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_RenewDelegationTokenOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr RenewDelegationTokenOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_RenewDelegationTokenOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void RenewDelegationTokenOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_RenewDelegationTokenOptions_set_renew_time_period_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void RenewDelegationTokenOptionsSetRenewTimePeriodMs(
        IntPtr self, long renewTimePeriodMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_RenewDelegationTokenOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void RenewDelegationTokenOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ExpireDelegationTokenOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ExpireDelegationTokenOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ExpireDelegationTokenOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ExpireDelegationTokenOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ExpireDelegationTokenOptions_set_expiry_time_period_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ExpireDelegationTokenOptionsSetExpiryTimePeriodMs(
        IntPtr self, long expiryTimePeriodMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ExpireDelegationTokenOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ExpireDelegationTokenOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeDelegationTokenOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeDelegationTokenOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeDelegationTokenOptions_set_owners_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeDelegationTokenOptionsSetOwners(IntPtr self, IntPtr owners, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeDelegationTokenOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeDelegationTokenOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeDelegationTokenOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeDelegationTokenOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_security_auth_KafkaPrincipal_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr KafkaPrincipalNew(IntPtr principalType, IntPtr name, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_security_auth_KafkaPrincipal_name_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr KafkaPrincipalName(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_security_auth_KafkaPrincipal_principal_type_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr KafkaPrincipalPrincipalType(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_security_auth_KafkaPrincipal_token_authenticated_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern sbyte KafkaPrincipalTokenAuthenticated(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_security_auth_KafkaPrincipal_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void KafkaPrincipalDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_security_token_delegation_DelegationToken_token_info_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DelegationTokenTokenInfo(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_security_token_delegation_DelegationToken_hmac_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern KafkaBytes DelegationTokenHmac(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_security_token_delegation_TokenInformation_owner_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TokenInformationOwner(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_security_token_delegation_TokenInformation_token_requester_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TokenInformationTokenRequester(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_security_token_delegation_TokenInformation_renewers_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TokenInformationRenewers(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_security_token_delegation_TokenInformation_issue_timestamp_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long TokenInformationIssueTimestamp(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_security_token_delegation_TokenInformation_expiry_timestamp_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long TokenInformationExpiryTimestamp(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_security_token_delegation_TokenInformation_token_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TokenInformationTokenId(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_security_token_delegation_TokenInformation_max_timestamp_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long TokenInformationMaxTimestamp(IntPtr self, ref IntPtr outErr);
}
