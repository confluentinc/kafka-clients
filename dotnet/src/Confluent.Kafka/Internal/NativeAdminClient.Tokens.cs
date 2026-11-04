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
using System.Runtime.InteropServices;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Common.Security.Auth;
using Confluent.Kafka.Common.Security.Token.Delegation;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

internal sealed partial class NativeAdminClient
{
    private static readonly PrincipalListSetter s_createDelegationTokenOptionsSetRenewers =
        NativeMethods.CreateDelegationTokenOptionsSetRenewers;

    private static readonly PrincipalListSetter s_describeDelegationTokenOptionsSetOwners =
        NativeMethods.DescribeDelegationTokenOptionsSetOwners;

    private static readonly AdminOptionsMarshal.New s_newCreateDelegationTokenOptions =
        NativeMethods.CreateDelegationTokenOptionsNew;

    private static readonly AdminOptionsMarshal.Set<CreateDelegationTokenOptions> s_setCreateDelegationTokenOption =
        SetCreateDelegationTokenOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyCreateDelegationTokenOptions =
        NativeMethods.CreateDelegationTokenOptionsDestroy;

    private static readonly AdminOptionsMarshal.New s_newRenewDelegationTokenOptions =
        NativeMethods.RenewDelegationTokenOptionsNew;

    private static readonly AdminOptionsMarshal.Set<RenewDelegationTokenOptions> s_setRenewDelegationTokenOption =
        SetRenewDelegationTokenOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyRenewDelegationTokenOptions =
        NativeMethods.RenewDelegationTokenOptionsDestroy;

    private static readonly AdminOptionsMarshal.New s_newExpireDelegationTokenOptions =
        NativeMethods.ExpireDelegationTokenOptionsNew;

    private static readonly AdminOptionsMarshal.Set<ExpireDelegationTokenOptions> s_setExpireDelegationTokenOption =
        SetExpireDelegationTokenOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyExpireDelegationTokenOptions =
        NativeMethods.ExpireDelegationTokenOptionsDestroy;

    private static readonly AdminOptionsMarshal.New s_newDescribeDelegationTokenOptions =
        NativeMethods.DescribeDelegationTokenOptionsNew;

    private static readonly AdminOptionsMarshal.Set<DescribeDelegationTokenOptions>
        s_setDescribeDelegationTokenOption = SetDescribeDelegationTokenOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyDescribeDelegationTokenOptions =
        NativeMethods.DescribeDelegationTokenOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyCreateDelegationTokenResult =
        NativeMethods.CreateDelegationTokenResultDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyRenewDelegationTokenResult =
        NativeMethods.RenewDelegationTokenResultDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyExpireDelegationTokenResult =
        NativeMethods.ExpireDelegationTokenResultDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyDescribeDelegationTokenResult =
        NativeMethods.DescribeDelegationTokenResultDestroy;

    // Renew and expire have accessors of one shape, so each RPC reads its own through a field a test pins.
    private static readonly ResultFuture s_createDelegationTokenToken =
        NativeMethods.CreateDelegationTokenResultDelegationToken;

    private static readonly ResultFuture s_renewDelegationTokenExpiryTimestamp =
        NativeMethods.RenewDelegationTokenResultExpiryTimestamp;

    private static readonly ResultFuture s_expireDelegationTokenExpiryTimestamp =
        NativeMethods.ExpireDelegationTokenResultExpiryTimestamp;

    private static readonly ResultFuture s_describeDelegationTokenTokens =
        NativeMethods.DescribeDelegationTokenResultDelegationTokens;

    private static readonly Func<IntPtr, DelegationToken> s_copyOutDelegationToken = DelegationTokenMarshal.CopyOutToken;

    private static readonly Func<IntPtr, IReadOnlyList<DelegationToken>> s_copyOutDelegationTokens =
        DelegationTokenMarshal.CopyOutTokens;

    private static readonly Func<IntPtr, long> s_copyOutTimestamp = static value => Marshal.ReadInt64(value);

    internal (KafkaFutureImpl<DelegationToken> Token, SafeAdminResultHandle Result) CreateDelegationToken(
        CreateDelegationTokenOptions? options)
    {
        EnterUse();
        try
        {
            if (options is not null)
            {
                if (options.Renewers is null)
                {
                    throw new ArgumentException(
                        "CreateDelegationTokenOptions.Renewers must not be null; use an empty list.", nameof(options));
                }

                foreach (KafkaPrincipal renewer in options.Renewers)
                {
                    if (renewer is null)
                    {
                        throw new ArgumentException(
                            "CreateDelegationTokenOptions.Renewers must not contain a null element.", nameof(options));
                    }

                    ValidatePrincipal(renewer, nameof(options));
                }

                if (options.Owner is not null)
                {
                    ValidatePrincipal(options.Owner, nameof(options));
                }
            }

            SafeAdminResultHandle result = CallWithOptions(
                NewCreateDelegationTokenOptions(options),
                s_destroyCreateDelegationTokenOptions,
                (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                    NativeMethods.AdminCreateDelegationTokenWithOptions(admin, nativeOptions, ref outErr),
                s_destroyCreateDelegationTokenResult);
            return (StartBorrowedFuture(result, s_createDelegationTokenToken, s_copyOutDelegationToken), result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal (KafkaFutureImpl<long> ExpiryTimestamp, SafeAdminResultHandle Result) RenewDelegationToken(
        byte[]? hmac, RenewDelegationTokenOptions? options)
    {
        EnterUse();
        try
        {
            ThrowIfNullHmacOnClient(hmac);
            SafeAdminResultHandle result = DelegationTokenMarshal.WithHmac(
                hmac,
                bytes => CallWithOptions(
                    NewRenewDelegationTokenOptions(options),
                    s_destroyRenewDelegationTokenOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminRenewDelegationTokenWithOptions(admin, bytes, nativeOptions, ref outErr),
                    s_destroyRenewDelegationTokenResult));
            return (StartBorrowedFuture(result, s_renewDelegationTokenExpiryTimestamp, s_copyOutTimestamp), result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal (KafkaFutureImpl<long> ExpiryTimestamp, SafeAdminResultHandle Result) ExpireDelegationToken(
        byte[]? hmac, ExpireDelegationTokenOptions? options)
    {
        EnterUse();
        try
        {
            ThrowIfNullHmacOnClient(hmac);
            SafeAdminResultHandle result = DelegationTokenMarshal.WithHmac(
                hmac,
                bytes => CallWithOptions(
                    NewExpireDelegationTokenOptions(options),
                    s_destroyExpireDelegationTokenOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminExpireDelegationTokenWithOptions(admin, bytes, nativeOptions, ref outErr),
                    s_destroyExpireDelegationTokenResult));
            return (StartBorrowedFuture(result, s_expireDelegationTokenExpiryTimestamp, s_copyOutTimestamp), result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal (KafkaFutureImpl<IReadOnlyList<DelegationToken>> Tokens, SafeAdminResultHandle Result)
        DescribeDelegationToken(DescribeDelegationTokenOptions? options)
    {
        EnterUse();
        try
        {
            if (options?.Owners is not null)
            {
                foreach (KafkaPrincipal owner in options.Owners)
                {
                    if (owner is null)
                    {
                        throw new ArgumentException(
                            "DescribeDelegationTokenOptions.Owners must not contain a null element.", nameof(options));
                    }

                    ValidatePrincipal(owner, nameof(options));
                }
            }

            SafeAdminResultHandle result = CallWithOptions(
                NewDescribeDelegationTokenOptions(options),
                s_destroyDescribeDelegationTokenOptions,
                (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                    NativeMethods.AdminDescribeDelegationTokenWithOptions(admin, nativeOptions, ref outErr),
                s_destroyDescribeDelegationTokenResult);
            return (StartBorrowedFuture(result, s_describeDelegationTokenTokens, s_copyOutDelegationTokens), result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewCreateDelegationTokenOptions(CreateDelegationTokenOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newCreateDelegationTokenOptions,
            options,
            options?.Setters,
            s_setCreateDelegationTokenOption,
            s_destroyCreateDelegationTokenOptions);

    internal static void FreeCreateDelegationTokenOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyCreateDelegationTokenOptions);

    internal static IntPtr NewRenewDelegationTokenOptions(RenewDelegationTokenOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newRenewDelegationTokenOptions,
            options,
            options?.Setters,
            s_setRenewDelegationTokenOption,
            s_destroyRenewDelegationTokenOptions);

    internal static void FreeRenewDelegationTokenOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyRenewDelegationTokenOptions);

    internal static IntPtr NewExpireDelegationTokenOptions(ExpireDelegationTokenOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newExpireDelegationTokenOptions,
            options,
            options?.Setters,
            s_setExpireDelegationTokenOption,
            s_destroyExpireDelegationTokenOptions);

    internal static void FreeExpireDelegationTokenOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyExpireDelegationTokenOptions);

    internal static IntPtr NewDescribeDelegationTokenOptions(DescribeDelegationTokenOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newDescribeDelegationTokenOptions,
            options,
            options?.Setters,
            s_setDescribeDelegationTokenOption,
            s_destroyDescribeDelegationTokenOptions);

    internal static void FreeDescribeDelegationTokenOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyDescribeDelegationTokenOptions);

    private static void ValidatePrincipal(KafkaPrincipal principal, string paramName)
    {
        AdminStrings.Validate(principal.PrincipalType, paramName);
        AdminStrings.Validate(principal.Name, paramName);
    }

    // A null MAC reaches the mock, whose future fails as Java's mock does; the client refuses it at the call.
    private void ThrowIfNullHmacOnClient(byte[]? hmac)
    {
        if (hmac is null && !_isMock)
        {
            throw new ArgumentNullException(nameof(hmac));
        }
    }

    private static void SetCreateDelegationTokenOption(
        IntPtr native, CreateDelegationTokenOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(CreateDelegationTokenOptions.TimeoutMs):
                NativeMethods.CreateDelegationTokenOptionsSetTimeoutMs(
                    native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            case nameof(CreateDelegationTokenOptions.Renewers):
                SetPrincipals(native, options.Renewers, s_createDelegationTokenOptionsSetRenewers, ref outErr);
                break;
            case nameof(CreateDelegationTokenOptions.Owner):
                if (options.Owner is not null)
                {
                    IntPtr owner = DelegationTokenMarshal.NewPrincipal(options.Owner);
                    try
                    {
                        NativeMethods.CreateDelegationTokenOptionsSetOwner(native, owner, ref outErr);
                    }
                    finally
                    {
                        DelegationTokenMarshal.DestroyPrincipal(owner);
                    }
                }

                break;
            case nameof(CreateDelegationTokenOptions.MaxLifetimeMs):
                NativeMethods.CreateDelegationTokenOptionsSetMaxLifetimeMs(native, options.MaxLifetimeMs, ref outErr);
                break;
            default:
                throw new InvalidOperationException("CreateDelegationTokenOptions has no C setter for " + setter + ".");
        }
    }

    private static void SetRenewDelegationTokenOption(
        IntPtr native, RenewDelegationTokenOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(RenewDelegationTokenOptions.TimeoutMs):
                NativeMethods.RenewDelegationTokenOptionsSetTimeoutMs(
                    native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            case nameof(RenewDelegationTokenOptions.RenewTimePeriodMs):
                NativeMethods.RenewDelegationTokenOptionsSetRenewTimePeriodMs(
                    native, options.RenewTimePeriodMs, ref outErr);
                break;
            default:
                throw new InvalidOperationException("RenewDelegationTokenOptions has no C setter for " + setter + ".");
        }
    }

    private static void SetExpireDelegationTokenOption(
        IntPtr native, ExpireDelegationTokenOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(ExpireDelegationTokenOptions.TimeoutMs):
                NativeMethods.ExpireDelegationTokenOptionsSetTimeoutMs(
                    native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            case nameof(ExpireDelegationTokenOptions.ExpiryTimePeriodMs):
                NativeMethods.ExpireDelegationTokenOptionsSetExpiryTimePeriodMs(
                    native, options.ExpiryTimePeriodMs, ref outErr);
                break;
            default:
                throw new InvalidOperationException("ExpireDelegationTokenOptions has no C setter for " + setter + ".");
        }
    }

    private static void SetDescribeDelegationTokenOption(
        IntPtr native, DescribeDelegationTokenOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(DescribeDelegationTokenOptions.TimeoutMs):
                NativeMethods.DescribeDelegationTokenOptionsSetTimeoutMs(
                    native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            case nameof(DescribeDelegationTokenOptions.Owners):
                if (options.Owners is null)
                {
                    NativeMethods.DescribeDelegationTokenOptionsSetOwners(native, IntPtr.Zero, ref outErr);
                }
                else
                {
                    SetPrincipals(
                        native, options.Owners, s_describeDelegationTokenOptionsSetOwners, ref outErr);
                }

                break;
            default:
                throw new InvalidOperationException("DescribeDelegationTokenOptions has no C setter for " + setter + ".");
        }
    }

    private delegate void PrincipalListSetter(IntPtr native, IntPtr principals, ref IntPtr outErr);

    private static void SetPrincipals(
        IntPtr native, IReadOnlyList<KafkaPrincipal> principals, PrincipalListSetter set, ref IntPtr outErr)
    {
        IntPtr[] elements = DelegationTokenMarshal.NewPrincipals(principals);
        IntPtr list = IntPtr.Zero;
        try
        {
            list = NewList(elements);
            set(native, list, ref outErr);
        }
        finally
        {
            DestroyList(list);
            DelegationTokenMarshal.DestroyPrincipals(elements);
        }
    }
}
