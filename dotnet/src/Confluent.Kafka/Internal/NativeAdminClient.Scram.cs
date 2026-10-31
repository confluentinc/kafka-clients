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
using Confluent.Kafka.Common;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

internal sealed partial class NativeAdminClient
{
    private static readonly ResultFuture s_alterUserScramCredentialsValues =
        NativeMethods.AlterUserScramCredentialsResultValues;

    private static readonly ResultFuture s_describeUserScramCredentialsAll =
        NativeMethods.DescribeUserScramCredentialsResultAll;

    private static readonly KeyedResultFuture s_describeUserScramCredentialsDescription =
        NativeMethods.DescribeUserScramCredentialsResultDescription;

    private static readonly ResultFuture s_describeUserScramCredentialsUsers =
        NativeMethods.DescribeUserScramCredentialsResultUsers;

    private static readonly AdminOptionsMarshal.New s_newDescribeUserScramCredentialsOptions =
        NativeMethods.DescribeUserScramCredentialsOptionsNew;

    private static readonly AdminOptionsMarshal.Set<DescribeUserScramCredentialsOptions>
        s_setDescribeUserScramCredentialsOption = SetDescribeUserScramCredentialsOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyDescribeUserScramCredentialsOptions =
        NativeMethods.DescribeUserScramCredentialsOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyDescribeUserScramCredentialsResult =
        NativeMethods.DescribeUserScramCredentialsResultDestroy;

    private static readonly AdminOptionsMarshal.New s_newAlterUserScramCredentialsOptions =
        NativeMethods.AlterUserScramCredentialsOptionsNew;

    private static readonly AdminOptionsMarshal.Set<AlterUserScramCredentialsOptions>
        s_setAlterUserScramCredentialsOption = SetAlterUserScramCredentialsOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyAlterUserScramCredentialsOptions =
        NativeMethods.AlterUserScramCredentialsOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyAlterUserScramCredentialsResult =
        NativeMethods.AlterUserScramCredentialsResultDestroy;

    private static readonly Func<IntPtr, IReadOnlyDictionary<string, UserScramCredentialsDescription>>
        s_copyOutScramDescriptions = UserScramCredentialMarshal.CopyOutDescriptions;

    private static readonly Func<IntPtr, IReadOnlyList<string>> s_copyOutScramUsers =
        UserScramCredentialMarshal.CopyOutUsers;

    private static readonly Func<IntPtr, UserScramCredentialsDescription> s_copyOutScramDescription =
        UserScramCredentialMarshal.CopyOutDescription;

    internal SafeAdminResultHandle DescribeUserScramCredentials(
        IReadOnlyCollection<string>? users, DescribeUserScramCredentialsOptions? options)
    {
        EnterUse();
        try
        {
            List<string>? requested = RequestedScramUsers(users);
            List<Utf8Marshal.PinnedUtf8String> pins = new List<Utf8Marshal.PinnedUtf8String>(requested?.Count ?? 0);
            IntPtr list = IntPtr.Zero;
            try
            {
                if (requested is not null)
                {
                    IntPtr[] elements = new IntPtr[requested.Count];
                    for (int i = 0; i < elements.Length; i++)
                    {
                        Utf8Marshal.PinnedUtf8String pin = Utf8Marshal.Pin(requested[i]);
                        pins.Add(pin);
                        elements[i] = pin.Pointer;
                    }

                    list = NewList(elements);
                }

                return CallWithOptions(
                    NewDescribeUserScramCredentialsOptions(options),
                    s_destroyDescribeUserScramCredentialsOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminDescribeUserScramCredentialsWithUsersOptions(
                            admin, list, nativeOptions, ref outErr),
                    s_destroyDescribeUserScramCredentialsResult);
            }
            finally
            {
                DestroyList(list);
                ReleasePins(pins);
            }
        }
        finally
        {
            ExitUse();
        }
    }

    internal static KafkaFutureImpl<IReadOnlyDictionary<string, UserScramCredentialsDescription>>
        DescribeUserScramCredentialsAll(SafeAdminResultHandle result) =>
        StartResultFuture(result, s_describeUserScramCredentialsAll, s_copyOutScramDescriptions);

    internal static KafkaFutureImpl<IReadOnlyList<string>> DescribeUserScramCredentialsUsers(
        SafeAdminResultHandle result) =>
        StartResultFuture(result, s_describeUserScramCredentialsUsers, s_copyOutScramUsers);

    internal static KafkaFutureImpl<UserScramCredentialsDescription> DescribeUserScramCredentialsDescription(
        SafeAdminResultHandle result, string userName)
    {
        using Utf8Marshal.PinnedUtf8String user = Utf8Marshal.Pin(userName);
        IntPtr pointer = user.Pointer;
        return StartResultFuture(
            result,
            (IntPtr native, ref IntPtr outErr) =>
                s_describeUserScramCredentialsDescription(native, pointer, ref outErr),
            s_copyOutScramDescription);
    }

    internal static IntPtr NewDescribeUserScramCredentialsOptions(DescribeUserScramCredentialsOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newDescribeUserScramCredentialsOptions,
            options,
            options?.Setters,
            s_setDescribeUserScramCredentialsOption,
            s_destroyDescribeUserScramCredentialsOptions);

    internal static void FreeDescribeUserScramCredentialsOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyDescribeUserScramCredentialsOptions);

    private static void SetDescribeUserScramCredentialsOption(
        IntPtr native, DescribeUserScramCredentialsOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(DescribeUserScramCredentialsOptions.TimeoutMs):
                NativeMethods.DescribeUserScramCredentialsOptionsSetTimeoutMs(
                    native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            default:
                throw new InvalidOperationException(
                    "DescribeUserScramCredentialsOptions has no C setter for " + setter + ".");
        }
    }

    internal (IReadOnlyDictionary<string, KafkaFutureImpl<object?>> Values, SafeAdminResultHandle Result)
        AlterUserScramCredentials(
            IEnumerable<UserScramCredentialAlteration> alterations, AlterUserScramCredentialsOptions? options)
    {
        EnterUse();
        try
        {
            if (alterations is null)
            {
                throw new ArgumentNullException(nameof(alterations));
            }

            List<UserScramCredentialAlteration> requested = RequestedScramAlterations(alterations);
            IntPtr[] natives = new IntPtr[requested.Count];
            IntPtr list = IntPtr.Zero;
            SafeAdminResultHandle result;
            try
            {
                for (int i = 0; i < natives.Length; i++)
                {
                    natives[i] = UserScramCredentialMarshal.NewAlteration(requested[i]);
                }

                list = NewList(natives);
                result = CallWithOptions(
                    NewAlterUserScramCredentialsOptions(options),
                    s_destroyAlterUserScramCredentialsOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminAlterUserScramCredentialsWithOptions(admin, list, nativeOptions, ref outErr),
                    s_destroyAlterUserScramCredentialsResult);
            }
            finally
            {
                DestroyList(list);
                foreach (IntPtr native in natives)
                {
                    UserScramCredentialMarshal.DestroyAlteration(native);
                }
            }

            return (
                StartMapFutures(
                    result,
                    s_alterUserScramCredentialsValues,
                    s_stringKey,
                    StringComparer.Ordinal,
                    s_noValue),
                result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewAlterUserScramCredentialsOptions(AlterUserScramCredentialsOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newAlterUserScramCredentialsOptions,
            options,
            options?.Setters,
            s_setAlterUserScramCredentialsOption,
            s_destroyAlterUserScramCredentialsOptions);

    internal static void FreeAlterUserScramCredentialsOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyAlterUserScramCredentialsOptions);

    private static void SetAlterUserScramCredentialsOption(
        IntPtr native, AlterUserScramCredentialsOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(AlterUserScramCredentialsOptions.TimeoutMs):
                NativeMethods.AlterUserScramCredentialsOptionsSetTimeoutMs(
                    native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            default:
                throw new InvalidOperationException(
                    "AlterUserScramCredentialsOptions has no C setter for " + setter + ".");
        }
    }

    internal static List<string>? RequestedScramUsers(IReadOnlyCollection<string>? users)
    {
        if (users is null)
        {
            return null;
        }

        // A null user is skipped, as Java's request builder skips it; an empty request describes every user.
        List<string> requested = new List<string>(users.Count);
        foreach (string user in users)
        {
            if (user is null)
            {
                continue;
            }

            AdminStrings.Validate(user, nameof(users));
            requested.Add(user);
        }

        return requested;
    }

    private static List<UserScramCredentialAlteration> RequestedScramAlterations(
        IEnumerable<UserScramCredentialAlteration> alterations)
    {
        // Every alteration is sent, a repeated user included; the core answers each user once.
        List<UserScramCredentialAlteration> requested = new List<UserScramCredentialAlteration>();
        foreach (UserScramCredentialAlteration alteration in alterations)
        {
            if (alteration is null)
            {
                throw new ArgumentException(
                    "The SCRAM credential alterations must not contain a null element.", nameof(alterations));
            }

            AdminStrings.Validate(alteration.User, nameof(alterations));
            requested.Add(alteration);
        }

        return requested;
    }

    private static KafkaFutureImpl<T> StartResultFuture<T>(
        SafeAdminResultHandle result, ResultFuture accessor, Func<IntPtr, T> convert)
    {
        IntPtr future;
        bool added = false;
        try
        {
            result.DangerousAddRef(ref added);
            IntPtr outErr = IntPtr.Zero;
            future = accessor(result.DangerousGetHandle(), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
        finally
        {
            if (added)
            {
                result.DangerousRelease();
            }
        }

        return NativeFutureSource<T>.Start(SafeKafkaFutureHandle.Owned(future), convert);
    }
}
