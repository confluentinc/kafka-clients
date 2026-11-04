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

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

internal sealed partial class NativeAdminClient
{
    private static readonly AdminOptionsMarshal.New s_newUnregisterControllerOptions =
        NativeMethods.UnregisterControllerOptionsNew;

    private static readonly AdminOptionsMarshal.Set<UnregisterControllerOptions> s_setUnregisterControllerOption =
        SetUnregisterControllerOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyUnregisterControllerOptions =
        NativeMethods.UnregisterControllerOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyUnregisterControllerResult =
        NativeMethods.UnregisterControllerResultDestroy;

    private static readonly ResultFuture s_unregisterControllerAll = NativeMethods.UnregisterControllerResultAll;

    internal (KafkaFutureImpl<object?> All, SafeAdminResultHandle Result) UnregisterController(
        int controllerId, UnregisterControllerOptions? options)
    {
        EnterUse();
        try
        {
            SafeAdminResultHandle result = CallWithOptions(
                NewUnregisterControllerOptions(options),
                s_destroyUnregisterControllerOptions,
                (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                    NativeMethods.AdminUnregisterControllerWithOptions(admin, controllerId, nativeOptions, ref outErr),
                s_destroyUnregisterControllerResult);
            return (StartOwnedFuture(result, s_unregisterControllerAll, s_noValue), result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewUnregisterControllerOptions(UnregisterControllerOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newUnregisterControllerOptions,
            options,
            options?.Setters,
            s_setUnregisterControllerOption,
            s_destroyUnregisterControllerOptions);

    internal static void FreeUnregisterControllerOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyUnregisterControllerOptions);

    private static void SetUnregisterControllerOption(
        IntPtr native, UnregisterControllerOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(UnregisterControllerOptions.TimeoutMs):
                NativeMethods.UnregisterControllerOptionsSetTimeoutMs(
                    native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            default:
                throw new InvalidOperationException("UnregisterControllerOptions has no C setter for " + setter + ".");
        }
    }
}
