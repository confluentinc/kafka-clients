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
    private static readonly AdminOptionsMarshal.New s_newDescribeFeaturesOptions =
        NativeMethods.DescribeFeaturesOptionsNew;

    private static readonly AdminOptionsMarshal.Set<DescribeFeaturesOptions> s_setDescribeFeaturesOption =
        SetDescribeFeaturesOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyDescribeFeaturesOptions =
        NativeMethods.DescribeFeaturesOptionsDestroy;

    private static readonly AdminOptionsMarshal.New s_newUpdateFeaturesOptions = NativeMethods.UpdateFeaturesOptionsNew;

    private static readonly AdminOptionsMarshal.Set<UpdateFeaturesOptions> s_setUpdateFeaturesOption =
        SetUpdateFeaturesOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyUpdateFeaturesOptions =
        NativeMethods.UpdateFeaturesOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyDescribeFeaturesResult =
        NativeMethods.DescribeFeaturesResultDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyUpdateFeaturesResult =
        NativeMethods.UpdateFeaturesResultDestroy;

    private static readonly ResultFuture s_describeFeaturesFeatureMetadata =
        NativeMethods.DescribeFeaturesResultFeatureMetadata;

    private static readonly ResultFuture s_updateFeaturesValues = NativeMethods.UpdateFeaturesResultValues;

    private static readonly Func<IntPtr, FeatureMetadata> s_copyOutFeatureMetadata = FeatureMetadataMarshal.CopyOut;

    internal (KafkaFutureImpl<FeatureMetadata> FeatureMetadata, SafeAdminResultHandle Result) DescribeFeatures(
        DescribeFeaturesOptions? options)
    {
        EnterUse();
        try
        {
            SafeAdminResultHandle result = CallWithOptions(
                NewDescribeFeaturesOptions(options),
                s_destroyDescribeFeaturesOptions,
                (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                    NativeMethods.AdminDescribeFeaturesWithOptions(admin, nativeOptions, ref outErr),
                s_destroyDescribeFeaturesResult);
            return (StartOwnedFuture(result, s_describeFeaturesFeatureMetadata, s_copyOutFeatureMetadata), result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal (IReadOnlyDictionary<string, KafkaFutureImpl<object?>> Values, SafeAdminResultHandle Result) UpdateFeatures(
        IReadOnlyDictionary<string, FeatureUpdate> featureUpdates, UpdateFeaturesOptions? options)
    {
        EnterUse();
        try
        {
            if (featureUpdates is null)
            {
                throw new ArgumentNullException(nameof(featureUpdates));
            }

            List<string> keys = new List<string>(featureUpdates.Count);
            List<FeatureUpdate> requested = new List<FeatureUpdate>(featureUpdates.Count);
            HashSet<string> seen = new HashSet<string>(StringComparer.Ordinal);
            foreach (KeyValuePair<string, FeatureUpdate> entry in featureUpdates)
            {
                // A null name cannot cross; the core checks every other name.
                if (entry.Key is null)
                {
                    throw new ArgumentException("Provided feature can not be empty.", nameof(featureUpdates));
                }

                AdminStrings.Validate(entry.Key, nameof(featureUpdates));
                if (entry.Value is null)
                {
                    throw new ArgumentException(
                        "The feature updates must not contain a null update.", nameof(featureUpdates));
                }

                // A C map holds each name once, so a repeat the caller's comparer allows is refused here.
                if (!seen.Add(entry.Key))
                {
                    throw new ArgumentException(
                        $"The feature updates contain the feature '{entry.Key}' more than once.", nameof(featureUpdates));
                }

                keys.Add(entry.Key);
                requested.Add(entry.Value);
            }

            List<Utf8Marshal.PinnedUtf8String> pins = new List<Utf8Marshal.PinnedUtf8String>(keys.Count);
            IntPtr[] natives = new IntPtr[requested.Count];
            IntPtr map = IntPtr.Zero;
            SafeAdminResultHandle result;
            try
            {
                map = NewMap();
                for (int i = 0; i < natives.Length; i++)
                {
                    Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin(keys[i]);
                    pins.Add(key);
                    natives[i] = NewFeatureUpdate(requested[i]);
                    MapPut(map, key.Pointer, natives[i]);
                }

                result = CallWithOptions(
                    NewUpdateFeaturesOptions(options),
                    s_destroyUpdateFeaturesOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminUpdateFeaturesWithOptions(admin, map, nativeOptions, ref outErr),
                    s_destroyUpdateFeaturesResult);
            }
            finally
            {
                DestroyMap(map);
                foreach (IntPtr native in natives)
                {
                    DestroyFeatureUpdate(native);
                }

                ReleasePins(pins);
            }

            return (StartMapFutures(result, s_updateFeaturesValues, s_stringKey, StringComparer.Ordinal, s_noValue), result);
        }
        finally
        {
            ExitUse();
        }
    }

    // The upgrade type crosses by its code, so a value the enum does not define crosses as UNKNOWN, as Java's fromCode.
    internal static IntPtr NewFeatureUpdate(FeatureUpdate update)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr upgradeType = NativeMethods.UpgradeTypeFromCode((int)update.Type, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr native = NativeMethods.FeatureUpdateNew(update.MaxVersionLevel, upgradeType, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return native;
    }

    internal static void DestroyFeatureUpdate(IntPtr update)
    {
        if (update == IntPtr.Zero)
        {
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        NativeMethods.FeatureUpdateDestroy(update, ref outErr);
        if (outErr != IntPtr.Zero)
        {
            NativeMethods.ErrorDestroy(outErr);
        }
    }

    internal static IntPtr NewDescribeFeaturesOptions(DescribeFeaturesOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newDescribeFeaturesOptions,
            options,
            options?.Setters,
            s_setDescribeFeaturesOption,
            s_destroyDescribeFeaturesOptions);

    internal static void FreeDescribeFeaturesOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyDescribeFeaturesOptions);

    internal static IntPtr NewUpdateFeaturesOptions(UpdateFeaturesOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newUpdateFeaturesOptions,
            options,
            options?.Setters,
            s_setUpdateFeaturesOption,
            s_destroyUpdateFeaturesOptions);

    internal static void FreeUpdateFeaturesOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyUpdateFeaturesOptions);

    private static void SetDescribeFeaturesOption(
        IntPtr native, DescribeFeaturesOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(DescribeFeaturesOptions.TimeoutMs):
                NativeMethods.DescribeFeaturesOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            case nameof(DescribeFeaturesOptions.NodeId):
                NativeMethods.DescribeFeaturesOptionsSetNodeId(native, options.NodeId.GetValueOrDefault(), ref outErr);
                break;
            default:
                throw new InvalidOperationException("DescribeFeaturesOptions has no C setter for " + setter + ".");
        }
    }

    private static void SetUpdateFeaturesOption(
        IntPtr native, UpdateFeaturesOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(UpdateFeaturesOptions.TimeoutMs):
                NativeMethods.UpdateFeaturesOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            case nameof(UpdateFeaturesOptions.ValidateOnly):
                NativeMethods.UpdateFeaturesOptionsSetValidateOnly(
                    native, options.ValidateOnly ? (sbyte)1 : (sbyte)0, ref outErr);
                break;
            default:
                throw new InvalidOperationException("UpdateFeaturesOptions has no C setter for " + setter + ".");
        }
    }
}
