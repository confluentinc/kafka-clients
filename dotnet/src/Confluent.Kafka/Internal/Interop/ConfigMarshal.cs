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

using Confluent.Kafka.Common;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Creates a real client from its prepared configuration: rejects a key or text containing <c>'\0'</c>, pins each as
/// NUL-terminated UTF-8 in one <c>kafka_Map_t</c>, reads it with the client's config constructor, creates the client
/// from that config, and frees the config, the pins and the map on every path; and finds, in the prepared
/// configuration, the text the core keeps for a key: its last entry's.
/// </summary>
internal static class ConfigMarshal
{
    internal static IntPtr NewClient(IReadOnlyList<KeyValuePair<string, string>> configs, ClientKind client)
    {
        string parameter = client == ClientKind.Admin ? "conf" : "configs";
        foreach (KeyValuePair<string, string> entry in configs)
        {
            AdminStrings.Validate(entry.Key, AdminStrings.InvalidConfigKeyMessage, parameter);
            AdminStrings.Validate(entry.Value, AdminStrings.InvalidConfigValueMessage, parameter);
        }

        List<Utf8Marshal.PinnedUtf8String> pins = new List<Utf8Marshal.PinnedUtf8String>(configs.Count * 2);
        IntPtr map = IntPtr.Zero;
        IntPtr clientConfig = IntPtr.Zero;
        IntPtr outErr = IntPtr.Zero;
        try
        {
            map = NativeMethods.MapNew(ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            foreach (KeyValuePair<string, string> entry in configs)
            {
                Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin(entry.Key);
                pins.Add(key);
                Utf8Marshal.PinnedUtf8String value = Utf8Marshal.Pin(entry.Value);
                pins.Add(value);
                NativeMethods.MapPut(map, key.Pointer, value.Pointer, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }

            clientConfig = NewConfig(client, map, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr created = CreateClient(client, clientConfig, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return created;
        }
        finally
        {
            IntPtr destroyErr = IntPtr.Zero;
            if (clientConfig != IntPtr.Zero)
            {
                DestroyConfig(client, clientConfig, ref destroyErr);
                FreeError(ref destroyErr);
            }

            foreach (Utf8Marshal.PinnedUtf8String pin in pins)
            {
                pin.Dispose();
            }

            if (map != IntPtr.Zero)
            {
                NativeMethods.MapDestroy(map, ref destroyErr);
                FreeError(ref destroyErr);
            }
        }
    }

    internal static string? TextOf(IReadOnlyList<KeyValuePair<string, string>> configs, string key)
    {
        string? text = null;
        foreach (KeyValuePair<string, string> entry in configs)
        {
            if (string.Equals(entry.Key, key, StringComparison.Ordinal))
            {
                text = entry.Value;
            }
        }

        return text;
    }

    private static IntPtr NewConfig(ClientKind client, IntPtr map, ref IntPtr outErr) => client switch
    {
        ClientKind.Producer => NativeMethods.ProducerConfigNew(map, ref outErr),
        ClientKind.Consumer => NativeMethods.ConsumerConfigNew(map, ref outErr),
        ClientKind.Admin => NativeMethods.AdminClientConfigNew(map, ref outErr),
        _ => throw new ArgumentOutOfRangeException(nameof(client), client, null),
    };

    private static IntPtr CreateClient(ClientKind client, IntPtr clientConfig, ref IntPtr outErr) => client switch
    {
        ClientKind.Producer => NativeMethods.KafkaProducerNew(clientConfig, IntPtr.Zero, IntPtr.Zero, ref outErr),
        ClientKind.Consumer => NativeMethods.KafkaConsumerNew(clientConfig, IntPtr.Zero, IntPtr.Zero, ref outErr),
        ClientKind.Admin => NativeMethods.AdminClientCreate(clientConfig, ref outErr),
        _ => throw new ArgumentOutOfRangeException(nameof(client), client, null),
    };

    private static void DestroyConfig(ClientKind client, IntPtr clientConfig, ref IntPtr outErr)
    {
        switch (client)
        {
            case ClientKind.Producer:
                NativeMethods.ProducerConfigDestroy(clientConfig, ref outErr);
                break;
            case ClientKind.Consumer:
                NativeMethods.ConsumerConfigDestroy(clientConfig, ref outErr);
                break;
            case ClientKind.Admin:
                NativeMethods.AdminClientConfigDestroy(clientConfig, ref outErr);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(client), client, null);
        }
    }

    private static void FreeError(ref IntPtr error)
    {
        if (error != IntPtr.Zero)
        {
            NativeMethods.ErrorDestroy(error);
            error = IntPtr.Zero;
        }
    }
}
