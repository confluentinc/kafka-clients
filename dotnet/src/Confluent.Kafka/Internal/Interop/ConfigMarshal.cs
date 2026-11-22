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
using System.Threading;

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
        using ValidatedConfig config = Validate(configs, client);
        return config.CreateClient();
    }

    /// <summary>
    /// The first half of <see cref="NewClient"/>: the core has read and accepted the whole configuration and no
    /// client exists yet, which is where Java's client constructor enters its <c>try</c> and builds its serializers
    /// or deserializers. The caller creates the client from the result, or not, and disposes it.
    /// </summary>
    internal static ValidatedConfig Validate(IReadOnlyList<KeyValuePair<string, string>> configs, ClientKind client)
    {
        string parameter = client == ClientKind.Admin ? "conf" : "configs";
        foreach (KeyValuePair<string, string> entry in configs)
        {
            AdminStrings.Validate(entry.Key, AdminStrings.InvalidConfigKeyMessage, parameter);
            AdminStrings.Validate(entry.Value, AdminStrings.InvalidConfigValueMessage, parameter);
        }

        ValidatedConfig config = new ValidatedConfig(client, configs.Count);
        try
        {
            config.Read(configs);
            return config;
        }
        catch
        {
            config.Dispose();
            throw;
        }
    }

    /// <summary>
    /// A configuration the core accepted, with the map and the pinned texts it was read from, all freed once by
    /// <see cref="Dispose"/>.
    /// </summary>
    internal sealed class ValidatedConfig : IDisposable
    {
        private readonly ClientKind _client;

        private readonly List<Utf8Marshal.PinnedUtf8String> _pins;

        private IntPtr _map;

        private IntPtr _clientConfig;

        private int _disposed;

        internal ValidatedConfig(ClientKind client, int count)
        {
            _client = client;
            _pins = new List<Utf8Marshal.PinnedUtf8String>(count * 2);
        }

        internal void Read(IReadOnlyList<KeyValuePair<string, string>> configs)
        {
            IntPtr outErr = IntPtr.Zero;
            _map = NativeMethods.MapNew(ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            foreach (KeyValuePair<string, string> entry in configs)
            {
                Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin(entry.Key);
                _pins.Add(key);
                Utf8Marshal.PinnedUtf8String value = Utf8Marshal.Pin(entry.Value);
                _pins.Add(value);
                NativeMethods.MapPut(_map, key.Pointer, value.Pointer, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }

            _clientConfig = NewConfig(_client, _map, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }

        internal IntPtr CreateClient()
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr created = ConfigMarshal.CreateClient(_client, _clientConfig, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return created;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            IntPtr destroyErr = IntPtr.Zero;
            if (_clientConfig != IntPtr.Zero)
            {
                DestroyConfig(_client, _clientConfig, ref destroyErr);
                FreeError(ref destroyErr);
            }

            foreach (Utf8Marshal.PinnedUtf8String pin in _pins)
            {
                pin.Dispose();
            }

            if (_map != IntPtr.Zero)
            {
                NativeMethods.MapDestroy(_map, ref destroyErr);
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
