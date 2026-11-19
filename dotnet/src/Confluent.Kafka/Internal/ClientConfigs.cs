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
using System.Collections;
using System.Collections.Generic;
using System.Numerics;

using Confluent.Kafka.Common.Config;

namespace Confluent.Kafka.Internal;

/// <summary>
/// Prepares a client's configuration for the core, in Java's order: every key, then each value's null rule, its type
/// as Java's <c>ConfigDef.parseType</c> checks it, and the text the core parses. Ranges, valid strings, number text,
/// unknown keys and cross-key rules are the core's.
/// </summary>
internal static class ClientConfigs
{
    private const string HiddenPassword = "[hidden]";

    // The client's own serializer keys stay with the binding, neither checked nor sent: it always takes serde instances.
    private static readonly HashSet<string> s_producerSerdeKeys = new(StringComparer.Ordinal)
    {
        "key.serializer",
        "value.serializer",
        "serializer.encoding",
        "key.serializer.encoding",
        "value.serializer.encoding",
    };

    private static readonly HashSet<string> s_consumerSerdeKeys = new(StringComparer.Ordinal)
    {
        "key.deserializer",
        "value.deserializer",
        "deserializer.encoding",
        "key.deserializer.encoding",
        "value.deserializer.encoding",
    };

    private static readonly HashSet<string> s_noSerdeKeys = new(StringComparer.Ordinal);

    internal static IReadOnlyList<KeyValuePair<string, string>> Prepare(
        IReadOnlyDictionary<string, object?> configs,
        ClientKind client)
    {
        if (configs is null)
        {
            throw new ArgumentNullException(nameof(configs));
        }

        foreach (KeyValuePair<string, object?> entry in configs)
        {
            if (entry.Key is null)
            {
                throw new ConfigException(null, entry.Value, "Key must be a string.");
            }
        }

        (IReadOnlyDictionary<string, ConfigType> types,
            IReadOnlyDictionary<string, (ConfigNullForm Form, string Message)> nullRules,
            HashSet<string> serdeKeys) = client switch
            {
                ClientKind.Producer => (ConfigTypes.Producer, ConfigTypes.ProducerNull, s_producerSerdeKeys),
                ClientKind.Consumer => (ConfigTypes.Consumer, ConfigTypes.ConsumerNull, s_consumerSerdeKeys),
                ClientKind.Admin => (ConfigTypes.Admin, ConfigTypes.AdminNull, s_noSerdeKeys),
                _ => throw new ArgumentOutOfRangeException(nameof(client), client, null),
            };

        var pairs = new List<KeyValuePair<string, string>>(configs.Count);
        foreach (KeyValuePair<string, object?> entry in configs)
        {
            string key = entry.Key;
            object? value = entry.Value;
            if (serdeKeys.Contains(key))
            {
                continue;
            }

            if (value is null)
            {
                // Java holds null for a key whose validator allows it; the core has no null, so the key is not set.
                if (nullRules.TryGetValue(key, out (ConfigNullForm Form, string Message) rule))
                {
                    throw rule.Form == ConfigNullForm.Named
                        ? new ConfigException(key, null, rule.Message)
                        : new ConfigException(rule.Message);
                }

                continue;
            }

            ConfigType? type = null;
            if (types.TryGetValue(key, out ConfigType known))
            {
                CheckType(key, value, known);
                type = known;
            }

            pairs.Add(new KeyValuePair<string, string>(key, JavaText.CoreText(value, type)));
        }

        return pairs;
    }

    private static void CheckType(string key, object value, ConfigType type)
    {
        if (value is string)
        {
            return;
        }

        string? message = type switch
        {
            ConfigType.Boolean => value is bool ? null : "Expected value to be either true or false",
            ConfigType.String or ConfigType.Password => "Expected value to be a string, but it was a " + TypeName(value),
            ConfigType.Int => value is int ? null : "Expected value to be a 32-bit integer, but it was a " + TypeName(value),
            ConfigType.Short => value is short
                ? null
                : "Expected value to be a 16-bit integer (short), but it was a " + TypeName(value),
            ConfigType.Long => value is int or long
                ? null
                : "Expected value to be a 64-bit integer (long), but it was a " + TypeName(value),
            ConfigType.Double => IsNumber(value) ? null : "Expected value to be a double, but it was a " + TypeName(value),
            ConfigType.List => IsList(value) ? null : "Expected a comma separated list.",
            ConfigType.Class => value is Type ? null : "Expected a Class instance or class name.",
            _ => throw new InvalidOperationException("Unknown type."),
        };

        if (message is not null)
        {
            // A recorded deviation: a Password key's value is shown as Java's Password.toString() shows it, never in
            // full, where Java prints the value's toString().
            throw new ConfigException(key, type == ConfigType.Password ? HiddenPassword : value, message);
        }
    }

    private static string TypeName(object value) => value.GetType().ToString();

    private static bool IsNumber(object value) => value switch
    {
        sbyte or byte or short or ushort or int or uint or long or ulong => true,
        float or double or decimal or BigInteger => true,
#if NET5_0_OR_GREATER
        Half => true,
#endif
#if NET7_0_OR_GREATER
        Int128 or UInt128 => true,
#endif
        _ => false,
    };

    // Only a one-dimensional, zero-based array is a list: any other array's indexer throws, so it cannot be read by
    // position.
    private static bool IsList(object value) => value switch
    {
        Array array => array.Rank == 1 && array.GetLowerBound(0) == 0,
        IDictionary => false,
        IList => !IsGenericMap(value.GetType()),
        _ => false,
    };

    private static bool IsGenericMap(Type type)
    {
        foreach (Type contract in type.GetInterfaces())
        {
            if (contract.IsGenericType
                && (contract.GetGenericTypeDefinition() == typeof(IDictionary<,>)
                    || contract.GetGenericTypeDefinition() == typeof(IReadOnlyDictionary<,>)))
            {
                return true;
            }
        }

        return false;
    }
}
