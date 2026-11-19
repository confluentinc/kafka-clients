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

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The imports not yet moved onto <c>confluent_kafka_bindings.h</c>, each with the sub-phase that
/// rewrites its user: <see cref="Missing"/> holds the names the header does not declare, and
/// <see cref="Differing"/> the names it declares with another signature. Entries only leave: both
/// lists are empty once the port is complete.
/// </summary>
internal static class NativeAbiAllowlist
{
    internal static IReadOnlyDictionary<string, string> Missing { get; } = Build();

    internal static IReadOnlyDictionary<string, string> Differing { get; } = Build();

    internal static bool Contains(string entryPoint) =>
        Missing.ContainsKey(entryPoint) || Differing.ContainsKey(entryPoint);

    private static Dictionary<string, string> Build(params (string Tag, string[] EntryPoints)[] groups)
    {
        Dictionary<string, string> entries = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach ((string tag, string[] entryPoints) in groups)
        {
            foreach (string entryPoint in entryPoints)
            {
                entries.Add(entryPoint, tag);
            }
        }

        return entries;
    }
}
