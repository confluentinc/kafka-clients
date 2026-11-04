// Copyright 2026 Confluent Inc.
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
using System.Globalization;

namespace Confluent.Kafka.Soak;

/// <summary>
/// Environment-variable parsing, forked from the perf suite's <c>PerfEnv</c> (plus
/// <c>GetDouble</c>): a boolean is the case-sensitive <c>"True"</c>, as the Python perf tests read
/// it, and numbers parse with invariant culture.
/// </summary>
internal static class SoakEnv
{
    internal static string GetString(string name, string fallback)
    {
        string? value = Environment.GetEnvironmentVariable(name);
        return string.IsNullOrEmpty(value) ? fallback : value!;
    }

    internal static string? GetStringOrNull(string name)
    {
        string? value = Environment.GetEnvironmentVariable(name);
        return string.IsNullOrEmpty(value) ? null : value;
    }

    internal static int GetInt(string name, int fallback)
    {
        string? value = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrEmpty(value))
        {
            return fallback;
        }

        return int.Parse(value!, CultureInfo.InvariantCulture);
    }

    internal static double GetDouble(string name, double fallback)
    {
        string? value = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrEmpty(value))
        {
            return fallback;
        }

        return double.Parse(value!, NumberStyles.Float, CultureInfo.InvariantCulture);
    }

    internal static bool GetBool(string name, bool fallback)
    {
        string? value = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrEmpty(value))
        {
            return fallback;
        }

        return string.Equals(value, "True", StringComparison.Ordinal);
    }
}
