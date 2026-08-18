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
using System.Globalization;

namespace Confluent.Kafka.Performance;

/// <summary>
/// Environment-variable parsing helpers, matching the Python perf suite's conventions exactly so the
/// same env config drives every language (§7): booleans are the case-sensitive string <c>"True"</c>
/// (Python's <c>os.getenv(name, "False") == "True"</c>), ints parse with invariant culture, and
/// presence checks mirror Python's <c>'NAME' in os.environ</c>.
/// </summary>
public static class PerfEnv
{
    /// <summary>Returns the value of <paramref name="name"/>, or <paramref name="fallback"/> when unset.</summary>
    public static string GetString(string name, string fallback)
    {
        string? value = Environment.GetEnvironmentVariable(name);
        return value is null ? fallback : value;
    }

    /// <summary>Returns the value of <paramref name="name"/>, or <see langword="null"/> when unset (Python's <c>os.getenv(name, None)</c>).</summary>
    public static string? GetStringOrNull(string name) => Environment.GetEnvironmentVariable(name);

    /// <summary>Whether <paramref name="name"/> is set to a non-empty value (Python's <c>'NAME' in os.environ</c> for a set var).</summary>
    public static bool Has(string name) => !string.IsNullOrEmpty(Environment.GetEnvironmentVariable(name));

    /// <summary>Parses <paramref name="name"/> as an int, or returns <paramref name="fallback"/> when unset / empty.</summary>
    public static int GetInt(string name, int fallback)
    {
        string? value = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrEmpty(value))
        {
            return fallback;
        }

        return int.Parse(value, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Parses <paramref name="name"/> as the case-sensitive string <c>"True"</c> (Python
    /// <c>os.getenv(name, default) == "True"</c>); any other value — including <c>"true"</c> or
    /// <c>"1"</c> — is <see langword="false"/>, matching Python.
    /// </summary>
    public static bool GetBool(string name, bool fallback)
    {
        string? value = Environment.GetEnvironmentVariable(name);
        if (value is null)
        {
            return fallback;
        }

        return value == "True";
    }
}
