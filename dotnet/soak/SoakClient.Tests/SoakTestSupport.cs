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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Xunit;

namespace Confluent.Kafka.Soak.Tests;

/// <summary>
/// Builders shared by the soak tests: options read from the <c>SOAK_*</c> environment as
/// production reads them, and a <see cref="KafkaException"/> with a given code, built by reflection
/// over the binding's internal constructor (this assembly is not a friend of <c>Confluent.Kafka</c>).
/// </summary>
internal static class SoakTestSupport
{
    internal static SoakOptions Options(params (string Name, string Value)[] variables)
    {
        var saved = new Dictionary<string, string?>(StringComparer.Ordinal);
        foreach (DictionaryEntry entry in Environment.GetEnvironmentVariables())
        {
            var name = (string)entry.Key;
            if (name.StartsWith("SOAK_", StringComparison.Ordinal))
            {
                saved[name] = (string?)entry.Value;
                Environment.SetEnvironmentVariable(name, null);
            }
        }

        foreach ((string name, string value) in variables)
        {
            saved.TryAdd(name, null);
            Environment.SetEnvironmentVariable(name, value);
        }

        try
        {
            return SoakOptions.FromEnvironment();
        }
        finally
        {
            foreach (KeyValuePair<string, string?> entry in saved)
            {
                Environment.SetEnvironmentVariable(entry.Key, entry.Value);
            }
        }
    }

    internal static KafkaException KafkaError(int code, string message)
    {
        ConstructorInfo constructor = Assert.Single(
            typeof(KafkaException).GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic),
            candidate => candidate.GetParameters().Select(parameter => parameter.ParameterType)
                .SequenceEqual(new[] { typeof(int), typeof(string), typeof(bool) }));
        var error = (KafkaException)constructor.Invoke(new object[] { code, message, false });
        Assert.Equal(code, error.Code);
        return error;
    }
}

/// <summary>Records every counter increment the soak's metrics export, with its tags.</summary>
internal sealed class CapturingSink : ISoakTelemetrySink
{
    internal List<(string Name, long Increment, string Tags)> Counters { get; } = new List<(string, long, string)>();

    public void IncrCounter(string fullName, long increment, IReadOnlyDictionary<string, string> tags)
    {
        string rendered = string.Join(
            ",",
            tags.OrderBy(tag => tag.Key, StringComparer.Ordinal).Select(tag => tag.Key + "=" + tag.Value));
        lock (Counters)
        {
            Counters.Add((fullName, increment, rendered));
        }
    }

    public void SetGauge(string fullName, double value, IReadOnlyDictionary<string, string> tags)
    {
    }

    public void Shutdown()
    {
    }

    public void Dispose()
    {
    }
}
