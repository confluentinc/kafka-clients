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
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

using Confluent.Kafka.Internal;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The per-key null-config tables match the Python binding's, which the build generates from Java's config classes, so
/// a key Java adds or a validator message Java changes shows up here.
/// </summary>
public sealed class NullConfigValuesTests
{
    public static IEnumerable<object[]> Tables() => new[]
    {
        new object[] { "PRODUCER_NULL" },
        new object[] { "CONSUMER_NULL" },
        new object[] { "ADMIN_NULL" },
    };

    [Theory]
    [MemberData(nameof(Tables))]
    public void Table_MatchesThePythonBindingsGeneratedTable(string pythonTable)
    {
        IReadOnlyDictionary<string, NullConfigValues.NullRejection> table = pythonTable switch
        {
            "PRODUCER_NULL" => NullConfigValues.Producer,
            "CONSUMER_NULL" => NullConfigValues.Consumer,
            _ => NullConfigValues.Admin,
        };

        Dictionary<string, (bool Named, string Message)> expected = PythonTable(pythonTable);

        Assert.Equal(expected.Keys.OrderBy(key => key, StringComparer.Ordinal), table.Keys.OrderBy(key => key, StringComparer.Ordinal));
        foreach (KeyValuePair<string, (bool Named, string Message)> row in expected)
        {
            Assert.Equal(row.Value, (table[row.Key].Named, table[row.Key].Message));
        }
    }

    private static Dictionary<string, (bool Named, string Message)> PythonTable(string name)
    {
        string file = Path.GetFullPath(Path.Combine(
            PublicNamespaceLayoutTests.KafkaSourceRoot(), "..", "..", "..", "python", "confluent_kafka", "_config_types.py"));
        Assert.True(File.Exists(file), $"The Python config table is not at {file}.");

        string text = File.ReadAllText(file);
        Match body = Regex.Match(text, name + @": dict\[[^\n]*\] = \{\n(.*?)\n\}", RegexOptions.Singleline);
        Assert.True(body.Success, $"{file} has no {name} table.");

        // The Python binding spells Java's null as None inside a validator's own message.
        return Regex.Matches(body.Groups[1].Value, @"^\s+""([^""]+)"": \(""(named|plain)"", ""((?:[^""\\]|\\.)*)""\),", RegexOptions.Multiline)
            .Cast<Match>()
            .ToDictionary(
                match => match.Groups[1].Value,
                match => (match.Groups[2].Value == "named", match.Groups[3].Value.Replace("`None`", "`null`")),
                StringComparer.Ordinal);
    }
}
