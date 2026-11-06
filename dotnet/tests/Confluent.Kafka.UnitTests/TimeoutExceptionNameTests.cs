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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Library code names each of the three <c>TimeoutException</c> classes in full: inside a
/// <c>Confluent.Kafka.*</c> namespace a bare <c>TimeoutException</c> silently means the root class.
/// </summary>
public sealed class TimeoutExceptionNameTests
{
    private static readonly Regex s_bareName = new Regex(@"(?<![\w.:])TimeoutException\b", RegexOptions.Compiled);

    // The declarations of the root class and of Kafka's; System's is written as global::System.TimeoutException.
    private static readonly string[] s_declarations =
    {
        "TimeoutException.cs",
        Path.Combine("Common", "Errors", "TimeoutException.cs"),
    };

    [Fact]
    public void LibraryCode_NamesTimeoutExceptionInFull()
    {
        string root = LibrarySourceRoot();
        Assert.True(Directory.Exists(root), $"The library sources are not at {root}.");

        var problems = new List<string>();
        foreach (string file in Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories))
        {
            string relative = file.Substring(root.Length + 1);
            if (IsBuildOutput(relative) || s_declarations.Contains(relative, StringComparer.Ordinal))
            {
                continue;
            }

            string[] lines = CodeOnly(File.ReadAllText(file)).Split('\n');
            for (int i = 0; i < lines.Length; i++)
            {
                if (s_bareName.IsMatch(lines[i]))
                {
                    problems.Add($"{relative}:{i + 1}: {lines[i].Trim()}");
                }
            }
        }

        Assert.True(problems.Count == 0, "A bare TimeoutException:" + Environment.NewLine + string.Join(Environment.NewLine, problems));
    }

    [Theory]
    [InlineData("throw new TimeoutException();", true)]
    [InlineData("catch (TimeoutException)", true)]
    [InlineData("throw new Confluent.Kafka.TimeoutException();", false)]
    [InlineData("throw new global::System.TimeoutException();", false)]
    [InlineData("catch (Common.Errors.TimeoutException)", false)]
    [InlineData("// a bare TimeoutException in a comment", false)]
    [InlineData("/// <see cref=\"TimeoutException\"/>", false)]
    [InlineData("/* TimeoutException */ int x;", false)]
    [InlineData("string s = \"TimeoutException\";", false)]
    public void TheScan_FlagsOnlyABareNameInCode(string source, bool flagged) =>
        Assert.Equal(flagged, s_bareName.IsMatch(CodeOnly(source)));

    private static bool IsBuildOutput(string relative)
    {
        string first = relative.Split(Path.DirectorySeparatorChar)[0];
        return first == "obj" || first == "bin";
    }

    private static string LibrarySourceRoot() =>
        Path.GetFullPath(Path.Combine(
            PublicNamespaceLayoutTests.KafkaSourceRoot(), "..", "..", "..", "dotnet", "src", "Confluent.Kafka"));

    // The source with comments and string and character literals blanked, newlines kept.
    private static string CodeOnly(string source)
    {
        var code = new StringBuilder(source.Length);
        int i = 0;
        while (i < source.Length)
        {
            char c = source[i];
            char next = i + 1 < source.Length ? source[i + 1] : '\0';
            if (c == '/' && next == '/')
            {
                while (i < source.Length && source[i] != '\n')
                {
                    i++;
                }
            }
            else if (c == '/' && next == '*')
            {
                int end = source.IndexOf("*/", i + 2, StringComparison.Ordinal);
                end = end < 0 ? source.Length : end + 2;
                AppendNewlines(code, source, i, end);
                i = end;
            }
            else if (c == '"' || c == '\'')
            {
                bool verbatim = i > 0 && source[i - 1] == '@'
                    || i > 1 && source[i - 1] == '$' && source[i - 2] == '@';
                int end = i + 1;
                while (end < source.Length)
                {
                    if (!verbatim && source[end] == '\\')
                    {
                        end += 2;
                        continue;
                    }

                    if (source[end] == c)
                    {
                        if (verbatim && end + 1 < source.Length && source[end + 1] == c)
                        {
                            end += 2;
                            continue;
                        }

                        break;
                    }

                    if (!verbatim && source[end] == '\n')
                    {
                        break;
                    }

                    end++;
                }

                end = Math.Min(end + 1, source.Length);
                AppendNewlines(code, source, i, end);
                code.Append(c).Append(c);
                i = end;
            }
            else
            {
                code.Append(c);
                i++;
            }
        }

        return code.ToString();
    }

    private static void AppendNewlines(StringBuilder code, string source, int start, int end)
    {
        for (int j = start; j < end; j++)
        {
            if (source[j] == '\n')
            {
                code.Append('\n');
            }
        }
    }
}
