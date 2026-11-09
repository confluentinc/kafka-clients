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
using System.Text.RegularExpressions;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Confluent.Kafka.UnitTests.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The error classes are Java's: each extends the class of its Java parent, read from the Kafka sources (a JDK class
/// extends the .NET built-in that stands for Java's chain), every error class is a Java class, a class is abstract
/// where Java's is, a catch by class catches what Java's catches, and each class's id is the C enum's value for it,
/// read from the C header, so every enum id but <c>NONE</c> and the core's own <c>LOCAL_CALLBACK</c> and
/// <c>LOCAL_NULL_POINTER</c> has a class.
/// </summary>
public sealed class ErrorHierarchyTests
{
    private const string KafkaExceptionClass = "org.apache.kafka.common.KafkaException";

    private const string JavaRuntimeException = "java.lang.RuntimeException";

    private const string EnumEnd = "} kafka_common_Error_e;";

    // The generator's RUST_ONLY_IDS: the carried managed exception, and Java's NullPointerException, which has no class
    // (an ArgumentNullException here, as Python's TypeError).
    private static readonly HashSet<string> s_rustOnlyIds = new(StringComparer.Ordinal) { "LOCAL_CALLBACK", "LOCAL_NULL_POINTER" };

    /// <summary>
    /// The .NET base of each JDK class: the built-in .NET mapped the class to before it had one, standing for Java's
    /// chain, since .NET has no <c>RuntimeException</c> and no multiple inheritance.
    /// </summary>
    private static readonly IReadOnlyDictionary<string, Type> s_jdkBases = new Dictionary<string, Type>
    {
        ["java.lang.IllegalStateException"] = typeof(InvalidOperationException),
        ["java.lang.IllegalArgumentException"] = typeof(ArgumentException),
        ["java.lang.UnsupportedOperationException"] = typeof(NotSupportedException),
        ["java.lang.IndexOutOfBoundsException"] = typeof(ArgumentOutOfRangeException),
        ["java.util.ConcurrentModificationException"] = typeof(InvalidOperationException),
        ["java.util.NoSuchElementException"] = typeof(InvalidOperationException),
        ["java.util.concurrent.TimeoutException"] = typeof(System.TimeoutException),
        ["java.util.concurrent.ExecutionException"] = typeof(Exception),
    };

    /// <summary>The JDK classes the core has no id for.</summary>
    private static readonly string[] s_jdkWithoutId =
    {
        "java.lang.IndexOutOfBoundsException",
        "java.util.NoSuchElementException",
        "java.util.concurrent.ExecutionException",
    };

    /// <summary>Java's abstract exception classes on this surface: catch-only bases, with no id.</summary>
    private static readonly string[] s_abstractBases =
    {
        "org.apache.kafka.clients.consumer.InvalidOffsetException",
        "org.apache.kafka.common.errors.ApplicationRecoverableException",
        "org.apache.kafka.common.errors.InvalidMetadataException",
        "org.apache.kafka.common.errors.RefreshRetriableException",
        "org.apache.kafka.common.errors.RetriableException",
    };

    /// <summary>The <c>java.lang</c> classes a Kafka exception can extend without importing them.</summary>
    private static readonly string[] s_javaLangParents =
    {
        "RuntimeException", "Exception", "Throwable", "IllegalStateException", "IllegalArgumentException",
    };

    [Fact]
    public void EveryClass_ExtendsTheClassOfItsJavaParent()
    {
        IReadOnlyDictionary<string, Type> byJava = ByJava();
        var problems = new List<string>();

        foreach (KeyValuePair<string, Type> row in byJava.OrderBy(row => row.Key, StringComparer.Ordinal))
        {
            (string java, Type type) = (row.Key, row.Value);
            if (IsJdk(java))
            {
                if (!s_jdkBases.TryGetValue(java, out Type? expected))
                {
                    problems.Add($"{java} is a JDK class without a .NET base in this test's table.");
                }
                else if (type.BaseType != expected)
                {
                    problems.Add($"{type.FullName} extends {type.BaseType?.FullName}, not {expected.FullName}.");
                }

                continue;
            }

            string parent = JavaParent(java);
            if (parent == JavaRuntimeException)
            {
                if (java != KafkaExceptionClass)
                {
                    problems.Add($"{java} extends {parent}; only {KafkaExceptionClass} does.");
                }
                else if (type.BaseType != typeof(Exception))
                {
                    problems.Add($"{type.FullName} extends {type.BaseType?.FullName}, not System.Exception.");
                }
            }
            else if (!byJava.TryGetValue(parent, out Type? parentType))
            {
                problems.Add($"{java} extends {parent}, which has no .NET class.");
            }
            else if (type.BaseType != parentType)
            {
                problems.Add(
                    $"{type.FullName} extends {type.BaseType?.FullName}, not {parentType.FullName} ({java} extends {parent}).");
            }
        }

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
        Assert.Equal(
            s_jdkBases.Keys.OrderBy(java => java, StringComparer.Ordinal),
            byJava.Keys.Where(IsJdk).OrderBy(java => java, StringComparer.Ordinal));
    }

    [Fact]
    public void EveryErrorClass_IsAJavaClass_AndEveryJavaClassHasOne()
    {
        IReadOnlyDictionary<string, Type> byJava = ByJava();
        string[] exported = typeof(KafkaException).Assembly.GetExportedTypes()
            .Where(type => typeof(Exception).IsAssignableFrom(type))
            .Select(type => type.FullName!)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();
        string[] expected = Bridge().Values
            .Concat(s_jdkWithoutId)
            .Concat(s_abstractBases)
            .Concat(new[] { KafkaExceptionClass, "java.lang.UnsupportedOperationException" })
            .Distinct()
            .OrderBy(java => java, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(byJava.Values.Select(type => type.FullName!).OrderBy(name => name, StringComparer.Ordinal), exported);
        Assert.Equal(expected, byJava.Keys.OrderBy(java => java, StringComparer.Ordinal));
        Assert.Equal(175, byJava.Count);
    }

    [Fact]
    public void AClass_IsAbstract_WhereJavasIs()
    {
        IReadOnlyDictionary<string, Type> byJava = ByJava();
        var problems = new List<string>();

        foreach (KeyValuePair<string, Type> row in byJava.OrderBy(row => row.Key, StringComparer.Ordinal))
        {
            bool javaAbstract = !IsJdk(row.Key) && Regex.IsMatch(JavaSource(row.Key), @"public\s+abstract\s+class\s");
            if (row.Value.IsAbstract != javaAbstract)
            {
                problems.Add($"{row.Value.FullName} is {(row.Value.IsAbstract ? string.Empty : "not ")}abstract, unlike {row.Key}.");
            }
        }

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
        Assert.Equal(
            s_abstractBases,
            byJava.Where(row => row.Value.IsAbstract).Select(row => row.Key).OrderBy(java => java, StringComparer.Ordinal));
        Assert.DoesNotContain(ErrorClasses.Ids.Keys, type => type.IsAbstract);
    }

    [Fact]
    public void EachClassesId_IsTheCEnumsValueForItsClass()
    {
        IReadOnlyDictionary<string, Type> byJava = ByJava();
        IReadOnlyDictionary<string, int> cEnum = CEnum();
        IReadOnlyDictionary<string, string> bridge = Bridge();
        var problems = new List<string>();

        foreach (KeyValuePair<string, string> row in bridge.OrderBy(row => row.Key, StringComparer.Ordinal))
        {
            Type type = byJava[row.Value];
            if (!cEnum.TryGetValue(row.Key, out int value))
            {
                problems.Add($"{row.Key} ({row.Value}) is not in the C enum.");
            }
            else if (!ErrorClasses.Ids.TryGetValue(type, out int id))
            {
                problems.Add($"{type.FullName} has no id; the C enum's {row.Key} is {value}.");
            }
            else if (id != value)
            {
                problems.Add($"{type.FullName}'s id is {id}; the C enum's {row.Key} is {value}.");
            }
        }

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
        Assert.Equal(166, bridge.Count);
        Assert.Equal(166, bridge.Keys.Select(constant => cEnum[constant]).Distinct().Count());
        Assert.Equal(cEnum["UNKNOWN_SERVER_ERROR"], ErrorClasses.Ids[typeof(KafkaException)]);
        Assert.Equal(-1, ErrorClasses.Ids[typeof(KafkaException)]);
        Assert.Equal(
            bridge.Values.Select(java => byJava[java]).Concat(new[] { typeof(KafkaException) }).Select(type => type.FullName!)
                .OrderBy(name => name, StringComparer.Ordinal),
            ErrorClasses.Ids.Keys.Select(type => type.FullName!).OrderBy(name => name, StringComparer.Ordinal));
        foreach (string java in s_jdkWithoutId)
        {
            Assert.False(ErrorClasses.Ids.ContainsKey(byJava[java]), $"{java} has an id; the core has none for it.");
        }
    }

    [Fact]
    public void EveryCEnumId_ButNoneAndTheRustOnlyIds_HasAClass_AndAllButMinusOneAndMinus27_ARegistryEntry()
    {
        IReadOnlyDictionary<string, int> cEnum = CEnum();
        int[] classIds = cEnum.Where(entry => entry.Key != "NONE" && !s_rustOnlyIds.Contains(entry.Key))
            .Select(entry => entry.Value)
            .OrderBy(id => id)
            .ToArray();

        Assert.Equal(0, cEnum["NONE"]);
        Assert.Equal(-30, cEnum["LOCAL_CALLBACK"]);
        Assert.Equal(-32, cEnum["LOCAL_NULL_POINTER"]);
        Assert.Equal(
            cEnum.Keys.Where(name => name != "NONE" && !s_rustOnlyIds.Contains(name)).OrderBy(name => name, StringComparer.Ordinal),
            Bridge().Keys.OrderBy(name => name, StringComparer.Ordinal));
        Assert.Equal(classIds, ErrorClasses.Ids.Values.Distinct().OrderBy(id => id));
        Assert.Equal(
            classIds.Where(id => id != cEnum["UNKNOWN_SERVER_ERROR"] && id != cEnum["RECORD_DESERIALIZATION"]),
            ErrorClasses.Factories.Keys.OrderBy(id => id));
        Assert.Equal(cEnum["UNKNOWN_SERVER_ERROR"], ErrorClasses.Ids[typeof(UnknownServerException)]);
        Assert.Equal(cEnum["RECORD_DESERIALIZATION"], ErrorClasses.Ids[typeof(RecordDeserializationException)]);
    }

    [Fact]
    public void ACatchByClass_CatchesExactlyTheClassAndItsJavaSubclasses()
    {
        IReadOnlyDictionary<string, Type> byJava = ByJava();
        var problems = new List<string>();

        foreach (KeyValuePair<string, Exception> built in BuiltFromTheRegistry(byJava))
        {
            HashSet<string> javaChain = JavaChain(built.Key, byJava);
            foreach (KeyValuePair<string, Type> target in byJava)
            {
                bool caught = Catches(built.Value, target.Value);
                if (caught != javaChain.Contains(target.Key))
                {
                    problems.Add(
                        $"catch ({target.Value.Name}) {(caught ? "catches" : "misses")} {built.Value.GetType().Name}, "
                        + $"which Java {(caught ? "does not" : "does")} make a {target.Key}.");
                }
            }
        }

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Theory]
    [InlineData("group")]
    [InlineData("authorization")]
    [InlineData("retriable")]
    [InlineData("kafka")]
    public void TheGoalsExample_ClassifiesAMocksError_ByItsClass(string expected)
    {
        KafkaException injected = expected switch
        {
            "group" => new GroupAuthorizationException("not authorized for the group"),
            "authorization" => new ClusterAuthorizationException("not authorized for the cluster"),
            "retriable" => new NotLeaderOrFollowerException("not the leader"),
            _ => new InvalidRecordException("an invalid record"),
        };
        var tp = new TopicPartition("error-hierarchy-topic", 0);
        using var consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Assign(new[] { tp });
        consumer.Seek(tp, 0);
        consumer.SetPollException(injected);

        string classified;
        try
        {
            consumer.Poll(TimeSpan.Zero);
            classified = "none";
        }
        catch (GroupAuthorizationException e)
        {
            Assert.Same(injected, e);
            classified = "group";
        }
        catch (AuthorizationException e)
        {
            Assert.Same(injected, e);
            classified = "authorization";
        }
        catch (RetriableException e)
        {
            Assert.Same(injected, e);
            classified = "retriable";
        }
        catch (KafkaException e)
        {
            Assert.Same(injected, e);
            classified = "kafka";
        }

        Assert.Equal(expected, classified);
    }

    /// <summary>
    /// One instance of every class with an id, built as a core error of that id is: the registry entry, and for id
    /// -1 both of its classes.
    /// </summary>
    private static IEnumerable<KeyValuePair<string, Exception>> BuiltFromTheRegistry(IReadOnlyDictionary<string, Type> byJava)
    {
        Dictionary<Type, string> javaOf = byJava.ToDictionary(row => row.Value, row => row.Key);
        IEnumerable<Exception> built = ErrorClasses.Factories.Keys
            .Select(code => KafkaException.OfClass(code, isApiError: false, "m", null, IntPtr.Zero))
            .Concat(new[]
            {
                KafkaException.OfClass(-1, isApiError: true, "m", null, IntPtr.Zero),
                KafkaException.OfClass(-1, isApiError: false, "m", null, IntPtr.Zero),
            });
        foreach (Exception exception in built)
        {
            Assert.True(javaOf.TryGetValue(exception.GetType(), out string? java), $"{exception.GetType()} has no Java row.");
            yield return new KeyValuePair<string, Exception>(java!, exception);
        }
    }

    /// <summary>A class and its Java ancestors on this surface, read from the Kafka sources.</summary>
    private static HashSet<string> JavaChain(string java, IReadOnlyDictionary<string, Type> byJava)
    {
        var chain = new HashSet<string>(StringComparer.Ordinal);
        for (string? current = java; current is not null && byJava.ContainsKey(current);)
        {
            chain.Add(current);
            current = IsJdk(current) || current == KafkaExceptionClass ? null : JavaParent(current);
        }

        return chain;
    }

    /// <summary>Whether <c>catch (target)</c> catches <paramref name="exception"/>.</summary>
    private static bool Catches(Exception exception, Type target)
    {
        try
        {
            throw exception;
        }
        catch (Exception thrown) when (target.IsInstanceOfType(thrown))
        {
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }

    /// <summary>Each row's .NET type, keyed by its Java class.</summary>
    private static IReadOnlyDictionary<string, Type> ByJava() =>
        ErrorClassRows.Rows.ToDictionary(
            row => row.Java,
            row => PublicNamespaceLayoutTests.Exported(
                PublicNamespaceLayoutTests.NamespaceOf(PublicNamespaceLayoutTests.JavaPackage(row.Java)) + "." + row.DotNet),
            StringComparer.Ordinal);

    private static bool IsJdk(string java) => java.StartsWith("java.", StringComparison.Ordinal);

    /// <summary>A Kafka exception class's parent, from its source: an imported class, a <c>java.lang</c> one, or one
    /// of its own package.</summary>
    private static string JavaParent(string java)
    {
        string source = JavaSource(java);
        Match package = Regex.Match(source, @"package\s+([\w.]+);");
        Assert.True(package.Success, $"{java} declares no package.");
        Match extends = Regex.Match(source, @"public\s+(?:abstract\s+|final\s+)?class\s+\w+\s+extends\s+(\w+)");
        Assert.True(extends.Success, $"{java} declares no public class that extends another.");

        string parent = extends.Groups[1].Value;
        Match imported = Regex.Match(source, $@"import\s+([\w.]+\.{parent});");
        if (imported.Success)
        {
            return imported.Groups[1].Value;
        }

        return s_javaLangParents.Contains(parent) ? "java.lang." + parent : package.Groups[1].Value + "." + parent;
    }

    private static string JavaSource(string java)
    {
        string root = PublicNamespaceLayoutTests.KafkaSourceRoot();
        if (!Directory.Exists(Path.Combine(root, "main", "java")))
        {
            Assert.Fail($"The Kafka sources are not at {root}; run `git submodule update --init kafka`.");
        }

        string file = Path.Combine(new[] { root, "main", "java" }.Concat(java.Split('.')).ToArray()) + ".java";
        Assert.True(File.Exists(file), $"{java} has no source at {file}.");
        return File.ReadAllText(file);
    }

    /// <summary>The C enum <c>kafka_common_Error_e</c>: each constant's name below the prefix, and its value.</summary>
    internal static IReadOnlyDictionary<string, int> CEnum()
    {
        string header = NativeAbiConformanceTests.ReadHeader();
        int end = header.IndexOf(EnumEnd, StringComparison.Ordinal);
        Assert.True(end >= 0, $"The C header declares no {EnumEnd}");
        int start = header.LastIndexOf("typedef enum {", end, StringComparison.Ordinal);
        Assert.True(start >= 0, "The C header's kafka_common_Error_e has no opening typedef.");

        Dictionary<string, int> values = Regex
            .Matches(header.Substring(start, end - start), @"kafka_common_Error_([A-Z0-9_]+)\s*=\s*(-?\d+)")
            .Cast<Match>()
            .ToDictionary(match => match.Groups[1].Value, match => int.Parse(match.Groups[2].Value), StringComparer.Ordinal);
        Assert.Equal(169, values.Count);
        return values;
    }

    /// <summary>The error generator's reviewed table of each C enum constant's Java class.</summary>
    private static IReadOnlyDictionary<string, string> Bridge()
    {
        string generator = Path.GetFullPath(Path.Combine(
            PublicNamespaceLayoutTests.KafkaSourceRoot(), "..", "..", "..", "rust", "xtask", "src", "error_hierarchy.rs"));
        Assert.True(File.Exists(generator), $"The error generator is not at {generator}.");

        string text = File.ReadAllText(generator);
        int start = text.IndexOf("const BRIDGE:", StringComparison.Ordinal);
        Assert.True(start >= 0, $"{generator} has no BRIDGE table.");
        int end = text.IndexOf("];", start, StringComparison.Ordinal);
        return Regex.Matches(text.Substring(start, end - start), @"\(\s*""([A-Z0-9_]+)""\s*,\s*""([\w.]+)""\s*,?\s*\)")
            .Cast<Match>()
            .ToDictionary(match => match.Groups[1].Value, match => match.Groups[2].Value, StringComparer.Ordinal);
    }
}
