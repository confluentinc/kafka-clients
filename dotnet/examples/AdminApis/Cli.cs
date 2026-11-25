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
using System.Globalization;
using System.Linq;
using System.Numerics;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Common;

/// <summary>What the examples share: reporting a failure, and parsing and printing arguments.</summary>
internal static class Cli
{
    /// <summary>Set by <see cref="Fail"/>, so that the process exits with code 1 at the end.</summary>
    public static bool AnyFailed { get; private set; }

    /// <summary>
    /// Prints the failure of one of several operations to stderr. The example still reports the others, and
    /// the process exits with code 1 at the end.
    /// </summary>
    public static void Fail(string message, Exception error)
    {
        AnyFailed = true;
        Console.Error.WriteLine($"{message}: {Describe(error)}");
    }

    /// <summary>
    /// The error's class and message, followed by those of its causes. A failed future's Get() throws an
    /// ExecutionException whose InnerException is the error itself, so that one is described.
    /// </summary>
    public static string Describe(Exception error)
    {
        Exception first = error is ExecutionException { InnerException: { } failure } ? failure : error;
        StringBuilder text = new(Text(first));
        for (Exception? cause = first.InnerException; cause is not null; cause = cause.InnerException)
        {
            text.Append(" (caused by: ").Append(Text(cause)).Append(')');
        }

        return text.ToString();

        // Java's Throwable.toString(), "<class>: <message>": the class alone only for a null message, which
        // a .NET exception never has, so an empty message keeps the ": ".
        static string Text(Exception e)
        {
            return $"{e.GetType().Name}: {e.Message}";
        }
    }

    /// <summary>
    /// Parses <paramref name="text"/> as a <typeparamref name="T"/>, naming it in the usage error. Only a sign
    /// and digits are a number: no white space around it and no thousands separator, whatever the culture.
    /// </summary>
    public static T Parse<T>(string what, string text, NumberStyles styles = NumberStyles.AllowLeadingSign)
        where T : INumberBase<T>
    {
        return T.TryParse(text, styles, CultureInfo.InvariantCulture, out T? value)
            ? value
            : throw new UsageException($"invalid {what}: \"{text}\"");
    }

    /// <summary>Picks the value whose name matches <paramref name="text"/>, ignoring case and reading - as _.</summary>
    public static T Choose<T>(string what, string text, params (string Name, T Value)[] choices)
    {
        string wanted = text.ToLowerInvariant().Replace('-', '_');
        foreach ((string name, T value) in choices)
        {
            if (name == wanted)
            {
                return value;
            }
        }

        string names = string.Join(", ", choices.Select(choice => choice.Name));
        throw new UsageException($"invalid {what}: \"{text}\", expected one of {names}");
    }

    /// <summary>
    /// Splits the arguments into groups of <paramref name="size"/>, failing when they do not divide evenly or,
    /// with <paramref name="required"/>, when there are none.
    /// </summary>
    public static string[][] Groups(string[] args, int size, bool required)
    {
        if (args.Length % size != 0 || (required && args.Length == 0))
        {
            throw new UsageException($"expected arguments in groups of {size}");
        }

        return [.. args.Chunk(size)];
    }

    /// <summary>Parses <c>&lt;topic&gt; &lt;partition&gt;</c> at the start of the group.</summary>
    public static TopicPartition ParseTopicPartition(string[] group)
    {
        return new TopicPartition(group[0], Parse<int>("partition", group[1]));
    }

    /// <summary>Parses a sequence of <c>&lt;topic&gt; &lt;partition&gt;</c> pairs.</summary>
    public static List<TopicPartition> ParseTopicPartitions(string[] args, bool required)
    {
        return [.. Groups(args, 2, required).Select(ParseTopicPartition)];
    }

    /// <summary>Splits a comma-separated list, dropping empty items.</summary>
    public static string[] CommaList(string text)
    {
        return text.Split(',', StringSplitOptions.RemoveEmptyEntries);
    }

    /// <summary>Lowercase hexadecimal encoding.</summary>
    public static string ToHex(byte[] bytes)
    {
        return Convert.ToHexString(bytes).ToLowerInvariant();
    }

    /// <summary>
    /// Decodes the output of <see cref="ToHex"/>. The error does not repeat the text, which may be a secret
    /// such as a delegation token's HMAC.
    /// </summary>
    public static byte[] FromHex(string text)
    {
        try
        {
            return Convert.FromHexString(text);
        }
        catch (FormatException)
        {
            throw new UsageException("invalid hex string");
        }
    }

    /// <summary>The ids as <c>[1,2,3]</c>.</summary>
    public static string Ids(IEnumerable<int> ids)
    {
        return $"[{string.Join(",", ids)}]";
    }

    /// <summary>The partitions by topic, then by partition number.</summary>
    public static IEnumerable<TopicPartition> Sorted(IEnumerable<TopicPartition> partitions)
    {
        return partitions.OrderBy(partition => partition.Topic(), StringComparer.Ordinal)
            .ThenBy(partition => partition.Partition());
    }

    /// <summary>The number, or <c>none</c>.</summary>
    public static string OrNone(long? value)
    {
        return value?.ToString(CultureInfo.InvariantCulture) ?? "none";
    }
}
