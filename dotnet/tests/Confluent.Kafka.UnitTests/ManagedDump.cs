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
#if NET5_0_OR_GREATER
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
#endif

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Temporary, until the cause of the macOS CI hangs is found: when the first bound of at least
/// <see cref="TestTimeout.NativeStacks.MinimumBound"/> in this process expires while <see cref="ToolVariable"/> names
/// dotnet-dump and <see cref="TestTimeout.NativeStacks.DirectoryVariable"/> names a folder, writes a heap dump of this
/// process and the output of SOS over it to that folder, and summarizes the pending awaits of Confluent.Kafka and the
/// objects they hold.
/// </summary>
internal static class ManagedDump
{
    internal const string ToolVariable = "CONFLUENT_KAFKA_DOTNET_DUMP_TOOL";

    internal static string Capture(TimeSpan bound)
    {
#if NET5_0_OR_GREATER
        string? tool = Environment.GetEnvironmentVariable(ToolVariable);
        string? directory = Environment.GetEnvironmentVariable(TestTimeout.NativeStacks.DirectoryVariable);
        if (bound < TestTimeout.NativeStacks.MinimumBound || string.IsNullOrEmpty(tool) || string.IsNullOrEmpty(directory)
            || Interlocked.Increment(ref s_taken) > 1)
        {
            return string.Empty;
        }

        Stopwatch elapsed = Stopwatch.StartNew();
        string stamp = DateTime.UtcNow.ToString("yyyyMMdd'T'HHmmss.fff", CultureInfo.InvariantCulture);
        string dump = Path.Combine(directory, $"core-{Environment.ProcessId}-{stamp}.dmp");
        string report = Path.Combine(directory, $"dump-{Environment.ProcessId}-{stamp}.txt");
        StringBuilder text = new StringBuilder();
        text.AppendLine(Legend);
        string summary;
        try
        {
            Directory.CreateDirectory(directory);
            summary = Analyze(tool, dump, text, elapsed);
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException or Win32Exception
            or InvalidOperationException or NotSupportedException)
        {
            text.AppendLine($"Managed dump failed: {e}");
            summary = $"  failed: {e.Message}";
        }

        string written;
        try
        {
            File.WriteAllText(report, text.ToString());
            written = report;
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException)
        {
            written = $"not written: {e.Message}";
        }

        string kept = Compress(dump);
        return $"{Environment.NewLine}Managed dump after {elapsed.Elapsed} (report {written}; {kept}). {Legend}"
            + $"{Environment.NewLine}{summary}";
#else
        return string.Empty;
#endif
    }

#if NET5_0_OR_GREATER
    private const string Legend =
        "In an async entry, (N) is the state: the method waits at its await number N, counted from 0 in source order; "
        + "-1 means running or not started, -2 completed.";
    private const string Library = "Confluent.Kafka.";
    private const string TestNamespace = "Confluent.Kafka.UnitTests.";
    private const string NullAddress = "0000000000000000";
    private const int MaxObjects = 80;
    private const int MaxDepth = 3;
    private const int MaxStackLines = 80;
    private const int MaxObjectLines = 40;
    private const int MaxObjectLineLength = 400;
    private const int MaxThreadPoolLines = 20;
    private const long MaxKeptDumpBytes = 1L << 30;
    private static readonly TimeSpan s_budget = TimeSpan.FromMinutes(6);
    private static readonly TimeSpan s_compressReserve = TimeSpan.FromMinutes(1);
    private static readonly TimeSpan s_commandBound = TimeSpan.FromMinutes(3);
    private static readonly TimeSpan s_objectBound = TimeSpan.FromSeconds(30);
    private static readonly string[] s_firstPass =
    {
        "clrthreads", "threadpool -wi", "syncblk", "dumpasync --fields", "clrstack -all",
        "dumpheap -stat -type Confluent.Kafka",
    };
    private static readonly Regex s_stack = new Regex(@"^STACK \d+$", RegexOptions.Compiled);
    private static readonly Regex s_entry = new Regex(
        @"^\s*(?<address>[0-9a-f]{16}) [0-9a-f]{16} \((?<state>[^)]*)\) (?<type>.+)$", RegexOptions.Compiled);
    private static readonly Regex s_threadPoolLine = new Regex(
        @"^\s*(CPU|Worker|Global|Local|Queue|Work|Timer|Number of)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
    private static readonly Regex s_hexAddress = new Regex(@"^[0-9a-f]{16}$", RegexOptions.Compiled);
    private static int s_taken;

    private static string Analyze(string tool, string dump, StringBuilder text, Stopwatch elapsed)
    {
        ToolRun collect = RunTool(
            tool, new[] { "collect", "-p", $"{Environment.ProcessId}", "--type", "Heap", "-o", dump }, Remaining(elapsed, s_commandBound));
        text.AppendLine($"=== collect, {collect.Duration}, exit {collect.ExitCode}").AppendLine(collect.Output);
        if (collect.ExitCode != 0 || !File.Exists(dump))
        {
            return $"  dotnet-dump collect exited with {collect.ExitCode}: {collect.Output.Trim()}";
        }

        List<string> arguments = new List<string> { "analyze", dump };
        foreach (string command in s_firstPass)
        {
            arguments.Add("-c");
            arguments.Add(command);
        }

        arguments.Add("-c");
        arguments.Add("exit");
        ToolRun first = RunTool(tool, arguments, Remaining(elapsed, s_commandBound));
        text.AppendLine($"=== {string.Join(" | ", s_firstPass)}, {first.Duration}, exit {first.ExitCode}").AppendLine(first.Output);

        List<string> stackLines = new List<string>();
        Queue<(string Address, string Origin, int Depth)> pending = new Queue<(string, string, int)>();
        HashSet<string> seen = new HashSet<string>(StringComparer.Ordinal);
        ReadStacks(first.Output, stackLines, pending, seen);

        List<string> objectLines = new List<string>();
        int dumped = 0;
        while (pending.Count > 0 && dumped < MaxObjects && elapsed.Elapsed < s_budget - s_compressReserve)
        {
            (string address, string origin, int depth) = pending.Dequeue();
            ToolRun run = RunTool(tool, new[] { "analyze", dump, "-c", $"dumpobj {address}", "-c", "exit" }, s_objectBound);
            dumped++;
            text.AppendLine($"=== dumpobj {address} ({origin}), exit {run.ExitCode}").AppendLine(run.Output);
            ReadObject(address, depth, run.Output, objectLines, pending, seen);
        }

        text.AppendLine($"=== {dumped} objects dumped, {pending.Count} left, after {elapsed.Elapsed}");
        return "  Thread pool:" + Environment.NewLine
            + string.Join(Environment.NewLine, ReadThreadPool(first.Output)) + Environment.NewLine
            + "  Pending awaits that involve Confluent.Kafka:" + Environment.NewLine
            + string.Join(Environment.NewLine, stackLines.Take(MaxStackLines)) + Environment.NewLine
            + "  Objects they hold:" + Environment.NewLine
            + string.Join(Environment.NewLine, objectLines.Take(MaxObjectLines));
    }

    /// <summary>
    /// Keeps each <c>dumpasync</c> chain that involves Confluent.Kafka as its innermost entry, its Confluent.Kafka
    /// entries and its awaits, the chains with an entry of the library itself first, and queues the objects its
    /// Confluent.Kafka state machines hold by reference (a field whose address is its value).
    /// </summary>
    private static void ReadStacks(
        string output, List<string> lines, Queue<(string, string, int)> pending, HashSet<string> seen)
    {
        List<string> testOnly = new List<string>();
        List<string>? stack = null;
        int entries = 0;
        int omitted = 0;
        bool involvesLibrary = false;
        bool involvesInternals = false;
        string? holder = null;

        void End()
        {
            if (stack is not null && involvesLibrary)
            {
                if (omitted > 0)
                {
                    stack.Add($"    ... {omitted} other entries");
                }

                stack[0] += $" ({entries} entries)";
                (involvesInternals ? lines : testOnly).AddRange(stack);
            }

            stack = null;
        }

        foreach (string raw in output.Split('\n'))
        {
            string line = raw.TrimEnd('\r');
            if (s_stack.IsMatch(line))
            {
                End();
                stack = new List<string> { $"  {line}" };
                entries = 0;
                omitted = 0;
                involvesLibrary = false;
                involvesInternals = false;
                holder = null;
                continue;
            }

            if (stack is null)
            {
                continue;
            }

            if (line.Trim().Length == 0)
            {
                End();
                continue;
            }

            Match entry = s_entry.Match(line);
            if (entry.Success)
            {
                string type = entry.Groups["type"].Value;
                string address = entry.Groups["address"].Value;
                bool library = type.StartsWith(Library, StringComparison.Ordinal);
                involvesLibrary |= library;
                involvesInternals |= library && !type.StartsWith(TestNamespace, StringComparison.Ordinal);
                holder = library ? $"{type} @{address}" : null;
                if (library || entries == 0)
                {
                    stack.Add($"    ({entry.Groups["state"].Value}) {type} @{address}");
                }
                else
                {
                    omitted++;
                }

                entries++;
                continue;
            }

            if (line.Contains("<< Awaiting:", StringComparison.Ordinal))
            {
                stack.Add($"    {line.Trim()}");
                continue;
            }

            string[] tokens = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (holder is not null && tokens.Length >= 5 && tokens[0] == tokens[^2] && tokens[0] != NullAddress
                && s_hexAddress.IsMatch(tokens[0]) && seen.Add(tokens[0]))
            {
                pending.Enqueue((tokens[0], $"{tokens[^1]} of {holder}", 1));
            }
        }

        End();
        lines.AddRange(testOnly);
    }

    /// <summary>The thread pool's counts and queues, from the <c>threadpool</c> command.</summary>
    private static IEnumerable<string> ReadThreadPool(string output) =>
        output.Split('\n')
            .Select(line => line.TrimEnd('\r'))
            .SkipWhile(line => !line.StartsWith("Using the ", StringComparison.Ordinal))
            .TakeWhile(line => !line.StartsWith("Index", StringComparison.Ordinal) && !s_stack.IsMatch(line))
            .Where(line => s_threadPoolLine.IsMatch(line))
            .Take(MaxThreadPoolLines)
            .Select(line => $"    {line.Trim()}");

    /// <summary>
    /// Summarizes a Confluent.Kafka object, a semaphore, an event or a task completion source as its instance value
    /// fields, and queues what it holds by reference.
    /// </summary>
    private static void ReadObject(
        string address, int depth, string output, List<string> lines, Queue<(string, string, int)> pending, HashSet<string> seen)
    {
        string? name = null;
        List<string> values = new List<string>();
        List<(string Name, string Address)> references = new List<(string, string)>();
        foreach (string raw in output.Split('\n'))
        {
            string line = raw.TrimEnd('\r');
            if (line.StartsWith("Name:", StringComparison.Ordinal))
            {
                name = line.Substring("Name:".Length).Trim();
                continue;
            }

            string[] tokens = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (tokens.Length < 7 || !s_hexAddress.IsMatch(tokens[0]) || tokens[^3] != "instance")
            {
                continue;
            }

            if (tokens[^4] == "Yes")
            {
                values.Add($"{tokens[^1]}={tokens[^2]}");
            }
            else if (tokens[^4] == "No" && tokens[^2] != NullAddress && s_hexAddress.IsMatch(tokens[^2]))
            {
                references.Add((tokens[^1], tokens[^2]));
            }
        }

        if (name is null)
        {
            return;
        }

        bool library = name.StartsWith(Library, StringComparison.Ordinal);
        bool primitive = name.StartsWith("System.Threading.SemaphoreSlim", StringComparison.Ordinal)
            || name.StartsWith("System.Threading.ManualResetEventSlim", StringComparison.Ordinal)
            || name.StartsWith("System.Threading.Tasks.TaskCompletionSource", StringComparison.Ordinal);
        if (library || primitive)
        {
            string summary = $"    {name} @{address}: {string.Join(", ", values)}"
                + $"; references {string.Join(", ", references.Select(r => r.Name))}";
            lines.Add(summary.Length > MaxObjectLineLength ? summary.Substring(0, MaxObjectLineLength) + "…" : summary);
        }

        if (!(library || primitive) || depth >= MaxDepth)
        {
            return;
        }

        foreach ((string field, string target) in references)
        {
            if (seen.Add(target))
            {
                pending.Enqueue((target, $"{field} of {name} @{address}", depth + 1));
            }
        }
    }

    private static TimeSpan Remaining(Stopwatch elapsed, TimeSpan bound)
    {
        TimeSpan left = s_budget - elapsed.Elapsed;
        return left < bound ? (left > TimeSpan.FromSeconds(10) ? left : TimeSpan.FromSeconds(10)) : bound;
    }

    /// <summary>
    /// Runs dotnet-dump, reading its output on threads of its own, so a starved thread pool cannot hold up the read.
    /// </summary>
    private static ToolRun RunTool(string tool, IReadOnlyList<string> arguments, TimeSpan bound)
    {
        Stopwatch watch = Stopwatch.StartNew();
        ProcessStartInfo start = new ProcessStartInfo(tool)
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        foreach (string argument in arguments)
        {
            start.ArgumentList.Add(argument);
        }

        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DOTNET_ROOT")))
        {
            start.Environment["DOTNET_ROOT"] =
                Path.GetFullPath(Path.Combine(RuntimeEnvironment.GetRuntimeDirectory(), "..", "..", ".."));
        }

        using Process process = Process.Start(start)!;
        StringBuilder output = new StringBuilder();
        Thread outputReader = Reader(process.StandardOutput, output);
        Thread errorReader = Reader(process.StandardError, output);
        if (!process.WaitForExit((int)bound.TotalMilliseconds))
        {
            try
            {
                process.Kill(entireProcessTree: true);
            }
            catch (Exception e) when (e is InvalidOperationException or Win32Exception)
            {
            }

            return new ToolRun(-1, $"{Path.GetFileName(tool)} did not finish within {bound}", watch.Elapsed);
        }

        outputReader.Join(s_objectBound);
        errorReader.Join(s_objectBound);
        lock (output)
        {
            return new ToolRun(process.ExitCode, output.ToString(), watch.Elapsed);
        }
    }

    private static Thread Reader(StreamReader reader, StringBuilder output)
    {
        Thread thread = new Thread(
            () =>
            {
                string? line;
                while ((line = reader.ReadLine()) is not null)
                {
                    lock (output)
                    {
                        output.AppendLine(line);
                    }
                }
            })
        {
            IsBackground = true,
        };
        thread.Start();
        return thread;
    }

    private static string Compress(string dump)
    {
        if (!File.Exists(dump))
        {
            return "no dump";
        }

        string compressed = dump + ".gz";
        try
        {
            using (FileStream source = File.OpenRead(dump))
            using (FileStream target = File.Create(compressed))
            using (GZipStream gzip = new GZipStream(target, CompressionLevel.Fastest))
            {
                source.CopyTo(gzip);
            }

            long size = new FileInfo(compressed).Length;
            if (size > MaxKeptDumpBytes)
            {
                File.Delete(compressed);
                return $"dump dropped, {size} bytes compressed";
            }

            return $"dump {compressed}, {size / (1024 * 1024)} MiB";
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException)
        {
            return $"compressing the dump failed: {e.Message}";
        }
        finally
        {
            try
            {
                File.Delete(dump);
            }
            catch (Exception e) when (e is IOException or UnauthorizedAccessException)
            {
            }
        }
    }

    /// <summary>One dotnet-dump run: its exit code, its output and how long it took.</summary>
    private sealed record ToolRun(int ExitCode, string Output, TimeSpan Duration);
#endif
}
