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
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.Tools.Tests;

/// <summary>
/// Python's <c>test_common.py</c>: the properties reader, <c>printJson</c> on a good and a failing output, and the
/// shutdown hook's signal handling.
/// </summary>
public sealed class CommonTests : IDisposable
{
    private readonly string _tmpPath = Directory.CreateDirectory(
        Path.Combine(Path.GetTempPath(), "verifiable-common-tests-" + Guid.NewGuid().ToString("N"))).FullName;

    public void Dispose() => Directory.Delete(_tmpPath, recursive: true);

    [Fact]
    public void ParsePropertiesHandlesCommonForms()
    {
        string contents =
            "# a comment\n"
            + "! another comment\n"
            + "\n"
            + "bootstrap.servers=localhost:9092\n"
            + "linger.ms : 5\n"
            + "compression.type snappy\n"
            + "  padded.key = padded.value\n";
        AssertProperties(
            new Dictionary<string, string>
            {
                ["bootstrap.servers"] = "localhost:9092",
                ["linger.ms"] = "5",
                ["compression.type"] = "snappy",
                ["padded.key"] = "padded.value",
            },
            Utils.ParseProperties(contents));
    }

    [Fact]
    public void ParsePropertiesKeepsSeparatorsInsideTheValue()
    {
        AssertProperties(
            new Dictionary<string, string> { ["listeners"] = "PLAINTEXT://host:9092", ["key"] = "=value" },
            Utils.ParseProperties("listeners=PLAINTEXT://host:9092\nkey:=value\n"));
    }

    [Fact]
    public void ParsePropertiesLaterLineWins()
    {
        AssertProperties(
            new Dictionary<string, string> { ["acks"] = "all" }, Utils.ParseProperties("acks=1\nacks=all\n"));
    }

    [Fact]
    public void ParsePropertiesJoinsContinuedLines()
    {
        string jaas =
            "/**\n"
            + " * Licensed to the Apache Software Foundation (ASF)\n"
            + " */\n"
            + "\n"
            + "org.apache.kafka.common.security.plain.PlainLoginModule required\n"
            + "    username=\"client\"\n"
            + "    password=\"client-secret\";\n";
        string contents =
            "security.protocol=SASL_PLAINTEXT\n"
            + "sasl.jaas.config=" + jaas.Replace("\n", " \\\n", StringComparison.Ordinal) + "\n"
            + "sasl.mechanism=PLAIN\n";
        AssertProperties(
            new Dictionary<string, string>
            {
                ["security.protocol"] = "SASL_PLAINTEXT",
                ["sasl.jaas.config"] =
                    "/** * Licensed to the Apache Software Foundation (ASF) */ "
                    + "org.apache.kafka.common.security.plain.PlainLoginModule required "
                    + "username=\"client\" password=\"client-secret\";",
                ["sasl.mechanism"] = "PLAIN",
            },
            Utils.ParseProperties(contents));
    }

    [Fact]
    public void ParsePropertiesCommentCharactersOnAContinuationAreText()
    {
        AssertProperties(
            new Dictionary<string, string> { ["key"] = "a # not a comment" },
            Utils.ParseProperties("key=a \\\n  # not a comment\n"));
    }

    [Fact]
    public void ParsePropertiesEvenBackslashesDoNotContinue()
    {
        AssertProperties(
            new Dictionary<string, string> { ["path"] = "C:\\", ["next"] = "1" },
            Utils.ParseProperties("path=C:\\\\\nnext=1\n"));
    }

    [Fact]
    public void ParsePropertiesEscapes()
    {
        AssertProperties(
            new Dictionary<string, string> { ["a=b"] = "c\td", ["u"] = "Ax" },
            Utils.ParseProperties("a\\=b=c\\td\nu=\\u0041\\x\n"));
    }

    [Fact]
    public void ParsePropertiesMalformedUnicodeEscape()
    {
        var e = Assert.Throws<IllegalArgumentException>(() => Utils.ParseProperties("u=\\u00G1\n"));
        Assert.Equal("Malformed \\uxxxx encoding.", e.Message);
    }

    [Fact]
    public void ParsePropertiesHandlesCrlf()
    {
        AssertProperties(
            new Dictionary<string, string> { ["a"] = "1", ["b"] = "2", ["c"] = "3" },
            Utils.ParseProperties("a=1\r\nb=2\rc=3"));
    }

    [Fact]
    public void ParsePropertiesJoinsSurrogatePairs()
    {
        Dictionary<string, string> props = Utils.ParseProperties("client.id=\\uD83D\\uDE80\n");
        AssertProperties(new Dictionary<string, string> { ["client.id"] = "\U0001F680" }, props);
        Assert.Equal(new byte[] { 0xf0, 0x9f, 0x9a, 0x80 }, Encoding.UTF8.GetBytes(props["client.id"]));
    }

    [Fact]
    public void ParsePropertiesKeepsALoneSurrogate()
    {
        AssertProperties(
            new Dictionary<string, string> { ["a"] = "x\uD83Dy" }, Utils.ParseProperties("a=x\\uD83Dy\n"));
    }

    [UnixFact]
    public async Task ShutdownHookSurvivesASecondSignalDuringTheFirst()
    {
        string config = Path.Combine(_tmpPath, "p.properties");
        File.WriteAllText(config, "max.block.ms=1\n");
        using ToolProcess process = ToolProcess.Start(
            "VerifiableProducer", "--topic", "t", "--bootstrap-server", "localhost:1", "--command-config", config);
        Assert.Equal("startup_complete", Name(await process.ReadLineAsync()));
        // Python's re-entrant signal cannot happen here, so this checks that a second SIGTERM while the hook runs is
        // ignored: with stdout left unread, the failed sends fill the pipe, the delivery callback blocks printing,
        // and the hook's Close waits for it until the output is read.
        await Task.Delay(TimeSpan.FromSeconds(2));
        process.Signal("TERM");
        await Task.Delay(TimeSpan.FromMilliseconds(500));
        process.Signal("TERM");
        (int exitCode, string stdout, string stderr) = await process.WaitForExitAsync();

        Assert.True(exitCode == 0, stderr);
        List<string> names = stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries).Select(Name).ToList();
        Assert.Equal(new[] { "shutdown_complete", "tool_data" }, names.Where(n => n != "producer_send_error"));
    }

    [Fact]
    public void ShutdownHookIgnoresSignalsAfterTheFirst()
    {
        var installed = new Dictionary<PosixSignal, Action<PosixSignalContext>>();
        int runs = 0;
        using var ran = new ManualResetEventSlim();
        var hook = new ShutdownHook(
            () =>
            {
                Interlocked.Increment(ref runs);
                ran.Set();
            },
            "hook",
            (signal, handler) =>
            {
                installed[signal] = handler;
                return null;
            });
        Assert.Equal(new HashSet<PosixSignal> { PosixSignal.SIGINT, PosixSignal.SIGTERM }, installed.Keys.ToHashSet());

        var first = new PosixSignalContext(PosixSignal.SIGTERM);
        installed[PosixSignal.SIGTERM](first);
        Assert.True(ran.Wait(TimeSpan.FromSeconds(5)));
        var second = new PosixSignalContext(PosixSignal.SIGINT);
        installed[PosixSignal.SIGINT](second);
        hook.Finish();

        Assert.True(first.Cancel);
        Assert.True(second.Cancel);
        Assert.Equal(1, Volatile.Read(ref runs));
    }

    [Fact]
    public void PrintJsonDropsAFailedWrite()
    {
        var stdout = new BrokenWriter();
        var output = new JsonOutput(stdout);
        output.Print("startup_complete");
        output.Print("shutdown_complete");
        Assert.Equal(1, stdout.Writes);
    }

    [Fact]
    public void PrintJsonWritesOneCompactLine()
    {
        var stdout = new StringWriter();
        new JsonOutput(stdout).Print("x", writer => writer.WriteNull("key"));
        Assert.Matches(new Regex("^\\{\"timestamp\":[0-9]+,\"name\":\"x\",\"key\":null\\}\n\\z"), stdout.ToString());
    }

    private static string Name(string line) => JsonDocument.Parse(line).RootElement.GetProperty("name").GetString()!;

    private static void AssertProperties(Dictionary<string, string> expected, Dictionary<string, string> actual) =>
        Assert.Equal(
            expected.OrderBy(e => e.Key, StringComparer.Ordinal),
            actual.OrderBy(e => e.Key, StringComparer.Ordinal));

    /// <summary>An output whose every write fails as a closed pipe does.</summary>
    private sealed class BrokenWriter : StringWriter
    {
        internal int Writes { get; private set; }

        public override void Write(string? value)
        {
            Writes++;
            throw new IOException("Broken pipe");
        }
    }
}
