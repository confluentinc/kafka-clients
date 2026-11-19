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
using System.Text.Json;
using System.Threading;

using Xunit;

namespace Confluent.Kafka.Performance.Tests;

/// <summary>Checks the keys, their order and the unset measurement bounds of one <c>metrics.jsonl</c> line.</summary>
[Collection(PerfEngineCollection.Name)]
public sealed class MetricsLineSchemaTests
{
    private static readonly string[] s_baseKeys =
    {
        "rss", "cpu", "latency", "bytes", "messages",
        "window_start_ms", "window_end_ms", "measurement_start_ms", "measurement_end_ms",
    };

    private static readonly string[] s_bucketKeys = { "average", "max", "total", "count" };

    private static readonly string[] s_latencyKeys = { "average", "max", "total", "count", "p50", "p90", "p99", "p999" };

    [Fact]
    public void ProducerLine_HasThePythonKeysInOrder()
    {
        JsonElement line = OneLine(() => new Metrics());

        Assert.Equal(s_baseKeys, Keys(line));
        AssertBuckets(line, "latency");
        Assert.Equal("-inf", line.GetProperty("measurement_start_ms").GetString());
        Assert.Equal("-inf", line.GetProperty("measurement_end_ms").GetString());
    }

    [Fact]
    public void TransactionalLine_AppendsTheTransactionFields()
    {
        JsonElement line = OneLine(Metrics.ForTransactional);

        Assert.Equal(s_baseKeys.Concat(new[] { "transactions", "commit_latency", "abort_latency" }), Keys(line));
        AssertBuckets(line, "latency", "commit_latency", "abort_latency");
        Assert.Equal(s_bucketKeys, Keys(line.GetProperty("transactions")));
    }

    private static JsonElement OneLine(Func<Metrics> create)
    {
        string path = Path.Combine(Directory.GetCurrentDirectory(), "metrics-" + Guid.NewGuid().ToString("N") + ".jsonl");
        PerfEngineFixture.Apply(new Dictionary<string, string>(StringComparer.Ordinal) { ["METRICS_FILE"] = path });

        using (Metrics metrics = create())
        {
            metrics.StartCollecting(1);
            Thread.Sleep(1_500);
        }

        string first = File.ReadLines(path).First();
        File.Delete(path);
        using JsonDocument document = JsonDocument.Parse(first);
        return document.RootElement.Clone();
    }

    private static void AssertBuckets(JsonElement line, params string[] latencyKeys)
    {
        foreach (string key in new[] { "rss", "cpu", "bytes", "messages" })
        {
            Assert.Equal(s_bucketKeys, Keys(line.GetProperty(key)));
        }

        foreach (string key in latencyKeys)
        {
            Assert.Equal(s_latencyKeys, Keys(line.GetProperty(key)));
        }
    }

    private static string[] Keys(JsonElement element) => element.EnumerateObject().Select(p => p.Name).ToArray();
}
