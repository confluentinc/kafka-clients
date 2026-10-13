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
using System.Linq;
using System.Threading;
using Xunit;

namespace Confluent.Kafka.Soak.Tests;

/// <summary>Last-value gauge retention — an event-driven series must not vanish from the backend.</summary>
public sealed class LastValueGaugesTests
{
    private static Dictionary<string, string> Tags(params (string Key, string Value)[] entries)
    {
        var tags = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach ((string key, string value) in entries)
        {
            tags[key] = value;
        }

        return tags;
    }

    /// <summary>
    /// The regression this pins: the reference callback yielded the buffered values then
    /// CLEARED the buffer, so the <i>second</i> collection yielded nothing and the series
    /// vanished from the backend rather than holding its last value. Observed against a
    /// real collector as <c>consumer.assignment_size</c> missing entirely.
    /// </summary>
    [Fact]
    public void RetainsAcrossCollections()
    {
        var gauges = new LastValueGauges();
        gauges.Record("consumer.assignment_size", 2, Tags(("host", "h")));

        for (int i = 0; i < 6; i++)
        {
            IReadOnlyList<(double Value, IReadOnlyDictionary<string, string> Tags)> snapshot =
                gauges.Snapshot("consumer.assignment_size");
            (double value, IReadOnlyDictionary<string, string> tags) = Assert.Single(snapshot);
            Assert.Equal(2, value);
            Assert.Equal("h", tags["host"]);
        }
    }

    [Fact]
    public void OverwritesWithinATagSet()
    {
        var gauges = new LastValueGauges();
        foreach (double value in new double[] { 1, 2, 3 })
        {
            gauges.Record("g", value, Tags(("host", "h")));
        }

        Assert.Equal(3, Assert.Single(gauges.Snapshot("g")).Value);
        Assert.Equal(1, gauges.SeriesCount("g"));
    }

    /// <summary>Retention is per tag-set: partitions are distinct series.</summary>
    [Fact]
    public void RetainsPerTagSet()
    {
        var gauges = new LastValueGauges();
        gauges.Record("consumer.e2e_latency", 10.0, Tags(("partition", "0")));
        gauges.Record("consumer.e2e_latency", 20.0, Tags(("partition", "1")));
        gauges.Record("consumer.e2e_latency", 11.0, Tags(("partition", "0")));

        Assert.Equal(2, gauges.SeriesCount("consumer.e2e_latency"));

        List<(double Value, IReadOnlyDictionary<string, string> Tags)> ordered = gauges
            .Snapshot("consumer.e2e_latency")
            .OrderBy(item => item.Tags["partition"], StringComparer.Ordinal)
            .ToList();

        Assert.Equal(11.0, ordered[0].Value);
        Assert.Equal("0", ordered[0].Tags["partition"]);
        Assert.Equal(20.0, ordered[1].Value);
        Assert.Equal("1", ordered[1].Tags["partition"]);
    }

    [Fact]
    public void KeepsMetricsSeparate()
    {
        var gauges = new LastValueGauges();
        gauges.Record("a", 1, Tags());
        gauges.Record("b", 2, Tags());

        Assert.Equal(1, Assert.Single(gauges.Snapshot("a")).Value);
        Assert.Equal(2, Assert.Single(gauges.Snapshot("b")).Value);
        Assert.Empty(gauges.Snapshot("never-recorded"));
        Assert.Equal(0, gauges.SeriesCount("never-recorded"));
    }

    /// <summary>
    /// The SDK collects on its own thread; the tag map handed back must not alias the
    /// caller's, or a later mutation of that map would silently rewrite a retained series.
    /// </summary>
    [Fact]
    public void RecordSnapshotsTheTagMap()
    {
        var gauges = new LastValueGauges();
        Dictionary<string, string> tags = Tags(("k", "v"));
        gauges.Record("g", 1, tags);
        tags["k"] = "mutated";

        Assert.Equal("v", Assert.Single(gauges.Snapshot("g")).Tags["k"]);
    }

    /// <summary>
    /// Concurrent recorders must not lose or corrupt series. Four threads write in
    /// production (the producer loop, the consumer loop, the delivery continuations and
    /// the main thread's resource sampling) while the SDK collects from a fifth.
    /// </summary>
    [Fact]
    public void IsThreadSafe()
    {
        var gauges = new LastValueGauges();
        var errors = new List<Exception>();
        var errorLock = new object();

        void Writer(int partition)
        {
            try
            {
                for (int i = 0; i < 500; i++)
                {
                    gauges.Record("g", i, Tags(("partition", partition.ToString(System.Globalization.CultureInfo.InvariantCulture))));
                }
            }
            catch (Exception ex)
            {
                lock (errorLock)
                {
                    errors.Add(ex);
                }
            }
        }

        var threads = new List<Thread>();
        for (int partition = 0; partition < 4; partition++)
        {
            int captured = partition;
            threads.Add(new Thread(() => Writer(captured)));
        }

        threads.Add(new Thread(() =>
        {
            try
            {
                for (int i = 0; i < 500; i++)
                {
                    _ = gauges.Snapshot("g");
                }
            }
            catch (Exception ex)
            {
                lock (errorLock)
                {
                    errors.Add(ex);
                }
            }
        }));

        foreach (Thread thread in threads)
        {
            thread.Start();
        }

        foreach (Thread thread in threads)
        {
            thread.Join();
        }

        Assert.Empty(errors);
        Assert.Equal(4, gauges.SeriesCount("g"));
        Assert.Equal(new double[] { 499, 499, 499, 499 }, gauges.Snapshot("g").Select(item => item.Value).OrderBy(v => v).ToArray());
    }
}
