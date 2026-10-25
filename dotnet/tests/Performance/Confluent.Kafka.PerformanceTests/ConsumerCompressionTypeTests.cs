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

using Xunit;

namespace Confluent.Kafka.Performance.Tests;

/// <summary>
/// <c>COMPRESSION_TYPE</c> for the consumer benchmark's load driver: unset keeps the
/// producer's default, one of none / gzip / snappy / lz4 / zstd is kept, and anything else is
/// rejected with the consumer perf test's message.
/// </summary>
[Collection(PerfEngineCollection.Name)]
public sealed class ConsumerCompressionTypeTests
{
    [Theory]
    [InlineData("none")]
    [InlineData("gzip")]
    [InlineData("snappy")]
    [InlineData("lz4")]
    [InlineData("zstd")]
    public void CompressionType_AValidValue_IsKept(string compressionType)
    {
        Apply(new Dictionary<string, string>(StringComparer.Ordinal) { ["COMPRESSION_TYPE"] = compressionType });

        Assert.Equal(compressionType, ConsumerBenchmarkConfig.FromEnv().CompressionType);
    }

    [Fact]
    public void CompressionType_Unset_KeepsTheProducerDefault()
    {
        Apply(new Dictionary<string, string>(StringComparer.Ordinal));

        Assert.Null(ConsumerBenchmarkConfig.FromEnv().CompressionType);
    }

    [Theory]
    [InlineData("brotli")]
    [InlineData("GZIP")]
    public void CompressionType_AnInvalidValue_IsRejected(string compressionType)
    {
        Apply(new Dictionary<string, string>(StringComparer.Ordinal) { ["COMPRESSION_TYPE"] = compressionType });

        ArgumentException ex = Assert.Throws<ArgumentException>(() => ConsumerBenchmarkConfig.FromEnv());

        Assert.Equal(
            "COMPRESSION_TYPE must be one of none, gzip, snappy, lz4, zstd; got '" + compressionType + "'",
            ex.Message);
    }

    private static void Apply(IReadOnlyDictionary<string, string> values) => PerfEngineFixture.Apply(values);
}
