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
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The TFM-matrix smoke test for the internal <see cref="NativeProducer"/>: create (mock and config paths), a
/// valid handle, and teardown, with netstandard2.0-safe APIs only so it compiles on net462.
/// </summary>
public sealed class ProducerFoundationTfmSmokeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public void MockProducer_CreateDispose_RoundTrips()
    {
        NativeProducer producer = NativeProducer.CreateMock();
        Assert.False(producer.Handle.IsInvalid);
        TestTimeout.Run(producer.Dispose, s_deadline);
    }

    [Fact]
    public void KafkaProducer_CreateFromConfigDispose_RoundTrips()
    {
        var config = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "localhost:9092",
        };

        NativeProducer producer = NativeProducer.Create(config);
        Assert.False(producer.Handle.IsInvalid);
        TestTimeout.Run(producer.Dispose, s_deadline);
    }

    [Fact]
    public async Task MockProducer_CreateDisposeAsync_RoundTrips()
    {
        NativeProducer producer = NativeProducer.CreateMock();
        Assert.False(producer.Handle.IsInvalid);
        await producer.DisposeAsync();

        Assert.Throws<ObjectDisposedException>(() => producer.Handle);
    }
}
