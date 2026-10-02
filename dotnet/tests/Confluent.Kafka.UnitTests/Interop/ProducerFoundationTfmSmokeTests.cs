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
/// The TFM-matrix smoke test for the M11/P1 producer foundation: the internal
/// <see cref="NativeProducer"/> create (mock + config paths) → assert the handle is
/// valid → teardown round-trip. There is no public producer API yet (foundation
/// only), so the smoke rides the internal wrapper via <c>InternalsVisibleTo</c>.
///
/// It uses only APIs available on the netstandard2.0 floor so it <b>compiles on
/// net462</b> (via ns2.0) as well as net8.0 / net10.0 — the net462 leg's <em>build</em>
/// must pass locally even though its <em>run</em> is Windows/CI-only; net10.0 runs
/// locally. Nothing here uses modern-only APIs.
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

        // Post-teardown the handle is inaccessible (use-after-dispose guard) — the
        // same contract on every TFM.
        Assert.Throws<ObjectDisposedException>(() => producer.Handle);
    }
}
