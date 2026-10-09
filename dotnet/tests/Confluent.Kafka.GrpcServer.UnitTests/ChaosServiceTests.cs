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

using System.Threading.Tasks;

using Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

using Xunit;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer.UnitTests;

/// <summary>
/// The RPC lifecycle shared by every workload (PLAN §5.2–§5.4; §7.1 T4–T7, T17): the id registry,
/// the response headers, stop and mark, and cancellation. Each runs on every flavour.
/// </summary>
public sealed class ChaosServiceTests
{
    // ---- T5 ----

    [Theory]
    [MemberData(nameof(ChaosFlavours.All), MemberType = typeof(ChaosFlavours))]
    public async Task StopAndMark_OnAnUnknownId_AreOk_AndNotFound(ChaosFlavour flavour)
    {
        using ChaosHarness harness = ChaosHarness.Create(flavour);

        Proto.StatusResponse stopped = await harness.Stop("nobody");
        bool found = await harness.Mark("nobody", 1);

        Assert.Null(stopped.Error);
        Assert.False(found);
    }
}
