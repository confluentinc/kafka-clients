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

using Confluent.Kafka.ShareConsumer.Internal.Interop;
using Xunit;

namespace Confluent.Kafka.ShareConsumer.UnitTests;

/// <summary>
/// Structural scaffolding checks only: the test project's reference to the library
/// and the <c>InternalsVisibleTo</c> grant resolve, and the TFM-run matrix
/// (net8.0 + net10.0) executes. No native library is loaded or called.
/// </summary>
public class ScaffoldingTests
{
    [Fact]
    public void Native_DllName_MatchesAbiLibraryName()
    {
        // Reaching the internal Native.DllName const compiles only when the
        // InternalsVisibleTo grant to this test assembly resolves.
        Assert.Equal("confluent_kafka", Native.DllName);
    }

    [Fact]
    public void Native_IsInternalType_ReachableViaInternalsVisibleTo()
    {
        // typeof(Native) touches the internal type at runtime (not an inlined
        // const), proving the project reference + InternalsVisibleTo end to end.
        // A top-level internal type is not externally visible.
        Assert.False(typeof(Native).IsVisible);
    }
}
