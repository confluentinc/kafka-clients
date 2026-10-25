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
using System.Linq;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The <see cref="TransactionState"/> wire names a listTransactions state filter is parsed from, checked through
/// the C singletons they parse to.
/// </summary>
public sealed class AdminP8ResultMarshalTests
{
    // Asserted against Java's toString() spelling, not the C# identifier: a rename would change the wire format
    // without failing to compile.
    [Theory]
    [InlineData(TransactionState.Ongoing, "Ongoing")]
    [InlineData(TransactionState.PrepareAbort, "PrepareAbort")]
    [InlineData(TransactionState.PrepareCommit, "PrepareCommit")]
    [InlineData(TransactionState.CompleteAbort, "CompleteAbort")]
    [InlineData(TransactionState.CompleteCommit, "CompleteCommit")]
    [InlineData(TransactionState.Empty, "Empty")]
    [InlineData(TransactionState.PrepareEpochFence, "PrepareEpochFence")]
    [InlineData(TransactionState.Unknown, "Unknown")]
    public void TransactionState_RoundTripsThroughItsWireName(TransactionState state, string wireName)
    {
        Assert.Equal(wireName, TransactionMarshal.WireName(state));
        Assert.Equal(state, TransactionDescriptionMarshal.StateFrom(TransactionMarshal.Singleton(state)));
    }

    [Fact]
    public void TransactionState_WireNamesAreDistinct()
    {
        string[] wireNames = Enum.GetValues(typeof(TransactionState))
            .Cast<TransactionState>()
            .Select(TransactionMarshal.WireName)
            .ToArray();

        Assert.Equal(8, wireNames.Length);
        Assert.Equal(wireNames.Length, wireNames.Distinct(StringComparer.Ordinal).Count());
    }
}
