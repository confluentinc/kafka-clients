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
using System.Globalization;

using Confluent.Kafka.Admin;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The <see cref="TransactionState"/> part of Python's <c>test_enums</c> on <see cref="TransactionStateMethods"/>,
/// under Java's names; then Java's text of every constant and its round trip, the exact match Java's <c>parse</c>
/// makes, the text of an undefined value, and the form older C# calls.
/// </summary>
public sealed class PublicTransactionStateTests
{
    private static readonly (TransactionState State, string Text)[] s_texts =
    {
        (TransactionState.ONGOING, "Ongoing"),
        (TransactionState.PREPARE_ABORT, "PrepareAbort"),
        (TransactionState.PREPARE_COMMIT, "PrepareCommit"),
        (TransactionState.COMPLETE_ABORT, "CompleteAbort"),
        (TransactionState.COMPLETE_COMMIT, "CompleteCommit"),
        (TransactionState.EMPTY, "Empty"),
        (TransactionState.PREPARE_EPOCH_FENCE, "PrepareEpochFence"),
        (TransactionState.UNKNOWN, "Unknown"),
    };

    [Fact]
    public void AdminValueTypes_test_enums_TransactionStatesTextAndParse()
    {
        Assert.Equal("PrepareEpochFence", TransactionState.ToString(TransactionState.PREPARE_EPOCH_FENCE));
        Assert.Equal(TransactionState.COMPLETE_COMMIT, TransactionState.Parse("CompleteCommit"));
        Assert.Equal(TransactionState.UNKNOWN, TransactionState.Parse("nope"));
        Assert.Equal("ONGOING", TransactionState.ONGOING.ToString());
    }

    [Fact]
    public void ToString_IsJavasText_AndParseRoundTripsIt_ForEveryConstant()
    {
        Assert.Equal(Enum.GetValues(typeof(TransactionState)).Length, s_texts.Length);
        foreach ((TransactionState state, string text) in s_texts)
        {
            Assert.Equal(text, TransactionState.ToString(state));
            Assert.Equal(state, TransactionState.Parse(TransactionState.ToString(state)));
        }
    }

    [Fact]
    public void ToString_OfTheEnumValue_IsStillTheConstantName()
    {
        Assert.Equal("PREPARE_ABORT", TransactionState.PREPARE_ABORT.ToString());
        Assert.Equal("PREPARE_EPOCH_FENCE", $"{TransactionState.PREPARE_EPOCH_FENCE}");
        Assert.Equal("PrepareAbort", TransactionState.ToString(TransactionState.PREPARE_ABORT));
    }

    [Fact]
    public void Parse_OfNull_IsUnknown() =>
        Assert.Equal(TransactionState.UNKNOWN, TransactionState.Parse(null!));

    [Theory]
    [InlineData("completecommit")]
    [InlineData("COMPLETECOMMIT")]
    [InlineData("COMPLETE_COMMIT")]
    [InlineData("PREPARE_ABORT")]
    [InlineData(" Ongoing")]
    [InlineData("Ongoing ")]
    [InlineData("0")]
    [InlineData("Ongoing, Empty")]
    [InlineData("Dead")]
    [InlineData("")]
    public void Parse_IsUnknownForAnythingButJavasExactText(string name) =>
        Assert.Equal(TransactionState.UNKNOWN, TransactionState.Parse(name));

    [Theory]
    [InlineData(99, "99")]
    [InlineData(-1, "-1")]
    public void ToString_OfAnUndefinedValue_IsItsNumberInTheInvariantCulture(int value, string text)
    {
        CultureInfo previous = CultureInfo.CurrentCulture;
        var minus = (CultureInfo)CultureInfo.GetCultureInfo("sv-SE").Clone();
        minus.NumberFormat.NegativeSign = "\u2212";
        CultureInfo.CurrentCulture = minus;
        try
        {
            Assert.NotEqual("-1", (-1).ToString(CultureInfo.CurrentCulture));

            Assert.Equal(text, TransactionState.ToString((TransactionState)value));
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    [Fact]
    public void TheMethods_AreCalledOnTheMethodsClassToo()
    {
        Assert.Equal(TransactionState.EMPTY, TransactionStateMethods.Parse("Empty"));
        Assert.Equal(TransactionState.UNKNOWN, TransactionStateMethods.Parse(null!));
        Assert.Equal("Empty", TransactionStateMethods.ToString(TransactionState.EMPTY));
    }
}
