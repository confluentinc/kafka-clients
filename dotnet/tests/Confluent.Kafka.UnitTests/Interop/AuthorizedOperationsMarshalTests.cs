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
using System.Linq;

using Confluent.Kafka.Common.Acl;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The authorized-operations decode reads each code through <c>AclOperation.FromCode</c>: a byte code keeps
/// that table's answer, and a code outside a byte is <c>UNKNOWN</c>, never the member its low byte names.
/// </summary>
public sealed class AuthorizedOperationsMarshalTests
{
    [Fact]
    public void CopyOut_EveryByteCode_IsFromCodesAnswer()
    {
        int[] codes = Enumerable.Range(0, 256).ToArray();

        IReadOnlyCollection<AclOperation> operations = Decode(codes);

        Assert.Equal(codes.Select(code => AclOperation.FromCode((byte)code)), operations);
    }

    [Theory]
    [InlineData(0, AclOperation.UNKNOWN)]
    [InlineData(1, AclOperation.ANY)]
    [InlineData(15, AclOperation.TWO_PHASE_COMMIT)]
    [InlineData(16, AclOperation.UNKNOWN)]
    [InlineData(255, AclOperation.UNKNOWN)]
    [InlineData(256, AclOperation.UNKNOWN)]
    [InlineData(259, AclOperation.UNKNOWN)]
    [InlineData(271, AclOperation.UNKNOWN)]
    [InlineData(-1, AclOperation.UNKNOWN)]
    [InlineData(-253, AclOperation.UNKNOWN)]
    [InlineData(int.MaxValue, AclOperation.UNKNOWN)]
    [InlineData(int.MinValue, AclOperation.UNKNOWN)]
    public void CopyOut_ACode_IsItsMemberOrUnknown(int code, AclOperation expected)
    {
        Assert.Equal(expected, Assert.Single(Decode(new[] { code })));
    }

    [Fact]
    public void CopyOut_JavasNullSet_IsNull()
    {
        Assert.Null(AuthorizedOperationsMarshal.CopyOut(IntPtr.Zero, _ => false, _ => 0, (_, _) => 0));
    }

    private static IReadOnlyCollection<AclOperation> Decode(int[] codes) =>
        AuthorizedOperationsMarshal.CopyOut(IntPtr.Zero, _ => true, _ => codes.Length, (_, index) => codes[index])!;
}
