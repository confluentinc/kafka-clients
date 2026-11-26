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

using Microsoft.CodeAnalysis.Testing;

using Xunit;

using Verifier = Confluent.Kafka.Analyzers.UnitTests.AnalyzerVerifier<Confluent.Kafka.Analyzers.EnumMemberValueAnalyzer>;

namespace Confluent.Kafka.Analyzers.UnitTests;

/// <summary>
/// CKRDA0017 reports each member of an enum visible outside its assembly that has no written number, naming the number
/// to write, and no member of an enum that is not visible.
/// </summary>
public sealed class EnumMemberValueAnalyzerTests
{
    [Fact]
    public Task EveryMemberWrittenOut_IsNotReported() =>
        Verifier.VerifyAsync("public enum E { A = 0, B = 1, C = 9 }");

    [Fact]
    public Task AMemberWithNoWrittenNumber_IsReported() =>
        Verifier.VerifyAsync(
            "public enum E { A = 0, {|#0:B|}, C = 5 }",
            Finding(0, "E.B has no written number; write it out (B = 1), so a member added before it cannot renumber it"));

    [Fact]
    public Task EachMemberWithNoWrittenNumber_IsReported() =>
        Verifier.VerifyAsync(
            "public enum E { {|#0:A|}, {|#1:B|}, C = 7, {|#2:D|} }",
            Finding(0, "E.A has no written number; write it out (A = 0), so a member added before it cannot renumber it"),
            Finding(1, "E.B has no written number; write it out (B = 1), so a member added before it cannot renumber it"),
            Finding(2, "E.D has no written number; write it out (D = 8), so a member added before it cannot renumber it"));

    [Fact]
    public Task TheNumberNamed_FollowsANegativeOneAndTheUnderlyingType() =>
        Verifier.VerifyAsync(
            "public enum E : short { A = -1, {|#0:B|} }",
            Finding(0, "E.B has no written number; write it out (B = 0), so a member added before it cannot renumber it"));

    [Fact]
    public Task AMemberWhoseNumberIsFree_IsToldToKeepIt() =>
        Verifier.VerifyAsync(
            "public enum E { A = 0, B = 1, {|#0:C|}, D = 5 }",
            Finding(0, "E.C has no written number; write it out (C = 2), so a member added before it cannot renumber it"));

    [Fact]
    public Task AMemberWhoseNumberIsTaken_IsToldOneAboveTheHighest() =>
        Verifier.VerifyAsync(
            "public enum Color { RED = 0, {|#0:GREEN|}, BLUE = 1 }",
            Finding(
                0,
                "Color.GREEN has no written number, and 1 is BLUE's; write it out (GREEN = 2), so a member added "
                    + "before it cannot renumber it"));

    [Fact]
    public Task TwoMembersWhoseNumbersAreTaken_AreToldDifferentNumbers() =>
        Verifier.VerifyAsync(
            "public enum E { A = 0, {|#0:X|}, B = 1, {|#1:Y|}, C = 2 }",
            Finding(
                0,
                "E.X has no written number, and 1 is B's; write it out (X = 3), so a member added before it cannot "
                    + "renumber it"),
            Finding(
                1,
                "E.Y has no written number, and 2 is C's; write it out (Y = 4), so a member added before it cannot "
                    + "renumber it"));

    [Fact]
    public Task AMemberWithNoFreeNumberAboveTheHighest_IsToldToPickAFreeOne() =>
        Verifier.VerifyAsync(
            "public enum E : byte { A = 254, {|#0:X|}, B = 255 }",
            Finding(
                0,
                "E.X has no written number, and 255 is B's; write it out with a number no other member has, so a "
                    + "member added before it cannot renumber it"));

    [Fact]
    public Task AMemberOfAPublicEnumInAPublicClass_IsReported() =>
        Verifier.VerifyAsync(
            "public class Outer { public enum E { {|#0:A|} } }",
            Finding(
                0,
                "Outer.E.A has no written number; write it out (A = 0), so a member added before it cannot renumber it"));

    [Fact]
    public Task AMemberOfAProtectedEnumInAPublicClass_IsReported() =>
        Verifier.VerifyAsync(
            "public class Outer { protected enum E { {|#0:A|} } }",
            Finding(
                0,
                "Outer.E.A has no written number; write it out (A = 0), so a member added before it cannot renumber it"));

    [Fact]
    public Task AMemberWhoseNumberOverflowsItsType_IsLeftToTheCompiler() =>
        Verifier.VerifyAsync(
            "public enum E : byte { A = 255, {|#0:B|} }",
            DiagnosticResult.CompilerError("CS0543").WithLocation(0));

    [Fact]
    public Task AMemberAfterOneWhoseNumberDoesNotCompile_IsLeftToTheCompiler() =>
        Verifier.VerifyAsync(
            "public enum E { A = {|#0:Missing|}, B }",
            DiagnosticResult.CompilerError("CS0103").WithLocation(0));

    [Theory]
    [InlineData("internal enum E { A, B }")]
    [InlineData("enum E { A, B }")]
    [InlineData("public class Outer { private enum E { A, B } }")]
    [InlineData("public class Outer { internal enum E { A, B } }")]
    [InlineData("internal class Outer { public enum E { A, B } }")]
    public Task AMemberOfAnEnumNotVisibleOutsideItsAssembly_IsNotReported(string source) =>
        Verifier.VerifyAsync(source);

    private static DiagnosticResult Finding(int location, string message) =>
        Verifier.Diagnostic(DiagnosticIds.EnumMemberValue).WithLocation(location).WithMessage(message);
}
