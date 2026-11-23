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

using Verifier = Confluent.Kafka.Analyzers.UnitTests.AnalyzerVerifier<Confluent.Kafka.Analyzers.EnumValuesListAnalyzer>;

namespace Confluent.Kafka.Analyzers.UnitTests;

/// <summary>
/// CKRDA0015 reports an <c>&lt;Enum&gt;Methods</c> class's <c>s_values</c> or <c>VALUES</c> that misses a member,
/// lists one twice, lists them out of declaration order or does not write them out, and no other field.
/// </summary>
public sealed class EnumValuesListAnalyzerTests
{
    [Theory]
    [InlineData("internal static readonly E[] s_values = { E.A, E.B, E.C };")]
    [InlineData("internal static readonly E[] s_values = new[] { E.A, E.B, E.C };")]
    [InlineData("internal static readonly E[] s_values = [E.A, E.B, E.C];")]
    [InlineData("public static readonly IReadOnlyList<E> VALUES = new ReadOnlyCollection<E>(new[] { E.A, E.B, E.C });")]
    [InlineData("public static readonly IReadOnlyList<E> VALUES = [E.A, E.B, E.C];")]
    public Task EveryMemberOnceInDeclarationOrder_IsNotReported(string field) =>
        Verifier.VerifyAsync(Methods(field));

    [Fact]
    public Task AMissingMember_IsReported() =>
        Verifier.VerifyAsync(
            Methods("internal static readonly E[] {|#0:s_values|} = { E.A, E.C };"),
            Finding(0, "EMethods.s_values does not list E.B"));

    [Fact]
    public Task EachMissingMember_IsReported() =>
        Verifier.VerifyAsync(
            Methods("public static readonly IReadOnlyList<E> {|#0:VALUES|} = new ReadOnlyCollection<E>(new[] { E.B });"),
            Finding(0, "EMethods.VALUES does not list E.A"),
            Finding(0, "EMethods.VALUES does not list E.C"));

    [Fact]
    public Task AMemberListedTwice_IsReported() =>
        Verifier.VerifyAsync(
            Methods("internal static readonly E[] s_values = { E.A, E.B, {|#0:E.B|}, E.C };"),
            Finding(0, "EMethods.s_values lists E.B twice"));

    [Fact]
    public Task AMemberOutOfDeclarationOrder_IsReported() =>
        Verifier.VerifyAsync(
            Methods("internal static readonly E[] s_values = { E.C, {|#0:E.A|}, E.B };"),
            Finding(0, "EMethods.s_values lists E.C before E.A; list the members in declaration order"));

    [Theory]
    [InlineData("(E)1")]
    [InlineData("Pick()")]
    public Task AnElementThatIsNoMember_IsReported(string element) =>
        Verifier.VerifyAsync(
            Methods($$"""
                internal static readonly E[] {|#1:s_values|} = { E.A, {|#0:{{element}}|}, E.C };

                private static E Pick() => E.B;
                """),
            Finding(0, $"write E.<NAME> in EMethods.s_values, not '{element}'"),
            Finding(1, "EMethods.s_values does not list E.B"));

    [Theory]
    [InlineData("internal static readonly E[] {|#0:s_values|} = Build();\n\nprivate static E[] Build() => new E[3];")]
    [InlineData("internal static readonly E[] {|#0:s_values|} = new E[3];")]
    [InlineData("internal static readonly E[] {|#0:s_values|};\n\nstatic EMethods() => s_values = new E[3];")]
    public Task AListThatIsNotWrittenOut_IsReported(string field) =>
        Verifier.VerifyAsync(
            Methods(field),
            Finding(0, "EMethods.s_values must list the members of E written out, as E.<NAME>"));

    [Theory]
    [InlineData("EMethods", "internal static readonly E[] s_wakeable = { E.B };")]
    [InlineData("EMethods", "internal static readonly int[] s_values = { 1 };")]
    [InlineData("EMethods", "internal readonly E[] s_values = { E.B };")]
    [InlineData("Other", "internal static readonly E[] s_values = { E.B };")]
    public Task AnyOtherField_IsNotChecked(string className, string field) =>
        Verifier.VerifyAsync(Methods(field, className));

    private static DiagnosticResult Finding(int location, string message) =>
        Verifier.Diagnostic(DiagnosticIds.EnumValuesList).WithLocation(location).WithMessage(message);

    private static string Methods(string members, string className = "EMethods") =>
        $$"""
        using System.Collections.Generic;
        using System.Collections.ObjectModel;

        public enum E
        {
            A,
            B,
            C,
        }

        public class {{className}}
        {
            {{members}}
        }
        """;
}
