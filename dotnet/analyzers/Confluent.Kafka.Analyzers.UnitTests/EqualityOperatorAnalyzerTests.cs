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

using Verifier = Confluent.Kafka.Analyzers.UnitTests.AnalyzerVerifier<Confluent.Kafka.Analyzers.EqualityOperatorAnalyzer>;

namespace Confluent.Kafka.Analyzers.UnitTests;

/// <summary>
/// CKRDA0018 reports each <c>operator ==</c> and <c>operator !=</c> of a type visible outside its assembly, a record's
/// compiler-made pair included, and no other operator and no type that is not visible.
/// </summary>
public sealed class EqualityOperatorAnalyzerTests
{
    private const string Advice =
        "compares references as Java's does, and compare values with Equals, Java's equals";

    [Fact]
    public Task AClassWithEqualsAndNoOperator_IsNotReported() =>
        Verifier.VerifyAsync("""
            public sealed class Node
            {
                public override bool Equals(object obj) => obj is Node;

                public override int GetHashCode() => 0;
            }
            """);

    [Fact]
    public Task EachEqualityOperatorOfAPublicClass_IsReported() =>
        Verifier.VerifyAsync(
            """
            public sealed class Node
            {
                public static bool operator {|#0:==|}(Node left, Node right) => Equals(left, right);

                public static bool operator {|#1:!=|}(Node left, Node right) => !Equals(left, right);

                public override bool Equals(object obj) => obj is Node;

                public override int GetHashCode() => 0;
            }
            """,
            Finding(0, "Node declares operator ==; remove it, so == " + Advice),
            Finding(1, "Node declares operator !=; remove it, so != " + Advice));

    [Fact]
    public Task AnEqualityOperatorComparingReferences_IsReportedToo() =>
        Verifier.VerifyAsync(
            """
            public abstract class Future<T>
            {
                public static bool operator {|#0:==|}(Future<T> left, Future<T> right) => ReferenceEquals(left, right);

                public static bool operator {|#1:!=|}(Future<T> left, Future<T> right) => !ReferenceEquals(left, right);

                public override bool Equals(object obj) => ReferenceEquals(this, obj);

                public override int GetHashCode() => 0;
            }
            """,
            Finding(0, "Future<T> declares operator ==; remove it, so == " + Advice),
            Finding(1, "Future<T> declares operator !=; remove it, so != " + Advice));

    [Fact]
    public Task AnEqualityOperatorOfAPublicStruct_IsReported() =>
        Verifier.VerifyAsync(
            """
            public readonly struct Offset
            {
                public static bool operator {|#0:==|}(Offset left, Offset right) => true;

                public static bool operator {|#1:!=|}(Offset left, Offset right) => false;

                public override bool Equals(object obj) => obj is Offset;

                public override int GetHashCode() => 0;
            }
            """,
            Finding(0, "Offset declares operator ==; remove it, so == " + Advice),
            Finding(1, "Offset declares operator !=; remove it, so != " + Advice));

    [Fact]
    public Task AnEqualityOperatorOfANestedPublicOrProtectedClass_IsReported_WithItsOuterName() =>
        Verifier.VerifyAsync(
            """
            public class Outer
            {
                public sealed class Inner
                {
                    public static bool operator {|#0:==|}(Inner left, Inner right) => true;

                    public static bool operator {|#1:!=|}(Inner left, Inner right) => false;

                    public override bool Equals(object obj) => obj is Inner;

                    public override int GetHashCode() => 0;
                }

                protected sealed class Hidden
                {
                    public static bool operator {|#2:==|}(Hidden left, Hidden right) => true;

                    public static bool operator {|#3:!=|}(Hidden left, Hidden right) => false;

                    public override bool Equals(object obj) => obj is Hidden;

                    public override int GetHashCode() => 0;
                }
            }
            """,
            Finding(0, "Outer.Inner declares operator ==; remove it, so == " + Advice),
            Finding(1, "Outer.Inner declares operator !=; remove it, so != " + Advice),
            Finding(2, "Outer.Hidden declares operator ==; remove it, so == " + Advice),
            Finding(3, "Outer.Hidden declares operator !=; remove it, so != " + Advice));

    [Fact]
    public Task APublicRecord_IsReported_ForTheOperatorsTheCompilerGivesIt() =>
        Verifier.VerifyAsync(
            """
            public sealed record {|#0:Spec|}(string Topic, int Partition);

            public readonly record struct {|#1:Pair|}(int Left, int Right);
            """,
            Finding(0, "Spec is a record, whose compiler-made operator == compares values; make it a class, so == " + Advice),
            Finding(0, "Spec is a record, whose compiler-made operator != compares values; make it a class, so != " + Advice),
            Finding(1, "Pair is a record, whose compiler-made operator == compares values; make it a class, so == " + Advice),
            Finding(1, "Pair is a record, whose compiler-made operator != compares values; make it a class, so != " + Advice));

    [Fact]
    public Task AnInterfacesStaticAbstractEqualityOperator_IsReported() =>
        Verifier.VerifyAsync(
            """
            public interface IValue<T> where T : IValue<T>
            {
                static abstract bool operator {|#0:==|}(T left, T right);

                static abstract bool operator {|#1:!=|}(T left, T right);
            }
            """,
            Finding(0, "IValue<T> declares operator ==; remove it, so == " + Advice),
            Finding(1, "IValue<T> declares operator !=; remove it, so != " + Advice));

    [Fact]
    public Task AnyOtherOperatorOrConversion_IsNotReported() =>
        Verifier.VerifyAsync("""
            public sealed class Amount
            {
                public static Amount operator +(Amount left, Amount right) => left;

                public static bool operator <(Amount left, Amount right) => false;

                public static bool operator >(Amount left, Amount right) => false;

                public static implicit operator long(Amount amount) => 0;
            }
            """);

    [Theory]
    [InlineData("internal sealed class Node")]
    [InlineData("sealed class Node")]
    [InlineData("public class Outer { private sealed class Node")]
    [InlineData("public class Outer { internal sealed class Node")]
    [InlineData("internal class Outer { public sealed class Node")]
    [InlineData("public class Outer { private protected sealed class Node")]
    public Task AnEqualityOperatorOfATypeNotVisibleOutsideItsAssembly_IsNotReported(string declaration)
    {
        string close = declaration.Contains("Outer") ? "} }" : "}";
        return Verifier.VerifyAsync(
            declaration
            + " { public static bool operator ==(Node left, Node right) => true;"
            + " public static bool operator !=(Node left, Node right) => false;"
            + " public override bool Equals(object obj) => obj is Node;"
            + " public override int GetHashCode() => 0; "
            + close);
    }

    private static DiagnosticResult Finding(int location, string message) =>
        Verifier.Diagnostic(DiagnosticIds.EqualityOperator).WithLocation(location).WithMessage(message);
}
