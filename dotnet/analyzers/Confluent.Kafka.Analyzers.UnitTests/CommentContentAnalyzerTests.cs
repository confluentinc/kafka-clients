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

using Xunit;

using Verifier = Confluent.Kafka.Analyzers.UnitTests.AnalyzerVerifier<Confluent.Kafka.Analyzers.CommentContentAnalyzer>;

namespace Confluent.Kafka.Analyzers.UnitTests;

/// <summary>
/// CKRDA0011 reports a Java or Python source-line reference and CKRDA0012 a phase or review id, in every kind of
/// comment, and nothing that only looks like one.
/// </summary>
public sealed class CommentContentAnalyzerTests
{
    [Theory]
    [InlineData("// As {|#0:KafkaProducer.java:1299|}-1300 does.", "KafkaProducer.java:1299")]
    [InlineData("/* See {|#0:python/confluent_kafka/producer/_base.py:470|}. */", "python/confluent_kafka/producer/_base.py:470")]
    [InlineData("/// <summary>As {|#0:MockProducer.java:364|}.</summary>", "MockProducer.java:364")]
    public Task ASourceLineReference_IsReported(string comment, string reference) =>
        Verifier.VerifyAsync(
            Class(comment),
            Verifier.Diagnostic(DiagnosticIds.SourceLineReference).WithLocation(0)
                .WithMessage($"'{reference}' refers to a Java or Python source line; name the class or method instead"));

    [Theory]
    [InlineData("// {|#0:M11/P3.5|} decided it.", "M11/P3.5")]
    [InlineData("// {|#0:M5/P8a|} decided it.", "M5/P8a")]
    [InlineData("/* {|#0:Critic 42|} found it. */", "Critic 42")]
    [InlineData("/// <summary>Per {|#0:D2|}.</summary>", "D2")]
    [InlineData("// Per {|#0:D-6|}.", "D-6")]
    public Task APhaseOrReviewId_IsReported(string comment, string id) =>
        Verifier.VerifyAsync(
            Class(comment),
            Verifier.Diagnostic(DiagnosticIds.PhaseOrReviewId).WithLocation(0)
                .WithMessage($"'{id}' is a phase or review id; the design history holds those"));

    [Theory]
    [InlineData("// As KafkaProducer#flush does, in KafkaProducer.java.")]
    [InlineData("// An unpaired surrogate (U+D800, 0xD800) is rejected.")]
    [InlineData("// The 2D grid, the CD2 drive, the D800 range and the Critic's view.")]
    [InlineData("// M11 and P3 are not a phase id on their own.")]
    public Task TextThatOnlyLooksLikeOne_IsNotReported(string comment) =>
        Verifier.VerifyAsync(Class(comment));

    [Fact]
    public Task AStringLiteral_IsNotAComment() =>
        Verifier.VerifyAsync("""
            internal static class C
            {
                internal const string Text = "KafkaProducer.java:1299 and M11/P3.5";
            }
            """);

    private static string Class(string comment) =>
        $$"""
        {{comment}}
        internal static class C
        {
        }
        """;
}
