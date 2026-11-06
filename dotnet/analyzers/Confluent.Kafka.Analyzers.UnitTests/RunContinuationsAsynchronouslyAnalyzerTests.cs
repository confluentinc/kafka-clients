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

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Testing;
using Microsoft.CodeAnalysis.Testing;

using Xunit;

namespace Confluent.Kafka.Analyzers.UnitTests;

/// <summary>
/// CKRDA0001 reports a <c>TaskCompletionSource</c> created without
/// <c>TaskCreationOptions.RunContinuationsAsynchronously</c> as a constant argument, and nothing else.
/// </summary>
public sealed class RunContinuationsAsynchronouslyAnalyzerTests
{
    [Fact]
    public void TheRule_IsCkrda0001_AWarning_EnabledByDefault()
    {
        DiagnosticDescriptor rule = Assert.Single(new RunContinuationsAsynchronouslyAnalyzer().SupportedDiagnostics);

        Assert.Equal("CKRDA0001", rule.Id);
        Assert.Equal(DiagnosticSeverity.Warning, rule.DefaultSeverity);
        Assert.True(rule.IsEnabledByDefault);
    }

    [Fact]
    public Task TheDiagnostic_NamesTheTypeAndTheOption() =>
        VerifyAsync(
            Expression("object", "{|#0:new TaskCompletionSource<long>()|}"),
            CSharpAnalyzerVerifier<RunContinuationsAsynchronouslyAnalyzer, DefaultVerifier>.Diagnostic()
                .WithLocation(0)
                .WithMessage(
                    "'TaskCompletionSource<long>' is created without TaskCreationOptions.RunContinuationsAsynchronously, "
                        + "so a continuation can run inline on the thread that completes it"));

    [Theory]
    [InlineData("new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously)")]
    [InlineData("new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)")]
    [InlineData("new TaskCompletionSource<int>(new object(), TaskCreationOptions.RunContinuationsAsynchronously)")]
    [InlineData("new TaskCompletionSource<int>(TaskCreationOptions.AttachedToParent | TaskCreationOptions.RunContinuationsAsynchronously)")]
    [InlineData("new TaskCompletionSource<int>(creationOptions: TaskCreationOptions.RunContinuationsAsynchronously)")]
    public Task ACreation_WithTheConstantOption_IsNotReported(string creation) =>
        VerifyAsync(Expression("object", creation));

    [Fact]
    public Task ATargetTypedCreation_WithTheConstantOption_IsNotReported() =>
        VerifyAsync(Expression("TaskCompletionSource<int>", "new(TaskCreationOptions.RunContinuationsAsynchronously)"));

    [Theory]
    [InlineData("new TaskCompletionSource<int>()", "TaskCompletionSource<int>")]
    [InlineData("new TaskCompletionSource()", "TaskCompletionSource")]
    [InlineData("new TaskCompletionSource<int>(TaskCreationOptions.None)", "TaskCompletionSource<int>")]
    [InlineData("new TaskCompletionSource<int>(TaskCreationOptions.AttachedToParent)", "TaskCompletionSource<int>")]
    [InlineData("new TaskCompletionSource<int>(new object())", "TaskCompletionSource<int>")]
    [InlineData("new TaskCompletionSource<int>((object)TaskCreationOptions.RunContinuationsAsynchronously)", "TaskCompletionSource<int>")]
    [InlineData("new TaskCompletionSource<string>(new object(), TaskCreationOptions.None)", "TaskCompletionSource<string>")]
    public Task ACreation_WithoutTheConstantOption_IsReported(string creation, string type) =>
        VerifyAsync(
            Expression("object", "{|#0:" + creation + "|}"),
            CSharpAnalyzerVerifier<RunContinuationsAsynchronouslyAnalyzer, DefaultVerifier>.Diagnostic()
                .WithLocation(0)
                .WithArguments(type));

    [Fact]
    public Task ATargetTypedCreation_WithoutTheOption_IsReported() =>
        VerifyAsync(
            Expression("TaskCompletionSource<int>", "{|#0:new()|}"),
            CSharpAnalyzerVerifier<RunContinuationsAsynchronouslyAnalyzer, DefaultVerifier>.Diagnostic()
                .WithLocation(0)
                .WithArguments("TaskCompletionSource<int>"));

    [Fact]
    public Task AnOptionThatIsNotAConstant_IsReported() =>
        VerifyAsync(
            """
            using System.Threading.Tasks;

            class C
            {
                object M(TaskCreationOptions options) => {|#0:new TaskCompletionSource<int>(options)|};
            }
            """,
            CSharpAnalyzerVerifier<RunContinuationsAsynchronouslyAnalyzer, DefaultVerifier>.Diagnostic()
                .WithLocation(0)
                .WithArguments("TaskCompletionSource<int>"));

    [Theory]
    [InlineData("internal Source() : {|#0:base()|} { }")]
    [InlineData("internal Source() : {|#0:base(TaskCreationOptions.None)|} { }")]
    [InlineData("internal {|#0:Source|}() { }")]
    [InlineData("internal Source() : base(TaskCreationOptions.RunContinuationsAsynchronously) { }\ninternal {|#0:Source|}(int value) { }")]
    public Task ADerivedSourceWhoseBaseCallLacksTheOption_IsReported(string constructors) =>
        VerifyAsync(
            Derived(constructors),
            CSharpAnalyzerVerifier<RunContinuationsAsynchronouslyAnalyzer, DefaultVerifier>.Diagnostic()
                .WithLocation(0)
                .WithArguments("TaskCompletionSource<int>"));

    [Fact]
    public Task ADerivedSourceWithoutAConstructor_IsReported() =>
        VerifyAsync(
            """
            using System.Threading.Tasks;

            class {|#0:Source|} : TaskCompletionSource<int>
            {
            }
            """,
            CSharpAnalyzerVerifier<RunContinuationsAsynchronouslyAnalyzer, DefaultVerifier>.Diagnostic()
                .WithLocation(0)
                .WithArguments("TaskCompletionSource<int>"));

    [Theory]
    [InlineData("internal Source() : base(TaskCreationOptions.RunContinuationsAsynchronously) { }")]
    [InlineData("internal Source() : base(TaskCreationOptions.RunContinuationsAsynchronously) { }\ninternal Source(int value) : this() { }")]
    public Task ADerivedSourceWhoseBaseCallsPassTheOption_IsNotReported(string constructors) =>
        VerifyAsync(Derived(constructors) + "\nclass User\n{\n    object M() => new Source();\n}\n");

    [Theory]
    [InlineData("class {|#0:Source|}(int value) : TaskCompletionSource<int>")]
    [InlineData("class Source(int value) : {|#0:TaskCompletionSource<int>(TaskCreationOptions.None)|}")]
    [InlineData("class Source(int value) : {|#0:TaskCompletionSource<int>()|}")]
    public Task APrimaryConstructorThatCallsBaseWithoutTheOption_IsReported(string declaration) =>
        VerifyAsync(
            Primary(declaration),
            CSharpAnalyzerVerifier<RunContinuationsAsynchronouslyAnalyzer, DefaultVerifier>.Diagnostic()
                .WithLocation(0)
                .WithArguments("TaskCompletionSource<int>"));

    [Fact]
    public Task APrimaryConstructorThatPassesTheOption_IsNotReported() =>
        VerifyAsync(Primary("class Source(int value) : TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously)"));

    [Fact]
    public Task ATypeOfTheSameNameInAnotherNamespace_IsNotReported() =>
        VerifyAsync(
            """
            namespace Other
            {
                class TaskCompletionSource<T> { }

                class C
                {
                    object M() => new TaskCompletionSource<int>();
                }
            }
            """);

    private static string Primary(string declaration) =>
        $$"""
        using System.Threading.Tasks;

        {{declaration}}
        {
            internal int Value => value;
        }
        """;

    private static string Derived(string constructors) =>
        $$"""
        using System.Threading.Tasks;

        class Source : TaskCompletionSource<int>
        {
            {{constructors}}
        }
        """;

    private static string Expression(string returnType, string creation) =>
        $$"""
        using System.Threading.Tasks;

        class C
        {
            {{returnType}} M() => {{creation}};
        }
        """;

    private static Task VerifyAsync(string source, params DiagnosticResult[] expected)
    {
        CSharpAnalyzerTest<RunContinuationsAsynchronouslyAnalyzer, DefaultVerifier> test =
            new CSharpAnalyzerTest<RunContinuationsAsynchronouslyAnalyzer, DefaultVerifier>
            {
                TestCode = source,
                ReferenceAssemblies = ReferenceAssemblies.Net.Net80,
            };
        test.ExpectedDiagnostics.AddRange(expected);
        return test.RunAsync();
    }
}
