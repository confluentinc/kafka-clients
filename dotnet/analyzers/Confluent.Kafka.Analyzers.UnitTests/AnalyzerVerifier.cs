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

using Microsoft.CodeAnalysis.CSharp.Testing;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Testing;

namespace Confluent.Kafka.Analyzers.UnitTests;

/// <summary>
/// Runs one analyzer over a test source, with any additional files a test gives, compiled against the .NET 8
/// reference assemblies or others a test names, and compares its diagnostics with the expected ones.
/// </summary>
internal static class AnalyzerVerifier<TAnalyzer>
    where TAnalyzer : DiagnosticAnalyzer, new()
{
    internal static DiagnosticResult Diagnostic(string id) =>
        CSharpAnalyzerVerifier<TAnalyzer, DefaultVerifier>.Diagnostic(id);

    internal static Task VerifyAsync(string source, params DiagnosticResult[] expected) =>
        VerifyAsync(ReferenceAssemblies.Net.Net80, source, expected);

    internal static Task VerifyAsync(ReferenceAssemblies references, string source, params DiagnosticResult[] expected)
    {
        CSharpAnalyzerTest<TAnalyzer, DefaultVerifier> test = new CSharpAnalyzerTest<TAnalyzer, DefaultVerifier>
        {
            TestCode = source,
            ReferenceAssemblies = references,
        };
        test.ExpectedDiagnostics.AddRange(expected);
        return test.RunAsync();
    }

    internal static Task VerifyWithFilesAsync(
        string source,
        (string Name, string Content)[] additionalFiles,
        params DiagnosticResult[] expected)
    {
        CSharpAnalyzerTest<TAnalyzer, DefaultVerifier> test = new CSharpAnalyzerTest<TAnalyzer, DefaultVerifier>
        {
            TestCode = source,
            ReferenceAssemblies = ReferenceAssemblies.Net.Net80,
        };
        foreach ((string name, string content) in additionalFiles)
        {
            test.TestState.AdditionalFiles.Add((name, content));
        }

        test.ExpectedDiagnostics.AddRange(expected);
        return test.RunAsync();
    }
}
