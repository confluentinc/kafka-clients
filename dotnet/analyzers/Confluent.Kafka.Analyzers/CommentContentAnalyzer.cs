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

using System.Collections.Immutable;
using System.Text.RegularExpressions;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Text;

namespace Confluent.Kafka.Analyzers;

/// <summary>
/// Checks comments, documentation comments included, for what the comment rules keep out of them: references to a
/// Java or Python source line (CKRDA0011) and phase or review ids (CKRDA0012).
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class CommentContentAnalyzer : DiagnosticAnalyzer
{
    private static readonly Regex s_sourceLine = new Regex(
        @"[\w$./-]*\w\.(?:java|py):\d+",
        RegexOptions.CultureInvariant);

    // M11/P3.5, M5/P8a, Critic 42, D2, D-6. A decision id is one or two digits after a D that no word character or
    // '+' precedes, so U+D800 and 0xD2 do not match.
    private static readonly Regex s_phaseOrReviewId = new Regex(
        @"\bM\d+/P\d+(?:\.\d+)*[a-z]?\b|\bCritic \d+\b|(?<![\w+])D-?\d{1,2}\b",
        RegexOptions.CultureInvariant);

    private static readonly DiagnosticDescriptor s_sourceLineRule = new DiagnosticDescriptor(
        DiagnosticIds.SourceLineReference,
        title: "A comment names a class or method, not a Java or Python source line",
        messageFormat: "'{0}' refers to a Java or Python source line; name the class or method instead",
        category: "Documentation",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "Line numbers change with every upstream change and cause needless edits.");

    private static readonly DiagnosticDescriptor s_phaseOrReviewIdRule = new DiagnosticDescriptor(
        DiagnosticIds.PhaseOrReviewId,
        title: "A comment holds no phase or review id",
        messageFormat: "'{0}' is a phase or review id; the design history holds those",
        category: "Documentation",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "A comment states a decision, not its history: no phase ids (M11/P3.5), decision ids (D2) or "
            + "review ids (Critic 42).");

    /// <inheritdoc/>
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        ImmutableArray.Create(s_sourceLineRule, s_phaseOrReviewIdRule);

    /// <inheritdoc/>
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSyntaxTreeAction(AnalyzeTree);
    }

    private static void AnalyzeTree(SyntaxTreeAnalysisContext context)
    {
        foreach (SyntaxTrivia trivia in context.Tree.GetRoot(context.CancellationToken).DescendantTrivia())
        {
            if (!trivia.IsKind(SyntaxKind.SingleLineCommentTrivia)
                && !trivia.IsKind(SyntaxKind.MultiLineCommentTrivia)
                && !trivia.IsKind(SyntaxKind.SingleLineDocumentationCommentTrivia)
                && !trivia.IsKind(SyntaxKind.MultiLineDocumentationCommentTrivia))
            {
                continue;
            }

            string text = trivia.ToFullString();
            Report(context, s_sourceLineRule, s_sourceLine, text, trivia.FullSpan.Start);
            Report(context, s_phaseOrReviewIdRule, s_phaseOrReviewId, text, trivia.FullSpan.Start);
        }
    }

    private static void Report(SyntaxTreeAnalysisContext context, DiagnosticDescriptor rule, Regex pattern, string text, int start)
    {
        foreach (Match match in pattern.Matches(text))
        {
            Location location = Location.Create(context.Tree, new TextSpan(start + match.Index, match.Length));
            context.ReportDiagnostic(Diagnostic.Create(rule, location, match.Value));
        }
    }
}
