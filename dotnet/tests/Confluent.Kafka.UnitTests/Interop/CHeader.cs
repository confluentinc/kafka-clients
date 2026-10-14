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
using System.Text;
using System.Text.RegularExpressions;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The parts of the generated C header (<c>confluent_kafka.h</c>) a P/Invoke has to agree with:
/// the function prototypes, the callback typedefs, the enums and the structs.
/// </summary>
/// <remarks>
/// The header is cbindgen output (<c>rust/cbindgen.toml</c>, <c>style = "Type"</c>), so it has
/// four statement shapes: <c>typedef struct { … } name;</c> (opaque when the body is only
/// <c>uint8_t _private[0]</c>), <c>typedef enum { … } name;</c>, <c>typedef ret (*name)(…);</c>
/// and a function prototype. The parser is strict: a statement or declaration it cannot read
/// throws <see cref="FormatException"/> instead of being skipped, so a new header shape cannot
/// hide a function from <see cref="NativeAbiConformanceTests"/>.
/// </remarks>
internal sealed class CHeader
{
    private static readonly Regex s_struct = new Regex(@"^typedef struct \{(?<body>.*)\} (?<name>\w+)$", RegexOptions.Singleline);
    private static readonly Regex s_enum = new Regex(@"^typedef enum \{.*\} (?<name>\w+)$", RegexOptions.Singleline);
    private static readonly Regex s_callback = new Regex(@"^typedef (?<ret>[^(]+?) ?\( ?\* ?(?<name>\w+) ?\) ?\((?<params>.*)\)$", RegexOptions.Singleline);
    private static readonly Regex s_function = new Regex(@"^(?<ret>[^(]*?)\b(?<name>\w+) ?\((?<params>.*)\)$", RegexOptions.Singleline);
    private static readonly Regex s_functionPointer = new Regex(@"^(?<ret>[^(]+?) ?\( ?\* ?(?<name>\w*) ?\) ?\((?<params>.*)\)$", RegexOptions.Singleline);
    private static readonly Regex s_identifier = new Regex(@"^[A-Za-z_]\w*$");

    private CHeader(
        Dictionary<string, CSignature> functions,
        Dictionary<string, CSignature> callbacks,
        Dictionary<string, IReadOnlyList<CField>> structs,
        HashSet<string> opaqueStructs,
        HashSet<string> enums)
    {
        Functions = functions;
        Callbacks = callbacks;
        Structs = structs;
        OpaqueStructs = opaqueStructs;
        Enums = enums;
    }

    /// <summary>Every function prototype, by name.</summary>
    internal IReadOnlyDictionary<string, CSignature> Functions { get; }

    /// <summary>Every <c>typedef ret (*name)(…)</c>, by typedef name.</summary>
    internal IReadOnlyDictionary<string, CSignature> Callbacks { get; }

    /// <summary>Every struct with fields (a transparent, by-value ABI struct), by typedef name.</summary>
    internal IReadOnlyDictionary<string, IReadOnlyList<CField>> Structs { get; }

    /// <summary>Every opaque struct (<c>uint8_t _private[0]</c>), by typedef name.</summary>
    internal HashSet<string> OpaqueStructs { get; }

    /// <summary>Every enum typedef name. A C enum is <c>int</c>-sized.</summary>
    internal HashSet<string> Enums { get; }

    /// <summary>Parses the header text.</summary>
    /// <exception cref="FormatException">A statement or declaration has a shape the parser does not know.</exception>
    internal static CHeader Parse(string text)
    {
        Dictionary<string, CSignature> functions = new Dictionary<string, CSignature>(StringComparer.Ordinal);
        Dictionary<string, CSignature> callbacks = new Dictionary<string, CSignature>(StringComparer.Ordinal);
        Dictionary<string, IReadOnlyList<CField>> structs = new Dictionary<string, IReadOnlyList<CField>>(StringComparer.Ordinal);
        HashSet<string> opaqueStructs = new HashSet<string>(StringComparer.Ordinal);
        HashSet<string> enums = new HashSet<string>(StringComparer.Ordinal);

        foreach (string statement in Statements(StripComments(text)))
        {
            if (statement.StartsWith("typedef struct", StringComparison.Ordinal))
            {
                Match match = Require(s_struct, statement);
                string name = match.Groups["name"].Value;
                string body = match.Groups["body"].Value;
                if (body.Contains("_private[0]"))
                {
                    opaqueStructs.Add(name);
                }
                else
                {
                    structs.Add(name, body
                        .Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries)
                        .Where(field => field.Trim().Length > 0)
                        .Select(field => ParseField(field, name))
                        .ToList());
                }
            }
            else if (statement.StartsWith("typedef enum", StringComparison.Ordinal))
            {
                enums.Add(Require(s_enum, statement).Groups["name"].Value);
            }
            else if (statement.StartsWith("typedef", StringComparison.Ordinal))
            {
                Match match = Require(s_callback, statement);
                callbacks.Add(match.Groups["name"].Value, Signature(match));
            }
            else
            {
                Match match = Require(s_function, statement);
                functions.Add(match.Groups["name"].Value, Signature(match));
            }
        }

        return new CHeader(functions, callbacks, structs, opaqueStructs, enums);
    }

    /// <summary>
    /// The header with its comments and preprocessor lines removed: what <see cref="Parse"/> reads.
    /// A test that edits the header text edits this form, so a prototype quoted in a doc comment
    /// cannot be matched instead of the real one.
    /// </summary>
    internal static string StripComments(string text)
    {
        string code = Regex.Replace(text, @"/\*.*?\*/", " ", RegexOptions.Singleline);
        code = Regex.Replace(code, @"//[^\n]*", string.Empty);
        return Regex.Replace(code, @"^[ \t]*#[^\n]*", string.Empty, RegexOptions.Multiline);
    }

    /// <summary>The top-level statements: split at every <c>;</c> outside braces, whitespace collapsed.</summary>
    private static IEnumerable<string> Statements(string code)
    {
        StringBuilder current = new StringBuilder();
        int depth = 0;
        foreach (char c in code)
        {
            if (c == '{')
            {
                depth++;
            }
            else if (c == '}')
            {
                depth--;
            }

            if (c == ';' && depth == 0)
            {
                string statement = Regex.Replace(current.ToString(), @"\s+", " ").Trim();
                if (statement.Length > 0)
                {
                    yield return statement;
                }

                current.Clear();
            }
            else
            {
                current.Append(c);
            }
        }

        if (current.ToString().Trim().Length > 0)
        {
            throw new FormatException($"Unterminated statement at the end of the header: '{current.ToString().Trim()}'.");
        }
    }

    private static Match Require(Regex shape, string statement)
    {
        Match match = shape.Match(statement);
        if (!match.Success)
        {
            throw new FormatException($"Unrecognised header statement: '{statement}'.");
        }

        return match;
    }

    private static CSignature Signature(Match match) =>
        new CSignature(ParseDeclaration(match.Groups["ret"].Value).Type, ParseParameters(match.Groups["params"].Value));

    private static List<CParam> ParseParameters(string text)
    {
        string trimmed = text.Trim();
        List<CParam> parameters = new List<CParam>();
        if (trimmed.Length == 0 || trimmed == "void")
        {
            return parameters;
        }

        // Split at the commas outside parentheses: an inline function-pointer parameter has its own list.
        int depth = 0;
        int start = 0;
        for (int i = 0; i < trimmed.Length; i++)
        {
            char c = trimmed[i];
            if (c == '(')
            {
                depth++;
            }
            else if (c == ')')
            {
                depth--;
            }
            else if (c == ',' && depth == 0)
            {
                parameters.Add(ParseDeclaration(trimmed.Substring(start, i - start)));
                start = i + 1;
            }
        }

        parameters.Add(ParseDeclaration(trimmed.Substring(start)));
        return parameters;
    }

    private static CField ParseField(string text, string structName)
    {
        CParam field = ParseDeclaration(text);
        if (field.Name is null)
        {
            throw new FormatException($"A field of {structName} has no name: '{text.Trim()}'.");
        }

        return new CField(field.Name, field.Type);
    }

    /// <summary>
    /// One declaration, with or without a name: <c>const char *const *topics</c>, <c>void*</c>,
    /// or an inline function pointer such as <c>void (*user_data_destroy)(void*)</c>.
    /// </summary>
    private static CParam ParseDeclaration(string text)
    {
        string declaration = text.Trim();
        Match pointer = s_functionPointer.Match(declaration);
        if (pointer.Success)
        {
            string pointerName = pointer.Groups["name"].Value;
            return new CParam(
                pointerName.Length == 0 ? null : pointerName,
                CType.FunctionPointer(Signature(pointer)));
        }

        string[] tokens = declaration.Replace("*", " * ").Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
        int depth = tokens.Count(token => token == "*");
        List<string> words = tokens.Where(token => token != "*" && token != "const" && token != "struct").ToList();
        if (words.Count is < 1 or > 2 || !words.All(word => s_identifier.IsMatch(word)))
        {
            throw new FormatException($"Unrecognised declaration: '{declaration}'.");
        }

        return new CParam(words.Count == 2 ? words[1] : null, new CType(words[0], depth, function: null));
    }
}

/// <summary>A C type: a named base (<c>int32_t</c>, a typedef) under some pointer depth, or an inline function pointer.</summary>
internal sealed class CType
{
    internal CType(string baseName, int depth, CSignature? function)
    {
        Base = baseName;
        Depth = depth;
        Function = function;
    }

    /// <summary>The named type under the pointers; <c>"(*)"</c> for an inline function pointer.</summary>
    internal string Base { get; }

    /// <summary>The number of <c>*</c>s.</summary>
    internal int Depth { get; }

    /// <summary>The signature of an inline function-pointer declaration; <see langword="null"/> otherwise.</summary>
    internal CSignature? Function { get; }

    internal bool IsVoid => Depth == 0 && Function is null && Base == "void";

    internal static CType FunctionPointer(CSignature signature) => new CType("(*)", 0, signature);

    /// <summary>The type one <c>*</c> down.</summary>
    internal CType Pointee() => new CType(Base, Depth - 1, Function);

    public override string ToString() =>
        Function is not null ? $"{Function.Return} (*)({string.Join(", ", Function.Parameters.Select(p => p.Type))})"
        : Depth == 0 ? Base
        : $"{Base} {new string('*', Depth)}";
}

/// <summary>A parameter (or a return type, unnamed): an optional name and a <see cref="CType"/>.</summary>
internal sealed class CParam
{
    internal CParam(string? name, CType type)
    {
        Name = name;
        Type = type;
    }

    internal string? Name { get; }

    internal CType Type { get; }
}

/// <summary>A struct field.</summary>
internal sealed class CField
{
    internal CField(string name, CType type)
    {
        Name = name;
        Type = type;
    }

    internal string Name { get; }

    internal CType Type { get; }
}

/// <summary>A function prototype or callback typedef: the return type and the parameters, in order.</summary>
internal sealed class CSignature
{
    internal CSignature(CType returnType, IReadOnlyList<CParam> parameters)
    {
        Return = returnType;
        Parameters = parameters;
    }

    internal CType Return { get; }

    internal IReadOnlyList<CParam> Parameters { get; }

    public override string ToString() =>
        $"{Return} ({string.Join(", ", Parameters.Select(p => p.Name is null ? p.Type.ToString() : $"{p.Type} {p.Name}"))})";
}
