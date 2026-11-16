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
using System.Buffers;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Threading;

namespace Confluent.Kafka.Tools;

/// <summary>
/// The tools' standard output, Java's <c>System.out</c>: <c>println</c>, and <c>printJson</c> of an event
/// (<c>timestamp</c>, <c>name</c>, then its fields, as one compact line, or Java's "Bad data" line when they cannot be
/// written as JSON). Each line is written whole and flushed under a re-entrant lock, Java's
/// <c>synchronized (System.out)</c>, and every line is dropped once a write has failed.
/// </summary>
internal sealed class JsonOutput
{
    private static readonly JsonWriterOptions s_options = new JsonWriterOptions
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    private readonly TextWriter _writer;

    private bool _broken;

    internal JsonOutput(TextWriter writer)
    {
        _writer = writer;
    }

    internal object Lock { get; } = new object();

    internal void Print(string name, Action<Utf8JsonWriter>? fields = null)
    {
        string line;
        try
        {
            line = Event(name, fields);
        }
        catch (ArgumentException e)
        {
            line = "Bad data can't be written as json: " + e.Message;
        }

        Println(line);
    }

    internal void Println(string line)
    {
        lock (Lock)
        {
            if (_broken)
            {
                return;
            }

            try
            {
                _writer.Write(line + "\n");
                _writer.Flush();
            }
            catch (IOException)
            {
                _broken = true;
            }
        }
    }

    internal static void WriteNumber(Utf8JsonWriter writer, string propertyName, double value)
    {
        if (!double.IsFinite(value))
        {
            writer.WriteNumber(propertyName, value);
            return;
        }

        string text = value.ToString("R", CultureInfo.InvariantCulture);
        writer.WritePropertyName(propertyName);
        writer.WriteRawValue(text.Contains('.') || text.Contains('E') ? text : text + ".0");
    }

    private static string Event(string name, Action<Utf8JsonWriter>? fields)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer, s_options))
        {
            writer.WriteStartObject();
            writer.WriteNumber("timestamp", Utils.NowMillis());
            writer.WriteString("name", name);
            fields?.Invoke(writer);
            writer.WriteEndObject();
        }

        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }
}

/// <summary>
/// What the tools take from Java's <c>System</c> and <c>org.apache.kafka.common.utils.Utils</c>: the clock, and
/// <c>loadProps</c> (ISO 8859-1, <c>java.util.Properties.load</c>, the values trimmed), where an unreadable file is an
/// <see cref="ArgumentParserException"/> and a malformed <c>\uXXXX</c> an <see cref="IllegalArgumentException"/>.
/// </summary>
internal static class Utils
{
    private const string PropertiesWhitespace = " \t\f";

    internal static long NowMillis() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    internal static Dictionary<string, string> LoadProps(string filename)
    {
        string contents;
        try
        {
            contents = File.ReadAllText(filename, Encoding.Latin1);
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException or ArgumentException)
        {
            throw new ArgumentParserException(e.Message, e);
        }

        return ParseProperties(contents);
    }

    internal static Dictionary<string, string> ParseProperties(string contents)
    {
        var props = new Dictionary<string, string>();
        foreach (string line in LogicalLines(contents))
        {
            (string key, string value) = SplitProperty(line);
            props[key] = value.Trim(PropertiesWhitespace.ToCharArray());
        }

        return props;
    }

    private static List<string> LogicalLines(string contents)
    {
        var lines = new List<string>();
        string? current = null;
        foreach (string natural in contents.Replace("\r\n", "\n").Replace('\r', '\n').Split('\n'))
        {
            string line = natural.TrimStart(PropertiesWhitespace.ToCharArray());
            if (current is null && (line.Length == 0 || line[0] == '#' || line[0] == '!'))
            {
                continue;
            }

            int backslashes = line.Length - line.TrimEnd('\\').Length;
            bool continued = backslashes % 2 == 1;
            string part = continued ? line.Substring(0, line.Length - 1) : line;
            current = current is null ? part : current + part;
            if (!continued)
            {
                lines.Add(current);
                current = null;
            }
        }

        if (current is not null)
        {
            lines.Add(current);
        }

        return lines;
    }

    private static (string Key, string Value) SplitProperty(string line)
    {
        int keyEnd = line.Length;
        int valueStart = line.Length;
        bool hasSeparator = false;
        bool precedingBackslash = false;
        for (int i = 0; i < line.Length; i++)
        {
            char c = line[i];
            if (!precedingBackslash && (c == '=' || c == ':'))
            {
                keyEnd = i;
                valueStart = i + 1;
                hasSeparator = true;
                break;
            }

            if (!precedingBackslash && PropertiesWhitespace.Contains(c))
            {
                keyEnd = i;
                valueStart = i + 1;
                break;
            }

            precedingBackslash = c == '\\' && !precedingBackslash;
        }

        while (valueStart < line.Length)
        {
            char c = line[valueStart];
            if (!PropertiesWhitespace.Contains(c))
            {
                if (hasSeparator || (c != '=' && c != ':'))
                {
                    break;
                }

                hasSeparator = true;
            }

            valueStart++;
        }

        return (Unescape(line.Substring(0, keyEnd)), Unescape(line.Substring(valueStart)));
    }

    private static string Unescape(string text)
    {
        if (!text.Contains('\\'))
        {
            return text;
        }

        var builder = new StringBuilder(text.Length);
        int i = 0;
        while (i < text.Length)
        {
            char c = text[i++];
            if (c != '\\' || i == text.Length)
            {
                builder.Append(c);
                continue;
            }

            c = text[i++];
            if (c == 'u')
            {
                if (i + 4 > text.Length
                    || !int.TryParse(text.AsSpan(i, 4), NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out int code))
                {
                    throw new IllegalArgumentException("Malformed \\uxxxx encoding.");
                }

                builder.Append((char)code);
                i += 4;
            }
            else
            {
                builder.Append(c switch
                {
                    't' => '\t',
                    'r' => '\r',
                    'n' => '\n',
                    'f' => '\f',
                    _ => c,
                });
            }
        }

        return builder.ToString();
    }
}

/// <summary>
/// The tools' own log lines, Python's logging to stderr in its tools layout, at <c>WARNING</c> and above unless
/// <c>VERIFIABLE_CLIENT_LOG_LEVEL</c> names another level.
/// </summary>
internal static class Log
{
    private const int ErrorLevel = 40;

    private static readonly Dictionary<string, int> s_levels = new Dictionary<string, int>
    {
        ["CRITICAL"] = 50,
        ["FATAL"] = 50,
        ["ERROR"] = ErrorLevel,
        ["WARN"] = 30,
        ["WARNING"] = 30,
        ["INFO"] = 20,
        ["DEBUG"] = 10,
        ["NOTSET"] = 0,
    };

    private static int s_level = 30;

    internal static void Configure()
    {
        string name = (Environment.GetEnvironmentVariable("VERIFIABLE_CLIENT_LOG_LEVEL") ?? "WARNING")
            .ToUpperInvariant();
        if (!s_levels.TryGetValue(name, out int level))
        {
            throw new ArgumentException("Unknown level: " + ArgumentParser.Repr(name));
        }

        s_level = level;
    }

    internal static void Error(string logger, string message, Exception exception)
    {
        if (ErrorLevel < s_level)
        {
            return;
        }

        string timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss,fff", CultureInfo.InvariantCulture);
        Console.Error.Write($"[{timestamp}] ERROR {message} ({logger})\n{exception}\n");
    }
}

/// <summary>
/// argparse4j's <c>ArgumentParserException</c>: a command line the parser rejects, or one <c>createFromArgs</c>
/// rejects after parsing.
/// </summary>
internal class ArgumentParserException : Exception
{
    internal ArgumentParserException(string message)
        : base(message)
    {
    }

    internal ArgumentParserException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>argparse4j's <c>HelpScreenException</c>: <c>-h</c> or <c>--help</c>, once the help is printed.</summary>
internal sealed class HelpScreenException : ArgumentParserException
{
    internal HelpScreenException()
        : base("Help screen")
    {
    }
}

/// <summary>argparse4j's <c>Namespace</c>: the value of each argument by its destination.</summary>
internal sealed class Namespace
{
    private readonly Dictionary<string, object?> _values;

    internal Namespace(Dictionary<string, object?> values)
    {
        _values = values;
    }

    internal string? GetString(string dest) => (string?)_values[dest];

    internal int? GetInt(string dest) => (int?)_values[dest];

    internal long? GetLong(string dest) => (long?)_values[dest];
}

/// <summary>
/// The argparse parser the Python tools use (argparse4j, which the Java tools use, is its port), for the forms the
/// tools take: options with a value, required options, a required mutually exclusive group, <c>--opt=value</c>, a
/// value that looks like a negative number, the last repeated option winning, no abbreviations, and
/// <c>-h</c> / <c>--help</c>. Errors throw <see cref="ArgumentParserException"/> with argparse's Python 3.10 texts.
/// </summary>
internal sealed class ArgumentParser
{
    private const int MaxHelpPosition = 24;

    private static readonly Argument s_help = new Argument("-h", "help", null, "show this help message and exit", null, null, null, false, null);

    private readonly string _prog;

    private readonly string _description;

    private readonly List<Argument> _arguments = new List<Argument> { s_help };

    private readonly List<MutuallyExclusiveGroup> _groups = new List<MutuallyExclusiveGroup>();

    internal ArgumentParser(string prog, string description)
    {
        _prog = prog;
        _description = description;
    }

    internal MutuallyExclusiveGroup AddMutuallyExclusiveGroup(string title, string description, bool required)
    {
        var group = new MutuallyExclusiveGroup(title, description, required);
        _groups.Add(group);
        return group;
    }

    internal void AddArgument(
        string flag,
        string? dest = null,
        string? metavar = null,
        string? help = null,
        Type? type = null,
        object? defaultValue = null,
        IReadOnlyList<object>? choices = null,
        bool required = false,
        MutuallyExclusiveGroup? group = null)
    {
        var argument = new Argument(
            flag, dest ?? flag.TrimStart('-').Replace('-', '_'), metavar, help, type, defaultValue, choices, required, group);
        _arguments.Add(argument);
        group?.Arguments.Add(argument);
    }

    internal Namespace ParseArgs(IReadOnlyList<string> args)
    {
        var values = new Dictionary<string, object?>();
        foreach (Argument argument in _arguments)
        {
            if (argument != s_help)
            {
                values[argument.Dest] = argument.DefaultValue;
            }
        }

        int end = IndexOf(args, "--");
        var isOption = new bool[args.Count];
        for (int i = 0; i < end; i++)
        {
            isOption[i] = LooksLikeOption(args[i]);
        }

        var seen = new HashSet<Argument>();
        var extras = new List<string>();
        for (int i = 0; i < args.Count; i++)
        {
            if (!isOption[i])
            {
                extras.Add(args[i]);
                continue;
            }

            (Argument? argument, string? explicitArg, bool singleDash) = Match(args[i]);
            if (argument is null)
            {
                extras.Add(args[i]);
                continue;
            }

            if (argument == s_help)
            {
                CheckHelpArgument(explicitArg, singleDash);
                PrintHelp();
                throw new HelpScreenException();
            }

            string value;
            if (explicitArg is not null)
            {
                value = explicitArg;
            }
            else if (i + 1 < args.Count && i + 1 != end && !isOption[i + 1])
            {
                value = args[++i];
            }
            else
            {
                throw Error(argument, "expected one argument");
            }

            values[argument.Dest] = Convert(argument, value);
            seen.Add(argument);
        }

        var missing = new List<string>();
        foreach (Argument argument in _arguments)
        {
            if (argument.Required && !seen.Contains(argument))
            {
                missing.Add(argument.Flag);
            }
        }

        if (missing.Count > 0)
        {
            throw new ArgumentParserException("the following arguments are required: " + string.Join(", ", missing));
        }

        foreach (MutuallyExclusiveGroup group in _groups)
        {
            if (group.Required && !group.Arguments.Exists(seen.Contains))
            {
                throw new ArgumentParserException(
                    "one of the arguments " + string.Join(" ", group.Arguments.ConvertAll(a => a.Flag)) + " is required");
            }
        }

        if (extras.Count > 0)
        {
            throw new ArgumentParserException("unrecognized arguments: " + string.Join(" ", extras));
        }

        return new Namespace(values);
    }

    internal void PrintHelp() => Console.Out.Write(FormatHelp());

    internal void HandleError(ArgumentParserException e) =>
        Console.Error.Write(FormatUsage() + _prog + ": error: " + e.Message + "\n");

    internal static string Repr(object value)
    {
        if (value is not string text)
        {
            return System.Convert.ToString(value, CultureInfo.InvariantCulture)!;
        }

        char quote = text.Contains('\'') && !text.Contains('"') ? '"' : '\'';
        var builder = new StringBuilder(text.Length + 2).Append(quote);
        foreach (char c in text)
        {
            switch (c)
            {
                case '\\':
                    builder.Append("\\\\");
                    break;
                case '\n':
                    builder.Append("\\n");
                    break;
                case '\r':
                    builder.Append("\\r");
                    break;
                case '\t':
                    builder.Append("\\t");
                    break;
                default:
                    if (c == quote)
                    {
                        builder.Append('\\').Append(c);
                    }
                    else if (c < ' ' || c == '\x7f')
                    {
                        builder.Append("\\x").Append(((int)c).ToString("x2", CultureInfo.InvariantCulture));
                    }
                    else
                    {
                        builder.Append(c);
                    }

                    break;
            }
        }

        return builder.Append(quote).ToString();
    }

    private static int IndexOf(IReadOnlyList<string> args, string value)
    {
        for (int i = 0; i < args.Count; i++)
        {
            if (args[i] == value)
            {
                return i;
            }
        }

        return args.Count;
    }

    private static ArgumentParserException Error(Argument argument, string message) =>
        new ArgumentParserException("argument " + argument.Name + ": " + message);

    private static object Convert(Argument argument, string value)
    {
        object converted = value;
        if (argument.Type == typeof(int) || argument.Type == typeof(long))
        {
            const NumberStyles Styles = NumberStyles.AllowLeadingWhite | NumberStyles.AllowTrailingWhite
                | NumberStyles.AllowLeadingSign;
            if (argument.Type == typeof(int) && int.TryParse(value, Styles, CultureInfo.InvariantCulture, out int i))
            {
                converted = i;
            }
            else if (argument.Type == typeof(long)
                && long.TryParse(value, Styles, CultureInfo.InvariantCulture, out long l))
            {
                converted = l;
            }
            else
            {
                throw Error(argument, "invalid int value: " + Repr(value));
            }
        }

        if (argument.Choices is { } choices && !Contains(choices, converted))
        {
            throw Error(
                argument,
                "invalid choice: " + Repr(converted) + " (choose from " + string.Join(", ", ConvertAll(choices, Repr)) + ")");
        }

        return converted;
    }

    private static bool Contains(IReadOnlyList<object> choices, object value)
    {
        foreach (object choice in choices)
        {
            if (choice.Equals(value))
            {
                return true;
            }
        }

        return false;
    }

    private static List<string> ConvertAll(IReadOnlyList<object> values, Func<object, string> convert)
    {
        var converted = new List<string>(values.Count);
        foreach (object value in values)
        {
            converted.Add(convert(value));
        }

        return converted;
    }

    private static void CheckHelpArgument(string? explicitArg, bool singleDash)
    {
        if (explicitArg is null)
        {
            return;
        }

        string rest = singleDash && explicitArg.Length > 0 ? explicitArg.TrimStart('h') : explicitArg;
        if (rest.Length > 0 || explicitArg.Length == 0)
        {
            throw Error(s_help, "ignored explicit argument " + Repr(rest));
        }
    }

    private static bool IsNegativeNumber(string token)
    {
        int i = 1;
        while (i < token.Length && char.IsAsciiDigit(token[i]))
        {
            i++;
        }

        if (i == token.Length)
        {
            return i > 1;
        }

        if (token[i] != '.' || i + 1 == token.Length)
        {
            return false;
        }

        for (i++; i < token.Length; i++)
        {
            if (!char.IsAsciiDigit(token[i]))
            {
                return false;
            }
        }

        return true;
    }

    private bool LooksLikeOption(string token)
    {
        if (token.Length == 0 || token[0] != '-')
        {
            return false;
        }

        if (Find(token) is not null || token.StartsWith("-h", StringComparison.Ordinal))
        {
            return true;
        }

        if (token.Length == 1)
        {
            return false;
        }

        int equals = token.IndexOf('=', StringComparison.Ordinal);
        if (equals >= 0 && Find(token.Substring(0, equals)) is not null)
        {
            return true;
        }

        return !IsNegativeNumber(token) && !token.Contains(' ');
    }

    private (Argument? Argument, string? ExplicitArg, bool SingleDash) Match(string token)
    {
        bool singleDash = token[1] != '-';
        if (Find(token) is { } exact)
        {
            return (exact, null, singleDash);
        }

        int equals = token.IndexOf('=', StringComparison.Ordinal);
        if (equals >= 0 && Find(token.Substring(0, equals)) is { } withValue)
        {
            return (withValue, token.Substring(equals + 1), singleDash);
        }

        if (singleDash && token.StartsWith("-h", StringComparison.Ordinal))
        {
            return (s_help, token.Substring(2), singleDash);
        }

        return (null, null, singleDash);
    }

    private Argument? Find(string optionString)
    {
        if (optionString == "--help")
        {
            return s_help;
        }

        foreach (Argument argument in _arguments)
        {
            if (argument.Flag == optionString)
            {
                return argument;
            }
        }

        return null;
    }

    private int Width()
    {
        string? columns = Environment.GetEnvironmentVariable("COLUMNS");
        return (int.TryParse(columns, NumberStyles.None, CultureInfo.InvariantCulture, out int width) && width > 0
            ? width
            : 80) - 2;
    }

    private string FormatUsage()
    {
        int width = Width();
        var parts = new List<string> { _prog };
        foreach (Argument argument in _arguments)
        {
            if (argument == s_help)
            {
                parts.Add("[-h]");
            }
            else if (argument.Required || argument.Group is not null)
            {
                parts.Add(argument.Flag);
                parts.Add(argument.Metavar);
            }
            else
            {
                parts.Add("[" + argument.Flag + " " + argument.Metavar + "]");
            }
        }

        const string Prefix = "usage: ";
        string usage = string.Join(" ", parts);
        if (Prefix.Length + usage.Length <= width)
        {
            return Prefix + usage + "\n";
        }

        string indent = new string(' ', Prefix.Length + _prog.Length + 1);
        var lines = new List<string>();
        var line = new List<string>();
        int lineLength = Prefix.Length - 1;
        foreach (string part in parts)
        {
            if (lineLength + 1 + part.Length > width && line.Count > 0)
            {
                lines.Add(indent + string.Join(" ", line));
                line.Clear();
                lineLength = indent.Length - 1;
            }

            line.Add(part);
            lineLength += part.Length + 1;
        }

        lines.Add(indent + string.Join(" ", line));
        lines[0] = lines[0].Substring(indent.Length);
        return Prefix + string.Join("\n", lines) + "\n";
    }

    private string FormatHelp()
    {
        int width = Width();
        int longest = 0;
        foreach (Argument argument in _arguments)
        {
            longest = Math.Max(longest, argument.Invocation.Length + 2);
        }

        int helpPosition = Math.Min(longest + 2, Math.Min(MaxHelpPosition, Math.Max(width - 20, 4)));
        var help = new StringBuilder(FormatUsage());
        help.Append('\n').Append(Fill(_description, width, string.Empty)).Append("\n\noptions:\n");
        foreach (Argument argument in _arguments)
        {
            if (argument.Group is null)
            {
                FormatArgument(help, argument, width, helpPosition);
            }
        }

        foreach (MutuallyExclusiveGroup group in _groups)
        {
            help.Append('\n').Append(group.Title).Append(":\n")
                .Append(Fill(group.Description, width - 2, "  ")).Append("\n\n");
            foreach (Argument argument in group.Arguments)
            {
                FormatArgument(help, argument, width, helpPosition);
            }
        }

        return help.ToString();
    }

    private static void FormatArgument(StringBuilder help, Argument argument, int width, int helpPosition)
    {
        int actionWidth = helpPosition - 2 - 2;
        string text = argument.HelpText();
        if (argument.Invocation.Length <= actionWidth)
        {
            help.Append("  ").Append(argument.Invocation.PadRight(actionWidth)).Append("  ");
        }
        else
        {
            help.Append("  ").Append(argument.Invocation).Append('\n').Append(' ', helpPosition);
        }

        List<string> lines = Wrap(text, Math.Max(width - helpPosition, 11), 0);
        help.Append(lines[0]).Append('\n');
        for (int i = 1; i < lines.Count; i++)
        {
            help.Append(' ', helpPosition).Append(lines[i]).Append('\n');
        }
    }

    private static string Fill(string text, int width, string indent)
    {
        List<string> lines = Wrap(text, width, indent.Length);
        return indent + string.Join("\n" + indent, lines);
    }

    private static List<string> Wrap(string text, int width, int indent)
    {
        var lines = new List<string>();
        var line = new StringBuilder();
        foreach (string word in text.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries))
        {
            if (line.Length > 0 && indent + line.Length + 1 + word.Length > width)
            {
                lines.Add(line.ToString());
                line.Clear();
            }

            if (line.Length > 0)
            {
                line.Append(' ');
            }

            line.Append(word);
        }

        lines.Add(line.ToString());
        return lines;
    }

    /// <summary>A mutually exclusive group, inside an argument group of its own title and description.</summary>
    internal sealed class MutuallyExclusiveGroup
    {
        internal MutuallyExclusiveGroup(string title, string description, bool required)
        {
            Title = title;
            Description = description;
            Required = required;
        }

        internal string Title { get; }

        internal string Description { get; }

        internal bool Required { get; }

        internal List<Argument> Arguments { get; } = new List<Argument>();
    }

    /// <summary>One option the parser takes.</summary>
    internal sealed class Argument
    {
        internal Argument(
            string flag,
            string dest,
            string? metavar,
            string? help,
            Type? type,
            object? defaultValue,
            IReadOnlyList<object>? choices,
            bool required,
            MutuallyExclusiveGroup? group)
        {
            Flag = flag;
            Dest = dest;
            Metavar = metavar ?? dest.ToUpperInvariant();
            Help = help;
            Type = type;
            DefaultValue = defaultValue;
            Choices = choices;
            Required = required;
            Group = group;
        }

        internal string Flag { get; }

        internal string Dest { get; }

        internal string Metavar { get; }

        internal string? Help { get; }

        internal Type? Type { get; }

        internal object? DefaultValue { get; }

        internal IReadOnlyList<object>? Choices { get; }

        internal bool Required { get; }

        internal MutuallyExclusiveGroup? Group { get; }

        internal string Name => ReferenceEquals(this, s_help) ? "-h/--help" : Flag;

        internal string Invocation => ReferenceEquals(this, s_help) ? "-h, --help" : Flag + " " + Metavar;

        internal string HelpText()
        {
            string help = Help ?? string.Empty;
            if (ReferenceEquals(this, s_help))
            {
                return help;
            }

            if (!help.Contains("%(default)", StringComparison.Ordinal))
            {
                help += " (default: %(default)s)";
            }

            return help.Replace("%(default)s", PythonStr(DefaultValue), StringComparison.Ordinal);
        }

        private static string PythonStr(object? value) => value switch
        {
            null => "None",
            _ => System.Convert.ToString(value, CultureInfo.InvariantCulture)!,
        };
    }
}

/// <summary>
/// Java's <c>Runtime.addShutdownHook(new Thread(hook, name))</c>: at the first SIGINT or SIGTERM the hook runs once,
/// on a thread of that name; the signals never end the process, and those after the first are ignored.
/// </summary>
internal sealed class ShutdownHook
{
    // Held for the process: a collected registration removes its handler.
    private static readonly List<PosixSignalRegistration> s_registrations = new List<PosixSignalRegistration>();

    private readonly object _gate = new object();

    private readonly ManualResetEventSlim _requested = new ManualResetEventSlim();

    private readonly string _name;

    private readonly Thread _thread;

    private bool _finished;

    internal ShutdownHook(Action hook, string name)
    {
        _name = name;
        _thread = new Thread(() => Run(hook)) { Name = name, IsBackground = true };
        _thread.Start();
        lock (s_registrations)
        {
            s_registrations.Add(PosixSignalRegistration.Create(PosixSignal.SIGINT, OnSignal));
            s_registrations.Add(PosixSignalRegistration.Create(PosixSignal.SIGTERM, OnSignal));
        }
    }

    internal void Finish()
    {
        bool started;
        lock (_gate)
        {
            _finished = true;
            started = _requested.IsSet;
        }

        if (started)
        {
            _thread.Join();
        }
    }

    private void OnSignal(PosixSignalContext context)
    {
        context.Cancel = true;
        lock (_gate)
        {
            if (!_finished)
            {
                _requested.Set();
            }
        }
    }

    private void Run(Action hook)
    {
        _requested.Wait();
        try
        {
            hook();
        }
        catch (Exception e)
        {
            Log.Error(_name, "Exception in shutdown hook", e);
        }
    }
}
