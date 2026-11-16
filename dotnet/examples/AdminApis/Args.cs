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

using System;
using System.Collections.Generic;
using System.Linq;

/// <summary>
/// The command line of one example: <c>[-X key=value]... [--flag | --name=value]... &lt;positional&gt;...</c>.
/// A bare <c>--</c> ends the options: every argument after it is positional, so a value that starts with
/// <c>--</c>, such as a password, can be given.
/// </summary>
internal sealed class Args
{
    private readonly List<(string Name, string? Value)> _options = [];
    private readonly List<string> _positional = [];

    /// <summary>The client properties given with <c>-X key=value</c>.</summary>
    public Dictionary<string, string> Config { get; } = [];

    /// <summary>Whether <c>--help</c> was given.</summary>
    public bool Help { get; private set; }

    /// <summary>Parses the arguments that follow the example's name.</summary>
    public static Args Parse(IReadOnlyList<string> raw)
    {
        Args args = new();
        for (int i = 0; i < raw.Count; i++)
        {
            string arg = raw[i];
            if (arg == "--")
            {
                args._positional.AddRange(raw.Skip(i + 1));
                break;
            }
            else if (arg == "--help")
            {
                args.Help = true;
                return args;
            }
            else if (arg == "-X")
            {
                // The property is not repeated in the error: it may be a secret such as sasl.jaas.config.
                int separator = i + 1 < raw.Count ? raw[i + 1].IndexOf('=') : -1;
                if (separator < 0)
                {
                    throw new UsageException("-X needs a key=value argument");
                }

                string property = raw[++i];
                args.Config[property[..separator]] = property[(separator + 1)..];
            }
            else if (arg.StartsWith("--", StringComparison.Ordinal))
            {
                string option = arg[2..];
                int separator = option.IndexOf('=');
                args._options.Add(separator < 0 ? (option, null) : (option[..separator], option[(separator + 1)..]));
            }
            else
            {
                args._positional.Add(arg);
            }
        }

        return args;
    }

    /// <summary>Takes the boolean option <c>--name</c>.</summary>
    public bool Flag(string name)
    {
        List<string?> values = Take(name);
        if (values.Any(value => value is not null))
        {
            throw new UsageException($"--{name} takes no value");
        }

        return values.Count > 0;
    }

    /// <summary>Takes the option <c>--name=value</c>, if given; the last one wins.</summary>
    public string? Value(string name)
    {
        return Values(name).LastOrDefault();
    }

    /// <summary>Takes every <c>--name=value</c> option, in command-line order.</summary>
    public List<string> Values(string name)
    {
        List<string> values = [];
        foreach (string? value in Take(name))
        {
            values.Add(value ?? throw new UsageException($"--{name} needs a value: --{name}=..."));
        }

        return values;
    }

    /// <summary>Returns the positional arguments, failing on any option not taken before.</summary>
    public string[] Positional()
    {
        if (_options.Count > 0)
        {
            // The argument is not repeated in the error: it may be a secret, such as a password that starts
            // with --.
            throw new UsageException("unknown option; an argument that starts with -- goes after a bare --");
        }

        return [.. _positional];
    }

    private List<string?> Take(string name)
    {
        List<string?> taken = [.. _options.Where(option => option.Name == name).Select(option => option.Value)];
        _options.RemoveAll(option => option.Name == name);
        return taken;
    }
}
