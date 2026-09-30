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

namespace Confluent.Kafka.Internal;

/// <summary>
/// The one precondition every string that is part of a per-key admin key must meet: it
/// crosses the C ABI unchanged, so the key the core answers is the key the caller asked for.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Why this exists at all</b> (M15/P13.3 (c), decision D9;
/// <c>definition-of-done.md</c> §7 — Java has no such type, because a Java string never
/// crosses a C boundary). The binding hands every string to the ABI as NUL-terminated UTF-8
/// (<see cref="Interop.Utf8Marshal"/>). Two kinds of C# string do not survive that:
/// </para>
/// <list type="bullet">
/// <item>one containing <c>'\0'</c> — the C string ends at the first NUL, so <c>"a\0b"</c>
/// and <c>"a\0c"</c> both reach the core as <c>"a"</c>, and a topic named <c>"a\0b"</c>
/// silently becomes topic <c>"a"</c>;</item>
/// <item>one containing an unpaired UTF-16 surrogate — <c>Encoding.UTF8</c> replaces it with
/// U+FFFD, so two different lone surrogates become the same key.</item>
/// </list>
/// <para>
/// Since PR #201 round 70 the core fires one callback per <b>distinct</b> key by C-string
/// equality (M15/P13.3 F4). Two C# keys that collapse to one C key therefore get one
/// callback where the binding's countdown waits for two — the call would never complete.
/// Rejecting such a string before anything is pinned, allocated or ref-counted (ffi §B5
/// order) is the only fix that keeps one result per caller key: de-duplicating on the
/// encoded bytes instead would silently merge two of the caller's keys into one answer.
/// </para>
/// <para>
/// Scope: the strings that make up a per-key key of a per-key admin RPC, and nothing else.
/// Producer and consumer strings, config names and values, log-directory paths and the
/// admin RPCs that answer with one callback are outside it — none of them keys a countdown,
/// so none of them can hang on a collapsed key.
/// </para>
/// </remarks>
internal static class AdminKeyStrings
{
    /// <summary>
    /// The <see cref="ArgumentException"/> message every rejection carries — one text for
    /// every guarded string, so that the caller is pointed at the argument by
    /// <see cref="ArgumentException.ParamName"/>, not by a per-site wording.
    /// </summary>
    internal const string InvalidKeyMessage =
        "An admin request key must not contain a NUL character or an unpaired UTF-16 " +
        "surrogate: such a string cannot be passed to the native client unchanged.";

    /// <summary>
    /// Rejects <paramref name="value"/> when it contains <c>'\0'</c> or an unpaired
    /// surrogate. A <c>null</c> value passes: whether null is allowed is each site's own
    /// precondition (an ACL filter's null name means "any").
    /// </summary>
    /// <param name="value">The key string the caller passed.</param>
    /// <param name="parameterName">The caller's parameter to blame.</param>
    /// <exception cref="ArgumentException">
    /// <paramref name="value"/> cannot cross the C ABI unchanged.
    /// </exception>
    internal static void Validate(string? value, string parameterName)
    {
        if (value is not null && !CrossesUnchanged(value))
        {
            throw new ArgumentException(InvalidKeyMessage, parameterName);
        }
    }

    /// <summary>
    /// Whether <paramref name="value"/> encodes to NUL-free UTF-8 that decodes back to itself:
    /// no <c>'\0'</c>, and every high surrogate is immediately followed by a low one.
    /// </summary>
    /// <param name="value">A non-null string.</param>
    /// <returns><c>true</c> when the string reaches the core as the same string.</returns>
    private static bool CrossesUnchanged(string value)
    {
        for (int i = 0; i < value.Length; i++)
        {
            char character = value[i];
            if (character == '\0')
            {
                return false;
            }

            if (char.IsHighSurrogate(character))
            {
                if (i + 1 >= value.Length || !char.IsLowSurrogate(value[i + 1]))
                {
                    return false;
                }

                // A well-formed pair: skip its low half, which the next check would
                // otherwise read as a lone low surrogate.
                i++;
            }
            else if (char.IsLowSurrogate(character))
            {
                return false;
            }
        }

        return true;
    }
}
