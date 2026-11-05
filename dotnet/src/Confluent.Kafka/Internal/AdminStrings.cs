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
/// Rejects a string that cannot cross the C ABI unchanged, an admin request string or a producer record's topic or
/// header key: one containing <c>'\0'</c>, which ends the C string. An unpaired UTF-16 surrogate is not rejected: it
/// crosses as <c>'?'</c>, as Java's <c>String.getBytes(UTF_8)</c> writes it
/// (<see cref="Interop.Utf8Marshal.JavaUtf8"/>).
/// </summary>
/// <remarks>
/// A recorded deviation: Java sends a NUL whole, with no error. Here the C ABI would cut the string at the NUL, so the
/// string could act on another target; it is rejected with <see cref="ArgumentException"/> before anything is pinned.
/// </remarks>
internal static class AdminStrings
{
    internal const string InvalidStringMessage =
        "An admin request string must not contain a NUL character: such a string cannot be passed to the native " +
        "client unchanged.";

    internal const string InvalidTopicMessage =
        "A topic must not contain a NUL character: such a string cannot be passed to the native client unchanged.";

    internal const string InvalidHeaderKeyMessage =
        "A header key must not contain a NUL character: such a string cannot be passed to the native client " +
        "unchanged.";

    internal static void Validate(string? value, string parameterName) =>
        Validate(value, InvalidStringMessage, parameterName);

    internal static void Validate(string? value, string message, string parameterName)
    {
        if (value is not null && value.IndexOf('\0') >= 0)
        {
            throw new ArgumentException(message, parameterName);
        }
    }
}
