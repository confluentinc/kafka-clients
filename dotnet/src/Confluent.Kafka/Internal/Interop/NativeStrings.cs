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

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Rejects a string argument that cannot cross the C ABI unchanged: one containing <c>'\0'</c>, which ends the C
/// string, so the core would act on the part before it. Each kind of string has its own message. An unpaired UTF-16
/// surrogate is not rejected: it crosses as <c>'?'</c>, as Java's <c>String.getBytes(UTF_8)</c> writes it
/// (<see cref="Utf8Marshal.QuestionMarkUtf8"/>).
/// </summary>
/// <remarks>
/// A recorded deviation: Java sends a NUL whole, with no error. Here the C ABI would cut the string at the NUL, so the
/// string could act on another target; it is rejected with <see cref="ArgumentException"/> before anything is pinned.
/// </remarks>
internal static class NativeStrings
{
    internal const string InvalidAdminStringMessage =
        "An admin request string must not contain a NUL character: such a string cannot be passed to the native " +
        "client unchanged.";

    internal const string InvalidTopicMessage =
        "A topic must not contain a NUL character: such a string cannot be passed to the native client unchanged.";

    internal const string InvalidHeaderKeyMessage =
        "A header key must not contain a NUL character: such a string cannot be passed to the native client " +
        "unchanged.";

    internal const string InvalidSubscriptionPatternMessage =
        "A subscription pattern must not contain a NUL character: such a string cannot be passed to the native " +
        "client unchanged.";

    internal const string InvalidConfigKeyMessage =
        "A configuration key must not contain a NUL character: such a string cannot be passed to the native client " +
        "unchanged.";

    internal const string InvalidConfigValueMessage =
        "A configuration value must not contain a NUL character: such a string cannot be passed to the native " +
        "client unchanged.";

    internal const string InvalidArgumentStringMessage =
        "A string argument must not contain a NUL character: such a string cannot be passed to the native client " +
        "unchanged.";

    internal const string InvalidOffsetMetadataMessage =
        "Offset metadata must not contain a NUL character: such a string cannot be passed to the native client " +
        "unchanged.";

    internal const string InvalidGroupMetadataMessage =
        "Group metadata must not contain a NUL character: such a string cannot be passed to the native client " +
        "unchanged.";

    internal static bool ContainsNul(string? value) => value is not null && value.IndexOf('\0') >= 0;

    internal static void Validate(string? value, string message, string parameterName)
    {
        if (ContainsNul(value))
        {
            throw new ArgumentException(message, parameterName);
        }
    }
}
