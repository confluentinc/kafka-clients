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
/// Rejects an admin string that cannot cross the C ABI unchanged: one containing <c>'\0'</c>, which
/// ends the C string, or an unpaired UTF-16 surrogate, which UTF-8 encoding replaces.
/// </summary>
/// <remarks>
/// A recorded deviation: Java length-prefixes every string, so it never alters one. Here such a
/// string would act on another target, or merge two of the caller's keys into one, so it is
/// rejected with <see cref="ArgumentException"/> before anything is pinned.
/// </remarks>
internal static class AdminStrings
{
    internal const string InvalidStringMessage =
        "An admin request string must not contain a NUL character or an unpaired UTF-16 " +
        "surrogate: such a string cannot be passed to the native client unchanged.";

    internal static void Validate(string? value, string parameterName)
    {
        if (value is not null && !CrossesUnchanged(value))
        {
            throw new ArgumentException(InvalidStringMessage, parameterName);
        }
    }

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
