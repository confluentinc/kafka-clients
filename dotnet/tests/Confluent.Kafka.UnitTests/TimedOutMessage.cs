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
using System.Text.RegularExpressions;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>Asserts a timed-out call's message: the call's own, or the core's event reaper's when it reaches the same deadline first.</summary>
internal static class TimedOutMessage
{
    private static readonly Regex s_reaper =
        new Regex(@"^.+ was \d+ ms past its expiration of \d+$", RegexOptions.CultureInvariant);

    internal static void AssertCallerOrReaper(string expected, string? actual)
    {
        if (!string.Equals(expected, actual, StringComparison.Ordinal))
        {
            Assert.Matches(s_reaper, actual);
        }
    }
}
