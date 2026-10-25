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

namespace Confluent.Kafka.Soak;

/// <summary>
/// The process exit codes, as Python's soak: a contract with <c>run.sh</c>, whose restart policy
/// stops only on <see cref="Fatal"/> (<c>SoakExitCodeTests</c> checks <c>run.sh</c> agrees).
/// </summary>
internal static class SoakExitCodes
{
    internal const int Ok = 0;

    internal const int MessageLoss = 1;

    internal const int Fatal = 2;

    internal const int TransientStartup = 3;

    internal const int ConsumerWedged = 4;

    // Message loss outranks a wedged loop, as Python's main().
    internal static int ExitCodeFor(long missedCount, string? fatalReason)
    {
        if (missedCount > 0)
        {
            return MessageLoss;
        }

        if (fatalReason is not null)
        {
            return ConsumerWedged;
        }

        return Ok;
    }
}

/// <summary>
/// A startup failure a restart cannot fix (bad credentials, no authorization). Mapped to
/// <see cref="SoakExitCodes.Fatal"/> so the supervisor stops instead of crash-looping.
/// </summary>
internal sealed class SoakFatalStartupException : Exception
{
    internal SoakFatalStartupException()
    {
    }

    internal SoakFatalStartupException(string? message)
        : base(message)
    {
    }

    internal SoakFatalStartupException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>
/// A startup failure that may clear on its own (broker unreachable). Mapped to
/// <see cref="SoakExitCodes.TransientStartup"/> so the supervisor retries, bounded by its
/// own consecutive-rapid-failure limit.
/// </summary>
internal sealed class SoakTransientStartupException : Exception
{
    internal SoakTransientStartupException()
    {
    }

    internal SoakTransientStartupException(string? message)
        : base(message)
    {
    }

    internal SoakTransientStartupException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }
}
