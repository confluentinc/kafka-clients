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
/// The process exit codes. <b>This is a hard contract with <c>run.sh</c></b>, which keys
/// its restart policy off these numbers verbatim: only <see cref="Fatal"/> means "a
/// restart cannot possibly help", and it is the only one that stops the supervisor dead.
/// <c>SoakExitCodeTests</c> pins every value, and additionally reads <c>run.sh</c> to
/// confirm the two agree on <see cref="Fatal"/>.
/// </summary>
internal static class SoakExitCodes
{
    /// <summary>Clean shutdown.</summary>
    internal const int Ok = 0;

    /// <summary>Ran fine, but detected a gap — the headline failure this soak exists to catch.</summary>
    internal const int MessageLoss = 1;

    /// <summary>Config rejected, assembly/native missing, authentication failed. NEVER restarted.</summary>
    internal const int Fatal = 2;

    /// <summary>Broker unreachable at startup — worth retrying.</summary>
    internal const int TransientStartup = 3;

    /// <summary>
    /// Consumer wedged: poll failed past its bound, or shutdown wedged. A restart
    /// re-authenticates and re-joins the group, so the supervisor should retry (bounded).
    /// </summary>
    internal const int ConsumerWedged = 4;

    /// <summary>
    /// The run's verdict as an exit code.
    /// <para>
    /// Message loss outranks everything else — it is the result the soak exists to
    /// report — so it is checked <b>before</b> the fatal reason, matching Python's
    /// <c>main()</c>. A wedged loop exits distinctly so the supervisor can restart it and
    /// a human can see why in one line.
    /// </para>
    /// <para>
    /// Extracted so the teardown path in <c>Program.Main</c> is a single call over a
    /// tested function (74.1): the guard that keeps a throwing shutdown inside this
    /// contract is worth nothing if the verdict it protects is computed by untested
    /// inline branches.
    /// </para>
    /// </summary>
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
    /// <summary>Creates the exception with no message.</summary>
    internal SoakFatalStartupException()
    {
    }

    /// <summary>Creates the exception with <paramref name="message"/>.</summary>
    internal SoakFatalStartupException(string? message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with <paramref name="message"/> and <paramref name="innerException"/>.</summary>
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
    /// <summary>Creates the exception with no message.</summary>
    internal SoakTransientStartupException()
    {
    }

    /// <summary>Creates the exception with <paramref name="message"/>.</summary>
    internal SoakTransientStartupException(string? message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with <paramref name="message"/> and <paramref name="innerException"/>.</summary>
    internal SoakTransientStartupException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }
}
