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
using System.Threading.Tasks;

using Confluent.Kafka.Internal;
using Confluent.Kafka.Java.Util.Concurrent;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Java's <c>TestUtils.assertFutureThrows</c>: a failed future's <c>Get</c> or <c>GetNow</c> throws
/// <see cref="ExecutionException"/> with Java's message, the failure's <c>toString()</c>, and the failure as its
/// <see cref="Exception.InnerException"/>, of exactly the expected class. Each form returns the failure.
/// </summary>
internal static class FutureAssert
{
    internal static T Throws<T>(Action testCode)
        where T : Exception =>
        Cause<T>(Assert.Throws<ExecutionException>(testCode));

    internal static T Throws<T>(Func<object?> testCode)
        where T : Exception =>
        Cause<T>(Assert.Throws<ExecutionException>(testCode));

    internal static async Task<T> ThrowsAsync<T>(Func<Task> testCode)
        where T : Exception =>
        Cause<T>(await Assert.ThrowsAsync<ExecutionException>(testCode).ConfigureAwait(false));

    internal static T ThrowsAny<T>(Action testCode)
        where T : Exception =>
        CauseAny<T>(Assert.Throws<ExecutionException>(testCode));

    internal static T ThrowsAny<T>(Func<object?> testCode)
        where T : Exception =>
        CauseAny<T>(Assert.Throws<ExecutionException>(testCode));

    internal static async Task<T> ThrowsAnyAsync<T>(Func<Task> testCode)
        where T : Exception =>
        CauseAny<T>(await Assert.ThrowsAsync<ExecutionException>(testCode).ConfigureAwait(false));

    /// <summary>
    /// The failure an <see cref="ExecutionException"/> carries, of exactly the class <typeparamref name="T"/>.
    /// </summary>
    internal static T Cause<T>(Exception thrown)
        where T : Exception =>
        Assert.IsType<T>(Wrapped(thrown));

    /// <summary>
    /// The failure an <see cref="ExecutionException"/> carries, of exactly the class <paramref name="expectedType"/>.
    /// </summary>
    internal static T Cause<T>(Exception thrown, Type expectedType)
        where T : Exception
    {
        Exception cause = Wrapped(thrown);
        Assert.IsType(expectedType, cause);
        return Assert.IsAssignableFrom<T>(cause);
    }

    /// <summary>
    /// The failure an <see cref="ExecutionException"/> carries, of the class <typeparamref name="T"/> or a subclass.
    /// </summary>
    internal static T CauseAny<T>(Exception thrown)
        where T : Exception =>
        Assert.IsAssignableFrom<T>(Wrapped(thrown));

    /// <summary>
    /// The failure an <see cref="ExecutionException"/> carries, given as the outcome a <see cref="BlockingCall"/>
    /// or <c>Record.Exception</c> recorded.
    /// </summary>
    internal static Exception Unwrapped(object? outcome) => Wrapped(Assert.IsAssignableFrom<Exception>(outcome));

    private static Exception Wrapped(Exception thrown)
    {
        ExecutionException wrapper = Assert.IsType<ExecutionException>(thrown);
        Exception cause = Assert.IsAssignableFrom<Exception>(wrapper.InnerException);
        Assert.Equal(JavaText.ToString(cause), wrapper.Message);
        return cause;
    }
}
