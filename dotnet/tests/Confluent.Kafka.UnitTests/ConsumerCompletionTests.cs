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
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// A consumer operation's completion settles its <see cref="Task"/> on every path, a failure to convert the core's
/// error included.
/// </summary>
public sealed class ConsumerCompletionTests
{
    [Fact]
    public async Task AnErrorItsConverterFailsToConvert_FaultsTheTaskWithThatFailure()
    {
        InvalidOperationException failure = new InvalidOperationException("the error could not be read");
        ConsumerCompletion<bool> completion = new ConsumerCompletion<bool>(
            (_, _) => true,
            exceptionOf: error =>
            {
                NativeMethods.ErrorDestroy(error);
                throw failure;
            });
        IntPtr error = ManagedExceptionCarrier.ToError(new KafkaException("boom"));

        ConsumerCompletion.Void(error, completion.Root());

        Assert.True(completion.Result.IsCompleted, "the completion left its Task pending");
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => completion.Result));
    }
}
