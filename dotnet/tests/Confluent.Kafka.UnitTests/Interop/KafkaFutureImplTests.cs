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


using Confluent.Kafka.Internal;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The engine tests run on the sync engine, <see cref="KafkaFutureImpl{T}"/>.
/// </summary>
public sealed class KafkaFutureImplTests : KafkaFutureImplTestBase
{
    private protected override IKafkaFutureImpl<T> New<T>(bool mayCancel, IFutureValueSource<T>? source) =>
        new KafkaFutureImpl<T>(mayCancel, source);
}
