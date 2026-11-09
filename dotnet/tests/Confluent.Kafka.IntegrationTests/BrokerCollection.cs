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

using Confluent.Kafka.Performance.Tests;

using Xunit;

namespace Confluent.Kafka.IntegrationTests;

/// <summary>
/// One broker for every broker-backed test class, as Python's session-scoped <c>kafka_broker</c>: a class
/// fixture would start one Kafka container per class, in parallel. The classes in it run one after another;
/// each test uses its own topics and ids.
/// </summary>
[CollectionDefinition(Name)]
public sealed class BrokerCollection : ICollectionFixture<KafkaBrokerFixture>
{
    public const string Name = "broker";
}
