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

using Confluent.Kafka.Consumer;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// A user's own <see cref="IConsumerGroupMetadata"/>, where Java's tests call the class's deprecated constructor.
/// </summary>
internal sealed class TestGroupMetadata : IConsumerGroupMetadata
{
    private readonly string _groupId;
    private readonly int _generationId;
    private readonly string _memberId;
    private readonly string? _groupInstanceId;

    internal TestGroupMetadata(string groupId, int generationId = -1, string memberId = "", string? groupInstanceId = null)
    {
        _groupId = groupId;
        _generationId = generationId;
        _memberId = memberId;
        _groupInstanceId = groupInstanceId;
    }

    public string GroupId() => _groupId;

    public int GenerationId() => _generationId;

    public string MemberId() => _memberId;

    public string? GroupInstanceId() => _groupInstanceId;
}
