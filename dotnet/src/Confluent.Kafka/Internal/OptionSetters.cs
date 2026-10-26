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

using System.Collections.Generic;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The setters an admin Options object ran, each at its last run, so an RPC builds the core's options from
/// <c>&lt;X&gt;Options_new</c> and replays only those setters, in that order.
/// </summary>
internal sealed class OptionSetters
{
    private readonly List<string> _ran = new List<string>();

    internal IReadOnlyList<string> InOrder => _ran;

    internal void Ran(string setter)
    {
        _ran.Remove(setter);
        _ran.Add(setter);
    }
}
