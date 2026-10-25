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

namespace Confluent.Kafka;

/// <summary>
/// The isolation of a read, Java's <c>IsolationLevel</c>, used by
/// <see cref="Admin.ListOffsetsOptions.IsolationLevel"/>.
/// </summary>
/// <remarks>Each value is Java's <c>IsolationLevel.id()</c>, which the C ABI takes unchanged.</remarks>
public enum IsolationLevel
{
    /// <summary>Read every record, including those of open transactions: Java's <c>READ_UNCOMMITTED</c>.</summary>
    ReadUncommitted = 0,

    /// <summary>Read only the records of committed transactions: Java's <c>READ_COMMITTED</c>.</summary>
    ReadCommitted = 1,
}
