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
using System.Collections.Generic;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Copies the values the four <c>kafka_admin_DescribeClusterResult_t</c> futures deliver into
/// owned managed values; each value is borrowed from its future, so it frees nothing.
/// </summary>
internal static class DescribeClusterMarshal
{
    internal static IReadOnlyCollection<Node> CopyOutNodes(IntPtr list) =>
        NodeMarshal.CopyOutList(list) ?? new List<Node>();

    internal static Node? CopyOutController(IntPtr node) => NodeMarshal.CopyOut(node);

    internal static string CopyOutClusterId(IntPtr clusterId) => Utf8Marshal.PtrToString(clusterId) ?? string.Empty;

    internal static IReadOnlyCollection<AclOperation>? CopyOutAuthorizedOperations(IntPtr list) =>
        AuthorizedOperationsMarshal.CopyOutList(list);
}
