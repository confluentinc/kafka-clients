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
using System.Runtime.InteropServices;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// The blittable mirror of <c>kafka_Bytes_t</c>: a pointer to the bytes and their length.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal readonly struct KafkaBytes
{
    internal readonly IntPtr Data;
    internal readonly int Length;

    internal KafkaBytes(IntPtr data, int length)
    {
        Data = data;
        Length = length;
    }
}
