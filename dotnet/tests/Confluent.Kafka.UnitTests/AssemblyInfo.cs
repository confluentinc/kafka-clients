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

using Xunit;

// Tests run in parallel: each consumer call holds its SafeHandle reference for the call, so a teardown on another
// thread cannot free a consumer in use. An allocation test therefore measures GC.GetAllocatedBytesForCurrentThread()
// and does not await inside the measured region.
[assembly: CollectionBehavior(DisableTestParallelization = false)]
