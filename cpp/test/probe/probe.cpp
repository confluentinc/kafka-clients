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

// The ABI probe's one translation unit: see test/probe/CMakeLists.txt.
#include <cstdint>

#include "probe_includes.hpp"

namespace confluent::kafka::probe {

/// Exported so the library is never empty: the version the headers declare.
extern "C" std::int32_t confluent_kafka_cpp_probe_version() {
    return (((VERSION_MAJOR * 1000) + VERSION_MINOR) * 1000) + VERSION_PATCH;
}

}  // namespace confluent::kafka::probe
