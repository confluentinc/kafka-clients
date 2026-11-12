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

#pragma once

#include <cstdint>
#include <string_view>

namespace confluent::kafka {

/// The version of the C++ binding. The three numbers match the `project()`
/// version in `cpp/CMakeLists.txt`, which the CMake configure step checks.
inline constexpr std::int32_t VERSION_MAJOR = 0;
inline constexpr std::int32_t VERSION_MINOR = 1;
inline constexpr std::int32_t VERSION_PATCH = 0;
inline constexpr std::string_view VERSION = "0.1.0";

}  // namespace confluent::kafka
