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

#pragma once

// CONFLUENT_KAFKA_CPP_API marks what the library exports (cpp/CLAUDE.md §3.2): every class, free
// function and explicit specialization a public header declares and src/ defines. The library
// builds with hidden visibility, so nothing else leaves it.
//
// The library's own sources define CONFLUENT_KAFKA_CPP_BUILDING; a program linking the static
// library defines CONFLUENT_KAFKA_CPP_STATIC, which ConfluentKafka::cpp_static carries.

#ifdef CONFLUENT_KAFKA_CPP_STATIC
#define CONFLUENT_KAFKA_CPP_API
#elif defined(_WIN32)
#ifdef CONFLUENT_KAFKA_CPP_BUILDING
#define CONFLUENT_KAFKA_CPP_API __declspec(dllexport)
#else
#define CONFLUENT_KAFKA_CPP_API __declspec(dllimport)
#endif
// C4251: an exported class holds a private std::shared_ptr or std::unique_ptr (cpp/CLAUDE.md
// §12.3), which has no DLL interface. No caller reaches it, and the library and the program use
// the same C++ runtime, as every C++ DLL requires.
#pragma warning(disable : 4251)
#else
#define CONFLUENT_KAFKA_CPP_API __attribute__((visibility("default")))
#endif
