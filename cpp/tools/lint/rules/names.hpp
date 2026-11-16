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

// cpp/CLAUDE.md §2 and §3.1 as functions: what a Java name is called in C++
// and where it lives.

#include <string>
#include <string_view>
#include <vector>

#include "java_model.hpp"

namespace cklint::rules {

/// camelCase → snake_case keeping acronyms together (`getAPIVersion` →
/// `get_api_version`); the same function as `snake_case` in rust/xtask/src/java.rs.
std::string snake_case(std::string_view java_name);

/// §2.2 on a member name: snake_case, then the words `exception(s)` become
/// `error(s)` (`injectTimeoutException` → `inject_timeout_error`).
std::string member_name(std::string_view java_name);

/// §2.2 on a type name: the word `Exception(s)` becomes `Error(s)`, nothing
/// else (`SSLException` → `SSLError`, `InvalidRegularExpression` unchanged).
std::string type_name(std::string_view java_simple_name);

/// §2.1: `org.apache.kafka.clients.consumer` → `confluent::kafka::consumer`,
/// `org.apache.kafka.common.record` → `confluent::kafka::common::record`.
std::string namespace_of(std::string_view java_package);

/// §2.1, §2.4: the C++ qualified name of a type, nested types as nested
/// classes: `confluent::kafka::common::config::ConfigResource::Type`.
std::string qualified_name(const java::Model& model, const java::Type& type);

/// §3.1: the header of a type, its top-level type's, under include/:
/// `confluent/kafka/consumer/close_options.hpp`.
std::string header_of(const java::Model& model, const java::Type& type);

/// §3.1: the module's umbrella header: `confluent/kafka/consumer.hpp`.
std::string umbrella_of(std::string_view java_package);

/// §2.3: a method's C++ name. A leading `get` is dropped iff the method takes
/// no parameter, returns a value and what remains is not a C++ keyword.
std::string method_name(const java::Method& method);

/// §2.3: the parameter names of `method` on `owner`, from the declaration of
/// the nearest interface among `owner`'s supertypes that declares it, else
/// from `method` itself; snake_cased as members (§2.2).
std::vector<std::string> parameter_names(const java::Model& model, const java::Type& owner, const java::Method& method);

/// §3.3: the free function a Java enum member becomes, prefixed with the
/// enum's name and its enclosing classes': `timestamp_type_for_name`,
/// `config_resource_type_for_id`; `toString` is `to_string`.
std::string enum_function_name(const java::Model& model, const java::Type& enum_type, std::string_view member_cpp_name);

/// Whether `word` is a C++ keyword or alternative token.
bool is_cpp_keyword(std::string_view word);

}  // namespace cklint::rules
