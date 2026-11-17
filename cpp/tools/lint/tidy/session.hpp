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

// What every confluent-kafka-* check shares: where the inputs are, the Java
// side (model, scope, expected shapes; built once per process), and the C++
// side (the binding's public types in the translation unit).

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "clang-tidy/ClangTidyCheck.h"
#include "data_files.hpp"
#include "expected.hpp"
#include "java_model.hpp"
#include "scope.hpp"

namespace cklint::tidy {

/// Where the checks read their inputs. Each comes from a clang-tidy option,
/// global or per check, and defaults to the paths CMake compiled in:
///   ConfluentKafkaJavaRoot     the Java source root (kafka/clients/src/main/java)
///   ConfluentKafkaDataDir      the linter's data files (cpp/tools/lint/data)
///   ConfluentKafkaIncludeRoot  the binding's include directory (cpp/include)
///   ConfluentKafkaLedger       the contract-change ledger
///                              (cpp/design/current/doc-contract-changes.md)
struct Settings {
    std::string java_root;
    std::string data_dir;
    std::string include_root;
    std::string ledger;

    static Settings of(const clang::tidy::ClangTidyCheck::OptionsView& options);
    [[nodiscard]] std::string key() const { return java_root + '\n' + data_dir + '\n' + include_root; }
};

/// The Java side, built once per process for a given set of inputs.
class JavaSide {
public:
    static const JavaSide& get(const Settings& settings);

    [[nodiscard]] const java::Model& model() const { return model_; }
    [[nodiscard]] const rules::Scope& scope() const { return scope_; }
    /// The Java type whose C++ qualified name (cpp/CLAUDE.md §2.1) is `cpp_name`.
    [[nodiscard]] const java::Type* by_cpp_name(std::string_view cpp_name) const;
    /// The expected shape of `type`, computed on first use.
    [[nodiscard]] const rules::ExpectedType& expected(const java::Type& type) const;
    /// data/decisions.txt (§4.6) and data/baseline.txt (§1.4).
    [[nodiscard]] const rules::DataFile& decisions() const { return decisions_; }
    [[nodiscard]] const rules::DataFile& baseline() const { return baseline_; }

private:
    explicit JavaSide(const Settings& settings);

    java::Model model_;
    rules::DataFile excluded_;
    rules::DataFile decisions_;
    rules::DataFile baseline_;
    rules::Scope scope_;
    std::map<std::string, std::string, std::less<>> by_cpp_name_;
    mutable std::mutex expected_lock_;
    mutable std::map<std::string, rules::ExpectedType> expected_;
};

/// A public type of the binding found in the translation unit: a class,
/// struct, union or enum in `confluent::kafka` (not `detail`), defined in a
/// header under the include root, at namespace scope or nested publicly.
struct CppType {
    const clang::TagDecl* decl = nullptr;
    std::string qualified_name;
    /// The header, relative to the include root: `confluent/kafka/common/node.hpp`.
    std::string header;
};

/// The binding's public types in `context`, in declaration order.
std::vector<CppType> public_types(clang::ASTContext& context, const Settings& settings);

/// The path of `file` relative to the include root, or empty when the file is
/// not under it.
std::string relative_to_include_root(std::string_view file, const Settings& settings);

/// The Java built-in exceptions translated at `confluent::kafka` (cpp/CLAUDE.md §6.2).
bool is_java_builtin_error(std::string_view qualified_name);

}  // namespace cklint::tidy
