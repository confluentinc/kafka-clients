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

// Which C++ function translates which Java overload. Shared by the members,
// signature and surface checks, so they agree on every pair.
//
// A type's C++ functions are its public constructors and member functions,
// the free functions beside it whose first parameter is the type (and the
// friends it defines), and, for an enum, the functions in the namespace named
// after it (§3.3). Each Java overload is paired with the C++ function of the
// same group, namespace qualifier, name and parameter count that mismatches
// the fewest parameter and return shapes, ties broken by declaration order;
// a §5 convention (`operator==`, `to_string`, `std::hash`, ...) by name.

#include <string>
#include <vector>

#include "clang/AST/ASTContext.h"
#include "clang/AST/Decl.h"
#include "expected.hpp"
#include "session.hpp"

namespace cklint::tidy {

struct FunctionPair {
    const rules::ExpectedFunction* expected = nullptr;
    const clang::FunctionDecl* cpp = nullptr;
};

/// A constant found for a type: a static data member, or a variable in an
/// enum's statics namespace.
struct CppConstant {
    const clang::VarDecl* decl = nullptr;
    std::string qualifier;
};

struct TypePairing {
    const CppType* type = nullptr;
    const java::Type* java = nullptr;
    const rules::ExpectedType* expected = nullptr;
    std::vector<FunctionPair> pairs;
    /// Java overloads and §5 conventions with no C++ function.
    std::vector<const rules::ExpectedFunction*> missing;
    /// Public C++ functions that translate nothing of Java's.
    std::vector<const clang::FunctionDecl*> extra;
    std::vector<CppConstant> constants;
    /// Public non-static data members.
    std::vector<const clang::FieldDecl*> fields;
};

struct Pairing {
    std::vector<CppType> cpp_types;
    std::vector<TypePairing> types;
    /// Free functions of the binding that belong to no translated type.
    std::vector<const clang::FunctionDecl*> unowned;
};

Pairing pair_all(clang::ASTContext& context, const Settings& settings, const JavaSide& java);

/// The number of parameters (and the return) of `cpp` whose written type is
/// not one of the shapes `expected` allows.
int mismatches(const rules::ExpectedFunction& expected, const clang::FunctionDecl& cpp,
               const clang::ASTContext& context);

/// The shape of `mapping` that `type` matches, or nullptr.
const rules::Shape* matching_shape(const rules::Mapping& mapping, const rules::CppType& type);

}  // namespace cklint::tidy
