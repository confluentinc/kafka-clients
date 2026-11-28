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

// A type as a header writes it, read into the CppType the rules produce.
// Typedefs are kept (`int32_t` stays `int32_t`, not `int`; `std::string_view`
// stays itself); elaboration, using-declarations and inline namespaces
// (`std::__cxx11`) are looked through; names are fully qualified.

#include <string>

#include "clang/AST/ASTContext.h"
#include "clang/AST/Decl.h"
#include "cpp_type.hpp"

namespace cklint::tidy {

rules::CppType read_type(clang::QualType type, const clang::ASTContext& context);

/// `decl`'s qualified name, inline namespaces left out.
std::string qualified_name_of(const clang::NamedDecl& decl, const clang::ASTContext& context);

}  // namespace cklint::tidy
