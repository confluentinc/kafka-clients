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

// confluent-kafka-abi (cpp/CLAUDE.md §12.3): the constructs that would freeze
// a later change. Static: no baseline, no previous release; comparing with a
// release is `make -C cpp abi-check`.

#include <map>
#include <string>

#include "checks.hpp"
#include "clang/AST/ASTContext.h"
#include "clang/AST/DeclCXX.h"
#include "clang/AST/DeclTemplate.h"

namespace cklint::tidy {
namespace {

using clang::ast_matchers::MatchFinder;

/// Whether `type` is one pointer: a raw pointer, `std::shared_ptr`,
/// `std::unique_ptr` or the binding's `detail::UniqueHandle`.
bool is_one_pointer(clang::QualType type) {
    type = type.getCanonicalType();
    if (type->isPointerType()) {
        return true;
    }
    const auto* record = type->getAsCXXRecordDecl();
    if (record == nullptr) {
        return false;
    }
    std::string name;
    llvm::raw_string_ostream os(name);
    record->printQualifiedName(os);
    return name == "std::shared_ptr" || name == "std::unique_ptr" || name == "confluent::kafka::detail::UniqueHandle";
}

}  // namespace

void AbiCheck::check(const MatchFinder::MatchResult& result) {
    const auto& java = JavaSide::get(settings());
    for (const auto& t : public_types(*result.Context, settings())) {
        if (const auto* en = llvm::dyn_cast<clang::EnumDecl>(t.decl)) {
            if (!en->isScoped()) {
                diag(en->getLocation(),
                     "%0 is an unscoped enum: an enum class keeps its enumerators out of the "
                     "namespace and its type fixed (cpp/CLAUDE.md §3.3, §12.3)")
                    << en;
            } else if (en->getIntegerTypeSourceInfo() == nullptr) {
                diag(en->getLocation(),
                     "%0 does not write its underlying type, so it is frozen by accident "
                     "(cpp/CLAUDE.md §12.3)")
                    << en;
            }
            continue;
        }
        const auto* record = llvm::dyn_cast<clang::CXXRecordDecl>(t.decl);
        if (record == nullptr || !record->hasDefinition()) {
            continue;
        }
        record = record->getDefinition();
        const auto* jt = java.by_cpp_name(t.qualified_name);
        const bool interface = jt != nullptr && (jt->kind == java::TypeKind::INTERFACE || jt->has(java::MOD_ABSTRACT));

        for (const auto& base : record->bases()) {
            if (base.isVirtual()) {
                diag(base.getBeginLoc(),
                     "%0 inherits virtually: the base's offset is then read at run time and "
                     "fixed by the release (cpp/CLAUDE.md §12.3)")
                    << record;
            }
        }

        // §12.3: one pointer, so a field Java adds later keeps the layout.
        unsigned fields = 0;
        const clang::FieldDecl* other = nullptr;
        for (const auto* f : record->fields()) {
            ++fields;
            if (f->isBitField()) {
                diag(f->getLocation(), "%0 is a bit-field: its layout is the compiler's (cpp/CLAUDE.md §12.3)") << f;
            }
            if (!is_one_pointer(f->getType())) {
                other = f;
            }
        }
        if (fields > 1 || other != nullptr) {
            diag(record->getLocation(),
                 "%0 holds %1 data member(s), %2 not a pointer: a class with data holds one pointer, its C handle "
                 "or a private implementation, so a field Java adds later keeps the layout (cpp/CLAUDE.md §12.3)")
                << record << fields << (other != nullptr ? other->getName().str() : std::string("one"));
        }
        if (fields > 0 && record->canPassInRegisters()) {
            diag(record->getLocation(),
                 "%0 is passed in registers: declaring its destructor, copy or move later would change how it is "
                 "passed, so declare them now or hold a std::shared_ptr (cpp/CLAUDE.md §12.3)")
                << record;
        }
        if (fields > 0 && record->isAggregate()) {
            diag(record->getLocation(), "%0 is an aggregate: its fields are its interface (cpp/CLAUDE.md §12.3)")
                << record;
        }

        // §12.3: only an interface's vtable exists, and it only grows by appending.
        std::map<std::string, unsigned> virtual_names;
        for (const auto* m : record->methods()) {
            if (!m->isVirtual() || llvm::isa<clang::CXXDestructorDecl>(m) || m->getIdentifier() == nullptr) {
                continue;
            }
            if (!interface && m->size_overridden_methods() == 0) {
                diag(m->getLocation(),
                     "%0 is virtual, but %1 translates no Java interface or abstract class: only "
                     "an interface's vtable exists (cpp/CLAUDE.md §12.3)")
                    << m << record;
            }
            if (++virtual_names[m->getName().str()] == 2) {
                diag(m->getLocation(),
                     "%0 overloads a virtual name: some compilers lay overloads out as one group, "
                     "so appending one can move existing slots (cpp/CLAUDE.md §12.3)")
                    << m;
            }
        }
    }
}

}  // namespace cklint::tidy
