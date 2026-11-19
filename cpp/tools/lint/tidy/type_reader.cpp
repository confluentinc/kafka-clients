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

#include "type_reader.hpp"

#include <algorithm>

#include "clang/AST/DeclCXX.h"
#include "clang/AST/DeclTemplate.h"
#include "clang/AST/PrettyPrinter.h"
#include "clang/AST/Type.h"
#include "llvm/Support/raw_ostream.h"

namespace cklint::tidy {
namespace {

clang::PrintingPolicy policy_of(const clang::ASTContext& context) {
    clang::PrintingPolicy policy(context.getLangOpts());
    policy.SuppressInlineNamespace = clang::PrintingPolicy::SuppressInlineNamespaceMode::All;
    policy.SuppressTagKeyword = true;
    return policy;
}

/// `void ()` -> `void()`: the spelling the rules write a function type in.
std::string compact(std::string text) {
    text.erase(std::remove(text.begin(), text.end(), ' '), text.end());
    return text;
}

void add_arguments(rules::CppType& out, llvm::ArrayRef<clang::TemplateArgument> arguments,
                   const clang::ASTContext& context) {
    for (const auto& arg : arguments) {
        if (arg.getKind() == clang::TemplateArgument::Type) {
            out.arguments.push_back(read_type(arg.getAsType(), context));
        } else if (arg.getKind() == clang::TemplateArgument::Pack) {
            add_arguments(out, arg.pack_elements(), context);
        }
        // A value argument (std::array's size) is not compared.
    }
}

}  // namespace

std::string qualified_name_of(const clang::NamedDecl& decl, const clang::ASTContext& context) {
    std::string out;
    llvm::raw_string_ostream os(out);
    decl.printQualifiedName(os, policy_of(context));
    return out;
}

rules::CppType read_type(clang::QualType type, const clang::ASTContext& context) {
    const auto* raw = type.getTypePtr();
    if (const auto* lvalue = llvm::dyn_cast<clang::LValueReferenceType>(raw)) {
        auto out = read_type(lvalue->getPointeeTypeAsWritten(), context);
        out.ref = rules::CppType::Ref::LVALUE;
        return out;
    }
    if (const auto* rvalue = llvm::dyn_cast<clang::RValueReferenceType>(raw)) {
        auto out = read_type(rvalue->getPointeeTypeAsWritten(), context);
        out.ref = rules::CppType::Ref::RVALUE;
        return out;
    }
    const bool is_const = type.isLocalConstQualified();
    clang::QualType t = type.getLocalUnqualifiedType();
    // Sugar that names nothing of its own.
    while (true) {
        if (const auto* e = llvm::dyn_cast<clang::ElaboratedType>(t.getTypePtr())) {
            t = e->getNamedType();
        } else if (const auto* u = llvm::dyn_cast<clang::UsingType>(t.getTypePtr())) {
            // `using __exception_ptr::exception_ptr;` in std: the name is the
            // one the header wrote, `std::exception_ptr`, not where it lives.
            const auto* target = u->getUnderlyingType().getTypePtr();
            const auto* tag = llvm::dyn_cast<clang::TagType>(target);
            if (tag != nullptr && !llvm::isa<clang::ClassTemplateSpecializationDecl>(tag->getDecl())) {
                rules::CppType out;
                out.is_const = is_const || t.isLocalConstQualified();
                out.name = qualified_name_of(*u->getFoundDecl(), context);
                return out;
            }
            t = u->getUnderlyingType();
        } else if (const auto* p = llvm::dyn_cast<clang::ParenType>(t.getTypePtr())) {
            t = p->getInnerType();
        } else if (const auto* a = llvm::dyn_cast<clang::AttributedType>(t.getTypePtr())) {
            t = a->getModifiedType();
        } else {
            break;
        }
        if (t.isLocalConstQualified()) {
            auto inner = read_type(t, context);
            inner.is_const = true;
            return inner;
        }
    }
    rules::CppType out;
    out.is_const = is_const;
    const auto* tp = t.getTypePtr();
    if (const auto* td = llvm::dyn_cast<clang::TypedefType>(tp)) {
        out.name = qualified_name_of(*td->getDecl(), context);
    } else if (const auto* ts = llvm::dyn_cast<clang::TemplateSpecializationType>(tp)) {
        if (const auto* decl = ts->getTemplateName().getAsTemplateDecl()) {
            out.name = qualified_name_of(*decl, context);
        } else {
            out.name = compact(t.getAsString(policy_of(context)));
        }
        add_arguments(out, ts->template_arguments(), context);
    } else if (const auto* injected = llvm::dyn_cast<clang::InjectedClassNameType>(tp)) {
        auto spec = read_type(injected->getInjectedSpecializationType(), context);
        spec.is_const = is_const;
        return spec;
    } else if (const auto* tag = llvm::dyn_cast<clang::TagType>(tp)) {
        out.name = qualified_name_of(*tag->getDecl(), context);
        if (const auto* spec = llvm::dyn_cast<clang::ClassTemplateSpecializationDecl>(tag->getDecl())) {
            add_arguments(out, spec->getTemplateArgs().asArray(), context);
        }
    } else if (const auto* param = llvm::dyn_cast<clang::TemplateTypeParmType>(tp)) {
        out.name = param->getIdentifier() != nullptr ? param->getIdentifier()->getName().str()
                                                     : compact(t.getAsString(policy_of(context)));
    } else {
        // Builtins (`int`, `bool`, `void`), function types (`void()`), dependent names.
        out.name = compact(t.getAsString(policy_of(context)));
    }
    return out;
}

}  // namespace cklint::tidy
