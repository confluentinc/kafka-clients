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

// confluent-kafka-inventory: with ConfluentKafkaInventoryFile set, writes the
// binding's API inventory there, one public declaration per line, for
// `ck_java_model api-diff` (cpp/CLAUDE.md §12.4). It reports nothing.
//
//   type        <name>          <class|struct|enum class : T> [final] [abstract] [: <bases>]
//   enumerator  <enum>::<name>  #<index> = <value>
//   function    <name>(<parameter types>)[ const][ &|&&]  -> <return>[ static][ virtual]...
//   variable    <name>          <type>[ static][ constexpr]

#include <fstream>
#include <string>

#include "checks.hpp"
#include "clang/AST/ASTContext.h"
#include "clang/AST/DeclCXX.h"
#include "clang/AST/PrettyPrinter.h"
#include "public_decls.hpp"

namespace cklint::tidy {
namespace {

std::string qualified(const clang::NamedDecl& decl) {
    std::string out;
    llvm::raw_string_ostream os(out);
    decl.printQualifiedName(os);
    return out;
}

std::string line_of(const clang::NamedDecl& decl, const clang::PrintingPolicy& policy) {
    const auto type_text = [&](clang::QualType t) { return t.getAsString(policy); };
    if (const auto* c = llvm::dyn_cast<clang::EnumConstantDecl>(&decl)) {
        const auto* en = llvm::cast<clang::EnumDecl>(c->getDeclContext());
        unsigned index = 0;
        for (const auto* e : en->enumerators()) {
            if (e == c) {
                break;
            }
            ++index;
        }
        return "enumerator\t" + qualified(*c) + "\t#" + std::to_string(index) + " = " +
               std::to_string(c->getInitVal().getSExtValue());
    }
    if (const auto* en = llvm::dyn_cast<clang::EnumDecl>(&decl)) {
        return "type\t" + qualified(*en) + "\t" + (en->isScoped() ? "enum class" : "enum") + " : " +
               type_text(en->getIntegerType());
    }
    if (const auto* r = llvm::dyn_cast<clang::CXXRecordDecl>(&decl)) {
        std::string detail = r->isStruct() ? "struct" : r->isUnion() ? "union" : "class";
        if (r->hasAttr<clang::FinalAttr>()) {
            detail += " final";
        }
        if (r->hasDefinition() && r->isAbstract()) {
            detail += " abstract";
        }
        std::string bases;
        for (const auto& b : r->bases()) {
            bases += (bases.empty() ? " : " : ", ") + type_text(b.getType());
        }
        return "type\t" + qualified(*r) + "\t" + detail + bases;
    }
    if (const auto* f = llvm::dyn_cast<clang::FunctionDecl>(&decl)) {
        std::string key = qualified(*f) + "(";
        for (unsigned i = 0; i < f->getNumParams(); ++i) {
            key += (i > 0 ? ", " : "") + type_text(f->getParamDecl(i)->getType());
        }
        key += ")";
        std::string detail;
        if (const auto* m = llvm::dyn_cast<clang::CXXMethodDecl>(f)) {
            key += m->isConst() ? " const" : "";
            key += m->getRefQualifier() == clang::RQ_LValue   ? " &"
                   : m->getRefQualifier() == clang::RQ_RValue ? " &&"
                                                              : "";
            detail += m->isStatic() ? " static" : "";
            detail += m->isVirtual() ? " virtual" : "";
            detail += m->isPureVirtual() ? " pure" : "";
        }
        if (const auto* ctor = llvm::dyn_cast<clang::CXXConstructorDecl>(f); ctor != nullptr && ctor->isExplicit()) {
            detail += " explicit";
        }
        detail += f->isConstexpr() ? " constexpr" : "";
        detail += f->getFriendObjectKind() != clang::Decl::FOK_None ? " friend" : "";
        if (const auto* proto = f->getType()->getAs<clang::FunctionProtoType>();
            proto != nullptr && proto->isNothrow()) {
            detail += " noexcept";
        }
        for (unsigned i = 0; i < f->getNumParams(); ++i) {
            if (f->getParamDecl(i)->hasDefaultArg()) {
                detail += " default(" + std::to_string(i) + ")";
            }
        }
        const auto returns = llvm::isa<clang::CXXConstructorDecl>(f) ? std::string("-") : type_text(f->getReturnType());
        return "function\t" + key + "\t-> " + returns + detail;
    }
    if (const auto* v = llvm::dyn_cast<clang::VarDecl>(&decl)) {
        std::string detail = type_text(v->getType());
        detail += v->isStaticDataMember() ? " static" : "";
        detail += v->isConstexpr() ? " constexpr" : "";
        return "variable\t" + qualified(*v) + "\t" + detail;
    }
    return "declaration\t" + qualified(decl) + "\t-";
}

}  // namespace

void InventoryCheck::check(const clang::ast_matchers::MatchFinder::MatchResult& result) {
    const auto path = Options.getLocalOrGlobal("ConfluentKafkaInventoryFile", "").str();
    if (path.empty()) {
        return;
    }
    clang::PrintingPolicy policy(result.Context->getLangOpts());
    policy.FullyQualifiedName = true;
    policy.SuppressScope = false;
    policy.SuppressInlineNamespace = clang::PrintingPolicy::SuppressInlineNamespaceMode::All;
    std::ofstream out(path);
    for (const auto* decl : public_declarations(*result.Context, settings())) {
        out << line_of(*decl, policy) << '\n';
    }
}

}  // namespace cklint::tidy
