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

// confluent-kafka-library (cpp/CLAUDE.md §3.2): what a public header defines is
// compiled into the programs including it, so a header defines only what must be
// there, and what the library defines is exported.
// confluent-kafka-source (cpp/CLAUDE.md §1.3): the library's sources call no _cb
// function.

#include <string>

#include "checks.hpp"
#include "clang/AST/ASTContext.h"
#include "clang/AST/Attr.h"
#include "clang/AST/DeclCXX.h"
#include "clang/AST/DeclTemplate.h"
#include "clang/Basic/SourceManager.h"

namespace cklint::tidy {
namespace {

using clang::ast_matchers::MatchFinder;

/// Whether `decl` carries CONFLUENT_KAFKA_CPP_API: default visibility, or a
/// DLL export or import.
bool is_exported(const clang::Decl& decl) {
    if (const auto* visibility = decl.getAttr<clang::VisibilityAttr>()) {
        return visibility->getVisibility() == clang::VisibilityAttr::Default;
    }
    return decl.hasAttr<clang::DLLExportAttr>() || decl.hasAttr<clang::DLLImportAttr>();
}

/// Whether `decl` is spelled in a public header, under the include root.
bool in_public_header(const clang::Decl& decl, const clang::SourceManager& sm, const Settings& settings) {
    const auto loc = sm.getSpellingLoc(decl.getLocation());
    const auto entry = sm.getFileEntryRefForID(sm.getFileID(loc));
    return entry && !relative_to_include_root(entry->getName(), settings).empty();
}

/// Whether `function` is a template, a member of one, or an instantiation:
/// those are compiled where they are used, by design (§3.2).
bool is_templated(const clang::FunctionDecl& function) {
    if (function.isTemplated()) {
        return true;
    }
    const auto kind = function.getTemplateSpecializationKind();
    return kind == clang::TSK_ImplicitInstantiation || kind == clang::TSK_ExplicitInstantiationDeclaration ||
           kind == clang::TSK_ExplicitInstantiationDefinition;
}

/// Whether `function` is inside another function or a lambda: it is judged
/// with the function that contains it.
bool is_local(const clang::FunctionDecl& function) {
    if (function.getDeclContext()->isFunctionOrMethod()) {
        return true;
    }
    const auto* method = llvm::dyn_cast<clang::CXXMethodDecl>(&function);
    return method != nullptr && (method->getParent()->isLambda() || method->getParent()->isLocalClass() != nullptr);
}

std::string qualified(const clang::NamedDecl& decl) {
    std::string out;
    llvm::raw_string_ostream os(out);
    decl.printQualifiedName(os);
    return out;
}

}  // namespace

void LibraryCheck::registerMatchers(MatchFinder* finder) {
    using namespace clang::ast_matchers;
    finder->addMatcher(functionDecl(unless(isImplicit())).bind("function"), this);
    finder->addMatcher(cxxRecordDecl(isDefinition(), unless(isImplicit())).bind("record"), this);
}

void LibraryCheck::check(const MatchFinder::MatchResult& result) {
    const auto& sm = *result.SourceManager;
    if (const auto* record = result.Nodes.getNodeAs<clang::CXXRecordDecl>("record")) {
        // A namespace-scope class the library defines: a nested class takes its
        // enclosing class's visibility, a template is compiled where it is used.
        if (!in_public_header(*record, sm, settings()) || !record->getDeclContext()->isFileContext() ||
            record->getDescribedClassTemplate() != nullptr || record->isLambda()) {
            return;
        }
        if (const auto* spec = llvm::dyn_cast<clang::ClassTemplateSpecializationDecl>(record)) {
            if (spec->getSpecializationKind() != clang::TSK_ExplicitSpecialization ||
                llvm::isa<clang::ClassTemplatePartialSpecializationDecl>(spec)) {
                return;
            }
        }
        if (!is_exported(*record)) {
            diag(record->getLocation(),
                 "%0 is not marked CONFLUENT_KAFKA_CPP_API: the library builds with hidden visibility, so a "
                 "program cannot link what it defines (cpp/CLAUDE.md §3.2)")
                << record;
        }
        return;
    }
    const auto* function = result.Nodes.getNodeAs<clang::FunctionDecl>("function");
    if (function == nullptr || !in_public_header(*function, sm, settings()) || is_templated(*function) ||
        is_local(*function) || function->isConstexpr() || function->isDeleted()) {
        return;
    }
    const auto* method = llvm::dyn_cast<clang::CXXMethodDecl>(function);

    // §3.2: a header defines only what must be compiled into the program; of an
    // interface, its constructor, which records the program's header version (§12.3).
    if (function->doesThisDeclarationHaveABody() || function->isExplicitlyDefaulted()) {
        bool interface_constructor = false;
        if (llvm::isa<clang::CXXConstructorDecl>(function)) {
            const auto* jt = JavaSide::get(settings()).by_cpp_name(qualified(*method->getParent()));
            interface_constructor =
                jt != nullptr && (jt->kind == java::TypeKind::INTERFACE || jt->has(java::MOD_ABSTRACT));
        }
        if (!interface_constructor) {
            diag(function->getLocation(),
                 "%0 is defined in a public header, so it is compiled into the programs calling it and a fix "
                 "reaches them only when they are rebuilt: define it in src/ (cpp/CLAUDE.md §3.2)")
                << function;
        }
    }

    // A member takes its class's export; a free function, a friend included,
    // carries its own.
    if (method == nullptr && function->isFirstDecl() && !is_exported(*function)) {
        diag(function->getLocation(),
             "%0 is not marked CONFLUENT_KAFKA_CPP_API: the library builds with hidden visibility, so a "
             "program cannot link it (cpp/CLAUDE.md §3.2)")
            << function;
    }
}

void SourceCheck::registerMatchers(MatchFinder* finder) {
    using namespace clang::ast_matchers;
    finder->addMatcher(declRefExpr(to(functionDecl(matchesName("_cb$")))).bind("cb"), this);
}

void SourceCheck::check(const MatchFinder::MatchResult& result) {
    const auto* cb = result.Nodes.getNodeAs<clang::DeclRefExpr>("cb");
    if (cb == nullptr || result.SourceManager->isInSystemHeader(cb->getLocation())) {
        return;
    }
    diag(cb->getLocation(),
         "%0 is a _cb function: the binding calls only the own-name fallible functions and the _err twins "
         "(cpp/CLAUDE.md §1.3)")
        << cb->getDecl();
}

}  // namespace cklint::tidy
