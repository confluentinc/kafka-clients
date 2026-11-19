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

#include "public_decls.hpp"

#include <set>
#include <string>

#include "clang/AST/DeclCXX.h"
#include "clang/AST/DeclFriend.h"
#include "clang/AST/DeclTemplate.h"
#include "clang/Basic/SourceManager.h"

namespace cklint::tidy {
namespace {

bool under_include_root(const clang::Decl& decl, const clang::SourceManager& sm, const Settings& settings) {
    const auto loc = sm.getExpansionLoc(decl.getLocation());
    const auto entry = sm.getFileEntryRefForID(sm.getFileID(loc));
    return entry && !relative_to_include_root(entry->getName(), settings).empty();
}

/// Every public declaration of the binding that needs a comment, in order.
class Collector {
public:
    Collector(const clang::SourceManager& sm, const Settings& settings) : sm_(sm), settings_(settings) {}

    void namespaces(const clang::DeclContext& context, const std::string& path) {
        for (const auto* d : context.decls()) {
            if (const auto* ns = llvm::dyn_cast<clang::NamespaceDecl>(d)) {
                const auto name = ns->getName().str();
                const auto inner = path.empty() ? name : path + "::" + name;
                if (inner == "confluent" || inner == "confluent::kafka" ||
                    (inner.rfind("confluent::kafka::", 0) == 0 && inner.find("::detail") == std::string::npos)) {
                    namespaces(*ns, inner);
                }
                continue;
            }
            if (path.rfind("confluent::kafka", 0) == 0) {
                member(*d);
            }
        }
    }

    std::vector<const clang::NamedDecl*> decls;

private:
    void add(const clang::NamedDecl* d) {
        if (d != nullptr && !d->isImplicit() && under_include_root(*d, sm_, settings_) && seen_.insert(d).second) {
            decls.push_back(d);
        }
    }

    void member(const clang::Decl& d) {
        if (d.isImplicit()) {
            return;
        }
        if (const auto* t = llvm::dyn_cast<clang::ClassTemplateDecl>(&d)) {
            member(*t->getTemplatedDecl());
            return;
        }
        if (const auto* t = llvm::dyn_cast<clang::FunctionTemplateDecl>(&d)) {
            member(*t->getTemplatedDecl());
            return;
        }
        if (const auto* friend_decl = llvm::dyn_cast<clang::FriendDecl>(&d)) {
            if (const auto* f = friend_decl->getFriendDecl()) {
                member(*f);
            }
            return;
        }
        if (const auto* record = llvm::dyn_cast<clang::CXXRecordDecl>(&d)) {
            if (!record->isThisDeclarationADefinition() || llvm::isa<clang::ClassTemplateSpecializationDecl>(record)) {
                return;
            }
            add(record);
            for (const auto* m : record->decls()) {
                const auto access = m->getAccess();
                if (llvm::isa<clang::FriendDecl>(m) || access == clang::AS_public || access == clang::AS_none) {
                    member(*m);
                }
            }
            return;
        }
        if (const auto* en = llvm::dyn_cast<clang::EnumDecl>(&d)) {
            add(en);
            for (const auto* c : en->enumerators()) {
                add(c);
            }
            return;
        }
        if (const auto* f = llvm::dyn_cast<clang::FunctionDecl>(&d)) {
            const auto* method = llvm::dyn_cast<clang::CXXMethodDecl>(f);
            if (llvm::isa<clang::CXXDestructorDecl>(f) || f->isDefaulted() || f->isDeleted() ||
                (method != nullptr && method->getAccess() != clang::AS_public)) {
                return;
            }
            add(f);
            return;
        }
        if (const auto* v = llvm::dyn_cast<clang::VarDecl>(&d)) {
            add(v);
        }
    }

    const clang::SourceManager& sm_;
    const Settings& settings_;
    std::set<const clang::Decl*> seen_;
};

}  // namespace

std::vector<const clang::NamedDecl*> public_declarations(clang::ASTContext& context, const Settings& settings) {
    Collector collector(context.getSourceManager(), settings);
    collector.namespaces(*context.getTranslationUnitDecl(), "");
    return collector.decls;
}

}  // namespace cklint::tidy
