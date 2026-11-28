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

#include "pairing.hpp"

#include <algorithm>
#include <limits>
#include <map>
#include <set>

#include "clang/AST/DeclCXX.h"
#include "clang/AST/DeclFriend.h"
#include "clang/AST/DeclTemplate.h"
#include "names.hpp"
#include "type_reader.hpp"

namespace cklint::tidy {
namespace {

enum class Group : std::uint8_t { CONSTRUCTOR, MEMBER, FREE };

struct CppFunction {
    const clang::FunctionDecl* decl = nullptr;
    Group group = Group::FREE;
    std::string qualifier;
    std::string name;
    bool paired = false;
};

Group group_of(const rules::ExpectedFunction& f) {
    switch (f.kind) {
        case rules::FunctionKind::CONSTRUCTOR:
            return Group::CONSTRUCTOR;
        case rules::FunctionKind::FREE_FUNCTION:
            return Group::FREE;
        case rules::FunctionKind::CONSTANT_FUNCTION:
            return f.qualifier.empty() ? Group::MEMBER : Group::FREE;
        case rules::FunctionKind::METHOD:
        case rules::FunctionKind::STATIC_METHOD:
            return Group::MEMBER;
    }
    return Group::MEMBER;
}

const clang::FunctionDecl* as_function(const clang::Decl* d) {
    if (const auto* t = llvm::dyn_cast<clang::FunctionTemplateDecl>(d)) {
        return t->getTemplatedDecl();
    }
    return llvm::dyn_cast<clang::FunctionDecl>(d);
}

/// The tag a parameter's type names, references and qualifiers removed.
const clang::TagDecl* tag_of(clang::QualType parameter) {
    const auto type = parameter.getNonReferenceType().getUnqualifiedType();
    if (const auto* tag = type->getAsTagDecl()) {
        return tag;
    }
    if (const auto* injected = type->getAs<clang::InjectedClassNameType>()) {
        return injected->getDecl();
    }
    if (const auto* ts = type->getAs<clang::TemplateSpecializationType>()) {
        if (const auto* td = ts->getTemplateName().getAsTemplateDecl()) {
            return llvm::dyn_cast_or_null<clang::TagDecl>(td->getTemplatedDecl());
        }
    }
    return nullptr;
}

bool is_special_member(const clang::CXXMethodDecl& m) {
    if (llvm::isa<clang::CXXDestructorDecl>(m)) {
        return true;
    }
    if (const auto* c = llvm::dyn_cast<clang::CXXConstructorDecl>(&m)) {
        return c->isCopyOrMoveConstructor();
    }
    return m.isCopyAssignmentOperator() || m.isMoveAssignmentOperator();
}

class Builder {
public:
    Builder(clang::ASTContext& context, const Settings& settings, const JavaSide& java)
        : context_(context), settings_(settings), java_(java) {}

    Pairing build() {
        Pairing out;
        out.cpp_types = public_types(context_, settings_);
        out.types.reserve(out.cpp_types.size());
        for (const auto& t : out.cpp_types) {
            const auto* jt = java_.by_cpp_name(t.qualified_name);
            if (jt == nullptr || !rules::is_public_api(*jt)) {
                continue;
            }
            const auto& e = java_.expected(*jt);
            if (e.shape == rules::TypeShape::ERROR_CLASS) {
                continue;
            }
            index_.emplace(t.decl->getCanonicalDecl(), out.types.size());
            TypePairing tp;
            tp.type = &t;
            tp.java = jt;
            tp.expected = &e;
            out.types.push_back(std::move(tp));
            functions_.emplace_back();
            if (jt->kind == java::TypeKind::ENUM) {
                const auto* ns = llvm::cast<clang::NamespaceDecl>(t.decl->getEnclosingNamespaceContext());
                statics_.emplace(
                    qualified_name_of(*ns, context_) + "::" + rules::enum_statics_namespace(java_.model(), *jt),
                    out.types.size() - 1);
            }
        }
        for (std::size_t i = 0; i < out.types.size(); ++i) {
            if (const auto* rd = llvm::dyn_cast<clang::CXXRecordDecl>(out.types[i].type->decl)) {
                collect_members(*rd, out.types[i], i);
            }
        }
        for (const auto* d : context_.getTranslationUnitDecl()->decls()) {
            if (const auto* ns = llvm::dyn_cast<clang::NamespaceDecl>(d);
                ns != nullptr && ns->getName() == "confluent") {
                walk_namespace(*ns, out);
            }
        }
        for (std::size_t i = 0; i < out.types.size(); ++i) {
            pair(out.types[i], functions_[i]);
        }
        return out;
    }

private:
    bool in_include_root(const clang::Decl& d) const {
        const auto& sm = context_.getSourceManager();
        const auto entry = sm.getFileEntryRefForID(sm.getFileID(sm.getSpellingLoc(d.getLocation())));
        return entry && relative_to_include_root(entry->getName(), settings_).rfind("confluent/kafka/", 0) == 0;
    }

    void add_function(std::size_t owner, const clang::FunctionDecl* f, Group group, std::string qualifier) {
        const auto* canonical = f->getCanonicalDecl();
        if (!seen_.insert(canonical).second) {
            return;
        }
        functions_[owner].push_back(CppFunction{f, group, std::move(qualifier), f->getNameAsString()});
    }

    void collect_members(const clang::CXXRecordDecl& rd, TypePairing& tp, std::size_t owner) {
        for (const auto* d : rd.decls()) {
            if (const auto* field = llvm::dyn_cast<clang::FieldDecl>(d)) {
                if (field->getAccess() == clang::AS_public || field->getAccess() == clang::AS_protected) {
                    tp.fields.push_back(field);
                }
                continue;
            }
            if (const auto* var = llvm::dyn_cast<clang::VarDecl>(d)) {
                if (var->isStaticDataMember() && var->getAccess() == clang::AS_public) {
                    tp.constants.push_back(CppConstant{var, {}});
                }
                continue;
            }
            if (const auto* fr = llvm::dyn_cast<clang::FriendDecl>(d)) {
                if (const auto* f = llvm::dyn_cast_or_null<clang::FunctionDecl>(fr->getFriendDecl())) {
                    add_function(owner, f, Group::FREE, {});
                }
                continue;
            }
            const auto* f = as_function(d);
            const auto* m = llvm::dyn_cast_or_null<clang::CXXMethodDecl>(f);
            if (m == nullptr || m->isImplicit() || m->isDeleted() || m->getAccess() != clang::AS_public ||
                is_special_member(*m)) {
                continue;
            }
            add_function(owner, m, llvm::isa<clang::CXXConstructorDecl>(m) ? Group::CONSTRUCTOR : Group::MEMBER, {});
        }
    }

    void walk_namespace(const clang::NamespaceDecl& ns, Pairing& out) {
        const auto name = qualified_name_of(ns, context_);
        if (ns.getName() == "detail") {
            return;
        }
        const auto statics = statics_.find(name);
        for (const auto* d : ns.decls()) {
            if (const auto* inner = llvm::dyn_cast<clang::NamespaceDecl>(d)) {
                walk_namespace(*inner, out);
                continue;
            }
            if (const auto* var = llvm::dyn_cast<clang::VarDecl>(d)) {
                if (statics != statics_.end() && in_include_root(*var)) {
                    const auto& tp = out.types[statics->second];
                    out.types[statics->second].constants.push_back(CppConstant{
                        var, name.substr(qualified_name_of(*llvm::cast<clang::NamespaceDecl>(
                                                               tp.type->decl->getEnclosingNamespaceContext()),
                                                           context_)
                                             .size() +
                                         2)});
                }
                continue;
            }
            const auto* f = as_function(d);
            if (f == nullptr || !in_include_root(*f)) {
                continue;
            }
            if (statics != statics_.end()) {
                const auto& tp = out.types[statics->second];
                const auto base = qualified_name_of(
                    *llvm::cast<clang::NamespaceDecl>(tp.type->decl->getEnclosingNamespaceContext()), context_);
                add_function(statics->second, f, Group::FREE, name.substr(base.size() + 2));
                continue;
            }
            // The first parameter of a translated type: `operator<<(std::ostream&, const T&)` belongs to T.
            auto it = index_.end();
            for (const auto* param : f->parameters()) {
                const auto* tag = tag_of(param->getType());
                if (tag != nullptr && (it = index_.find(tag->getCanonicalDecl())) != index_.end()) {
                    break;
                }
            }
            if (it == index_.end()) {
                if (seen_.insert(f->getCanonicalDecl()).second) {
                    out.unowned.push_back(f);
                }
                continue;
            }
            add_function(it->second, f, Group::FREE, {});
        }
    }

    /// Whether `std::hash<type>` is specialized. A specialization written at
    /// global scope (`template <> struct std::hash<T>`) is lexically outside
    /// namespace std, so the template's own list of specializations is read.
    bool has_hash_specialization(const clang::TagDecl& type) const {
        auto& idents = context_.Idents;
        for (const auto* d : context_.getTranslationUnitDecl()->decls()) {
            const auto* std_ns = llvm::dyn_cast<clang::NamespaceDecl>(d);
            if (std_ns == nullptr || std_ns->getName() != "std") {
                continue;
            }
            for (const auto* found : std_ns->lookup(clang::DeclarationName(&idents.get("hash")))) {
                const auto* hash = llvm::dyn_cast<clang::ClassTemplateDecl>(found);
                if (hash == nullptr) {
                    continue;
                }
                for (const auto* spec : hash->specializations()) {
                    const auto& args = spec->getTemplateArgs();
                    if (args.size() != 1 || args[0].getKind() != clang::TemplateArgument::Type) {
                        continue;
                    }
                    const auto* tag = args[0].getAsType()->getAsTagDecl();
                    if (tag != nullptr && tag->getCanonicalDecl() == type.getCanonicalDecl()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    void pair(TypePairing& tp, std::vector<CppFunction>& cpp) {
        for (const auto& f : tp.expected->functions) {
            if (f.java_member.empty()) {
                // A §5 convention: present under its name, whatever its parameters.
                bool found = false;
                for (auto& c : cpp) {
                    if (!c.paired && c.name == f.name) {
                        c.paired = true;
                        found = true;
                    }
                }
                if (!found && f.name == "std::hash") {
                    found = has_hash_specialization(*tp.type->decl);
                }
                if (!found) {
                    tp.missing.push_back(&f);
                }
                continue;
            }
            CppFunction* best = nullptr;
            int best_mismatches = std::numeric_limits<int>::max();
            for (auto& c : cpp) {
                if (c.paired || c.group != group_of(f) || c.qualifier != f.qualifier || c.name != f.name ||
                    c.decl->getNumParams() != f.parameters.size()) {
                    continue;
                }
                const int m = mismatches(f, *c.decl, context_);
                if (m < best_mismatches) {
                    best = &c;
                    best_mismatches = m;
                }
            }
            if (best == nullptr) {
                // Java's implicit default constructor is C++'s implicit one.
                const auto* record = llvm::dyn_cast<clang::CXXRecordDecl>(tp.type->decl);
                const bool implicit_default = group_of(f) == Group::CONSTRUCTOR && f.parameters.empty() &&
                                              record != nullptr && !record->hasUserDeclaredConstructor() &&
                                              record->hasDefaultConstructor();
                if (!implicit_default) {
                    tp.missing.push_back(&f);
                }
                continue;
            }
            best->paired = true;
            tp.pairs.push_back(FunctionPair{&f, best->decl});
        }
        for (const auto& c : cpp) {
            if (!c.paired) {
                tp.extra.push_back(c.decl);
            }
        }
    }

    clang::ASTContext& context_;
    const Settings& settings_;
    const JavaSide& java_;
    std::map<const clang::Decl*, std::size_t> index_;
    std::map<std::string, std::size_t> statics_;
    std::vector<std::vector<CppFunction>> functions_;
    std::set<const clang::Decl*> seen_;
};

}  // namespace

const rules::Shape* matching_shape(const rules::Mapping& mapping, const rules::CppType& type) {
    for (const auto& s : mapping.shapes) {
        if (s.type == type) {
            return &s;
        }
    }
    return nullptr;
}

int mismatches(const rules::ExpectedFunction& expected, const clang::FunctionDecl& cpp,
               const clang::ASTContext& context) {
    if (const auto* m = llvm::dyn_cast<clang::CXXMethodDecl>(&cpp); m != nullptr && m->size_overridden_methods() > 0) {
        return 0;  // the base decides an override's types
    }
    int count = 0;
    for (std::size_t i = 0; i < expected.parameters.size() && i < cpp.getNumParams(); ++i) {
        if (matching_shape(expected.parameters[i].mapping, read_type(cpp.getParamDecl(i)->getType(), context)) ==
            nullptr) {
            ++count;
        }
    }
    if (!llvm::isa<clang::CXXConstructorDecl>(cpp) && !expected.returns.shapes.empty() &&
        matching_shape(expected.returns, read_type(cpp.getReturnType(), context)) == nullptr) {
        ++count;
    }
    return count;
}

Pairing pair_all(clang::ASTContext& context, const Settings& settings, const JavaSide& java) {
    return Builder(context, settings, java).build();
}

}  // namespace cklint::tidy
