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

#include "java_model.hpp"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <sstream>
#include <thread>
#include <utility>

#include "JavaLexer.h"
#include "JavaParser.h"
#include "antlr4-runtime.h"

namespace cklint::java {
namespace {

using P = JavaParser;

/// The sentence the package-info.java of an unsupported package carries, the
/// same test as `unsupported_package` in rust/xtask/src/java.rs.
constexpr std::string_view UNSUPPORTED_API_DISCLAIMER = "is not a supported Kafka API";

/// The java.lang types a Kafka source may name without an import.
const std::set<std::string, std::less<>>& java_lang_types() {
    static const std::set<std::string, std::less<>> names = {
        "AbstractMethodError",
        "Appendable",
        "ArithmeticException",
        "ArrayIndexOutOfBoundsException",
        "ArrayStoreException",
        "AssertionError",
        "AutoCloseable",
        "Boolean",
        "Byte",
        "CharSequence",
        "Character",
        "Class",
        "ClassCastException",
        "ClassLoader",
        "ClassNotFoundException",
        "CloneNotSupportedException",
        "Cloneable",
        "Comparable",
        "Deprecated",
        "Double",
        "Enum",
        "Error",
        "Exception",
        "ExceptionInInitializerError",
        "Float",
        "FunctionalInterface",
        "IllegalAccessException",
        "IllegalArgumentException",
        "IllegalMonitorStateException",
        "IllegalStateException",
        "IncompatibleClassChangeError",
        "IndexOutOfBoundsException",
        "InheritableThreadLocal",
        "InstantiationException",
        "Integer",
        "InternalError",
        "InterruptedException",
        "Iterable",
        "LinkageError",
        "Long",
        "Math",
        "Module",
        "NegativeArraySizeException",
        "NoClassDefFoundError",
        "NoSuchFieldError",
        "NoSuchFieldException",
        "NoSuchMethodError",
        "NoSuchMethodException",
        "NullPointerException",
        "Number",
        "NumberFormatException",
        "Object",
        "OutOfMemoryError",
        "Override",
        "Package",
        "Process",
        "ProcessBuilder",
        "ProcessHandle",
        "Readable",
        "Record",
        "ReflectiveOperationException",
        "Runnable",
        "Runtime",
        "RuntimeException",
        "SafeVarargs",
        "SecurityException",
        "Short",
        "StackOverflowError",
        "StackTraceElement",
        "StackWalker",
        "StrictMath",
        "String",
        "StringBuffer",
        "StringBuilder",
        "StringIndexOutOfBoundsException",
        "SuppressWarnings",
        "System",
        "Thread",
        "ThreadGroup",
        "ThreadLocal",
        "Throwable",
        "TypeNotPresentException",
        "UnsatisfiedLinkError",
        "UnsupportedOperationException",
        "VirtualMachineError",
        "Void",
    };
    return names;
}

std::string join(const std::vector<std::string>& parts, std::size_t from, std::size_t to, char sep) {
    std::string out;
    for (std::size_t i = from; i < to; ++i) {
        if (i > from) {
            out += sep;
        }
        out += parts[i];
    }
    return out;
}

std::vector<std::string> split(std::string_view text, char sep) {
    std::vector<std::string> out;
    std::size_t start = 0;
    while (true) {
        const auto end = text.find(sep, start);
        out.emplace_back(text.substr(start, end - start));
        if (end == std::string_view::npos) {
            return out;
        }
        start = end + 1;
    }
}

bool starts_upper(std::string_view s) {
    return !s.empty() && std::isupper(static_cast<unsigned char>(s[0])) != 0;
}

/// The simple name of a binary name: after the last `.` or `$`.
std::string simple_of(std::string_view binary_name) {
    const auto pos = binary_name.find_last_of(".$");
    return std::string(pos == std::string_view::npos ? binary_name : binary_name.substr(pos + 1));
}

/// The text of a context without its outer parentheses.
std::string strip_parens(const std::string& text) {
    if (text.size() >= 2 && text.front() == '(' && text.back() == ')') {
        return text.substr(1, text.size() - 2);
    }
    return text;
}

struct Mods {
    std::uint16_t flags = 0;
    std::vector<Annotation> annotations;
};

/// What a nested declaration needs to know of the type it is declared in.
struct Enclosing {
    std::string binary_name;
    TypeKind kind;
};

/// One parsed file, before name resolution.
struct FileUnit {
    std::string path;
    std::string package;
    bool is_package_info = false;
    bool unsupported_disclaimer = false;
    /// Simple name -> the imported name as written (`java.util.Map`).
    std::map<std::string, std::string> single_imports;
    /// Simple name -> a statically imported member as written, used only when
    /// the name is used as a type.
    std::map<std::string, std::string> static_imports;
    /// The packages (or types) of `import x.y.*;` and `import static x.Y.*;`.
    std::vector<std::string> on_demand_imports;
    std::vector<Type> types;
    std::vector<ModelDiagnostic> diagnostics;
};

class CollectingErrorListener final : public antlr4::BaseErrorListener {
public:
    CollectingErrorListener(std::string path, std::vector<ModelDiagnostic>& out) : path_(std::move(path)), out_(out) {}

    void syntaxError(antlr4::Recognizer* /*recognizer*/, antlr4::Token* /*offending_symbol*/, std::size_t line,
                     std::size_t char_position_in_line, const std::string& msg, std::exception_ptr /*e*/) override {
        out_.push_back(ModelDiagnostic{
            Location{path_, line}, "syntax error at column " + std::to_string(char_position_in_line + 1) + ": " + msg});
    }

private:
    std::string path_;
    std::vector<ModelDiagnostic>& out_;
};

/// Walks one parse tree into the FileUnit's types. Type variables are known
/// here (their scopes are lexical); every other name is left as written for
/// the Resolver.
class Walker {
public:
    Walker(antlr4::CommonTokenStream& tokens, FileUnit& unit) : tokens_(tokens), unit_(unit) {}

    void compilation_unit(P::CompilationunitContext* ctx) {
        if (auto* pkg = ctx->packageDeclaration()) {
            unit_.package = pkg->qualifiedName()->getText();
        }
        for (auto* imp : ctx->importDeclaration()) {
            const auto name = imp->qualifiedName()->getText();
            if (imp->MUL() != nullptr) {
                // `import static a.B.*` brings B's static member types, as `import a.B.*` does.
                unit_.on_demand_imports.push_back(name);
            } else if (imp->STATIC() != nullptr) {
                // Names a static member, which may be a member type:
                // `import static java.lang.Thread.UncaughtExceptionHandler`.
                unit_.static_imports[simple_of(name)] = name;
            } else {
                unit_.single_imports[simple_of(name)] = name;
            }
        }
        for (auto* decl : ctx->typeDeclaration()) {
            Mods mods;
            for (auto* m : decl->classOrInterfaceModifier()) {
                add_modifier(mods, m);
            }
            declare_type(mods, decl, type_declaration_of(decl), nullptr);
        }
    }

private:
    static antlr4::ParserRuleContext* type_declaration_of(P::TypeDeclarationContext* decl) {
        if (decl->classDeclaration() != nullptr) {
            return decl->classDeclaration();
        }
        if (decl->enumDeclaration() != nullptr) {
            return decl->enumDeclaration();
        }
        if (decl->interfaceDeclaration() != nullptr) {
            return decl->interfaceDeclaration();
        }
        if (decl->annotationTypeDeclaration() != nullptr) {
            return decl->annotationTypeDeclaration();
        }
        return decl->recordDeclaration();
    }

    // ---- modifiers and annotations

    Annotation annotation(P::AnnotationContext* ctx) {
        Annotation a;
        a.name = ctx->qualifiedName()->getText();
        if (auto* values = ctx->annotationFieldValues()) {
            a.arguments = strip_parens(text_of(values));
        }
        return a;
    }

    void add_modifier(Mods& mods, P::ClassOrInterfaceModifierContext* m) {
        if (m->annotation() != nullptr) {
            mods.annotations.push_back(annotation(m->annotation()));
        } else if (m->PUBLIC() != nullptr) {
            mods.flags |= MOD_PUBLIC;
        } else if (m->PROTECTED() != nullptr) {
            mods.flags |= MOD_PROTECTED;
        } else if (m->PRIVATE() != nullptr) {
            mods.flags |= MOD_PRIVATE;
        } else if (m->STATIC() != nullptr) {
            mods.flags |= MOD_STATIC;
        } else if (m->ABSTRACT() != nullptr) {
            mods.flags |= MOD_ABSTRACT;
        } else if (m->FINAL() != nullptr) {
            mods.flags |= MOD_FINAL;
        } else if (m->STRICTFP() != nullptr) {
            mods.flags |= MOD_STRICTFP;
        } else if (m->SEALED() != nullptr) {
            mods.flags |= MOD_SEALED;
        } else if (m->NON_SEALED() != nullptr) {
            mods.flags |= MOD_NON_SEALED;
        }
    }

    void add_modifier(Mods& mods, P::ModifierContext* m) {
        if (m->classOrInterfaceModifier() != nullptr) {
            add_modifier(mods, m->classOrInterfaceModifier());
        } else if (m->NATIVE() != nullptr) {
            mods.flags |= MOD_NATIVE;
        } else if (m->SYNCHRONIZED() != nullptr) {
            mods.flags |= MOD_SYNCHRONIZED;
        } else if (m->TRANSIENT() != nullptr) {
            mods.flags |= MOD_TRANSIENT;
        } else if (m->VOLATILE() != nullptr) {
            mods.flags |= MOD_VOLATILE;
        }
    }

    void add_modifier(Mods& mods, P::InterfaceMethodModifierContext* m) {
        if (m->annotation() != nullptr) {
            mods.annotations.push_back(annotation(m->annotation()));
        } else if (m->PUBLIC() != nullptr) {
            mods.flags |= MOD_PUBLIC;
        } else if (m->ABSTRACT() != nullptr) {
            mods.flags |= MOD_ABSTRACT;
        } else if (m->DEFAULT() != nullptr) {
            mods.flags |= MOD_DEFAULT;
        } else if (m->STATIC() != nullptr) {
            mods.flags |= MOD_STATIC;
        } else if (m->STRICTFP() != nullptr) {
            mods.flags |= MOD_STRICTFP;
        }
    }

    template <typename Ctx>
    Mods modifiers(const std::vector<Ctx*>& list) {
        Mods mods;
        for (auto* m : list) {
            add_modifier(mods, m);
        }
        return mods;
    }

    static Visibility visibility_of(std::uint16_t flags) {
        if ((flags & MOD_PUBLIC) != 0) {
            return Visibility::PUBLIC;
        }
        if ((flags & MOD_PROTECTED) != 0) {
            return Visibility::PROTECTED;
        }
        if ((flags & MOD_PRIVATE) != 0) {
            return Visibility::PRIVATE;
        }
        return Visibility::PACKAGE;
    }

    static bool is_interface_like(TypeKind kind) { return kind == TypeKind::INTERFACE || kind == TypeKind::ANNOTATION; }

    /// A member's visibility, with what Java implies in an interface.
    static Visibility member_visibility(std::uint16_t flags, TypeKind owner) {
        if (is_interface_like(owner)) {
            return (flags & MOD_PRIVATE) != 0 ? Visibility::PRIVATE : Visibility::PUBLIC;
        }
        return visibility_of(flags);
    }

    // ---- source text

    static bool is_word_char(char c) {
        return std::isalnum(static_cast<unsigned char>(c)) != 0 || c == '_' || c == '$';
    }

    /// The text of `ctx`'s tokens, a space only between two word-like tokens:
    /// `new Node(-1, "", -1)` -> `new Node(-1,"",-1)`. ANTLR's getText() joins
    /// them with nothing, which would give `newNode(...)`.
    std::string text_of(antlr4::ParserRuleContext* ctx) const {
        std::string out;
        const auto interval = ctx->getSourceInterval();
        for (auto i = interval.a; i >= 0 && i <= interval.b; ++i) {
            auto* token = tokens_.get(static_cast<std::size_t>(i));
            if (token->getChannel() != antlr4::Token::DEFAULT_CHANNEL) {
                continue;
            }
            const auto text = token->getText();
            if (!out.empty() && !text.empty() && is_word_char(out.back()) && is_word_char(text.front())) {
                out += ' ';
            }
            out += text;
        }
        return out;
    }

    // ---- comments and locations

    std::optional<Javadoc> javadoc_before(antlr4::ParserRuleContext* ctx) {
        const auto hidden = tokens_.getHiddenTokensToLeft(ctx->getStart()->getTokenIndex());
        for (auto it = hidden.rbegin(); it != hidden.rend(); ++it) {
            const auto text = (*it)->getText();
            if ((*it)->getType() == JavaLexer::COMMENT && text.rfind("/**", 0) == 0 && text != "/**/") {
                return Javadoc{text, (*it)->getLine()};
            }
        }
        return std::nullopt;
    }

    Location location_of(antlr4::ParserRuleContext* ctx) const {
        return Location{unit_.path, ctx->getStart()->getLine()};
    }

    // ---- types

    bool is_type_variable(const std::string& name) const {
        return std::any_of(scopes_.rbegin(), scopes_.rend(), [&](const auto& scope) { return scope.count(name) != 0; });
    }

    TypeRef named(std::vector<std::string> segments) {
        TypeRef ref;
        if (segments.size() == 1 && is_type_variable(segments.front())) {
            ref.kind = TypeRef::Kind::TYPE_VARIABLE;
            ref.name = segments.front();
            return ref;
        }
        ref.kind = TypeRef::Kind::NAMED;
        ref.name = join(segments, 0, segments.size(), '.');
        ref.written = std::move(segments);
        ref.resolved = false;
        return ref;
    }

    TypeRef class_type(P::ClassTypeContext* ctx) {
        std::vector<std::string> segments;
        std::vector<TypeRef> arguments;
        for (auto* child : ctx->children) {
            if (auto* pkg = dynamic_cast<P::PackageNameContext*>(child)) {
                for (auto* id : pkg->identifier()) {
                    segments.push_back(id->getText());
                }
                arguments.clear();
            } else if (auto* id = dynamic_cast<P::TypeIdentifierContext*>(child)) {
                segments.push_back(id->getText());
                arguments.clear();  // the arguments of an outer segment are not kept
            } else if (auto* args = dynamic_cast<P::TypeArgumentsContext*>(child)) {
                arguments = type_arguments(args);
            }
        }
        auto ref = named(std::move(segments));
        if (ref.kind == TypeRef::Kind::NAMED) {
            ref.arguments = std::move(arguments);
        }
        return ref;
    }

    std::vector<TypeRef> type_arguments(P::TypeArgumentsContext* ctx) {
        std::vector<TypeRef> out;
        for (auto* arg : ctx->typeArgument()) {
            if (arg->QUESTION() != nullptr) {
                TypeRef wildcard;
                wildcard.kind = TypeRef::Kind::WILDCARD;
                if (arg->typeType() != nullptr) {
                    wildcard.bound = arg->EXTENDS() != nullptr ? TypeRef::Bound::EXTENDS : TypeRef::Bound::SUPER;
                    wildcard.arguments.push_back(type_ref(arg->typeType()));
                }
                out.push_back(std::move(wildcard));
            } else {
                out.push_back(type_ref(arg->typeType()));
            }
        }
        return out;
    }

    TypeRef type_ref(P::TypeTypeContext* ctx) {
        TypeRef ref;
        if (ctx->primitiveType() != nullptr) {
            ref.kind = TypeRef::Kind::PRIMITIVE;
            ref.name = ctx->primitiveType()->getText();
        } else {
            ref = class_type(ctx->classOrInterfaceType()->classType());
        }
        ref.dimensions = static_cast<int>(ctx->LBRACK().size());
        return ref;
    }

    TypeRef type_or_void(P::TypeTypeOrVoidContext* ctx) {
        if (ctx->VOID() != nullptr) {
            TypeRef ref;
            ref.kind = TypeRef::Kind::VOID;
            ref.name = "void";
            return ref;
        }
        return type_ref(ctx->typeType());
    }

    TypeRef qualified(P::QualifiedNameContext* ctx) {
        std::vector<std::string> segments;
        for (auto* id : ctx->identifier()) {
            segments.push_back(id->getText());
        }
        return named(std::move(segments));
    }

    std::vector<TypeRef> type_list(P::TypeListContext* ctx) {
        std::vector<TypeRef> out;
        for (auto* t : ctx->typeType()) {
            out.push_back(type_ref(t));
        }
        return out;
    }

    /// Opens the scope of the type parameters first, as a bound may name them
    /// (`<T extends Comparable<T>>`); the caller closes it.
    std::vector<TypeParameter> open_type_parameters(P::TypeParametersContext* ctx) {
        scopes_.emplace_back();
        std::vector<TypeParameter> out;
        if (ctx == nullptr) {
            return out;
        }
        for (auto* tp : ctx->typeParameter()) {
            scopes_.back().insert(tp->identifier()->getText());
        }
        for (auto* tp : ctx->typeParameter()) {
            TypeParameter p;
            p.name = tp->identifier()->getText();
            if (auto* bound = tp->typeBound()) {
                for (auto* t : bound->typeType()) {
                    p.bounds.push_back(type_ref(t));
                }
            }
            out.push_back(std::move(p));
        }
        return out;
    }

    void close_type_parameters() { scopes_.pop_back(); }

    // ---- declarations

    /// Declares the type `decl` (a class, enum, interface, annotation or
    /// record declaration) and its members; returns its binary name.
    std::string declare_type(const Mods& mods, antlr4::ParserRuleContext* doc_anchor, antlr4::ParserRuleContext* decl,
                             const Enclosing* outer) {
        Type t;
        t.package = unit_.package;
        t.modifiers = mods.flags;
        t.annotations = mods.annotations;
        t.javadoc = javadoc_before(doc_anchor);
        t.location = location_of(decl);
        P::IdentifierContext* name = nullptr;
        P::TypeParametersContext* tparams = nullptr;
        if (auto* c = dynamic_cast<P::ClassDeclarationContext*>(decl)) {
            t.kind = TypeKind::CLASS;
            name = c->identifier();
            tparams = c->typeParameters();
        } else if (auto* e = dynamic_cast<P::EnumDeclarationContext*>(decl)) {
            t.kind = TypeKind::ENUM;
            name = e->identifier();
        } else if (auto* i = dynamic_cast<P::InterfaceDeclarationContext*>(decl)) {
            t.kind = TypeKind::INTERFACE;
            name = i->identifier();
            tparams = i->typeParameters();
        } else if (auto* a = dynamic_cast<P::AnnotationTypeDeclarationContext*>(decl)) {
            t.kind = TypeKind::ANNOTATION;
            name = a->identifier();
        } else if (auto* r = dynamic_cast<P::RecordDeclarationContext*>(decl)) {
            t.kind = TypeKind::RECORD;
            name = r->identifier();
            tparams = r->typeParameters();
        }
        t.simple_name = name->getText();
        if (outer != nullptr) {
            t.enclosing = outer->binary_name;
            t.binary_name = outer->binary_name + "$" + t.simple_name;
            t.visibility = member_visibility(mods.flags, outer->kind);
        } else {
            t.binary_name = unit_.package.empty() ? t.simple_name : unit_.package + "." + t.simple_name;
            t.visibility = visibility_of(mods.flags);
        }

        t.type_parameters = open_type_parameters(tparams);
        const Enclosing self{t.binary_name, t.kind};
        if (auto* c = dynamic_cast<P::ClassDeclarationContext*>(decl)) {
            if (c->EXTENDS() != nullptr) {
                t.extends.push_back(type_ref(c->typeType()));
            }
            if (c->IMPLEMENTS() != nullptr) {
                t.implements = type_list(c->typeList(0));  // `implements` comes before `permits`
            }
            for (auto* member : c->classBody()->classBodyDeclaration()) {
                class_member(t, self, member);
            }
        } else if (auto* e = dynamic_cast<P::EnumDeclarationContext*>(decl)) {
            if (e->IMPLEMENTS() != nullptr) {
                t.implements = type_list(e->typeList());
            }
            if (auto* constants = e->enumConstants()) {
                for (auto* constant : constants->enumConstant()) {
                    enum_constant(t, constant);
                }
            }
            if (auto* body = e->enumBodyDeclarations()) {
                for (auto* member : body->classBodyDeclaration()) {
                    class_member(t, self, member);
                }
            }
        } else if (auto* i = dynamic_cast<P::InterfaceDeclarationContext*>(decl)) {
            if (i->EXTENDS() != nullptr) {
                t.extends = type_list(i->typeList(0));
            }
            for (auto* member : i->interfaceBody()->interfaceBodyDeclaration()) {
                interface_member(t, self, member);
            }
        } else if (auto* a = dynamic_cast<P::AnnotationTypeDeclarationContext*>(decl)) {
            for (auto* element : a->annotationTypeBody()->annotationTypeElementDeclaration()) {
                annotation_element(t, self, element);
            }
        } else if (auto* r = dynamic_cast<P::RecordDeclarationContext*>(decl)) {
            if (r->IMPLEMENTS() != nullptr) {
                t.implements = type_list(r->typeList());
            }
            record_members(t, self, r);
        }
        close_type_parameters();
        add_implicit_members(t);
        auto binary_name = t.binary_name;
        unit_.types.push_back(std::move(t));
        return binary_name;
    }

    void class_member(Type& t, const Enclosing& self, P::ClassBodyDeclarationContext* cbd) {
        auto* member = cbd->memberDeclaration();
        if (member == nullptr) {
            return;  // `;` or an initializer block
        }
        const auto mods = modifiers(cbd->modifier());
        if (auto* m = member->methodDeclaration()) {
            method(t, mods, cbd, nullptr, m);
        } else if (auto* g = member->genericMethodDeclaration()) {
            method(t, mods, cbd, g->typeParameters(), g->methodDeclaration());
        } else if (auto* f = member->fieldDeclaration()) {
            fields(t, mods, cbd, f->typeType(), f->variableDeclarators());
        } else if (auto* c = member->constructorDeclaration()) {
            constructor(t, mods, cbd, nullptr, c);
        } else if (auto* gc = member->genericConstructorDeclaration()) {
            constructor(t, mods, cbd, gc->typeParameters(), gc->constructorDeclaration());
        } else if (member->interfaceDeclaration() != nullptr) {
            t.nested.push_back(declare_type(mods, cbd, member->interfaceDeclaration(), &self));
        } else if (member->annotationTypeDeclaration() != nullptr) {
            t.nested.push_back(declare_type(mods, cbd, member->annotationTypeDeclaration(), &self));
        } else if (member->classDeclaration() != nullptr) {
            t.nested.push_back(declare_type(mods, cbd, member->classDeclaration(), &self));
        } else if (member->enumDeclaration() != nullptr) {
            t.nested.push_back(declare_type(mods, cbd, member->enumDeclaration(), &self));
        } else if (member->recordDeclaration() != nullptr) {
            t.nested.push_back(declare_type(mods, cbd, member->recordDeclaration(), &self));
        }
    }

    void interface_member(Type& t, const Enclosing& self, P::InterfaceBodyDeclarationContext* ibd) {
        auto* member = ibd->interfaceMemberDeclaration();
        if (member == nullptr) {
            return;
        }
        auto mods = modifiers(ibd->modifier());
        if (auto* c = member->constDeclaration()) {
            const auto type = type_ref(c->typeType());
            const auto javadoc = javadoc_before(ibd);
            for (auto* d : c->constantDeclarator()) {
                Field f;
                f.name = d->identifier()->getText();
                f.visibility = Visibility::PUBLIC;
                f.modifiers = mods.flags;
                f.annotations = mods.annotations;
                f.type = type;
                f.type.dimensions += static_cast<int>(d->LBRACK().size());
                f.initializer = text_of(d->variableInitializer());
                f.javadoc = javadoc;
                f.location = location_of(d);
                t.fields.push_back(std::move(f));
            }
        } else if (auto* m = member->interfaceMethodDeclaration()) {
            for (auto* mm : m->interfaceMethodModifier()) {
                add_modifier(mods, mm);
            }
            interface_method(t, mods, ibd, nullptr, m->interfaceCommonBodyDeclaration());
        } else if (auto* g = member->genericInterfaceMethodDeclaration()) {
            for (auto* mm : g->interfaceMethodModifier()) {
                add_modifier(mods, mm);
            }
            interface_method(t, mods, ibd, g->typeParameters(), g->interfaceCommonBodyDeclaration());
        } else if (member->interfaceDeclaration() != nullptr) {
            t.nested.push_back(declare_type(mods, ibd, member->interfaceDeclaration(), &self));
        } else if (member->annotationTypeDeclaration() != nullptr) {
            t.nested.push_back(declare_type(mods, ibd, member->annotationTypeDeclaration(), &self));
        } else if (member->classDeclaration() != nullptr) {
            t.nested.push_back(declare_type(mods, ibd, member->classDeclaration(), &self));
        } else if (member->enumDeclaration() != nullptr) {
            t.nested.push_back(declare_type(mods, ibd, member->enumDeclaration(), &self));
        } else if (member->recordDeclaration() != nullptr) {
            t.nested.push_back(declare_type(mods, ibd, member->recordDeclaration(), &self));
        }
    }

    void annotation_element(Type& t, const Enclosing& self, P::AnnotationTypeElementDeclarationContext* ctx) {
        auto* rest = ctx->annotationTypeElementRest();
        if (rest == nullptr) {
            return;
        }
        const auto mods = modifiers(ctx->modifier());
        if (rest->typeType() != nullptr) {
            auto* mc = rest->annotationMethodOrConstantRest();
            if (auto* m = mc->annotationMethodRest()) {
                Method method;
                method.name = m->identifier()->getText();
                method.visibility = Visibility::PUBLIC;
                method.modifiers = mods.flags;
                method.annotations = mods.annotations;
                method.return_type = type_ref(rest->typeType());
                method.javadoc = javadoc_before(ctx);
                method.location = location_of(m);
                t.methods.push_back(std::move(method));
            } else if (auto* c = mc->annotationConstantRest()) {
                fields(t, mods, ctx, rest->typeType(), c->variableDeclarators());
            }
        } else if (rest->classDeclaration() != nullptr) {
            t.nested.push_back(declare_type(mods, ctx, rest->classDeclaration(), &self));
        } else if (rest->interfaceDeclaration() != nullptr) {
            t.nested.push_back(declare_type(mods, ctx, rest->interfaceDeclaration(), &self));
        } else if (rest->enumDeclaration() != nullptr) {
            t.nested.push_back(declare_type(mods, ctx, rest->enumDeclaration(), &self));
        } else if (rest->annotationTypeDeclaration() != nullptr) {
            t.nested.push_back(declare_type(mods, ctx, rest->annotationTypeDeclaration(), &self));
        } else if (rest->recordDeclaration() != nullptr) {
            t.nested.push_back(declare_type(mods, ctx, rest->recordDeclaration(), &self));
        }
    }

    void record_members(Type& t, const Enclosing& self, P::RecordDeclarationContext* r) {
        if (auto* list = r->recordHeader()->recordComponentList()) {
            for (auto* c : list->recordComponent()) {
                Field f;
                f.name = c->identifier()->getText();
                f.visibility = Visibility::PRIVATE;
                f.modifiers = MOD_PRIVATE | MOD_FINAL;
                f.type = type_ref(c->typeType());
                if (c->ELLIPSIS() != nullptr) {
                    ++f.type.dimensions;
                }
                f.location = location_of(c);
                t.fields.push_back(std::move(f));
            }
        }
        auto* body = r->recordBody();
        for (auto* member : body->classBodyDeclaration()) {
            class_member(t, self, member);
        }
        for (auto* compact : body->compactConstructorDeclaration()) {
            Method ctor;
            ctor.name = t.simple_name;
            ctor.is_constructor = true;
            const auto mods = modifiers(compact->modifier());
            ctor.modifiers = mods.flags;
            ctor.annotations = mods.annotations;
            ctor.visibility = visibility_of(mods.flags);
            ctor.return_type.kind = TypeRef::Kind::VOID;
            for (const auto& f : t.fields) {
                ctor.parameters.push_back(Parameter{f.name, f.type, false, false, {}});
            }
            ctor.javadoc = javadoc_before(compact);
            ctor.location = location_of(compact);
            body_facts(ctor, compact->block());
            t.constructors.push_back(std::move(ctor));
        }
    }

    void fields(Type& t, const Mods& mods, antlr4::ParserRuleContext* doc_anchor, P::TypeTypeContext* type_ctx,
                P::VariableDeclaratorsContext* declarators) {
        const auto type = type_ref(type_ctx);
        const auto javadoc = javadoc_before(doc_anchor);
        for (auto* d : declarators->variableDeclarator()) {
            Field f;
            f.name = d->variableDeclaratorId()->identifier()->getText();
            f.visibility = member_visibility(mods.flags, t.kind);
            f.modifiers = mods.flags;
            f.annotations = mods.annotations;
            f.type = type;
            f.type.dimensions += static_cast<int>(d->variableDeclaratorId()->LBRACK().size());
            if (d->variableInitializer() != nullptr) {
                f.initializer = text_of(d->variableInitializer());
            }
            f.javadoc = javadoc;
            f.location = location_of(d);
            t.fields.push_back(std::move(f));
        }
    }

    void enum_constant(Type& t, P::EnumConstantContext* ctx) {
        EnumConstant c;
        c.name = ctx->identifier()->getText();
        for (auto* a : ctx->annotation()) {
            c.annotations.push_back(annotation(a));
        }
        if (auto* args = ctx->arguments(); args != nullptr && args->expressionList() != nullptr) {
            for (auto* e : args->expressionList()->expression()) {
                c.arguments.push_back(text_of(e));
            }
        }
        c.has_body = ctx->classBody() != nullptr;
        c.javadoc = javadoc_before(ctx);
        c.location = location_of(ctx);
        t.enum_constants.push_back(std::move(c));
    }

    std::vector<Parameter> parameters(P::FormalParametersContext* ctx) {
        std::vector<Parameter> out;
        auto add = [&](P::FormalParameterContext* fp) {
            Parameter p;
            for (auto* vm : fp->variableModifier()) {
                if (vm->FINAL() != nullptr) {
                    p.is_final = true;
                } else if (vm->annotation() != nullptr) {
                    p.annotations.push_back(annotation(vm->annotation()));
                }
            }
            p.type = type_ref(fp->typeType());
            p.type.dimensions += static_cast<int>(fp->variableDeclaratorId()->LBRACK().size());
            p.varargs = fp->ELLIPSIS() != nullptr;
            p.name = fp->variableDeclaratorId()->identifier()->getText();
            out.push_back(std::move(p));
        };
        if (ctx->formalParameter() != nullptr) {
            add(ctx->formalParameter());
        }
        for (auto* list : ctx->formalParameterList()) {
            for (auto* fp : list->formalParameter()) {
                add(fp);
            }
        }
        return out;
    }

    std::vector<TypeRef> throws_of(P::QualifiedNameListContext* ctx) {
        std::vector<TypeRef> out;
        if (ctx != nullptr) {
            for (auto* q : ctx->qualifiedName()) {
                out.push_back(qualified(q));
            }
        }
        return out;
    }

    void method(Type& t, const Mods& mods, antlr4::ParserRuleContext* doc_anchor, P::TypeParametersContext* tparams,
                P::MethodDeclarationContext* ctx) {
        Method m;
        m.name = ctx->identifier()->getText();
        m.modifiers = mods.flags;
        m.annotations = mods.annotations;
        m.visibility = member_visibility(mods.flags, t.kind);
        m.type_parameters = open_type_parameters(tparams);
        m.return_type = type_or_void(ctx->typeTypeOrVoid());
        m.return_type.dimensions += static_cast<int>(ctx->LBRACK().size());
        m.parameters = parameters(ctx->formalParameters());
        m.throws = throws_of(ctx->qualifiedNameList());
        close_type_parameters();
        m.javadoc = javadoc_before(doc_anchor);
        m.location = location_of(ctx);
        body_facts(m, ctx->methodBody()->block());
        t.methods.push_back(std::move(m));
    }

    void interface_method(Type& t, Mods mods, antlr4::ParserRuleContext* doc_anchor, P::TypeParametersContext* tparams,
                          P::InterfaceCommonBodyDeclarationContext* ctx) {
        for (auto* a : ctx->annotation()) {
            mods.annotations.push_back(annotation(a));
        }
        Method m;
        m.name = ctx->identifier()->getText();
        m.modifiers = mods.flags;
        m.annotations = mods.annotations;
        m.visibility = member_visibility(mods.flags, t.kind);
        m.type_parameters = open_type_parameters(tparams);
        m.return_type = type_or_void(ctx->typeTypeOrVoid());
        m.return_type.dimensions += static_cast<int>(ctx->LBRACK().size());
        m.parameters = parameters(ctx->formalParameters());
        m.throws = throws_of(ctx->qualifiedNameList());
        close_type_parameters();
        m.javadoc = javadoc_before(doc_anchor);
        m.location = location_of(ctx);
        body_facts(m, ctx->methodBody()->block());
        t.methods.push_back(std::move(m));
    }

    void constructor(Type& t, const Mods& mods, antlr4::ParserRuleContext* doc_anchor,
                     P::TypeParametersContext* tparams, P::ConstructorDeclarationContext* ctx) {
        Method m;
        m.name = ctx->identifier()->getText();
        m.is_constructor = true;
        m.modifiers = mods.flags;
        m.annotations = mods.annotations;
        // An enum's constructors are private whatever they say.
        m.visibility = t.kind == TypeKind::ENUM ? Visibility::PRIVATE : visibility_of(mods.flags);
        m.return_type.kind = TypeRef::Kind::VOID;
        m.return_type.name = "void";
        m.type_parameters = open_type_parameters(tparams);
        m.parameters = parameters(ctx->formalParameters());
        m.throws = throws_of(ctx->qualifiedNameList());
        close_type_parameters();
        m.javadoc = javadoc_before(doc_anchor);
        m.location = location_of(ctx);
        body_facts(m, ctx->block());
        t.constructors.push_back(std::move(m));
    }

    // ---- bodies

    std::optional<Delegation> as_call(P::ExpressionContext* e) const {
        P::MethodCallContext* call = nullptr;
        Delegation d;
        if (auto* mc = dynamic_cast<P::MethodCallExpressionContext*>(e)) {
            call = mc->methodCall();
        } else if (auto* mr = dynamic_cast<P::MemberReferenceExpressionContext*>(e)) {
            call = mr->methodCall();
            if (call != nullptr) {
                d.receiver = text_of(mr->expression());
            }
        }
        if (call == nullptr) {
            return std::nullopt;
        }
        if (call->identifier() != nullptr) {
            d.target = call->identifier()->getText();
        } else if (call->THIS() != nullptr) {
            d.target = "this";
        } else {
            d.target = "super";
        }
        if (auto* list = call->arguments()->expressionList()) {
            for (auto* arg : list->expression()) {
                d.arguments.push_back(text_of(arg));
            }
        }
        return d;
    }

    /// The call a statement consists of, with or without `return`.
    std::optional<Delegation> statement_call(P::BlockStatementContext* bs, bool allow_return) const {
        auto* s = bs->statement();
        if (s == nullptr) {
            return std::nullopt;
        }
        if (s->statementExpression != nullptr) {
            return as_call(s->statementExpression);
        }
        if (allow_return && s->RETURN() != nullptr && s->expression().size() == 1) {
            return as_call(s->expression(0));
        }
        return std::nullopt;
    }

    /// The `return` statements of a body, not those of a lambda or a nested class.
    static void collect_returns(antlr4::tree::ParseTree* node, std::vector<P::StatementContext*>& out) {
        if (dynamic_cast<P::LambdaExpressionContext*>(node) != nullptr ||
            dynamic_cast<P::ClassBodyContext*>(node) != nullptr ||
            dynamic_cast<P::LocalTypeDeclarationContext*>(node) != nullptr) {
            return;
        }
        if (auto* s = dynamic_cast<P::StatementContext*>(node); s != nullptr && s->RETURN() != nullptr) {
            out.push_back(s);
        }
        for (auto* child : node->children) {
            collect_returns(child, out);
        }
    }

    void body_facts(Method& m, P::BlockContext* block) const {
        m.has_body = block != nullptr;
        if (block == nullptr) {
            return;
        }
        const auto statements = block->blockStatement();
        if (m.is_constructor) {
            // `this(...)` comes first, whatever follows it.
            if (!statements.empty()) {
                if (auto call = statement_call(statements.front(), false);
                    call && call->receiver.empty() && call->target == "this") {
                    m.delegation = std::move(call);
                }
            }
        } else if (statements.size() == 1) {
            m.delegation = statement_call(statements.front(), true);
        }

        std::vector<P::StatementContext*> returns;
        collect_returns(block, returns);
        m.returns_this = !returns.empty() && std::all_of(returns.begin(), returns.end(), [this](auto* r) {
            return r->expression().size() == 1 && text_of(r->expression(0)) == "this";
        });

        for (auto* bs : statements) {
            auto* s = bs->statement();
            if (s == nullptr || s->statementExpression == nullptr) {
                continue;
            }
            auto* assign = dynamic_cast<P::BinaryOperatorExpressionContext*>(s->statementExpression);
            if (assign == nullptr || assign->bop == nullptr || assign->bop->getText() != "=") {
                continue;
            }
            auto target = text_of(assign->expression(0));
            if (target.rfind("this.", 0) == 0) {
                target = target.substr(5);
            }
            m.assigned.push_back(std::move(target));
        }
    }

    /// What the compiler declares: a class's default constructor, an enum's
    /// `values()` and `valueOf(String)`, a record's accessors and canonical constructor.
    static void add_implicit_members(Type& t) {
        auto implicit_constructor = [&t](Visibility visibility, std::vector<Parameter> parameters) {
            Method ctor;
            ctor.name = t.simple_name;
            ctor.is_constructor = true;
            ctor.implicit = true;
            ctor.visibility = visibility;
            ctor.return_type.kind = TypeRef::Kind::VOID;
            ctor.return_type.name = "void";
            ctor.parameters = std::move(parameters);
            ctor.location = t.location;
            t.constructors.push_back(std::move(ctor));
        };
        auto self_ref = [&t](int dimensions) {
            TypeRef ref;
            ref.name = t.binary_name;
            ref.dimensions = dimensions;
            return ref;
        };
        switch (t.kind) {
            case TypeKind::CLASS:
                if (t.constructors.empty()) {
                    // JLS 8.8.9: the default constructor has the class's access.
                    implicit_constructor(t.visibility, {});
                }
                break;
            case TypeKind::ENUM: {
                if (t.constructors.empty()) {
                    implicit_constructor(Visibility::PRIVATE, {});
                }
                Method values;
                values.name = "values";
                values.implicit = true;
                values.visibility = Visibility::PUBLIC;
                values.modifiers = MOD_PUBLIC | MOD_STATIC;
                values.return_type = self_ref(1);
                values.location = t.location;
                t.methods.push_back(values);
                Method value_of = values;
                value_of.name = "valueOf";
                value_of.return_type = self_ref(0);
                TypeRef string;
                string.name = "java.lang.String";
                string.written = {"String"};
                value_of.parameters.push_back(Parameter{"name", string, false, false, {}});
                t.methods.push_back(std::move(value_of));
                break;
            }
            case TypeKind::RECORD: {
                if (t.constructors.empty()) {
                    std::vector<Parameter> parameters;
                    for (const auto& f : t.fields) {
                        parameters.push_back(Parameter{f.name, f.type, false, false, {}});
                    }
                    implicit_constructor(t.visibility, std::move(parameters));
                }
                for (const auto& f : t.fields) {
                    const bool declared = std::any_of(t.methods.begin(), t.methods.end(), [&](const Method& m) {
                        return m.name == f.name && m.parameters.empty();
                    });
                    if (declared) {
                        continue;
                    }
                    Method accessor;
                    accessor.name = f.name;
                    accessor.implicit = true;
                    accessor.visibility = Visibility::PUBLIC;
                    accessor.modifiers = MOD_PUBLIC;
                    accessor.return_type = f.type;
                    accessor.location = f.location;
                    t.methods.push_back(std::move(accessor));
                }
                break;
            }
            case TypeKind::INTERFACE:
            case TypeKind::ANNOTATION:
                break;
        }
    }

    antlr4::CommonTokenStream& tokens_;
    FileUnit& unit_;
    std::vector<std::set<std::string>> scopes_;
};

FileUnit parse_unit(const std::string& path, const std::string& text) {
    FileUnit unit;
    unit.path = path;
    unit.is_package_info = std::filesystem::path(path).filename() == "package-info.java";
    unit.unsupported_disclaimer = unit.is_package_info && text.find(UNSUPPORTED_API_DISCLAIMER) != std::string::npos;

    CollectingErrorListener listener(path, unit.diagnostics);
    antlr4::ANTLRInputStream input(text);
    JavaLexer lexer(&input);
    lexer.removeErrorListeners();
    lexer.addErrorListener(&listener);
    antlr4::CommonTokenStream tokens(&lexer);
    tokens.fill();
    JavaParser parser(&tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(&listener);
    // Full LL, as in java_syntax.cpp: SLL mispredicts this grammar.
    parser.getInterpreter<antlr4::atn::ParserATNSimulator>()->setPredictionMode(antlr4::atn::PredictionMode::LL);
    auto* tree = parser.compilationunit();
    if (unit.diagnostics.empty()) {
        Walker(tokens, unit).compilation_unit(tree);
    }
    return unit;
}

/// Resolves every written type name of the merged model.
class Resolver {
public:
    Resolver(Model& model, const std::vector<FileUnit>& units, const std::map<std::string, std::size_t>& unit_of)
        : model_(model), units_(units), unit_of_(unit_of) {}

    void run() {
        for (auto& [binary_name, t] : model_.types) {
            const auto& unit = units_[unit_of_.at(binary_name)];
            ensure_supertypes(binary_name);
            for (auto& a : t.annotations) {
                resolve_annotation(a, t, unit, t.location);
            }
            for (auto& tp : t.type_parameters) {
                for (auto& b : tp.bounds) {
                    resolve(b, t, unit, t.location);
                }
            }
            for (auto& f : t.fields) {
                resolve(f.type, t, unit, f.location);
                for (auto& a : f.annotations) {
                    resolve_annotation(a, t, unit, f.location);
                }
            }
            for (auto* list : {&t.constructors, &t.methods}) {
                for (auto& m : *list) {
                    resolve_method(m, t, unit);
                }
            }
            for (auto& c : t.enum_constants) {
                for (auto& a : c.annotations) {
                    resolve_annotation(a, t, unit, c.location);
                }
            }
        }
    }

private:
    void resolve_method(Method& m, const Type& t, const FileUnit& unit) {
        for (auto& a : m.annotations) {
            resolve_annotation(a, t, unit, m.location);
        }
        for (auto& tp : m.type_parameters) {
            for (auto& b : tp.bounds) {
                resolve(b, t, unit, m.location);
            }
        }
        resolve(m.return_type, t, unit, m.location);
        for (auto& p : m.parameters) {
            resolve(p.type, t, unit, m.location);
            for (auto& a : p.annotations) {
                resolve_annotation(a, t, unit, m.location);
            }
        }
        for (auto& e : m.throws) {
            resolve(e, t, unit, m.location);
        }
    }

    void resolve_annotation(Annotation& a, const Type& ctx, const FileUnit& unit, const Location& where) {
        TypeRef ref;
        ref.written = split(a.name, '.');
        ref.name = a.name;
        ref.resolved = false;
        resolve(ref, ctx, unit, where);
        a.name = ref.name;
    }

    void resolve(TypeRef& ref, const Type& ctx, const FileUnit& unit, const Location& where) {
        for (auto& arg : ref.arguments) {
            resolve(arg, ctx, unit, where);
        }
        if (ref.kind != TypeRef::Kind::NAMED || ref.resolved) {
            return;
        }
        if (auto name = resolve_segments(ref.written, ctx, unit)) {
            ref.name = std::move(*name);
            ref.resolved = true;
        } else {
            model_.diagnostics.push_back(
                ModelDiagnostic{where, "cannot resolve type " + ref.name + " in " + ctx.binary_name});
        }
    }

    const Type* find(const std::string& binary_name) const {
        const auto it = model_.types.find(binary_name);
        return it == model_.types.end() ? nullptr : &it->second;
    }

    bool is_kafka_type(const std::string& binary_name) const { return find(binary_name) != nullptr; }

    /// Resolves supertypes before their member types are searched; a type
    /// whose supertypes are being resolved is searched without them.
    void ensure_supertypes(const std::string& binary_name) {
        auto& state = supertypes_state_[binary_name];
        if (state != 0) {
            return;
        }
        state = 1;
        auto& t = model_.types.at(binary_name);
        const auto& unit = units_[unit_of_.at(binary_name)];
        for (auto* list : {&t.extends, &t.implements}) {
            for (auto& ref : *list) {
                resolve(ref, t, unit, t.location);
            }
        }
        state = 2;
    }

    /// The member type `simple` of `owner`, declared or inherited.
    std::optional<std::string> member_type(const std::string& owner, const std::string& simple, int depth = 0) {
        const auto* t = find(owner);
        if (t == nullptr || depth > 32) {
            return std::nullopt;
        }
        for (const auto& n : t->nested) {
            if (simple_of(n) == simple) {
                return n;
            }
        }
        ensure_supertypes(owner);
        for (const auto* list : {&t->extends, &t->implements}) {
            for (const auto& ref : *list) {
                if (ref.kind == TypeRef::Kind::NAMED && ref.resolved && is_kafka_type(ref.name)) {
                    if (auto found = member_type(ref.name, simple, depth + 1)) {
                        return found;
                    }
                }
            }
        }
        return std::nullopt;
    }

    /// A dotted name that does not start inside the current scope: the
    /// longest prefix that is a Kafka top-level type, then member types; else
    /// a JDK name, split where the segments turn upper case.
    std::optional<std::string> resolve_qualified(const std::vector<std::string>& segments) {
        for (std::size_t k = segments.size(); k >= 1; --k) {
            const auto prefix = join(segments, 0, k, '.');
            if (is_kafka_type(prefix)) {
                return descend(prefix, segments, k);
            }
        }
        std::size_t first_class = 0;
        while (first_class < segments.size() && !starts_upper(segments[first_class])) {
            ++first_class;
        }
        if (first_class == 0 || first_class == segments.size()) {
            return std::nullopt;
        }
        return join(segments, 0, first_class + 1, '.') +
               (first_class + 1 < segments.size() ? "$" + join(segments, first_class + 1, segments.size(), '$') : "");
    }

    /// Appends the member types `segments[from..]` to `base`.
    std::optional<std::string> descend(std::string base, const std::vector<std::string>& segments, std::size_t from) {
        for (std::size_t i = from; i < segments.size(); ++i) {
            if (is_kafka_type(base)) {
                auto member = member_type(base, segments[i]);
                if (!member) {
                    return std::nullopt;
                }
                base = std::move(*member);
            } else {
                base += "$" + segments[i];
            }
        }
        return base;
    }

    std::optional<std::string> lookup_simple(const std::string& simple, const Type& ctx, const FileUnit& unit) {
        for (const Type* t = &ctx; t != nullptr; t = t->enclosing.empty() ? nullptr : find(t->enclosing)) {
            if (t->simple_name == simple) {
                return t->binary_name;
            }
            if (auto member = member_type(t->binary_name, simple)) {
                return member;
            }
        }
        if (const auto it = unit.single_imports.find(simple); it != unit.single_imports.end()) {
            return resolve_qualified(split(it->second, '.'));
        }
        if (const auto it = unit.static_imports.find(simple); it != unit.static_imports.end()) {
            if (auto member = resolve_qualified(split(it->second, '.'))) {
                return member;
            }
        }
        const auto same_package = unit.package.empty() ? simple : unit.package + "." + simple;
        if (is_kafka_type(same_package)) {
            return same_package;
        }
        for (const auto& on_demand : unit.on_demand_imports) {
            if (auto container = resolve_qualified(split(on_demand, '.')); container && is_kafka_type(*container)) {
                if (auto member = member_type(*container, simple)) {
                    return member;
                }
            }
            if (is_kafka_type(on_demand + "." + simple)) {
                return on_demand + "." + simple;
            }
        }
        if (java_lang_types().count(simple) != 0) {
            return "java.lang." + simple;
        }
        return std::nullopt;
    }

    std::optional<std::string> resolve_segments(const std::vector<std::string>& segments, const Type& ctx,
                                                const FileUnit& unit) {
        if (segments.empty()) {
            return std::nullopt;
        }
        if (auto base = lookup_simple(segments.front(), ctx, unit)) {
            return descend(std::move(*base), segments, 1);
        }
        return resolve_qualified(segments);
    }

    Model& model_;
    const std::vector<FileUnit>& units_;
    const std::map<std::string, std::size_t>& unit_of_;
    std::map<std::string, int> supertypes_state_;
};

std::vector<FileUnit> parse_units(const std::vector<std::pair<std::string, std::string>>& sources, unsigned jobs) {
    std::vector<FileUnit> units(sources.size());
    std::atomic<std::size_t> next{0};
    auto worker = [&] {
        for (std::size_t i = next++; i < sources.size(); i = next++) {
            units[i] = parse_unit(sources[i].first, sources[i].second);
        }
    };
    std::vector<std::thread> threads;
    for (unsigned t = 1; t < jobs; ++t) {
        threads.emplace_back(worker);
    }
    worker();
    for (auto& thread : threads) {
        thread.join();
    }
    return units;
}

Model merge(std::vector<FileUnit> units) {
    Model model;
    model.files = units.size();
    std::map<std::string, std::size_t> unit_of;
    for (std::size_t i = 0; i < units.size(); ++i) {
        auto& unit = units[i];
        for (auto& d : unit.diagnostics) {
            model.diagnostics.push_back(std::move(d));
        }
        unit.diagnostics.clear();
        if (unit.unsupported_disclaimer) {
            model.unsupported_packages.insert(unit.package);
        }
        for (auto& t : unit.types) {
            auto name = t.binary_name;
            if (!model.types.emplace(name, std::move(t)).second) {
                model.diagnostics.push_back(ModelDiagnostic{Location{unit.path, 0}, "type declared twice: " + name});
                continue;
            }
            unit_of.emplace(std::move(name), i);
        }
        unit.types.clear();
    }

    Resolver(model, units, unit_of).run();

    for (auto& [name, t] : model.types) {
        const Type* top = &t;
        bool deprecated = t.has_annotation(DEPRECATED);
        while (!top->enclosing.empty()) {
            top = &model.types.at(top->enclosing);
            deprecated = deprecated || top->has_annotation(DEPRECATED);
        }
        t.audience_public = top->has_annotation(AUDIENCE_PUBLIC);
        t.deprecated = deprecated;
        t.unsupported_package = model.unsupported_packages.count(t.package) != 0;
    }
    return model;
}

std::string read_file(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    std::ostringstream text;
    text << in.rdbuf();
    return text.str();
}

const char* kind_name(TypeKind kind) {
    switch (kind) {
        case TypeKind::CLASS:
            return "class";
        case TypeKind::INTERFACE:
            return "interface";
        case TypeKind::ENUM:
            return "enum";
        case TypeKind::RECORD:
            return "record";
        case TypeKind::ANNOTATION:
            return "annotation";
    }
    return "?";
}

const char* visibility_name(Visibility v) {
    switch (v) {
        case Visibility::PUBLIC:
            return "public";
        case Visibility::PROTECTED:
            return "protected";
        case Visibility::PACKAGE:
            return "package";
        case Visibility::PRIVATE:
            return "private";
    }
    return "?";
}

std::string modifiers_text(std::uint16_t flags) {
    static const std::pair<Modifier, const char*> names[] = {
        {MOD_STATIC, "static"},       {MOD_ABSTRACT, "abstract"},         {MOD_FINAL, "final"},
        {MOD_DEFAULT, "default"},     {MOD_SYNCHRONIZED, "synchronized"}, {MOD_NATIVE, "native"},
        {MOD_TRANSIENT, "transient"}, {MOD_VOLATILE, "volatile"},         {MOD_STRICTFP, "strictfp"},
        {MOD_SEALED, "sealed"},       {MOD_NON_SEALED, "non-sealed"},
    };
    std::string out;
    for (const auto& [flag, name] : names) {
        if ((flags & flag) != 0) {
            out += ' ';
            out += name;
        }
    }
    return out;
}

std::string annotations_text(const std::vector<Annotation>& annotations) {
    std::string out;
    for (const auto& a : annotations) {
        out += " @" + a.name;
        if (!a.arguments.empty()) {
            out += "(" + a.arguments + ")";
        }
    }
    return out;
}

std::string joined(const std::vector<std::string>& parts, std::string_view sep) {
    std::string out;
    for (std::size_t i = 0; i < parts.size(); ++i) {
        if (i > 0) {
            out += sep;
        }
        out += parts[i];
    }
    return out;
}

}  // namespace

std::string TypeRef::erased_simple_name() const {
    std::string base;
    switch (kind) {
        case Kind::NAMED:
            base = resolved ? simple_of(name) : (written.empty() ? name : written.back());
            break;
        case Kind::WILDCARD:
            base = "?";
            break;
        case Kind::PRIMITIVE:
        case Kind::VOID:
        case Kind::TYPE_VARIABLE:
            base = name;
            break;
    }
    for (int i = 0; i < dimensions; ++i) {
        base += "[]";
    }
    return base;
}

std::string TypeRef::to_string() const {
    std::string out;
    if (kind == Kind::WILDCARD) {
        out = "?";
        if (bound != Bound::NONE && !arguments.empty()) {
            out += bound == Bound::EXTENDS ? " extends " : " super ";
            out += arguments.front().to_string();
        }
        return out;
    }
    out = name;
    if (!arguments.empty()) {
        out += '<';
        for (std::size_t i = 0; i < arguments.size(); ++i) {
            if (i > 0) {
                out += ", ";
            }
            out += arguments[i].to_string();
        }
        out += '>';
    }
    for (int i = 0; i < dimensions; ++i) {
        out += "[]";
    }
    return out;
}

std::string Method::signature() const {
    std::string out = name + "(";
    for (std::size_t i = 0; i < parameters.size(); ++i) {
        if (i > 0) {
            out += ',';
        }
        out += parameters[i].type.erased_simple_name();
        if (parameters[i].varargs) {
            out += "[]";
        }
    }
    return out + ")";
}

bool Type::has_annotation(std::string_view binary_name_of) const {
    return std::any_of(annotations.begin(), annotations.end(),
                       [&](const Annotation& a) { return a.name == binary_name_of; });
}

const Type* Model::find(std::string_view binary_name) const {
    const auto it = types.find(std::string(binary_name));
    return it == types.end() ? nullptr : &it->second;
}

Model build_model(const std::vector<std::pair<std::string, std::string>>& sources) {
    return merge(parse_units(sources, 1));
}

Model load_model(const std::string& root, unsigned jobs) {
    std::vector<std::filesystem::path> files;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        if (entry.is_regular_file() && entry.path().extension() == ".java") {
            files.push_back(entry.path());
        }
    }
    std::sort(files.begin(), files.end());
    std::vector<std::pair<std::string, std::string>> sources;
    sources.reserve(files.size());
    for (const auto& file : files) {
        sources.emplace_back(std::filesystem::relative(file, root).generic_string(), read_file(file));
    }
    return merge(parse_units(sources, std::max(1U, jobs)));
}

std::string dump(const Model& model) {
    std::ostringstream out;
    for (const auto& [name, t] : model.types) {
        out << name << "\ttype\t" << kind_name(t.kind) << ' ' << visibility_name(t.visibility)
            << modifiers_text(t.modifiers) << annotations_text(t.annotations);
        if (t.audience_public) {
            out << " [audience-public]";
        }
        if (t.deprecated) {
            out << " [deprecated]";
        }
        if (t.unsupported_package) {
            out << " [unsupported-package]";
        }
        out << '\t' << t.location.file << ':' << t.location.line << '\n';
        for (const auto& tp : t.type_parameters) {
            std::vector<std::string> bounds;
            for (const auto& b : tp.bounds) {
                bounds.push_back(b.to_string());
            }
            out << name << "\ttypeparam\t" << tp.name << (bounds.empty() ? "" : " extends " + joined(bounds, " & "))
                << '\n';
        }
        for (const auto& e : t.extends) {
            out << name << "\textends\t" << e.to_string() << '\n';
        }
        for (const auto& i : t.implements) {
            out << name << "\timplements\t" << i.to_string() << '\n';
        }
        for (const auto& c : t.enum_constants) {
            out << name << "\tconstant\t" << c.name << '(' << joined(c.arguments, ", ") << ')'
                << (c.has_body ? " {...}" : "") << annotations_text(c.annotations) << '\n';
        }
        for (const auto& f : t.fields) {
            out << name << "\tfield\t" << visibility_name(f.visibility) << modifiers_text(f.modifiers)
                << annotations_text(f.annotations) << ' ' << f.type.to_string() << ' ' << f.name;
            if (!f.initializer.empty()) {
                out << " = " << f.initializer;
            }
            out << '\n';
        }
        for (const auto* list : {&t.constructors, &t.methods}) {
            for (const auto& m : *list) {
                out << name << '\t' << (m.is_constructor ? "constructor" : "method") << '\t'
                    << visibility_name(m.visibility) << modifiers_text(m.modifiers) << annotations_text(m.annotations);
                if (!m.type_parameters.empty()) {
                    std::vector<std::string> names;
                    for (const auto& tp : m.type_parameters) {
                        names.push_back(tp.name);
                    }
                    out << " <" << joined(names, ", ") << '>';
                }
                if (!m.is_constructor) {
                    out << ' ' << m.return_type.to_string();
                }
                out << ' ' << m.name << '(';
                for (std::size_t i = 0; i < m.parameters.size(); ++i) {
                    const auto& p = m.parameters[i];
                    out << (i > 0 ? ", " : "") << p.type.to_string() << (p.varargs ? "..." : "") << ' ' << p.name;
                }
                out << ')';
                if (!m.throws.empty()) {
                    std::vector<std::string> names;
                    for (const auto& e : m.throws) {
                        names.push_back(e.to_string());
                    }
                    out << " throws " << joined(names, ", ");
                }
                if (m.implicit) {
                    out << " [implicit]";
                }
                if (m.javadoc) {
                    out << " [javadoc]";
                }
                if (m.returns_this) {
                    out << " [returns-this]";
                }
                if (m.delegation) {
                    const auto& d = *m.delegation;
                    out << " [delegates " << (d.receiver.empty() ? "" : d.receiver + ".") << d.target << '('
                        << joined(d.arguments, ", ") << ")]";
                }
                if (!m.assigned.empty()) {
                    out << " [assigns " << joined(m.assigned, ", ") << ']';
                }
                out << '\n';
            }
        }
    }
    for (const auto& d : model.diagnostics) {
        out << "diagnostic\t" << d.location.file << ':' << d.location.line << '\t' << d.message << '\n';
    }
    return out.str();
}

}  // namespace cklint::java
