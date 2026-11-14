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

// The Java model: the declarations of a Java source tree, as plain structs.
//
// Built by parsing every file with the ANTLR-generated parser, then resolving
// every written type name to a fully qualified one syntactically (enclosing
// and inherited member types, single-type and on-demand imports, the package,
// java.lang). No ANTLR type appears here (see java_syntax.hpp).
//
// Names: a type is identified by its binary name, the package and the chain of
// enclosing classes joined by `$` (`org.apache.kafka.common.config.ConfigResource$Type`),
// the form `design/current/java-deprecated.txt` and the Rust lints use.

#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <vector>

namespace cklint::java {

/// A type as written in a declaration, with its names resolved.
struct TypeRef {
    enum class Kind : std::uint8_t {
        PRIMITIVE,      ///< `name` is the keyword: int, long, boolean, ...
        VOID,           ///< a method's `void`
        NAMED,          ///< a class, interface, enum or record: `name` is its binary name
        TYPE_VARIABLE,  ///< `name` is the variable: T, K, V
        WILDCARD,       ///< `?`, with its bound (if any) in `arguments[0]`
    };
    enum class Bound : std::uint8_t { NONE, EXTENDS, SUPER };

    Kind kind = Kind::NAMED;
    /// See `Kind`. For a NAMED type nothing resolved, the name as written.
    std::string name;
    /// False when a NAMED type's name could not be resolved.
    bool resolved = true;
    /// Type arguments, in order.
    std::vector<TypeRef> arguments;
    /// A wildcard's bound kind.
    Bound bound = Bound::NONE;
    /// Array dimensions (`byte[]` is 1). A varargs parameter is not counted
    /// here but in `Parameter::varargs`.
    int dimensions = 0;

    /// The written segments of a NAMED type before resolution (`Map`, `Entry`).
    std::vector<std::string> written;

    /// The simple name of the erased type, as the deprecated list writes a
    /// parameter: `Map<String, ?>` -> `Map`, `Map.Entry` -> `Entry`, `T` -> `T`,
    /// `byte[]` -> `byte[]`.
    [[nodiscard]] std::string erased_simple_name() const;
    /// Java source-like text with resolved names: `java.util.Map<java.lang.String, ?>`.
    [[nodiscard]] std::string to_string() const;
};

/// An annotation on a declaration.
struct Annotation {
    /// The resolved binary name: `org.apache.kafka.common.annotation.InterfaceAudience$Public`.
    std::string name;
    /// The arguments as written, without the parentheses; empty if none.
    std::string arguments;
};

enum class Visibility : std::uint8_t { PUBLIC, PROTECTED, PACKAGE, PRIVATE };

/// Modifier keywords, as written. Implied ones are not added here; the
/// effective visibility of a declaration is in its `visibility` field.
enum Modifier : std::uint16_t {
    MOD_PUBLIC = 1U << 0U,
    MOD_PROTECTED = 1U << 1U,
    MOD_PRIVATE = 1U << 2U,
    MOD_STATIC = 1U << 3U,
    MOD_ABSTRACT = 1U << 4U,
    MOD_FINAL = 1U << 5U,
    MOD_DEFAULT = 1U << 6U,
    MOD_SYNCHRONIZED = 1U << 7U,
    MOD_NATIVE = 1U << 8U,
    MOD_TRANSIENT = 1U << 9U,
    MOD_VOLATILE = 1U << 10U,
    MOD_STRICTFP = 1U << 11U,
    MOD_SEALED = 1U << 12U,
    MOD_NON_SEALED = 1U << 13U,
};

/// The Javadoc comment of a declaration: the last `/** ... */` comment
/// before it (and before its annotations), if any.
struct Javadoc {
    std::string text;  ///< the comment, `/**` and `*/` included
    std::size_t line = 0;
};

/// Where a declaration is.
struct Location {
    std::string file;
    std::size_t line = 0;
};

struct TypeParameter {
    std::string name;
    std::vector<TypeRef> bounds;
};

struct Parameter {
    std::string name;
    TypeRef type;
    bool varargs = false;
    bool is_final = false;
    std::vector<Annotation> annotations;
};

/// A call a body consists of: `this(a, b)`, `return name(a, b)`, `name(a, b)`,
/// or the same through a receiver (`return delegate.name(a, b)`).
struct Delegation {
    /// The receiver as written (`delegate`, `this`), empty for an unqualified call.
    std::string receiver;
    /// `this` for a constructor's `this(...)`, else the method called.
    std::string target;
    /// The argument expressions as written, whitespace removed.
    std::vector<std::string> arguments;
};

/// A method or a constructor.
struct Method {
    std::string name;  ///< a constructor's is its class's simple name
    bool is_constructor = false;
    Visibility visibility = Visibility::PACKAGE;
    std::uint16_t modifiers = 0;
    std::vector<Annotation> annotations;
    std::vector<TypeParameter> type_parameters;
    TypeRef return_type;  ///< VOID for a constructor
    std::vector<Parameter> parameters;
    std::vector<TypeRef> throws;
    std::optional<Javadoc> javadoc;
    Location location;
    /// Declared by the compiler (an enum's `values()` and `valueOf(String)`).
    bool implicit = false;

    // Facts about the body, for the rules that read Java's code.
    bool has_body = false;
    /// The body is one statement delegating to another method or constructor.
    std::optional<Delegation> delegation;
    /// Every `return` of the body is `return this;`, and there is at least one.
    bool returns_this = false;
    /// The names assigned by the body's top-level statements (`this.x = ...`
    /// and `x = ...`), in order: what a constructor sets.
    std::vector<std::string> assigned;

    /// `name(T1,T2)` with erased simple parameter types, as the deprecated list
    /// writes an overload: `close(Duration)`, `keep(Map,byte[],String[])`.
    [[nodiscard]] std::string signature() const;
    [[nodiscard]] bool has(Modifier m) const { return (modifiers & m) != 0; }
};

struct Field {
    std::string name;
    Visibility visibility = Visibility::PACKAGE;
    std::uint16_t modifiers = 0;
    std::vector<Annotation> annotations;
    TypeRef type;
    /// The initializer as written, whitespace removed; empty if none.
    std::string initializer;
    std::optional<Javadoc> javadoc;
    Location location;

    [[nodiscard]] bool has(Modifier m) const { return (modifiers & m) != 0; }
};

struct EnumConstant {
    std::string name;
    std::vector<Annotation> annotations;
    /// The constructor arguments as written, whitespace removed.
    std::vector<std::string> arguments;
    bool has_body = false;
    std::optional<Javadoc> javadoc;
    Location location;
};

enum class TypeKind : std::uint8_t { CLASS, INTERFACE, ENUM, RECORD, ANNOTATION };

struct Type {
    TypeKind kind = TypeKind::CLASS;
    std::string package;
    std::string simple_name;
    /// `package.Outer$Inner`.
    std::string binary_name;
    /// The binary name of the enclosing type; empty for a top-level type.
    std::string enclosing;
    Visibility visibility = Visibility::PACKAGE;
    std::uint16_t modifiers = 0;
    std::vector<Annotation> annotations;
    std::vector<TypeParameter> type_parameters;
    /// A class's superclass (at most one); an interface's extended interfaces.
    std::vector<TypeRef> extends;
    std::vector<TypeRef> implements;
    std::vector<Field> fields;
    std::vector<Method> constructors;
    std::vector<Method> methods;
    std::vector<EnumConstant> enum_constants;
    /// Binary names of the member types, in declaration order.
    std::vector<std::string> nested;
    std::optional<Javadoc> javadoc;
    Location location;

    /// `@InterfaceAudience.Public` on this type or, for a nested type, on its
    /// top-level type (the annotation targets top-level types only).
    bool audience_public = false;
    /// `@Deprecated` on this type or an enclosing one.
    bool deprecated = false;
    /// The package's package-info.java carries the "not a supported Kafka API"
    /// disclaimer.
    bool unsupported_package = false;

    [[nodiscard]] bool has(Modifier m) const { return (modifiers & m) != 0; }
    [[nodiscard]] bool has_annotation(std::string_view binary_name) const;
};

/// A problem found while building the model: a syntax error, or a name that
/// could not be resolved.
struct ModelDiagnostic {
    Location location;
    std::string message;
};

struct Model {
    /// Every type, by binary name.
    std::map<std::string, Type> types;
    /// Packages whose package-info.java carries the unsupported-API disclaimer.
    std::set<std::string> unsupported_packages;
    std::size_t files = 0;
    std::vector<ModelDiagnostic> diagnostics;

    [[nodiscard]] const Type* find(std::string_view binary_name) const;
};

/// The binary name of `@InterfaceAudience.Public`.
inline constexpr std::string_view AUDIENCE_PUBLIC = "org.apache.kafka.common.annotation.InterfaceAudience$Public";
/// The binary name of `@Deprecated`.
inline constexpr std::string_view DEPRECATED = "java.lang.Deprecated";

/// Parses every `.java` file under `root` (a source root such as
/// `kafka/clients/src/main/java`) on `jobs` threads and builds the model.
/// The model does not depend on `jobs`.
Model load_model(const std::string& root, unsigned jobs);

/// Builds the model of in-memory sources, for tests: (path relative to the
/// source root, text) pairs.
Model build_model(const std::vector<std::pair<std::string, std::string>>& sources);

/// A deterministic text dump of the model, one declaration per line.
std::string dump(const Model& model);

}  // namespace cklint::java
