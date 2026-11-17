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

#include "types.hpp"

#include <algorithm>
#include <map>
#include <set>
#include <utility>

#include "names.hpp"

namespace cklint::rules {
namespace {

using java::TypeRef;

const std::map<std::string, std::string, std::less<>>& primitives() {
    static const std::map<std::string, std::string, std::less<>> table = {
        {"boolean", "bool"}, {"byte", "int8_t"}, {"short", "int16_t"}, {"int", "int32_t"},
        {"long", "int64_t"}, {"float", "float"}, {"double", "double"},
    };
    return table;
}

const std::map<std::string, std::string, std::less<>>& boxed() {
    static const std::map<std::string, std::string, std::less<>> table = {
        {"java.lang.Boolean", "boolean"}, {"java.lang.Byte", "byte"}, {"java.lang.Short", "short"},
        {"java.lang.Integer", "int"},     {"java.lang.Long", "long"}, {"java.lang.Float", "float"},
        {"java.lang.Double", "double"},
    };
    return table;
}

Shape shape(CppType t, Decision d = Decision::NONE) {
    return Shape{std::move(t), d};
}

Mapping shapes(std::vector<Shape> list) {
    return Mapping{std::move(list), {}};
}

Mapping unmapped(std::string why) {
    return Mapping{{}, std::move(why)};
}

CppType optional_of(CppType t) {
    return value("std::optional", {std::move(t)});
}

CppType span_of(CppType element) {
    return value("std::span", {const_of(std::move(element))});
}

CppType shared_of(CppType t) {
    return value("std::shared_ptr", {std::move(t)});
}

/// The default shape of `ref` as a type argument, or the reason it has none.
/// A constant's elements are constants too (§2.4: `std::array<std::string_view>`).
std::optional<CppType> argument(const java::Model& model, const TypeRef& ref, std::string& why,
                                Position outer = Position::TYPE_ARGUMENT) {
    auto m = map_type(model, ref, outer == Position::CONSTANT ? Position::CONSTANT : Position::TYPE_ARGUMENT);
    if (!m.ok()) {
        why = m.unmapped;
        return std::nullopt;
    }
    return m.shapes.front().type;
}

/// `byte[]` and `ByteBuffer`.
Mapping bytes(Position position) {
    const auto span = span_of(value("std::byte"));
    switch (position) {
        case Position::PARAMETER:
            return shapes({shape(span)});
        case Position::RETURN:
            return shapes({shape(span), shape(value("confluent::kafka::common::utils::Bytes"))});
        case Position::TYPE_ARGUMENT:
        case Position::CONSTANT:
            return shapes({shape(value("std::vector", {value("std::byte")}))});
    }
    return unmapped("unreachable");
}

/// An array, a Collection, a List, an Iterable or a Set of `element`.
Mapping sequence(std::string_view kind, CppType element, Position position) {
    const bool is_set = kind == "java.util.Set";
    const bool is_list = kind == "java.util.List" || kind == "array";
    switch (position) {
        case Position::PARAMETER:
            return shapes({shape(span_of(std::move(element)))});
        case Position::CONSTANT:
            // §2.4: a constant Set or List is a std::array; its size is not compared.
            return shapes({shape(value("std::array", {std::move(element)}))});
        case Position::RETURN:
        case Position::TYPE_ARGUMENT:
            if (is_set) {
                return shapes({shape(value("std::unordered_set", {element})),
                               shape(value("std::set", {element}), Decision::ORDERED)});
            }
            if (is_list) {
                return shapes({shape(value("std::vector", {std::move(element)}))});
            }
            return shapes(
                {shape(value("std::vector", {element})), shape(value("std::unordered_set", {element}), Decision::SET)});
    }
    return unmapped("unreachable");
}

/// A reference type that Java may pass or return as null (§4.6).
Mapping nullable_value(CppType parameter, CppType result, Position position) {
    switch (position) {
        case Position::PARAMETER:
            return shapes({shape(parameter), shape(optional_of(parameter), Decision::NULLABLE)});
        case Position::RETURN:
            return shapes({shape(result), shape(optional_of(result), Decision::NULLABLE)});
        case Position::TYPE_ARGUMENT:
        case Position::CONSTANT:
            return shapes({shape(std::move(result))});
    }
    return unmapped("unreachable");
}

Mapping kafka_type(const java::Model& model, const TypeRef& ref, Position position) {
    const auto* type = model.find(ref.name);
    if (type == nullptr) {
        return unmapped("no Java type " + ref.name + " in the model");
    }
    if (!is_public_api(*type)) {
        // §5: a non-Public type in a public signature is a detail type with Java's method names.
        const auto detail = value("confluent::kafka::detail::" + type_name(type->simple_name));
        return shapes({shape(detail), shape(const_ref(detail)), shape(mutable_ref(detail)), shape(shared_of(detail))});
    }
    CppType self = value(qualified_name(model, *type));
    for (const auto& arg : ref.arguments) {
        std::string why;
        auto mapped = argument(model, arg, why);
        if (!mapped) {
            return unmapped(why);
        }
        self.arguments.push_back(std::move(*mapped));
    }
    if (is_throwable(model, ref.name)) {
        // §5: a Kafka exception is its class; a cause travels as std::exception_ptr.
        const auto ptr = value("std::exception_ptr");
        if (position == Position::PARAMETER) {
            return shapes({shape(const_ref(self)), shape(ptr)});
        }
        return shapes({shape(self), shape(ptr)});
    }
    switch (type->kind) {
        case java::TypeKind::ENUM:
            return shapes({shape(self)});
        case java::TypeKind::INTERFACE:
            // §4.7: an interface the client keeps is a std::shared_ptr; §5.4: one handed out has
            // no public implementation.
            if (position == Position::PARAMETER) {
                return shapes({shape(shared_of(self)), shape(const_ref(self)), shape(mutable_ref(self))});
            }
            if (position == Position::RETURN) {
                return shapes({shape(shared_of(self)), shape(value("std::unique_ptr", {self})), shape(const_ref(self)),
                               shape(mutable_ref(self))});
            }
            return shapes({shape(shared_of(self))});
        case java::TypeKind::CLASS:
        case java::TypeKind::RECORD:
        case java::TypeKind::ANNOTATION:
            break;
    }
    // §4.7: a value type in as const T&, out by value or, from a getter, by const&.
    switch (position) {
        case Position::PARAMETER:
            return shapes({shape(const_ref(self)), shape(optional_of(self), Decision::NULLABLE)});
        case Position::RETURN:
            return shapes({shape(self), shape(const_ref(self)), shape(optional_of(self), Decision::NULLABLE)});
        case Position::TYPE_ARGUMENT:
        case Position::CONSTANT:
            return shapes({shape(self)});
    }
    return unmapped("unreachable");
}

Mapping named(const java::Model& model, const TypeRef& ref, Position position, std::string_view parameter_name) {
    const std::string& n = ref.name;
    if (!ref.resolved) {
        return unmapped("unresolved Java type " + n);
    }
    if (n == "java.lang.String") {
        return position == Position::CONSTANT
                   ? shapes({shape(value("std::string_view"))})
                   : nullable_value(value("std::string_view"), value("std::string"), position);
    }
    if (const auto it = boxed().find(n); it != boxed().end()) {
        const auto prim = value(primitives().at(it->second));
        if (position == Position::PARAMETER && n == "java.lang.Integer" && parameter_name == "timeoutMs") {
            return shapes({shape(optional_of(prim))});  // §4.8
        }
        return nullable_value(prim, prim, position);
    }
    if (n == "java.lang.Object") {
        return unmapped("java.lang.Object does not appear on the surface (§5)");
    }
    if (n == "java.lang.Void") {
        return position == Position::TYPE_ARGUMENT ? shapes({shape(value("void"))})
                                                   : unmapped("java.lang.Void outside a type argument has no §5 row");
    }
    if (n == "java.nio.ByteBuffer") {
        return bytes(position);
    }
    if (n == "java.util.OptionalInt" || n == "java.util.OptionalLong" || n == "java.util.OptionalDouble") {
        const char* inner = n == "java.util.OptionalInt"    ? "int32_t"
                            : n == "java.util.OptionalLong" ? "int64_t"
                                                            : "double";
        return shapes({shape(optional_of(value(inner)))});
    }
    if (n == "java.util.Optional") {
        if (ref.arguments.size() != 1) {
            return unmapped("a raw Optional has no §5 row");
        }
        std::string why;
        auto inner = argument(model, ref.arguments[0], why);
        return inner ? shapes({shape(optional_of(std::move(*inner)))}) : unmapped(why);
    }
    if (n == "java.util.Collection" || n == "java.util.List" || n == "java.lang.Iterable" || n == "java.util.Set") {
        if (ref.arguments.size() != 1) {
            return unmapped("a raw " + n + " has no §5 row");
        }
        const auto& element_ref = ref.arguments[0];
        if (position == Position::PARAMETER && element_ref.kind == TypeRef::Kind::NAMED &&
            element_ref.name == "org.apache.kafka.common.header.Header") {
            // §5: Headers or Iterable<Header> taken.
            TypeRef headers;
            headers.name = "org.apache.kafka.common.header.Headers";
            return map_type(model, headers, position);
        }
        std::string why;
        auto element = argument(model, element_ref, why, position);
        return element ? sequence(n, std::move(*element), position) : unmapped(why);
    }
    if (n == "java.util.Map") {
        if (ref.arguments.size() != 2) {
            return unmapped("a raw Map has no §5 row");
        }
        std::string why;
        auto k = argument(model, ref.arguments[0], why);
        if (!k) {
            return unmapped(why);
        }
        auto v = argument(model, ref.arguments[1], why);
        if (!v) {
            return unmapped(why);
        }
        if (position == Position::PARAMETER) {
            return shapes({shape(const_ref(value("std::unordered_map", {*k, *v})))});  // §5, one overload (§4.1)
        }
        return shapes(
            {shape(value("std::unordered_map", {*k, *v})), shape(value("std::map", {*k, *v}), Decision::ORDERED)});
    }
    if (n == "java.time.Duration") {
        return shapes({shape(value("confluent::kafka::Duration"))});
    }
    if (n == "java.util.UUID") {
        // §5: no confluent::kafka::Uuid is added; common::Uuid is the value type.
        const auto uuid = value("confluent::kafka::common::Uuid");
        return position == Position::PARAMETER ? shapes({shape(const_ref(uuid))}) : shapes({shape(uuid)});
    }
    if (n == "org.apache.kafka.common.header.Headers") {
        const auto headers = value("confluent::kafka::common::header::Headers");
        const auto pairs = span_of(value("std::pair", {value("std::string_view"), span_of(value("std::byte"))}));
        if (position == Position::PARAMETER) {
            return shapes({shape(const_ref(headers)), shape(mutable_ref(headers)), shape(pairs)});
        }
        return shapes({shape(headers), shape(const_ref(headers)), shape(mutable_ref(headers))});
    }
    if (n == "java.lang.Throwable" || n == "java.lang.Exception" || n == "java.lang.RuntimeException") {
        return shapes({shape(value("std::exception_ptr"))});
    }
    if (n == "java.lang.Runnable") {
        return shapes({shape(value("std::function", {value("void()")}))});
    }
    if (is_untranslated_placeholder(n)) {
        const auto t = value(qualified_name(model, *model.find(n)));
        return shapes({shape(shared_of(t)), shape(const_ref(t)), shape(mutable_ref(t))});
    }
    if (n.rfind("org.apache.kafka.", 0) == 0) {
        return kafka_type(model, ref, position);
    }
    return unmapped("no §5 row for " + n);
}

}  // namespace

const char* decision_name(Decision d) {
    switch (d) {
        case Decision::NONE:
            return "none";
        case Decision::NULLABLE:
            return "nullable";
        case Decision::ORDERED:
            return "ordered";
        case Decision::SET:
            return "set";
    }
    return "?";
}

bool is_untranslated_placeholder(std::string_view binary_name) {
    static const std::set<std::string, std::less<>> names = {
        "org.apache.kafka.common.metrics.MetricConfig",
        "org.apache.kafka.common.metrics.Measurable",
        "org.apache.kafka.common.metrics.MetricValueProvider",
        "org.apache.kafka.common.metrics.Gauge",
    };
    return names.count(binary_name) != 0;
}

bool is_throwable(const java::Model& model, std::string_view binary_name) {
    std::string name(binary_name);
    for (int depth = 0; depth < 64; ++depth) {
        if (name == "java.lang.Throwable" || name == "java.lang.Exception" || name == "java.lang.RuntimeException" ||
            name == "java.lang.Error") {
            return true;
        }
        const auto* t = model.find(name);
        if (t == nullptr) {
            // A JDK supertype: java.io.IOException, java.util.concurrent.TimeoutException.
            return name.rfind("java.", 0) == 0 &&
                   (name.size() > 9 && (name.compare(name.size() - 9, 9, "Exception") == 0 ||
                                        name.compare(name.size() - 5, 5, "Error") == 0));
        }
        if (t->kind != java::TypeKind::CLASS || t->extends.empty()) {
            return false;
        }
        name = t->extends.front().name;
    }
    return false;
}

bool is_public_api(const java::Type& type) {
    if (!type.audience_public || type.unsupported_package) {
        return false;
    }
    const std::string pkg = "." + type.package + ".";
    return pkg.find(".internals.") == std::string::npos;
}

namespace {

/// §4.7: out, a value type by value or, from a getter on a value type, by const&.
Mapping with_const_ref_returns(Mapping m) {
    if (!m.ok()) {
        return m;
    }
    std::vector<Shape> out;
    for (const auto& s : m.shapes) {
        out.push_back(s);
    }
    for (const auto& s : m.shapes) {
        if (s.type.ref == CppType::Ref::NONE && s.type.name != "void") {
            Shape cref{const_ref(s.type), s.decision};
            const bool present =
                std::any_of(out.begin(), out.end(), [&](const Shape& o) { return o.type == cref.type; });
            if (!present) {
                out.push_back(std::move(cref));
            }
        }
    }
    m.shapes = std::move(out);
    return m;
}

Mapping map_type_unadjusted(const java::Model& model, const java::TypeRef& ref, Position position,
                            std::string_view parameter_name);

}  // namespace

Mapping map_type(const java::Model& model, const java::TypeRef& ref, Position position,
                 std::string_view parameter_name) {
    auto m = map_type_unadjusted(model, ref, position, parameter_name);
    return position == Position::RETURN ? with_const_ref_returns(std::move(m)) : m;
}

namespace {

Mapping map_type_unadjusted(const java::Model& model, const java::TypeRef& ref, Position position,
                            std::string_view parameter_name) {
    if (ref.dimensions > 0) {
        auto element = ref;
        element.dimensions -= 1;
        if (element.dimensions == 0 && element.kind == TypeRef::Kind::PRIMITIVE && element.name == "byte") {
            return bytes(position);
        }
        std::string why;
        auto mapped = argument(model, element, why, position);
        return mapped ? sequence("array", std::move(*mapped), position) : unmapped(why);
    }
    switch (ref.kind) {
        case TypeRef::Kind::VOID:
            return shapes({shape(value("void"))});
        case TypeRef::Kind::PRIMITIVE: {
            const auto it = primitives().find(ref.name);
            if (it == primitives().end()) {
                return unmapped("no §5 row for " + ref.name);
            }
            return shapes({shape(value(it->second))});
        }
        case TypeRef::Kind::TYPE_VARIABLE: {
            // §5: a template parameter with Java's letter.
            const auto t = value(ref.name);
            if (position == Position::PARAMETER) {
                return shapes({shape(const_ref(t)), shape(t)});
            }
            return shapes({shape(t)});
        }
        case TypeRef::Kind::WILDCARD:
            if (ref.bound == TypeRef::Bound::NONE || ref.arguments.empty()) {
                return unmapped("an unbounded wildcard has no §5 row");
            }
            return map_type(model, ref.arguments.front(), Position::TYPE_ARGUMENT);
        case TypeRef::Kind::NAMED:
            return named(model, ref, position, parameter_name);
    }
    return unmapped("unreachable");
}

}  // namespace
}  // namespace cklint::rules
