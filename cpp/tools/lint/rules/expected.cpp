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

#include "expected.hpp"

#include <algorithm>
#include <map>
#include <set>
#include <sstream>
#include <utility>

#include "names.hpp"

namespace cklint::rules {
namespace {

using java::TypeRef;

bool deprecated(const std::vector<java::Annotation>& annotations) {
    return std::any_of(annotations.begin(), annotations.end(),
                       [](const auto& a) { return a.name == java::DEPRECATED; });
}

std::string last_segment(const std::string& qualified) {
    const auto pos = qualified.rfind("::");
    return pos == std::string::npos ? qualified : qualified.substr(pos + 2);
}

bool is_integral(const TypeRef& ref) {
    return ref.kind == TypeRef::Kind::PRIMITIVE && ref.dimensions == 0 &&
           (ref.name == "byte" || ref.name == "short" || ref.name == "int" || ref.name == "long");
}

/// `(byte)32` -> `32`; Java's casts in an enum constant's argument.
std::string strip_casts(std::string text) {
    for (const char* cast : {"(byte)", "(short)", "(int)", "(long)"}) {
        while (text.rfind(cast, 0) == 0) {
            text = text.substr(std::string_view(cast).size());
        }
    }
    return text;
}

/// Whether a constant of this Java type can be `static constexpr` (§2.4): a
/// primitive, a String, an enum, or a Set / List / array of those.
bool constexpr_type(const java::Model& model, const TypeRef& ref) {
    if (ref.dimensions > 0) {
        auto element = ref;
        element.dimensions = 0;
        return constexpr_type(model, element);
    }
    if (ref.kind == TypeRef::Kind::PRIMITIVE) {
        return true;
    }
    if (ref.kind != TypeRef::Kind::NAMED) {
        return false;
    }
    if (ref.name == "java.lang.String") {
        return true;
    }
    if (ref.name == "java.util.Set" || ref.name == "java.util.List") {
        return ref.arguments.size() == 1 && constexpr_type(model, ref.arguments[0]);
    }
    const auto* t = model.find(ref.name);
    return t != nullptr && t->kind == java::TypeKind::ENUM;
}

class Builder {
public:
    Builder(const java::Model& model, const java::Type& type) : model_(model), type_(type) {}

    ExpectedType build() {
        e_.java_name = type_.binary_name;
        e_.cpp_name = qualified_name(model_, type_);
        e_.header = header_of(model_, type_);
        e_.umbrella = umbrella_of(type_.package);
        if (is_throwable(model_, type_.binary_name)) {
            e_.shape = TypeShape::ERROR_CLASS;
            return e_;
        }
        switch (type_.kind) {
            case java::TypeKind::ENUM:
                e_.shape = TypeShape::ENUM_CLASS;
                break;
            case java::TypeKind::INTERFACE:
                e_.shape = TypeShape::ABSTRACT_CLASS;
                break;
            case java::TypeKind::CLASS:
                e_.shape = type_.has(java::MOD_ABSTRACT) ? TypeShape::ABSTRACT_CLASS : TypeShape::CLASS;
                break;
            case java::TypeKind::RECORD:
            case java::TypeKind::ANNOTATION:
                problem("a Java " + std::string(type_.kind == java::TypeKind::RECORD ? "record" : "annotation type") +
                        " has no rule in cpp/CLAUDE.md");
                break;
        }
        e_.functional_interface = type_.has_annotation("java.lang.FunctionalInterface");
        supertypes();
        if (type_.kind == java::TypeKind::ENUM) {
            enum_body();
        } else {
            members();
        }
        for (const auto& n : type_.nested) {
            const auto* nested = model_.find(n);
            if (nested != nullptr && nested->visibility == java::Visibility::PUBLIC && is_public_api(*nested) &&
                !nested->deprecated) {
                e_.nested.push_back(qualified_name(model_, *nested));
            }
        }
        merge_overloads();
        return e_;
    }

private:
    void problem(const std::string& what) { e_.problems.push_back(what); }

    std::string member_id(const java::Method& m) const { return type_.binary_name + "#" + m.signature(); }

    Mapping mapped(const TypeRef& ref, Position position, const std::string& where, std::string_view java_name = {}) {
        auto m = map_type(model_, ref, position, java_name);
        if (!m.ok()) {
            problem(where + ": " + m.unmapped);
        }
        return m;
    }

    std::vector<ExpectedParameter> parameters(const java::Method& m) {
        const auto names = parameter_names(model_, type_, m);
        std::vector<ExpectedParameter> out;
        for (std::size_t i = 0; i < m.parameters.size(); ++i) {
            auto type = m.parameters[i].type;
            if (m.parameters[i].varargs) {
                ++type.dimensions;  // `String... s` is taken as `String[] s`
            }
            out.push_back(ExpectedParameter{
                names[i],
                mapped(type, Position::PARAMETER, member_id(m) + " " + m.parameters[i].name, m.parameters[i].name)});
        }
        return out;
    }

    /// Adds a §5 convention once, whichever Java member asks for it first
    /// (`Comparable` and `equals` both give `operator==`).
    void add_convention(std::string name, std::string note, FunctionKind kind = FunctionKind::FREE_FUNCTION) {
        const bool present = std::any_of(e_.functions.begin(), e_.functions.end(), [&](const ExpectedFunction& f) {
            return f.java_member.empty() && f.name == name;
        });
        if (!present) {
            auto f = convention(std::move(name), std::move(note));
            f.kind = kind;
            e_.functions.push_back(std::move(f));
        }
    }

    ExpectedFunction convention(std::string name, std::string note) {
        ExpectedFunction f;
        f.kind = FunctionKind::FREE_FUNCTION;
        f.name = std::move(name);
        f.note = std::move(note);
        return f;
    }

    void supertypes() {
        static const std::map<std::string, std::string, std::less<>> jdk_rows = {
            {"java.io.Closeable", "Closeable: the destructor calls close() when it was not called"},
            {"java.lang.AutoCloseable", "AutoCloseable: the destructor calls close() when it was not called"},
            {"java.lang.Comparable", "Comparable: operator<=> and operator==; compare_to not generated"},
            {"java.lang.Iterable", "Iterable: begin(), end() and size(); forEach and spliterator not generated"},
            {"java.util.concurrent.Future", "Future: KafkaFuture<T> (§8.4)"},
            {"java.security.Principal", "Principal: dropped"},
            {"java.io.Serializable", "Serializable: nothing"},
        };
        for (const auto* list : {&type_.extends, &type_.implements}) {
            for (const auto& ref : *list) {
                if (ref.kind != TypeRef::Kind::NAMED) {
                    continue;
                }
                if (ref.name.rfind("java.", 0) == 0) {
                    const auto it = jdk_rows.find(ref.name);
                    if (it == jdk_rows.end()) {
                        problem("JDK supertype " + ref.name + " has no row in §5");
                        continue;
                    }
                    e_.jdk_interfaces.push_back(it->second);
                    if (ref.name == "java.lang.Comparable") {
                        add_convention("operator<=>", "§5 Comparable");
                        add_convention("operator==", "§5 Comparable");
                    } else if (ref.name == "java.lang.Iterable") {
                        for (const char* n : {"begin", "end", "size"}) {
                            add_convention(n, "§5 Iterable", FunctionKind::METHOD);
                        }
                    }
                    continue;
                }
                const auto* t = model_.find(ref.name);
                if (t == nullptr || !is_public_api(*t)) {
                    continue;  // a non-Public supertype is an implementation detail
                }
                // The class itself with its type arguments, not the shared_ptr an
                // interface becomes in a signature.
                CppType base = value(qualified_name(model_, *t));
                bool ok = true;
                for (const auto& arg : ref.arguments) {
                    auto m = mapped(arg, Position::TYPE_ARGUMENT, "supertype " + ref.to_string());
                    if (!m.ok()) {
                        ok = false;
                        break;
                    }
                    base.arguments.push_back(m.shapes.front().type);
                }
                if (ok) {
                    e_.bases.push_back(base.to_string());
                }
            }
        }
    }

    /// equals / hashCode / toString declared on the type (§5).
    bool object_method(const java::Method& m) {
        if (m.name == "equals" && m.parameters.size() == 1) {
            add_convention("operator==", "§5 equals");
            return true;
        }
        if (m.name == "hashCode" && m.parameters.empty()) {
            add_convention("std::hash", "§5 hashCode: a std::hash specialisation");
            return true;
        }
        if (m.name == "toString" && m.parameters.empty()) {
            add_convention("to_string", "§5 toString");
            add_convention("operator<<", "§5 toString");
            return true;
        }
        return false;
    }

    bool implements_jdk(std::string_view name) const {
        return std::any_of(type_.implements.begin(), type_.implements.end(),
                           [&](const auto& r) { return r.name == name; }) ||
               std::any_of(type_.extends.begin(), type_.extends.end(), [&](const auto& r) { return r.name == name; });
    }

    void members() {
        const auto simple = last_segment(e_.cpp_name);
        for (const auto& c : type_.constructors) {
            if (c.visibility != java::Visibility::PUBLIC || deprecated(c.annotations)) {
                continue;
            }
            ExpectedFunction f;
            f.kind = FunctionKind::CONSTRUCTOR;
            f.name = simple;
            f.parameters = parameters(c);
            f.java_member = member_id(c);
            if (c.implicit) {
                f.note = "Java's default constructor";
            }
            e_.functions.push_back(std::move(f));
        }
        const bool is_interface = type_.kind == java::TypeKind::INTERFACE;
        for (const auto& m : type_.methods) {
            if (m.visibility != java::Visibility::PUBLIC || m.implicit || deprecated(m.annotations)) {
                continue;
            }
            if (object_method(m)) {
                continue;
            }
            if (m.name == "compareTo" && implements_jdk("java.lang.Comparable")) {
                continue;  // §5: operator<=> instead
            }
            if ((m.name == "iterator" || m.name == "forEach" || m.name == "spliterator") &&
                implements_jdk("java.lang.Iterable")) {
                continue;  // §5: begin() / end()
            }
            ExpectedFunction f;
            f.kind = m.has(java::MOD_STATIC) ? FunctionKind::STATIC_METHOD : FunctionKind::METHOD;
            f.name = method_name(m);
            f.parameters = parameters(m);
            f.returns = mapped(m.return_type, Position::RETURN, member_id(m));
            f.java_member = member_id(m);
            if (f.kind == FunctionKind::METHOD) {
                if (is_interface) {
                    f.is_virtual = !m.has(java::MOD_PRIVATE);
                    f.is_pure = !m.has_body;
                } else if (m.has(java::MOD_ABSTRACT)) {
                    f.is_virtual = true;
                    f.is_pure = true;
                }
            }
            e_.functions.push_back(std::move(f));
        }
        if (e_.functional_interface) {
            add_convention("of", "§5 FunctionalInterface: of(std::invocable auto&&) returning a std::shared_ptr",
                           FunctionKind::STATIC_METHOD);
        }
        fields();
    }

    void fields() {
        for (const auto& f : type_.fields) {
            if (f.visibility != java::Visibility::PUBLIC || deprecated(f.annotations)) {
                continue;
            }
            const auto id = type_.binary_name + "#" + f.name;
            const bool is_static = f.has(java::MOD_STATIC) || type_.kind == java::TypeKind::INTERFACE;
            const bool is_final = f.has(java::MOD_FINAL) || type_.kind == java::TypeKind::INTERFACE;
            if (is_static && is_final) {
                if (constexpr_type(model_, f.type)) {
                    e_.constants.push_back(
                        ExpectedConstant{f.name, {}, mapped(f.type, Position::CONSTANT, id), id, f.initializer});
                    continue;
                }
                // §2.4: a class-typed constant is a static function returning a const&.
                ExpectedFunction fn;
                fn.kind = FunctionKind::CONSTANT_FUNCTION;
                fn.name = f.name;
                fn.returns = mapped(f.type, Position::CONSTANT, id);
                for (auto& s : fn.returns.shapes) {
                    s.type = const_ref(s.type);
                }
                fn.java_member = id;
                fn.note = "§2.4 class-typed constant, = " + f.initializer;
                e_.functions.push_back(std::move(fn));
                continue;
            }
            if (is_static) {
                problem(id + ": a public static field that is not final has no rule");
                continue;
            }
            ExpectedFunction fn;
            fn.kind = FunctionKind::METHOD;
            fn.name = member_name(f.name);
            fn.java_member = id;
            if (is_final) {
                fn.returns = mapped(f.type, Position::RETURN, id);
                fn.note = "§2.4 a final field's getter";
            } else {
                fn.parameters.push_back(
                    ExpectedParameter{member_name(f.name), mapped(f.type, Position::PARAMETER, id)});
                TypeRef void_ref;
                void_ref.kind = TypeRef::Kind::VOID;
                void_ref.name = "void";
                fn.returns = map_type(model_, void_ref, Position::RETURN);
                fn.note = "§2.4 a mutable field's setter";
            }
            e_.functions.push_back(std::move(fn));
        }
    }

    void enum_body() {
        // §3.3: the underlying type is the one integer field the constructor sets.
        std::set<std::string> integral;
        for (const auto& f : type_.fields) {
            if (!f.has(java::MOD_STATIC) && is_integral(f.type)) {
                integral.insert(f.name);
            }
        }
        std::set<std::string> set_fields;
        for (const auto& c : type_.constructors) {
            for (const auto& a : c.assigned) {
                if (integral.count(a) != 0) {
                    set_fields.insert(a);
                }
            }
        }
        std::optional<std::string> value_field;
        if (set_fields.size() == 1) {
            value_field = *set_fields.begin();
            const auto& field = *std::find_if(type_.fields.begin(), type_.fields.end(),
                                              [&](const auto& f) { return f.name == *value_field; });
            e_.underlying = map_type(model_, field.type, Position::TYPE_ARGUMENT).shapes.front().type;
        } else if (set_fields.size() > 1) {
            problem("the constructor sets " + std::to_string(set_fields.size()) +
                    " integer fields; §3.3 decides only for exactly one");
        }
        for (const auto& constant : type_.enum_constants) {
            ExpectedEnumerator en{constant.name, {}};
            if (value_field) {
                bool found = false;
                for (const auto& c : type_.constructors) {
                    if (c.parameters.size() != constant.arguments.size()) {
                        continue;
                    }
                    for (std::size_t i = 0; i < c.parameters.size(); ++i) {
                        if (c.parameters[i].name == *value_field) {
                            en.value = strip_casts(constant.arguments[i]);
                            found = true;
                        }
                    }
                }
                if (!found) {
                    problem(constant.name + ": no constructor parameter named " + *value_field + " gives its value");
                }
            }
            e_.enumerators.push_back(std::move(en));
        }

        // §3.3: the enum's members are free functions beside it. A field or an
        // instance method takes the enum first and keeps its name (found by
        // ADL); a static method or constant goes in the namespace named after
        // the enum.
        const auto self = value(e_.cpp_name);
        const auto self_name = snake_case(last_segment(e_.cpp_name));
        const auto statics = enum_statics_namespace(model_, type_);
        for (const auto& m : type_.methods) {
            if (m.visibility != java::Visibility::PUBLIC || m.implicit || deprecated(m.annotations)) {
                continue;
            }
            if (m.name == "toString" && m.parameters.empty()) {
                add_convention("to_string", "§3.3 toString");
                add_convention("operator<<", "§3.3 toString");
                continue;
            }
            ExpectedFunction f;
            f.kind = FunctionKind::FREE_FUNCTION;
            f.name = method_name(m);
            if (m.has(java::MOD_STATIC)) {
                f.qualifier = statics;
            } else {
                f.parameters.push_back(ExpectedParameter{self_name, Mapping{{Shape{self, Decision::NONE}}, {}}});
            }
            for (auto& p : parameters(m)) {
                f.parameters.push_back(std::move(p));
            }
            f.returns = mapped(m.return_type, Position::RETURN, member_id(m));
            f.java_member = member_id(m);
            e_.functions.push_back(std::move(f));
        }
        for (const auto& field : type_.fields) {
            if (field.visibility != java::Visibility::PUBLIC || deprecated(field.annotations)) {
                continue;
            }
            const auto id = type_.binary_name + "#" + field.name;
            if (!field.has(java::MOD_STATIC)) {
                ExpectedFunction f;
                f.kind = FunctionKind::FREE_FUNCTION;
                f.name = member_name(field.name);
                f.parameters.push_back(ExpectedParameter{self_name, Mapping{{Shape{self, Decision::NONE}}, {}}});
                f.returns = mapped(field.type, Position::RETURN, id);
                f.java_member = id;
                f.note = "§2.4 an enum's public field";
                e_.functions.push_back(std::move(f));
            } else if (constexpr_type(model_, field.type)) {
                ExpectedConstant c{field.name, statics, mapped(field.type, Position::CONSTANT, id), id,
                                   field.initializer};
                e_.constants.push_back(std::move(c));
            } else {
                // §2.4: a class-typed constant is a function returning a const&.
                ExpectedFunction fn;
                fn.kind = FunctionKind::CONSTANT_FUNCTION;
                fn.name = field.name;
                fn.qualifier = statics;
                fn.returns = mapped(field.type, Position::CONSTANT, id);
                for (auto& sh : fn.returns.shapes) {
                    sh.type = const_ref(sh.type);
                }
                fn.java_member = id;
                fn.note = "§2.4, §3.3 an enum's class-typed constant";
                e_.functions.push_back(std::move(fn));
            }
        }
    }

    /// §4.1: overloads whose parameters map to the same C++ types merge,
    /// named after the earliest declared.
    void merge_overloads() {
        std::vector<ExpectedFunction> out;
        for (auto& f : e_.functions) {
            auto same = std::find_if(out.begin(), out.end(), [&](const ExpectedFunction& g) {
                if (g.kind != f.kind || g.name != f.name || g.parameters.size() != f.parameters.size() ||
                    g.java_member.empty() || f.java_member.empty()) {
                    return false;
                }
                for (std::size_t i = 0; i < f.parameters.size(); ++i) {
                    const auto& a = f.parameters[i].mapping;
                    const auto& b = g.parameters[i].mapping;
                    if (!a.ok() || !b.ok() || !(a.shapes.front().type == b.shapes.front().type)) {
                        return false;
                    }
                }
                return true;
            });
            if (same != out.end()) {
                same->note += (same->note.empty() ? "" : "; ") + std::string("§4.1 merges ") + f.java_member;
                continue;
            }
            out.push_back(std::move(f));
        }
        e_.functions = std::move(out);
    }

    const java::Model& model_;
    const java::Type& type_;
    ExpectedType e_;
};

std::string shape_text(const Mapping& m) {
    if (!m.ok()) {
        return "/* no §5 row */";
    }
    if (m.shapes.empty()) {
        return "";
    }
    return m.shapes.front().type.to_string();
}

std::string alternatives(const Mapping& m) {
    if (!m.ok() || m.shapes.size() < 2) {
        return "";
    }
    std::string out;
    for (std::size_t i = 1; i < m.shapes.size(); ++i) {
        out += (out.empty() ? "" : " | ") + m.shapes[i].type.to_string();
        if (m.shapes[i].decision != Decision::NONE) {
            out += std::string(" [decisions.txt: ") + decision_name(m.shapes[i].decision) + "]";
        }
    }
    return out;
}

}  // namespace

ExpectedType expected(const java::Model& model, const java::Type& type) {
    return Builder(model, type).build();
}

std::string render(const ExpectedType& e) {
    std::ostringstream out;
    const auto ns_end = e.cpp_name.rfind("::");
    out << "// " << e.java_name << "\n// header <" << e.header << ">, module <" << e.umbrella << ">\n";
    for (const auto& j : e.jdk_interfaces) {
        out << "// " << j << '\n';
    }
    for (const auto& p : e.problems) {
        out << "// PROBLEM: " << p << '\n';
    }
    if (e.shape == TypeShape::ERROR_CLASS) {
        out << "// an error class: generated by the error emitter (§6)\n";
        return out.str();
    }
    const auto simple = last_segment(e.cpp_name);
    if (e.shape == TypeShape::ENUM_CLASS) {
        out << "enum class " << simple;
        if (e.underlying) {
            out << " : " << e.underlying->to_string();
        }
        out << " {\n";
        for (const auto& en : e.enumerators) {
            out << "    " << en.name;
            out << (en.value.empty() ? " = /* the C header's _e value */" : " = " + en.value) << ",\n";
        }
        out << "};\n";
        for (const auto& c : e.constants) {
            out << "inline constexpr " << shape_text(c.mapping) << ' ' << c.qualifier << "::" << c.name << " = "
                << c.initializer << ";  // " << c.java_member << '\n';
        }
    } else {
        out << "class " << simple;
        for (std::size_t i = 0; i < e.bases.size(); ++i) {
            out << (i == 0 ? " : public " : ", public ") << e.bases[i];
        }
        out << " {\npublic:\n";
        if (e.shape == TypeShape::ABSTRACT_CLASS) {
            out << "    virtual ~" << simple << "();\n";
        }
        for (const auto& c : e.constants) {
            out << "    static constexpr " << shape_text(c.mapping) << ' ' << c.name << " = " << c.initializer << ";\n";
        }
        for (const auto& n : e.nested) {
            out << "    // nested: " << n << " (its own expected shape)\n";
        }
    }
    auto line = [&](const ExpectedFunction& f, bool member) {
        out << (member ? "    " : "");
        if (f.kind == FunctionKind::STATIC_METHOD || f.kind == FunctionKind::CONSTANT_FUNCTION) {
            out << "static ";
        }
        if (f.is_virtual) {
            out << "virtual ";
        }
        if (f.kind != FunctionKind::CONSTRUCTOR && !f.returns.shapes.empty()) {
            out << shape_text(f.returns) << ' ';
        }
        out << (f.qualifier.empty() ? "" : f.qualifier + "::") << f.name << '(';
        for (std::size_t i = 0; i < f.parameters.size(); ++i) {
            out << (i > 0 ? ", " : "") << shape_text(f.parameters[i].mapping) << ' ' << f.parameters[i].name;
        }
        out << ')' << (f.is_pure ? " = 0" : "") << ';';
        std::string comment = f.java_member;
        if (!f.note.empty()) {
            comment += (comment.empty() ? "" : "; ") + f.note;
        }
        if (!comment.empty()) {
            out << "  // " << comment;
        }
        out << '\n';
        if (const auto alt = alternatives(f.returns); !alt.empty()) {
            out << (member ? "    " : "") << "//   returns or: " << alt << '\n';
        }
        for (const auto& p : f.parameters) {
            if (const auto alt = alternatives(p.mapping); !alt.empty()) {
                out << (member ? "    " : "") << "//   " << p.name << " or: " << alt << '\n';
            }
        }
    };
    for (const auto& f : e.functions) {
        if (f.kind != FunctionKind::FREE_FUNCTION && f.qualifier.empty() && e.shape != TypeShape::ENUM_CLASS) {
            line(f, true);
        }
    }
    if (e.shape != TypeShape::ENUM_CLASS) {
        out << "};\n";
    }
    for (const auto& f : e.functions) {
        if (f.kind == FunctionKind::FREE_FUNCTION || !f.qualifier.empty()) {
            line(f, false);
        }
    }
    out << "// in " << e.cpp_name.substr(0, ns_end) << '\n';
    return out.str();
}

}  // namespace cklint::rules
