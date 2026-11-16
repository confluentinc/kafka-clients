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

#include "names.hpp"

#include <algorithm>
#include <cctype>
#include <deque>
#include <functional>
#include <set>

namespace cklint::rules {
namespace {

bool is_upper(char c) {
    return std::isupper(static_cast<unsigned char>(c)) != 0;
}
bool is_lower(char c) {
    return std::islower(static_cast<unsigned char>(c)) != 0;
}
bool is_digit(char c) {
    return std::isdigit(static_cast<unsigned char>(c)) != 0;
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

/// The C++ names of `type` and its enclosing types, outermost first.
std::vector<std::string> type_chain(const java::Model& model, const java::Type& type) {
    std::vector<std::string> chain;
    for (const java::Type* t = &type; t != nullptr; t = t->enclosing.empty() ? nullptr : model.find(t->enclosing)) {
        chain.push_back(type_name(t->simple_name));
    }
    std::reverse(chain.begin(), chain.end());
    return chain;
}

/// The namespace segments below `confluent::kafka`.
std::vector<std::string> module_segments(std::string_view java_package) {
    auto segments = split(java_package, '.');
    std::size_t skip = 0;
    if (segments.size() >= 3 && segments[0] == "org" && segments[1] == "apache" && segments[2] == "kafka") {
        skip = 3;
        if (segments.size() > 3 && segments[3] == "clients") {
            skip = 4;
        }
    }
    return {segments.begin() + static_cast<std::ptrdiff_t>(skip), segments.end()};
}

/// Whether the erased parameter types of `a` and `b` agree, a type variable
/// on either side matching anything (`serialize(String, T)` declared on
/// `Serializer<T>` and `serialize(String, String)` on `StringSerializer`).
bool same_parameters(const java::Method& a, const java::Method& b) {
    if (a.name != b.name || a.parameters.size() != b.parameters.size()) {
        return false;
    }
    for (std::size_t i = 0; i < a.parameters.size(); ++i) {
        const auto& pa = a.parameters[i].type;
        const auto& pb = b.parameters[i].type;
        if (pa.kind == java::TypeRef::Kind::TYPE_VARIABLE || pb.kind == java::TypeRef::Kind::TYPE_VARIABLE) {
            continue;
        }
        if (pa.erased_simple_name() != pb.erased_simple_name() || a.parameters[i].varargs != b.parameters[i].varargs) {
            return false;
        }
    }
    return true;
}

}  // namespace

std::string snake_case(std::string_view name) {
    std::string out;
    out.reserve(name.size() + 4);
    for (std::size_t i = 0; i < name.size(); ++i) {
        const char c = name[i];
        if (is_upper(c) && i > 0) {
            const char prev = name[i - 1];
            const bool next_lower = i + 1 < name.size() && is_lower(name[i + 1]);
            if (is_lower(prev) || is_digit(prev) || (is_upper(prev) && next_lower)) {
                out += '_';
            }
        }
        out += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return out;
}

std::string member_name(std::string_view java_name) {
    auto words = split(snake_case(java_name), '_');
    std::string out;
    for (std::size_t i = 0; i < words.size(); ++i) {
        if (words[i] == "exception") {
            words[i] = "error";
        } else if (words[i] == "exceptions") {
            words[i] = "errors";
        }
        out += (i > 0 ? "_" : "") + words[i];
    }
    return out;
}

std::string type_name(std::string_view java_simple_name) {
    // Split before every upper-case letter, as translate_camel_words in
    // rust/xtask/src/java.rs does, and replace the whole words only.
    std::string out;
    std::size_t start = 0;
    for (std::size_t i = 1; i <= java_simple_name.size(); ++i) {
        if (i == java_simple_name.size() || is_upper(java_simple_name[i])) {
            const auto word = java_simple_name.substr(start, i - start);
            out += word == "Exception" ? "Error" : word == "Exceptions" ? "Errors" : std::string(word);
            start = i;
        }
    }
    return out;
}

std::string namespace_of(std::string_view java_package) {
    std::string out = "confluent::kafka";
    for (const auto& segment : module_segments(java_package)) {
        out += "::" + segment;
    }
    return out;
}

std::string qualified_name(const java::Model& model, const java::Type& type) {
    std::string out = namespace_of(type.package);
    for (const auto& name : type_chain(model, type)) {
        out += "::" + name;
    }
    return out;
}

std::string header_of(const java::Model& model, const java::Type& type) {
    std::string out = "confluent/kafka/";
    for (const auto& segment : module_segments(type.package)) {
        out += segment + "/";
    }
    return out + snake_case(type_chain(model, type).front()) + ".hpp";
}

std::string umbrella_of(std::string_view java_package) {
    std::string out = "confluent/kafka";
    for (const auto& segment : module_segments(java_package)) {
        out += "/" + segment;
    }
    return out + ".hpp";
}

bool is_cpp_keyword(std::string_view word) {
    static const std::set<std::string, std::less<>> keywords = {
        "alignas",     "alignof",   "and",        "and_eq",    "asm",      "auto",         "bitand",
        "bitor",       "bool",      "break",      "case",      "catch",    "char",         "char8_t",
        "char16_t",    "char32_t",  "class",      "compl",     "concept",  "const",        "consteval",
        "constexpr",   "constinit", "const_cast", "continue",  "co_await", "co_return",    "co_yield",
        "decltype",    "default",   "delete",     "do",        "double",   "dynamic_cast", "else",
        "enum",        "explicit",  "export",     "extern",    "false",    "float",        "for",
        "friend",      "goto",      "if",         "inline",    "int",      "long",         "mutable",
        "namespace",   "new",       "noexcept",   "not",       "not_eq",   "nullptr",      "operator",
        "or",          "or_eq",     "private",    "protected", "public",   "register",     "reinterpret_cast",
        "requires",    "return",    "short",      "signed",    "sizeof",   "static",       "static_assert",
        "static_cast", "struct",    "switch",     "template",  "this",     "thread_local", "throw",
        "true",        "try",       "typedef",    "typeid",    "typename", "union",        "unsigned",
        "using",       "virtual",   "void",       "volatile",  "wchar_t",  "while",        "xor",
        "xor_eq",
    };
    return keywords.count(word) != 0;
}

std::string method_name(const java::Method& method) {
    const std::string_view name = method.name;
    if (name.size() > 3 && name.substr(0, 3) == "get" && is_upper(name[3]) && method.parameters.empty() &&
        method.return_type.kind != java::TypeRef::Kind::VOID) {
        auto rest = member_name(name.substr(3));
        if (!is_cpp_keyword(rest)) {
            return rest;
        }
    }
    return member_name(name);
}

std::vector<std::string> parameter_names(const java::Model& model, const java::Type& owner,
                                         const java::Method& method) {
    const java::Method* declaration = &method;
    if (!method.is_constructor && !method.has(java::MOD_STATIC)) {
        // Breadth first over the supertypes: the nearest interface wins.
        std::deque<std::string> queue;
        std::set<std::string> seen;
        auto push_supertypes = [&](const java::Type& t) {
            for (const auto* list : {&t.extends, &t.implements}) {
                for (const auto& ref : *list) {
                    if (ref.kind == java::TypeRef::Kind::NAMED && seen.insert(ref.name).second) {
                        queue.push_back(ref.name);
                    }
                }
            }
        };
        push_supertypes(owner);
        bool found = false;
        while (!queue.empty() && !found) {
            const auto* t = model.find(queue.front());
            queue.pop_front();
            if (t == nullptr) {
                continue;
            }
            if (t->kind == java::TypeKind::INTERFACE) {
                for (const auto& m : t->methods) {
                    if (same_parameters(m, method)) {
                        declaration = &m;
                        found = true;
                        break;
                    }
                }
            }
            push_supertypes(*t);
        }
    }
    std::vector<std::string> names;
    names.reserve(declaration->parameters.size());
    for (const auto& p : declaration->parameters) {
        names.push_back(member_name(p.name));
    }
    return names;
}

std::string enum_function_name(const java::Model& model, const java::Type& enum_type,
                               std::string_view member_cpp_name) {
    if (member_cpp_name == "to_string") {
        return "to_string";
    }
    std::string out;
    for (const auto& name : type_chain(model, enum_type)) {
        out += snake_case(name) + "_";
    }
    return out + std::string(member_cpp_name);
}

}  // namespace cklint::rules
