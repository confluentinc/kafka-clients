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

#include "scope.hpp"

#include <deque>
#include <optional>

#include "types.hpp"

namespace cklint::rules {
namespace {

bool deprecated(const std::vector<java::Annotation>& annotations) {
    for (const auto& a : annotations) {
        if (a.name == java::DEPRECATED) {
            return true;
        }
    }
    return false;
}

/// The line of scope-excluded.txt that excludes `binary_name`, if any: an
/// exact name, `package.*` for a package and its subpackages, or
/// `Outer$*` for a class's nested types.
std::optional<std::size_t> exclusion(const DataFile& excluded, const std::string& binary_name) {
    for (const auto& line : excluded.lines) {
        const auto& pattern = line.fields[0];
        if (pattern == binary_name) {
            return line.line;
        }
        if (pattern.size() > 1 && pattern.back() == '*') {
            const auto prefix = pattern.substr(0, pattern.size() - 1);
            if (binary_name.rfind(prefix, 0) == 0) {
                return line.line;
            }
        }
    }
    return std::nullopt;
}

class Walker {
public:
    Walker(const java::Model& model, const DataFile& excluded, Scope& scope)
        : model_(model), excluded_(excluded), scope_(scope) {}

    void run() {
        for (const auto& seed : scope_seeds()) {
            if (model_.find(seed) == nullptr) {
                scope_.missing_seeds.push_back(seed);
                continue;
            }
            add(seed, "seed");
        }
        while (!queue_.empty()) {
            const auto name = queue_.front();
            queue_.pop_front();
            visit(*model_.find(name));
        }
        // Public subclasses of an abstract class a translated method takes:
        // Java callers pass them where the parameter is declared.
        bool grew = true;
        while (grew) {
            grew = false;
            for (const auto& [name, t] : model_.types) {
                if (scope_.types.count(name) != 0 || t.kind != java::TypeKind::CLASS) {
                    continue;
                }
                for (const auto& ref : t.extends) {
                    if (abstract_parameters_.count(ref.name) != 0 && add(name, "subclass of " + ref.name)) {
                        grew = true;
                    }
                }
            }
            while (!queue_.empty()) {
                const auto name = queue_.front();
                queue_.pop_front();
                visit(*model_.find(name));
            }
        }
    }

private:
    /// Adds a type if it qualifies; true when it was new.
    bool add(const std::string& name, const std::string& reason) {
        if (scope_.types.count(name) != 0) {
            return false;
        }
        const auto* t = model_.find(name);
        if (t == nullptr || !is_public_api(*t) || t->deprecated || t->visibility != java::Visibility::PUBLIC ||
            is_throwable(model_, name)) {
            return false;
        }
        if (const auto line = exclusion(excluded_, name)) {
            scope_.used_exclusions.insert(*line);
            return false;
        }
        scope_.types.emplace(name, reason);
        queue_.push_back(name);
        return true;
    }

    void follow(const java::TypeRef& ref, const std::string& reason, bool is_parameter) {
        for (const auto& arg : ref.arguments) {
            follow(arg, reason, false);
        }
        if (ref.kind != java::TypeRef::Kind::NAMED || !ref.resolved) {
            return;
        }
        const auto* t = model_.find(ref.name);
        if (t == nullptr) {
            return;
        }
        if (is_parameter && t->kind == java::TypeKind::CLASS && t->has(java::MOD_ABSTRACT)) {
            abstract_parameters_.insert(ref.name);
        }
        add(ref.name, reason);
    }

    void visit(const java::Type& t) {
        for (const auto* list : {&t.extends, &t.implements}) {
            for (const auto& ref : *list) {
                follow(ref, "supertype of " + t.binary_name, false);
            }
        }
        for (const auto& nested : t.nested) {
            add(nested, "nested in " + t.binary_name);
        }
        for (const auto& tp : t.type_parameters) {
            for (const auto& bound : tp.bounds) {
                follow(bound, "bound of " + t.binary_name, false);
            }
        }
        // §1.4: public members only.
        for (const auto* list : {&t.constructors, &t.methods}) {
            for (const auto& m : *list) {
                if (m.visibility != java::Visibility::PUBLIC || m.implicit || deprecated(m.annotations)) {
                    continue;
                }
                const auto reason = t.binary_name + "#" + m.signature();
                follow(m.return_type, reason, false);
                for (const auto& p : m.parameters) {
                    follow(p.type, reason, true);
                }
                for (const auto& tp : m.type_parameters) {
                    for (const auto& bound : tp.bounds) {
                        follow(bound, reason, false);
                    }
                }
            }
        }
        for (const auto& f : t.fields) {
            if (f.visibility == java::Visibility::PUBLIC && !deprecated(f.annotations)) {
                follow(f.type, t.binary_name + "#" + f.name, false);
            }
        }
    }

    const java::Model& model_;
    const DataFile& excluded_;
    Scope& scope_;
    std::deque<std::string> queue_;
    std::set<std::string> abstract_parameters_;
};

}  // namespace

const std::vector<std::string>& scope_seeds() {
    static const std::vector<std::string> seeds = {
        // §1.4: the four client families.
        "org.apache.kafka.clients.producer.Producer",
        "org.apache.kafka.clients.producer.KafkaProducer",
        "org.apache.kafka.clients.producer.MockProducer",
        "org.apache.kafka.clients.consumer.Consumer",
        "org.apache.kafka.clients.consumer.KafkaConsumer",
        "org.apache.kafka.clients.consumer.MockConsumer",
        "org.apache.kafka.clients.consumer.ShareConsumer",
        "org.apache.kafka.clients.consumer.KafkaShareConsumer",
        "org.apache.kafka.clients.consumer.MockShareConsumer",
        "org.apache.kafka.clients.admin.Admin",
        "org.apache.kafka.clients.admin.AdminClient",
        "org.apache.kafka.clients.admin.KafkaAdminClient",
        // §7.1 and §7.4: the configuration classes.
        "org.apache.kafka.clients.producer.ProducerConfig",
        "org.apache.kafka.clients.consumer.ConsumerConfig",
        "org.apache.kafka.clients.admin.AdminClientConfig",
        "org.apache.kafka.common.config.TopicConfig",
        "org.apache.kafka.common.config.SaslConfigs",
        "org.apache.kafka.common.config.SslConfigs",
        "org.apache.kafka.common.config.SecurityConfig",
        "org.apache.kafka.common.config.LogLevelConfig",
        "org.apache.kafka.common.security.auth.SecurityProtocol",
        "org.apache.kafka.common.config.SslClientAuth",
        "org.apache.kafka.clients.consumer.GroupProtocol",
        // §9.3: Bytes, what BytesDeserializer returns.
        "org.apache.kafka.common.utils.Bytes",
    };
    static const std::vector<std::string> with_serdes = [] {
        // §10.2: the built-in serdes, reached through no signature.
        auto all = seeds;
        for (const char* kind : {"ByteArray", "Bytes", "String", "Short", "Integer", "Long", "Float", "Double",
                                 "Boolean", "UUID", "Void", "List"}) {
            for (const char* role : {"Serializer", "Deserializer"}) {
                all.push_back(std::string("org.apache.kafka.common.serialization.") + kind + role);
            }
        }
        return all;
    }();
    return with_serdes;
}

Scope compute_scope(const java::Model& model, const DataFile& excluded) {
    Scope scope;
    Walker(model, excluded, scope).run();
    return scope;
}

}  // namespace cklint::rules
