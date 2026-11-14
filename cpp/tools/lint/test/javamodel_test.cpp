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

// The Java model on small sources: what is parsed, how names resolve, and
// the facts read from bodies.

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "java_model.hpp"

namespace {

using cklint::java::Method;
using cklint::java::Model;
using cklint::java::Type;
using cklint::java::TypeKind;
using cklint::java::TypeRef;
using cklint::java::Visibility;

Model model_of(std::vector<std::pair<std::string, std::string>> sources) {
    auto model = cklint::java::build_model(sources);
    for (const auto& d : model.diagnostics) {
        ADD_FAILURE() << d.location.file << ':' << d.location.line << ": " << d.message;
    }
    return model;
}

const Type& type(const Model& model, const std::string& name) {
    const auto* t = model.find(name);
    if (t == nullptr) {
        ADD_FAILURE() << "no type " << name;
        static const Type none;
        return none;
    }
    return *t;
}

const Method& method(const Type& t, const std::string& signature) {
    for (const auto* list : {&t.methods, &t.constructors}) {
        for (const auto& m : *list) {
            if (m.signature() == signature) {
                return m;
            }
        }
    }
    ADD_FAILURE() << "no method " << signature << " in " << t.binary_name;
    static const Method none;
    return none;
}

TEST(JavaModelTest, ResolvesNamesFromEveryScope) {
    const auto model = model_of({
        {"a/b/Other.java", "package a.b; public class Other { public static class Inner {} }"},
        {"a/b/Base.java", "package a.b; public class Base { public interface Inherited {} }"},
        {"a/c/Imported.java", "package a.c; public class Imported {}"},
        {"a/b/User.java", R"(
            package a.b;
            import a.c.Imported;
            import java.util.Map;
            import static a.b.Other.Inner;
            public class User<T> extends Base {
                public class Member {}
                public Other same_package;
                public Imported single_import;
                public String java_lang;
                public Member member;
                public Inherited inherited;
                public Map.Entry<String, T> nested_jdk;
                public Inner static_import;
                public java.util.List<? extends Number> qualified;
                public <M> M method_variable(T t) { return null; }
            }
        )"},
    });
    const auto& user = type(model, "a.b.User");
    auto field_type = [&](const std::string& name) {
        const auto it =
            std::find_if(user.fields.begin(), user.fields.end(), [&](const auto& f) { return f.name == name; });
        return it == user.fields.end() ? std::string("<none>") : it->type.to_string();
    };
    EXPECT_EQ(field_type("same_package"), "a.b.Other");
    EXPECT_EQ(field_type("single_import"), "a.c.Imported");
    EXPECT_EQ(field_type("java_lang"), "java.lang.String");
    EXPECT_EQ(field_type("member"), "a.b.User$Member");
    EXPECT_EQ(field_type("inherited"), "a.b.Base$Inherited");
    EXPECT_EQ(field_type("nested_jdk"), "java.util.Map$Entry<java.lang.String, T>");
    EXPECT_EQ(field_type("static_import"), "a.b.Other$Inner");
    EXPECT_EQ(field_type("qualified"), "java.util.List<? extends java.lang.Number>");

    const auto& m = method(user, "method_variable(T)");
    EXPECT_EQ(m.return_type.kind, TypeRef::Kind::TYPE_VARIABLE);
    EXPECT_EQ(m.return_type.name, "M");
    EXPECT_EQ(m.parameters.at(0).type.kind, TypeRef::Kind::TYPE_VARIABLE);
}

TEST(JavaModelTest, ReportsANameItCannotResolve) {
    const auto model = cklint::java::build_model({{"a/A.java", "package a; class A { Missing m; }"}});
    ASSERT_EQ(model.diagnostics.size(), 1U);
    EXPECT_EQ(model.diagnostics[0].message, "cannot resolve type Missing in a.A");
    EXPECT_EQ(model.diagnostics[0].location.file, "a/A.java");
}

TEST(JavaModelTest, ReportsASyntaxError) {
    const auto model = cklint::java::build_model({{"a/A.java", "package a; class A { int }"}});
    ASSERT_FALSE(model.diagnostics.empty());
    EXPECT_NE(model.diagnostics[0].message.find("syntax error"), std::string::npos);
}

TEST(JavaModelTest, WritesSignaturesAsTheDeprecatedListDoes) {
    // The example of rust/xtask/src/java.rs's scans_deprecations test.
    const auto model = model_of({{"a/A.java", R"(
        package a;
        import java.util.List;
        import java.util.Map;
        class A {
            @SuppressWarnings("unchecked") public void keep(Map<String, List<Integer>> m, byte[] b, String... s) {}
            public void entry(Map.Entry<String, String> e, int c[]) {}
        }
    )"}});
    const auto& a = type(model, "a.A");
    EXPECT_EQ(a.methods.at(0).signature(), "keep(Map,byte[],String[])");
    EXPECT_EQ(a.methods.at(1).signature(), "entry(Entry,int[])");
}

TEST(JavaModelTest, ReadsJavadocBeforeAnnotations) {
    const auto model = model_of({{"a/A.java", R"(
        package a;
        /** The class. */
        public class A {
            /**
             * Documented.
             */
            @Deprecated
            public void documented() {}
            /* A block comment, not Javadoc. */
            public void plain() {}
        }
    )"}});
    const auto& a = type(model, "a.A");
    ASSERT_TRUE(a.javadoc.has_value());
    EXPECT_EQ(a.javadoc->text, "/** The class. */");
    const auto& documented = method(a, "documented()");
    ASSERT_TRUE(documented.javadoc.has_value());
    EXPECT_NE(documented.javadoc->text.find("Documented."), std::string::npos);
    EXPECT_FALSE(method(a, "plain()").javadoc.has_value());
}

TEST(JavaModelTest, AppliesJavasImpliedVisibility) {
    const auto model = model_of({{"a/I.java", R"(
        package a;
        public interface I {
            int CONSTANT = 1;
            void abstract_method();
            default void default_method() {}
            class Nested {}
        }
    )"},
                                 {"a/E.java", "package a; public enum E { X; E() {} }"},
                                 {"a/C.java", "package a; class C { protected static class P {} }"}});
    const auto& i = type(model, "a.I");
    EXPECT_EQ(i.kind, TypeKind::INTERFACE);
    EXPECT_EQ(i.fields.at(0).visibility, Visibility::PUBLIC);
    EXPECT_EQ(method(i, "abstract_method()").visibility, Visibility::PUBLIC);
    EXPECT_FALSE(method(i, "abstract_method()").has_body);
    EXPECT_TRUE(method(i, "default_method()").has(cklint::java::MOD_DEFAULT));
    EXPECT_EQ(type(model, "a.I$Nested").visibility, Visibility::PUBLIC);
    EXPECT_EQ(method(type(model, "a.E"), "E()").visibility, Visibility::PRIVATE);
    // JLS 8.8.9: a default constructor has its class's access.
    const auto& c = type(model, "a.C");
    EXPECT_EQ(method(c, "C()").visibility, Visibility::PACKAGE);
    EXPECT_TRUE(method(c, "C()").implicit);
    EXPECT_EQ(method(type(model, "a.C$P"), "P()").visibility, Visibility::PROTECTED);
}

TEST(JavaModelTest, ReadsEnumsAndRecords) {
    const auto model = model_of({{"a/E.java", R"(
        package a;
        public enum E {
            ONE(1, "one"), TWO(2, "two") { };
            public final int id;
            E(int id, String name) { this.id = id; }
        }
    )"},
                                 {"a/R.java", "package a; public record R(int x, String y) {}"}});
    const auto& e = type(model, "a.E");
    ASSERT_EQ(e.enum_constants.size(), 2U);
    EXPECT_EQ(e.enum_constants[0].arguments, (std::vector<std::string>{"1", "\"one\""}));
    EXPECT_FALSE(e.enum_constants[0].has_body);
    EXPECT_TRUE(e.enum_constants[1].has_body);
    EXPECT_TRUE(method(e, "values()").implicit);
    EXPECT_EQ(method(e, "values()").return_type.to_string(), "a.E[]");
    EXPECT_TRUE(method(e, "valueOf(String)").implicit);

    const auto& r = type(model, "a.R");
    EXPECT_EQ(r.kind, TypeKind::RECORD);
    EXPECT_EQ(method(r, "x()").return_type.to_string(), "int");
    EXPECT_TRUE(method(r, "R(int,String)").implicit);
}

TEST(JavaModelTest, ReadsFactsFromBodies) {
    const auto model = model_of({{"a/A.java", R"(
        package a;
        public class A {
            private final String host;
            private int port;
            private A delegate;
            public A(String host) { this(host, 9092); }
            public A(String host, int port) { this.host = host; port = port; }
            public A port(int port) { this.port = port; return this; }
            public A maybe(boolean b) { if (b) { return this; } return new A("x"); }
            public A nested() { Runnable r = () -> { return; }; return this; }
            public int poll(long timeout) { return poll(timeout, true); }
            public int poll(long timeout, boolean wait) { return delegate.poll(timeout, wait); }
            public static final A DEFAULT = new A("h", 1);
        }
    )"}});
    const auto& a = type(model, "a.A");

    const auto& shorter = method(a, "A(String)");
    ASSERT_TRUE(shorter.delegation.has_value());
    EXPECT_EQ(shorter.delegation->target, "this");
    EXPECT_EQ(shorter.delegation->arguments, (std::vector<std::string>{"host", "9092"}));
    EXPECT_EQ(method(a, "A(String,int)").assigned, (std::vector<std::string>{"host", "port"}));

    EXPECT_TRUE(method(a, "port(int)").returns_this);
    EXPECT_FALSE(method(a, "maybe(boolean)").returns_this);
    // A lambda's `return` is not the method's.
    EXPECT_TRUE(method(a, "nested()").returns_this);

    const auto& poll = method(a, "poll(long)");
    ASSERT_TRUE(poll.delegation.has_value());
    EXPECT_EQ(poll.delegation->receiver, "");
    EXPECT_EQ(poll.delegation->target, "poll");
    EXPECT_EQ(poll.delegation->arguments, (std::vector<std::string>{"timeout", "true"}));
    const auto& forward = method(a, "poll(long,boolean)");
    ASSERT_TRUE(forward.delegation.has_value());
    EXPECT_EQ(forward.delegation->receiver, "delegate");

    const auto it = std::find_if(a.fields.begin(), a.fields.end(), [](const auto& f) { return f.name == "DEFAULT"; });
    ASSERT_NE(it, a.fields.end());
    EXPECT_EQ(it->initializer, "new A(\"h\",1)");
}

TEST(JavaModelTest, PropagatesAudienceDeprecationAndUnsupportedPackages) {
    const auto model = model_of({
        {"org/apache/kafka/common/annotation/InterfaceAudience.java", R"(
            package org.apache.kafka.common.annotation;
            public final class InterfaceAudience { public @interface Public {} }
        )"},
        {"a/Pub.java", R"(
            package a;
            import org.apache.kafka.common.annotation.InterfaceAudience;
            @InterfaceAudience.Public
            public class Pub { @Deprecated public static class Old { public static class Older {} } }
        )"},
        {"a/Priv.java", "package a; public class Priv {}"},
        {"b/package-info.java", "/** This package is not a supported Kafka API. */\npackage b;"},
        {"b/Hidden.java", "package b; public class Hidden {}"},
    });
    EXPECT_TRUE(type(model, "a.Pub").audience_public);
    EXPECT_TRUE(type(model, "a.Pub$Old$Older").audience_public);
    EXPECT_FALSE(type(model, "a.Priv").audience_public);
    EXPECT_FALSE(type(model, "a.Pub").deprecated);
    EXPECT_TRUE(type(model, "a.Pub$Old").deprecated);
    EXPECT_TRUE(type(model, "a.Pub$Old$Older").deprecated);
    EXPECT_TRUE(type(model, "b.Hidden").unsupported_package);
    EXPECT_FALSE(type(model, "a.Pub").unsupported_package);
}

}  // namespace
