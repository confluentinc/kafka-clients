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

#include <cstdint>
#include <string>
#include <string_view>

namespace confluent::kafka::common {

/// A documented class.
class Doc {
public:
    /// Creates it.
    explicit Doc(std::string_view name);  // EXPECT: confluent-kafka-doc parameter 'name' of 'Doc' has no @param

    /// Returns the name.
    [[nodiscard]] std::string name() const;  // EXPECT: confluent-kafka-doc returns a value and has no @return

    /// Sets the size.
    /// @param size the size
    void size(int32_t size);  // EXPECT: confluent-kafka-doc has no `@throws IllegalArgumentError`

    /// Short, though this comment says far more than Java's.
    void short_one();  // EXPECT: confluent-kafka-doc more than 150% of the Javadoc's 1

    /// Undocumented in Java.
    ///
    /// @par Translation notes
    /// A note.
    void undocumented();  // EXPECT: confluent-kafka-doc translates nothing with a Javadoc

    /// Warns.
    ///
    /// @par Translation notes
    /// WARNING: blocks.
    void warns();  // EXPECT: confluent-kafka-doc with no entry `confluent::kafka::common::Doc::warns()`

    /// Warns too.
    ///
    /// @par Translation notes
    /// WARNING: blocks.
    void pending();  // EXPECT: confluent-kafka-doc its ledger entry `confluent::kafka::common::Doc::pending()` is not accepted

    /// Accepted.
    ///
    /// @par Translation notes
    /// WARNING: blocks.
    void accepted();

    /// Leftover {@link Doc}, see Doc.java:12 and <code>x</code>.
    void leftover();  // EXPECT: confluent-kafka-doc a Javadoc inline tag

    /// Twice the notes.
    /// WARNING: out.
    /// @par Translation notes
    /// One.
    /// @par Translation notes
    /// Two.
    void twice();  // EXPECT: confluent-kafka-doc 2 `@par Translation notes`

    /// Stray.
    /// @param nothing nothing
    void stray();  // EXPECT: confluent-kafka-doc @param nothing names no parameter

    /// Fine.
    /// @param size the size
    /// @return the size
    [[nodiscard]] int32_t fine(int32_t size) const;

    void bare();  // EXPECT: confluent-kafka-doc 'bare' has no Doxygen comment
};

// EXPECT@64: confluent-kafka-doc a Java file:line reference (Doc.java:12)
// EXPECT@64: confluent-kafka-doc an HTML tag (<code>)
// EXPECT@72: confluent-kafka-doc a `WARNING:` outside `@par Translation notes`

}  // namespace confluent::kafka::common
