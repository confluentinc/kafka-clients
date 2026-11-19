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

// Parsing a Java compilation unit with the ANTLR-generated parser. No ANTLR
// type appears here: the runtime needs RTTI and exceptions, while the
// clang-tidy plugin that links this library is built like LLVM, without RTTI.

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace cklint::java {

/// A syntax error reported by the lexer or the parser.
struct SyntaxError {
    std::size_t line = 0;
    std::size_t column = 0;
    std::string message;
};

/// The outcome of parsing one compilation unit.
struct SyntaxCheck {
    std::vector<SyntaxError> errors;
};

/// Parses `text` as a Java compilation unit and reports its syntax errors.
/// Safe to call from several threads at once: the recognizers share ANTLR's
/// prediction cache, which the runtime guards. Measured on the 1340 clients
/// sources: 9.7 s on one thread, 5.3 s on four, no faster beyond (a cache per
/// thread is slower still, each thread warming its own).
SyntaxCheck check_syntax(std::string_view text);

}  // namespace cklint::java
