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

#include "java_syntax.hpp"

#include <exception>
#include <string>

#include "JavaLexer.h"
#include "JavaParser.h"
#include "antlr4-runtime.h"

namespace cklint::java {
namespace {

class CollectingErrorListener final : public antlr4::BaseErrorListener {
public:
    explicit CollectingErrorListener(std::vector<SyntaxError>& errors) : errors_(errors) {}

    void syntaxError(antlr4::Recognizer* /*recognizer*/, antlr4::Token* /*offending_symbol*/, std::size_t line,
                     std::size_t char_position_in_line, const std::string& msg, std::exception_ptr /*e*/) override {
        errors_.push_back(SyntaxError{line, char_position_in_line, msg});
    }

private:
    std::vector<SyntaxError>& errors_;
};

}  // namespace

SyntaxCheck check_syntax(std::string_view text) {
    SyntaxCheck result;
    CollectingErrorListener listener(result.errors);

    antlr4::ANTLRInputStream input(text);
    JavaLexer lexer(&input);
    lexer.removeErrorListeners();
    lexer.addErrorListener(&listener);
    antlr4::CommonTokenStream tokens(&lexer);
    tokens.fill();

    JavaParser parser(&tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(&listener);
    // Full LL from the start. The usual two-stage scheme (SLL with a bail-out
    // strategy, LL only on failure) gains nothing with this grammar: SLL
    // mispredicts `Type name = ...` as a qualified type and fails on about
    // three files in four, which then parse twice.
    parser.getInterpreter<antlr4::atn::ParserATNSimulator>()->setPredictionMode(antlr4::atn::PredictionMode::LL);
    parser.compilationunit();
    return result;
}

}  // namespace cklint::java
