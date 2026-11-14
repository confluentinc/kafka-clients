# Java grammar

The four files here are copied unchanged from
[antlr/grammars-v4](https://github.com/antlr/grammars-v4), directory `java/java`, at commit
`c306207ac870e7c5a95763b518ba3e3a1ca61409`:

| File | Upstream path |
|---|---|
| `JavaLexer.g4` | `java/java/JavaLexer.g4` |
| `JavaParser.g4` | `java/java/JavaParser.g4` |
| `JavaParserBase.h` | `java/java/Cpp/JavaParserBase.h` |
| `JavaParserBase.cpp` | `java/java/Cpp/JavaParserBase.cpp` |

They keep their own license, the BSD license stated at the top of `JavaLexer.g4` and
`JavaParser.g4` (copyright Terence Parr, Sam Harwell, Ivan Kochurkin, Michał Lorek); they carry
no Apache header.

The grammar targets Java 24; the Kafka clients sources use nothing past Java 17.

Upstream adapts the grammar to the C++ target with `java/java/Cpp/transformGrammar.py`. The
build applies the same two edits to a copy in the build tree (`cmake/Antlr4.cmake`), so these
files stay byte-identical to upstream:

- `// Insert here @header.` becomes `@header {#include "JavaParserBase.h"}`;
- `this.` becomes `this->` (the two semantic predicates of `JavaParser.g4`).

To update: copy the four files from a newer commit, change the commit above, and rebuild with
`-DCONFLUENT_KAFKA_CPP_LINT=ON`; the whole-tree parse test (`java_parse_kafka_clients`) shows
whether the new grammar still parses every Kafka source.
