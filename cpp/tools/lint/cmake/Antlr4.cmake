# ANTLR 4 for the Java model: the tool jar that generates the Java parser, and
# the C++ runtime it links against, both pinned by version and hash.
#
# Defines:
#   ck_antlr4_runtime  static, position-independent library of the C++ runtime
#   ck_java_grammar    static library of the generated JavaLexer / JavaParser
#
# The runtime's own CMakeLists.txt is not used: it force-sets CMAKE_BUILD_TYPE
# in the cache when it is empty, which would change the build type of the
# whole binding. Its sources are compiled here as one target instead.

include_guard(GLOBAL)
include(FetchContent)

set(CK_ANTLR4_VERSION 4.13.2)
# SHA-1 7df86c341abb175a0f4b76a7845074cc45f82ff8, as published by Maven Central.
set(CK_ANTLR4_JAR_SHA256 eae2dfa119a64327444672aff63e9ec35a20180dc5b8090b7a6ab85125df4d76)
set(CK_ANTLR4_SOURCE_SHA256 9f18272a9b32b622835a3365f850dd1063d60f5045fb1e12ce475ae6e18a35bb)

# The 4.13 tool needs Java 11.
find_package(Java 11 COMPONENTS Runtime REQUIRED)

set(CK_ANTLR4_JAR "${CMAKE_BINARY_DIR}/_deps/antlr-${CK_ANTLR4_VERSION}-complete.jar")
# Skipped when the file is already there with this hash.
file(DOWNLOAD
    "https://repo1.maven.org/maven2/org/antlr/antlr4/${CK_ANTLR4_VERSION}/antlr4-${CK_ANTLR4_VERSION}-complete.jar"
    "${CK_ANTLR4_JAR}"
    EXPECTED_HASH SHA256=${CK_ANTLR4_JAR_SHA256}
    TLS_VERIFY ON)

FetchContent_Declare(ck_antlr4
    URL "https://github.com/antlr/antlr4/archive/refs/tags/${CK_ANTLR4_VERSION}.tar.gz"
    URL_HASH SHA256=${CK_ANTLR4_SOURCE_SHA256}
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    # A directory without a CMakeLists.txt: download and extract only.
    SOURCE_SUBDIR runtime/Cpp/runtime/src)
FetchContent_MakeAvailable(ck_antlr4)

set(_ck_antlr4_src "${ck_antlr4_SOURCE_DIR}/runtime/Cpp/runtime/src")
file(GLOB_RECURSE _ck_antlr4_sources CONFIGURE_DEPENDS "${_ck_antlr4_src}/*.cpp")
add_library(ck_antlr4_runtime STATIC ${_ck_antlr4_sources})
# The include directories the runtime's own CMakeLists.txt gives its users.
target_include_directories(ck_antlr4_runtime SYSTEM PUBLIC
    "${_ck_antlr4_src}"
    "${_ck_antlr4_src}/atn"
    "${_ck_antlr4_src}/dfa"
    "${_ck_antlr4_src}/internal"
    "${_ck_antlr4_src}/misc"
    "${_ck_antlr4_src}/support"
    "${_ck_antlr4_src}/tree"
    "${_ck_antlr4_src}/tree/pattern"
    "${_ck_antlr4_src}/tree/xpath")
target_compile_definitions(ck_antlr4_runtime PUBLIC ANTLR4CPP_STATIC)
target_compile_features(ck_antlr4_runtime PUBLIC cxx_std_17)
# PIC: it ends up inside the clang-tidy plugin's shared object.
set_target_properties(ck_antlr4_runtime PROPERTIES POSITION_INDEPENDENT_CODE ON)
# Third-party code, built as shipped, and optimized whatever the build type: a
# debug build of the binding would otherwise make the linter twice as slow.
set(_ck_antlr4_flags
    $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-w>
    $<$<AND:$<CXX_COMPILER_ID:GNU,Clang,AppleClang>,$<NOT:$<CONFIG:Release,RelWithDebInfo,MinSizeRel>>>:-O2>)
target_compile_options(ck_antlr4_runtime PRIVATE ${_ck_antlr4_flags})
find_package(Threads REQUIRED)
target_link_libraries(ck_antlr4_runtime PUBLIC Threads::Threads)

# The grammar, adapted to the C++ target as grammars-v4's
# java/java/Cpp/transformGrammar.py does (grammar/README.md). The adapted
# copies live in the build tree; the vendored files stay as upstream ships them.
set(_ck_grammar_dir "${CMAKE_CURRENT_LIST_DIR}/../grammar")
set(_ck_grammar_gen "${CMAKE_CURRENT_BINARY_DIR}/java_grammar")
file(MAKE_DIRECTORY "${_ck_grammar_gen}")
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS
    "${_ck_grammar_dir}/JavaLexer.g4" "${_ck_grammar_dir}/JavaParser.g4")
foreach(_g IN ITEMS JavaLexer JavaParser)
    file(READ "${_ck_grammar_dir}/${_g}.g4" _text)
    string(REPLACE "// Insert here @header." "@header {#include \"JavaParserBase.h\"}" _text "${_text}")
    string(REPLACE "this." "this->" _text "${_text}")
    # Written only when it changed, so a reconfigure does not regenerate the parser.
    file(WRITE "${_ck_grammar_gen}/${_g}.g4.tmp" "${_text}")
    file(COPY_FILE "${_ck_grammar_gen}/${_g}.g4.tmp" "${_ck_grammar_gen}/${_g}.g4" ONLY_IF_DIFFERENT)
endforeach()

set(_ck_grammar_outputs
    "${_ck_grammar_gen}/JavaLexer.cpp" "${_ck_grammar_gen}/JavaLexer.h"
    "${_ck_grammar_gen}/JavaParser.cpp" "${_ck_grammar_gen}/JavaParser.h")
add_custom_command(
    OUTPUT ${_ck_grammar_outputs}
    # The tool orders the two grammars itself: the parser's tokenVocab is the lexer.
    COMMAND "${Java_JAVA_EXECUTABLE}" -jar "${CK_ANTLR4_JAR}"
            -Dlanguage=Cpp -no-listener -no-visitor -Xexact-output-dir
            -o "${_ck_grammar_gen}" -lib "${_ck_grammar_gen}"
            "${_ck_grammar_gen}/JavaLexer.g4" "${_ck_grammar_gen}/JavaParser.g4"
    DEPENDS "${_ck_grammar_gen}/JavaLexer.g4" "${_ck_grammar_gen}/JavaParser.g4" "${CK_ANTLR4_JAR}"
    COMMENT "Generating the Java parser with ANTLR ${CK_ANTLR4_VERSION}"
    VERBATIM)

add_library(ck_java_grammar STATIC ${_ck_grammar_outputs} "${_ck_grammar_dir}/JavaParserBase.cpp")
target_include_directories(ck_java_grammar SYSTEM PUBLIC "${_ck_grammar_gen}" "${_ck_grammar_dir}")
target_link_libraries(ck_java_grammar PUBLIC ck_antlr4_runtime)
set_target_properties(ck_java_grammar PROPERTIES POSITION_INDEPENDENT_CODE ON)
# Generated and third-party code, as above.
target_compile_options(ck_java_grammar PRIVATE ${_ck_antlr4_flags})
