// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::{self, Write};

/// A buffer for generating code with automatic indentation support.
///
/// Translated from org.apache.kafka.message.CodeBuffer
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CodeBuffer {
    lines: Vec<String>,
    indent: usize,
}

impl CodeBuffer {
    /// Creates a new empty CodeBuffer.
    pub fn new() -> Self {
        CodeBuffer { lines: Vec::new(), indent: 0 }
    }

    /// Increments the indentation level.
    pub fn increment_indent(&mut self) {
        self.indent += 1;
    }

    /// Decrements the indentation level.
    ///
    /// # Panics
    /// Panics if indentation would become negative.
    pub fn decrement_indent(&mut self) {
        if self.indent == 0 {
            panic!("Indent < 0");
        }
        self.indent -= 1;
    }

    /// Adds a line to the buffer with current indentation.
    ///
    /// Use the `printf!` macro instead for formatted output.
    pub fn printf(&mut self, line: impl Into<String>) {
        let indent_str = self.indent_spaces();
        self.lines.push(format!("{}{}", indent_str, line.into()));
    }

    /// Writes the buffer contents to a writer.
    pub fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        for line in &self.lines {
            writer.write_all(line.as_bytes())?;
        }
        Ok(())
    }

    /// Writes this buffer's contents to another CodeBuffer with additional indentation.
    pub fn write_to_buffer(&self, other: &mut CodeBuffer) {
        let other_indent = other.indent_spaces();
        for line in &self.lines {
            other.lines.push(format!("{}{}", other_indent, line));
        }
    }

    /// Returns the indentation string for the current level.
    fn indent_spaces(&self) -> String {
        "    ".repeat(self.indent)
    }

    /// Get the lines in the buffer
    pub fn lines(&self) -> &[String] {
        &self.lines
    }
}

impl Default for CodeBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_printf() {
        let mut buffer = CodeBuffer::new();
        buffer.printf("hello");
        assert_eq!(buffer.lines.len(), 1);
        assert_eq!(buffer.lines[0], "hello");
    }

    #[test]
    fn test_write() {
        let mut buffer = CodeBuffer::new();
        buffer.printf("public static void main(String[] args) throws Exception {\n");
        buffer.increment_indent();
        buffer.printf(format!("System.out.println(\"{}\");\n", "hello world"));
        buffer.decrement_indent();
        buffer.printf("}\n");

        let mut output = Vec::new();
        buffer.write(&mut output).unwrap();
        let result = String::from_utf8(output).unwrap();

        assert_eq!(
            result,
            "public static void main(String[] args) throws Exception {\n    System.out.println(\"hello world\");\n}\n"
        );
    }

    #[test]
    fn test_indentation() {
        let mut buffer = CodeBuffer::new();
        buffer.printf("line1");
        buffer.increment_indent();
        buffer.printf("line2");
        buffer.increment_indent();
        buffer.printf("line3");
        buffer.decrement_indent();
        buffer.printf("line4");

        assert_eq!(buffer.lines[0], "line1");
        assert_eq!(buffer.lines[1], "    line2");
        assert_eq!(buffer.lines[2], "        line3");
        assert_eq!(buffer.lines[3], "    line4");
    }

    #[test]
    fn test_equals() {
        let mut buffer1 = CodeBuffer::new();
        let mut buffer2 = CodeBuffer::new();
        assert_eq!(buffer1, buffer2);

        buffer1.printf("hello world");
        assert_ne!(buffer1, buffer2);

        buffer2.printf("hello world");
        assert_eq!(buffer1, buffer2);

        buffer1.printf("foo, bar, and baz");
        buffer2.printf("foo, bar, and baz");
        assert_eq!(buffer1, buffer2);
    }

    #[test]
    #[should_panic(expected = "Indent < 0")]
    fn test_indent_must_be_non_negative() {
        let mut buffer = CodeBuffer::new();
        buffer.increment_indent();
        buffer.decrement_indent();
        buffer.decrement_indent(); // This should panic
    }

    #[test]
    fn test_formatted_output() {
        let mut buffer = CodeBuffer::new();
        buffer.printf(format!("value: {}", 42));
        assert_eq!(buffer.lines[0], "value: 42");
    }
}
