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

// The one place a string becomes bytes (private; `check-string-encoding`
// refuses `Buffer.from(<string>)` and `TextEncoder` anywhere else in src/).
//
// A JavaScript string is UTF-16 and may hold a lone surrogate, which UTF-8
// cannot encode. Java's `String.getBytes(UTF_8)` (and so `StringSerializer`)
// writes `?` for each one; Node's `Buffer.from(s, 'utf8')`, and Node-API's
// `napi_get_value_string_utf8`, write U+FFFD (`EF BF BD`) instead. So a string
// becomes bytes here, the Java way, byte for byte as the Python client's
// `_encoding.py` writes them (root CLAUDE.md, JavaScript Binding Conventions,
// Types): a key or value through stringSerializer(), a header key, and the
// configuration's keys and values. Topic names skip it. Java's other standard
// charsets (`StandardCharsets`), for stringSerializer()'s configured encoding,
// are written here too, by hand, as Node's encoders differ from Java's for
// unmappable characters and lone surrogates.

/**
 * The Java charsets the binding encodes, Java's `StandardCharsets`, by their
 * canonical names.
 */
export type JavaCharset = 'US-ASCII' | 'ISO-8859-1' | 'UTF-8' | 'UTF-16BE' | 'UTF-16LE' | 'UTF-16';

// Each standard charset's canonical name and the JDK's aliases
// (`Charset.aliases()`), lower case: `Charset.forName` matches either,
// ignoring case.
const CHARSET_NAMES: ReadonlyMap<string, JavaCharset> = new Map(
  (
    [
      ['US-ASCII', ['646', 'ANSI_X3.4-1968', 'ANSI_X3.4-1986', 'ASCII', 'IBM367', 'ISO646-US', 'ISO_646.irv:1991', 'ascii7', 'cp367', 'csASCII', 'iso-ir-6', 'iso_646.irv:1983', 'us']],
      ['ISO-8859-1', ['819', '8859_1', 'IBM-819', 'IBM819', 'ISO8859-1', 'ISO8859_1', 'ISO_8859-1', 'ISO_8859-1:1987', 'ISO_8859_1', 'cp819', 'csISOLatin1', 'iso-ir-100', 'l1', 'latin1']],
      ['UTF-8', ['UTF8', 'unicode-1-1-utf-8']],
      ['UTF-16BE', ['ISO-10646-UCS-2', 'UTF_16BE', 'UnicodeBigUnmarked', 'X-UTF-16BE']],
      ['UTF-16LE', ['UTF_16LE', 'UnicodeLittleUnmarked', 'X-UTF-16LE']],
      ['UTF-16', ['UTF_16', 'UnicodeBig', 'unicode', 'utf16']],
    ] as Array<[JavaCharset, string[]]>
  ).flatMap(([name, aliases]) => [name, ...aliases].map((alias): [string, JavaCharset] => [alias.toLowerCase(), name])),
);

/**
 * The standard charset `name` names, its canonical name or one of the JDK's
 * aliases, ignoring case, as Java's `Charset.forName` matches them; null for
 * any other name.
 */
export function javaCharsetForName(name: string): JavaCharset | null {
  return CHARSET_NAMES.get(name.toLowerCase()) ?? null;
}

const REPLACEMENT = 0xfffd;
const QUESTION_MARK = 0x3f;

function isHigh(unit: number): boolean {
  return unit >= 0xd800 && unit <= 0xdbff;
}

function isLow(unit: number): boolean {
  return unit >= 0xdc00 && unit <= 0xdfff;
}

/**
 * `s` in a single-byte charset, as Java's encoder writes it: a character
 * above `max` is unmappable and becomes `?`, a surrogate pair (one
 * character) one `?`, a lone surrogate `?` too.
 */
function encodeSingleByte(s: string, max: number): Buffer {
  const out = Buffer.allocUnsafe(s.length);
  let n = 0;
  for (let i = 0; i < s.length; i++) {
    const unit = s.charCodeAt(i);
    if (unit <= max) {
      out[n++] = unit;
    } else {
      if (isHigh(unit) && i + 1 < s.length && isLow(s.charCodeAt(i + 1))) i++;
      out[n++] = QUESTION_MARK;
    }
  }
  return out.subarray(0, n);
}

/**
 * `s` in UTF-16, big- or little-endian, as Java's encoder writes it: each
 * code unit, a lone surrogate as U+FFFD; with `bom`, the big-endian byte
 * order mark `FE FF` first, unless the string is empty.
 */
function encodeUtf16(s: string, littleEndian: boolean, bom: boolean): Buffer {
  if (s.length === 0) return Buffer.alloc(0);
  const offset = bom ? 2 : 0;
  const out = Buffer.allocUnsafe(offset + 2 * s.length);
  if (bom) out.writeUInt16BE(0xfeff, 0);
  for (let i = 0; i < s.length; i++) {
    let unit = s.charCodeAt(i);
    if (isHigh(unit)) {
      if (i + 1 < s.length && isLow(s.charCodeAt(i + 1))) {
        const low = s.charCodeAt(i + 1);
        if (littleEndian) {
          out.writeUInt16LE(unit, offset + 2 * i);
          out.writeUInt16LE(low, offset + 2 * i + 2);
        } else {
          out.writeUInt16BE(unit, offset + 2 * i);
          out.writeUInt16BE(low, offset + 2 * i + 2);
        }
        i++;
        continue;
      }
      unit = REPLACEMENT;
    } else if (isLow(unit)) {
      unit = REPLACEMENT;
    }
    if (littleEndian) out.writeUInt16LE(unit, offset + 2 * i);
    else out.writeUInt16BE(unit, offset + 2 * i);
  }
  return out;
}

/**
 * The bytes of `s` as Java's `String.getBytes(charset)` writes them, UTF-8
 * unless another standard charset is given. In UTF-8 each lone surrogate is
 * `?`, and a well-formed string (`isWellFormed()`, which V8 answers without
 * scanning a one-byte string) is encoded as it is. In US-ASCII and
 * ISO-8859-1 a character the charset cannot map is `?`; in UTF-16BE,
 * UTF-16LE and UTF-16 (big-endian after the byte order mark `FE FF`) a lone
 * surrogate is U+FFFD. The Buffer is the client's own, so the addon sends it
 * without a copy.
 *
 * @param s - the string
 * @param charset - the charset, UTF-8 when left out
 * @returns its bytes
 */
export function encodeLikeJava(s: string, charset: JavaCharset = 'UTF-8'): Buffer {
  switch (charset) {
    case 'UTF-8':
      return Buffer.from(s.isWellFormed() ? s : s.replace(/\p{Surrogate}/gu, '?'), 'utf8');
    case 'US-ASCII':
      return encodeSingleByte(s, 0x7f);
    case 'ISO-8859-1':
      return encodeSingleByte(s, 0xff);
    case 'UTF-16BE':
      return encodeUtf16(s, false, false);
    case 'UTF-16LE':
      return encodeUtf16(s, true, false);
    case 'UTF-16':
      return encodeUtf16(s, false, true);
  }
}
