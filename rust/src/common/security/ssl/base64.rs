/*
 * Copyright (c) 2012, 2025, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

//! Translated from OpenJDK 26 `java.util.Base64` (`java.base/java/util/Base64.java`), the basic
//! decoder (`Base64.getDecoder()`) only.
//!
//! This file is a translation of OpenJDK code and keeps its license: GPL version 2 with the
//! Classpath Exception (the notice above is the one of the original file). The rest of the crate
//! is under the Apache License 2.0.
//!
//! Kafka reads the body of a PEM entry with `Base64.getDecoder().decode(String)`
//! (`DefaultSslEngineFactory.PemParser.pemEntries`), which is lenient where RFC 4648 is strict
//! (the `=` padding is optional) and has its own messages for what it rejects, so reading the
//! same configuration the same way takes the same decoder.
//!
//! Omitted, as nothing here needs it: the URL and MIME variants (the `isURL` / `isMIME` flags),
//! the encoder, the stream wrappers, and `decodeBlock`, the intrinsic fast path, which decodes
//! complete 4-character groups with no other effect than speed.

/// The Base64 alphabet (RFC 4648 Table 1) as a decoding table: the value of each byte, `-1` for
/// a byte that is not part of it, and `-2` for the padding `=`.
///
/// `Base64.Decoder.fromBase64`.
const fn from_base64() -> [i32; 256] {
    let mut table = [-1i32; 256];
    let mut i = 0;
    while i < 64 {
        let c = match i {
            0..=25 => b'A' + i as u8,
            26..=51 => b'a' + (i - 26) as u8,
            52..=61 => b'0' + (i - 52) as u8,
            62 => b'+',
            _ => b'/',
        };
        table[c as usize] = i;
        i += 1;
    }
    table[b'=' as usize] = -2;
    table
}

static FROM_BASE64: [i32; 256] = from_base64();

/// This type consists exclusively of static methods for obtaining decoders for the Base64
/// encoding scheme.
#[doc(alias = "java.util.Base64")]
pub(crate) struct Base64;

impl Base64 {
    /// Returns a [`Decoder`] that decodes using the Basic type base64 encoding scheme.
    #[doc(alias = "java.util.Base64#getDecoder")]
    pub(crate) fn get_decoder() -> Decoder {
        Decoder
    }
}

/// This type implements a decoder for decoding byte data using the Base64 encoding scheme as
/// specified in RFC 4648 (the Basic type).
///
/// The Base64 padding character `=` is accepted and interpreted as the end of the encoded byte
/// data, but is not required. So if the final unit of the encoded byte data only has two or three
/// Base64 characters (without the corresponding padding character(s) padded), they are decoded as
/// if followed by padding character(s). If there is a padding character present in the final
/// unit, the correct number of padding character(s) must be present, otherwise
/// `IllegalArgumentException` (`NONE`) is thrown during decoding.
///
/// The Basic decoder rejects any character that is not in the Base64 alphabet.
#[doc(alias = "java.util.Base64$Decoder")]
pub(crate) struct Decoder;

impl Decoder {
    /// Decodes all bytes from the input byte array using the Base64 encoding scheme, writing the
    /// results into a newly-allocated output byte array.
    ///
    /// # Errors
    ///
    /// The message of the `IllegalArgumentException` the JDK throws when `src` is not in valid
    /// Base64 scheme.
    #[doc(alias = "java.util.Base64$Decoder#decode")]
    pub(crate) fn decode(&self, src: &[u8]) -> Result<Vec<u8>, String> {
        let dst_len = Self::decoded_out_length(src, 0, src.len())?;
        let mut dst = Vec::with_capacity(dst_len);
        self.decode0(src, 0, src.len(), &mut dst)?;
        Ok(dst)
    }

    /// Returns the length of the decoded bytes, the capacity the output needs.
    ///
    /// `Base64.Decoder.decodedOutLength`.
    #[doc(alias = "java.util.Base64$Decoder#decodedOutLength")]
    fn decoded_out_length(src: &[u8], sp: usize, sl: usize) -> Result<usize, String> {
        let mut paddings: usize = 0;
        let len = sl - sp;
        if len == 0 {
            return Ok(0);
        }
        if len < 2 {
            return Err("Input byte[] should at least have 2 bytes for base64 bytes".to_string());
        }
        if src[sl - 1] == b'=' {
            paddings += 1;
            if src[sl - 2] == b'=' {
                paddings += 1;
            }
        }
        if paddings == 0 && (len & 0x3) != 0 {
            paddings = 4 - (len & 0x3);
        }
        Ok(3 * len.div_ceil(4) - paddings)
    }

    /// Decodes base64 characters, appending the data bytes to `dst`.
    ///
    /// `Base64.Decoder.decode0`.
    #[doc(alias = "java.util.Base64$Decoder#decode0")]
    fn decode0(&self, src: &[u8], mut sp: usize, sl: usize, dst: &mut Vec<u8>) -> Result<(), String> {
        let mut bits: u32 = 0;
        let mut shiftto: i32 = 18; // pos of first byte of 4-byte atom

        while sp < sl {
            let byte = src[sp];
            sp += 1;
            let b = FROM_BASE64[usize::from(byte)];
            if b < 0 {
                if b == -2 {
                    // padding byte '='
                    // =     shiftto==18 unnecessary padding
                    // x=    shiftto==12 a dangling single x
                    // x     to be handled together with non-padding case
                    // xx=   shiftto==6&&sp==sl missing last =
                    // xx=y  shiftto==6 last is not =
                    let missing_second_padding = shiftto == 6 && {
                        let missing = sp == sl || src[sp] != b'=';
                        sp += 1;
                        missing
                    };
                    if missing_second_padding || shiftto == 18 {
                        return Err("Input byte array has wrong 4-byte ending unit".to_string());
                    }
                    break;
                }
                // `Integer.toString(src[sp - 1], 16)`: Java's `byte` is signed
                let signed = byte as i8;
                return Err(if signed < 0 {
                    format!("Illegal base64 character -{:x}", -i32::from(signed))
                } else {
                    format!("Illegal base64 character {signed:x}")
                });
            }
            bits |= (b as u32) << shiftto;
            shiftto -= 6;
            if shiftto < 0 {
                dst.push((bits >> 16) as u8);
                dst.push((bits >> 8) as u8);
                dst.push(bits as u8);
                shiftto = 18;
                bits = 0;
            }
        }
        // reached end of byte array or hit padding '=' characters.
        if shiftto == 6 {
            dst.push((bits >> 16) as u8);
        } else if shiftto == 0 {
            dst.push((bits >> 16) as u8);
            dst.push((bits >> 8) as u8);
        } else if shiftto == 12 {
            // dangling single "x", incorrectly encoded.
            return Err("Last unit does not have enough valid bits".to_string());
        }
        // anything left is invalid, if is not MIME.
        if sp < sl {
            return Err(format!("Input byte array has incorrect ending byte at {sp}"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every row is what `Base64.getDecoder().decode(s.getBytes(ISO_8859_1))` answered when run on
    /// OpenJDK 26 (`Base64Oracle.java`): the decoded bytes in hex, or the message of the exception.
    #[test]
    fn test_decode_is_what_the_jdk_decodes() {
        let cases: [(&str, Result<&str, &str>); 28] = [
            ("", Ok("")),
            ("A", Err("Input byte[] should at least have 2 bytes for base64 bytes")),
            ("AQ", Ok("01")),
            ("AQI", Ok("0102")),
            ("AQID", Ok("010203")),
            ("AQIDB", Err("Last unit does not have enough valid bits")),
            ("AQIDBA", Ok("01020304")),
            ("AQIDBAU", Ok("0102030405")),
            ("AQ=", Err("Input byte array has wrong 4-byte ending unit")),
            ("AQ==", Ok("01")),
            ("AQ===", Err("Input byte array has incorrect ending byte at 4")),
            ("A===", Err("Last unit does not have enough valid bits")),
            ("A=", Err("Last unit does not have enough valid bits")),
            ("=", Err("Input byte[] should at least have 2 bytes for base64 bytes")),
            ("====", Err("Input byte array has wrong 4-byte ending unit")),
            ("=AQID", Err("Input byte array has wrong 4-byte ending unit")),
            ("AQ=D", Err("Input byte array has wrong 4-byte ending unit")),
            ("AQID=", Err("Input byte array has wrong 4-byte ending unit")),
            ("AQID====", Err("Input byte array has wrong 4-byte ending unit")),
            ("AQ==AQID", Err("Input byte array has incorrect ending byte at 4")),
            ("AQ!D", Err("Illegal base64 character 21")),
            ("AQ D", Err("Illegal base64 character 20")),
            ("AQID\u{e9}", Err("Illegal base64 character -17")),
            ("AQI=", Ok("0102")),
            ("AQI==", Err("Input byte array has incorrect ending byte at 4")),
            ("AQIDAQ==", Ok("01020301")),
            ("AQIDAQ=", Err("Input byte array has wrong 4-byte ending unit")),
            ("Zm9vYg", Ok("666f6f62")),
        ];
        for (input, expected) in cases {
            // `getBytes(ISO_8859_1)`: a char is a byte
            let bytes: Vec<u8> = input.chars().map(|c| c as u32 as u8).collect();
            let actual = Base64::get_decoder()
                .decode(&bytes)
                .map(|out| out.iter().map(|b| format!("{b:02x}")).collect::<String>());
            assert_eq!(expected.map(str::to_string).map_err(str::to_string), actual, "{input:?}");
        }
    }

    /// The whole alphabet, and a longer text, round trip (the table above is about the edges).
    #[test]
    fn test_decode_alphabet_and_long_input() {
        assert_eq!(
            Ok(vec![0xFB, 0xFF, 0xBF]),
            Base64::get_decoder().decode(b"+/+/"),
            "62 and 63 are + and /"
        );
        let text = "The quick brown fox jumps over the lazy dog";
        let encoded = "VGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIHRoZSBsYXp5IGRvZw==";
        assert_eq!(Ok(text.as_bytes().to_vec()), Base64::get_decoder().decode(encoded.as_bytes()));
    }
}
