# Wire Protocol Implementation

## Byte Order
- **Big-endian** for all multi-byte integers (matching Java ByteBuffer)

## Varint Encoding
- Protocol Buffers unsigned and zig-zag signed encoding

## UUID Serialization
- Most significant 64 bits first, least significant 64 bits second

## String Encoding
- Standard: i16 length prefix + UTF-8 bytes
- Flexible: varint(length+1) + UTF-8 bytes (null=0, empty=1)

## Bytes Encoding
- Standard: i32 length prefix + raw bytes
- Flexible: varint(length+1) + raw bytes

## Array Encoding
- Standard: i32 length prefix + elements
- Flexible: varint(length+1) + elements

## Tagged Fields
- Encoded at end of struct in flexible version ranges
- Format: tag (varint) | size (varint) | data (size bytes)
- Struct tagged fields: serialized to temp buffer, size calculated, then written with tag and size

## ByteBufferAccessor
- Provides mutable byte buffer with position tracking
- All primitive types: byte, short, int, long, double, arrays
- Full compatibility with Java's Readable/Writable interfaces