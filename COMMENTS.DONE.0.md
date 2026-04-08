# Resolved Issues from Critic 0 — Layer 4 Review (commit e4502ef)

## Issue 19: EOF not detected in NetworkReceive::read_from — RESOLVED
- **Fix commit**: 9369f7f (fixup\! Implement Layer 4)
- **Resolution**: Ok(0) from async read now correctly signals EOF (UnexpectedEof error) when no bytes have been read yet in the current call. When total_read > 0 (e.g., size header was read), Ok(0) during payload is treated as "no more data available" (matching Java NIO returning 0). Added test_eof_during_size_read and test_eof_during_payload_read tests.

## Issue 20: TransportLayer does not implement std::io::Read — RESOLVED
- **Fix commit**: 9369f7f (fixup\! Implement Layer 4)
- **Resolution**: Changed Receive::read_from to take &mut dyn TransportLayer instead of &mut dyn io::Read, preserving the Java composability where TransportLayer extends ScatteringByteChannel.

## Issue 21: PlaintextTransportLayer uses std::net::TcpStream instead of tokio::net::TcpStream — RESOLVED
- **Fix commit**: 9369f7f (fixup\! Implement Layer 4)
- **Resolution**: Rewrote PlaintextTransportLayer to use tokio::net::TcpStream. Made all I/O methods async using boxed futures for dyn-safety. Updated all dependent traits and implementations.
