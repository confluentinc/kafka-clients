# Phase 2: ProduceRequest/Response Wrappers

## Goal
Create the request/response wrapper types so the network client can send produce requests to a broker and parse responses.

## Java Classes to Translate
| Java Class | Rust Module |
|---|---|
| `ProduceRequest.java` | `common/requests/produce_request.rs` |
| `ProduceResponse.java` | `common/requests/produce_response.rs` |
| Add `Produce` variant to `ConcreteRequest` | Edit `abstract_request.rs` |
| Add `Produce` variant to `ConcreteResponse` | Edit `abstract_response.rs` |

## Java Tests to Translate
| Java Test | Count | Rust Target |
|---|---|---|
| `ProduceRequestTest.java` | 14 tests | `#[cfg(test)]` in `produce_request.rs` or `tests/` |
| `ProduceResponseTest.java` | 2 tests | `#[cfg(test)]` in `produce_response.rs` or `tests/` |

## Dependencies
- Generated `ProduceRequestData` / `ProduceResponseData` (from `generator/messages/ProduceRequest.json` / `ProduceResponse.json`)
- Phase 1 `MemoryRecords` (for request body containing record batches)
- Existing `RequestBuilder` trait, `RequestHeader`, `ResponseHeader`, `SendBuilder`

## Scope
- All versions supported by the generated data types
- No transactional produce (skip acks validation for transactional)
