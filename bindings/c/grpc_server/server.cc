// Copyright 2025 Confluent Inc.
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

// gRPC C++ server exposing the public C FFI (confluent_kafka.h) over the
// ProducerService schema in
// multilanguage-test-server/proto/producer_service.proto.
//
// Used by the Rust integration test harness under the multilanguage-tests
// feature: the Rust MultilanguageProducer client tunnels every Producer
// trait call to this server, which calls the kafka_producer_* FFI
// directly. See design/history/MILESTONE-6/DESIGN-multilanguage-tests.md.
//
// Why C++ rather than pure C: grpc_cpp is the realistic gRPC stack here.
// Pure C gRPC (grpc-c) is much less mature; the test target is the C
// FFI itself, not the gRPC layer.
//
// User callbacks (delivery / offset-commit / rebalance listener) are covered by
// the callback-log section below: the harness sets a per-request flag, this
// server registers real C callbacks, and GetCallbackLog reports what they saw.

#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

extern "C" {
#include "confluent_kafka.h"
}

#include "producer_service.grpc.pb.h"
#include "producer_service.pb.h"
#include "consumer_service.grpc.pb.h"
#include "consumer_service.pb.h"
#include "admin_service.grpc.pb.h"
#include "admin_service.pb.h"

using confluent::kafka::test::CloseRequest;
using confluent::kafka::test::CloseTimeoutRequest;
using confluent::kafka::test::CreateProducerRequest;
using confluent::kafka::test::CreateProducerResponse;
using confluent::kafka::test::FlushRequest;
using confluent::kafka::test::KafkaError;
using confluent::kafka::test::PartitionsForRequest;
using confluent::kafka::test::PartitionsForResponse;
using confluent::kafka::test::ProducerService;
using confluent::kafka::test::RecordMetadata;
using confluent::kafka::test::SendRequest;
using confluent::kafka::test::SendResponse;
using confluent::kafka::test::StatusResponse;
// ConsumerService messages (consumer_service.proto).
using confluent::kafka::test::AssignRequest;
using confluent::kafka::test::CallbackLogRequest;
using confluent::kafka::test::CommitAsyncRequest;
using confluent::kafka::test::CommittedRequest;
using confluent::kafka::test::CommittedResponse;
using confluent::kafka::test::CommitSyncRequest;
using confluent::kafka::test::ConsumerCloseRequest;
using confluent::kafka::test::ConsumerIdRequest;
using confluent::kafka::test::ConsumerPartitionsForRequest;
using confluent::kafka::test::ConsumerRecordList;
using confluent::kafka::test::ConsumerService;
using confluent::kafka::test::CreateConsumerRequest;
using confluent::kafka::test::CreateConsumerResponse;
using confluent::kafka::test::ListTopicsResponse;
using confluent::kafka::test::LongOffsetMap;
using confluent::kafka::test::LongOffsetsResponse;
using confluent::kafka::test::OffsetAndTimestampMap;
using confluent::kafka::test::OffsetAndTimestampResponse;
using confluent::kafka::test::OffsetMap;
using confluent::kafka::test::OffsetsForTimesRequest;
using confluent::kafka::test::PollRequest;
using confluent::kafka::test::PollResponse;
using confluent::kafka::test::PositionRequest;
using confluent::kafka::test::PositionResponse;
using confluent::kafka::test::SeekRequest;
using confluent::kafka::test::SubscribeRequest;
using confluent::kafka::test::Metric;
using confluent::kafka::test::MetricList;
using confluent::kafka::test::MetricsRequest;
using confluent::kafka::test::MetricsResponse;
using confluent::kafka::test::SubscriptionResponse;
using confluent::kafka::test::TopicListing;
using confluent::kafka::test::TopicPartitionList;
using confluent::kafka::test::TopicPartitionListRequest;
using confluent::kafka::test::TopicPartitionListResponse;
// AdminService messages (admin_service.proto).
using confluent::kafka::test::AclOperationList;
using confluent::kafka::test::AdminCloseRequest;
using confluent::kafka::test::AdminListTopicsRequest;
using confluent::kafka::test::AdminListTopicsResponse;
using confluent::kafka::test::AdminService;
using confluent::kafka::test::AdminTopicListing;
using confluent::kafka::test::ConfigEntry;
using confluent::kafka::test::CreateAdminRequest;
using confluent::kafka::test::CreateAdminResponse;
using confluent::kafka::test::CreatePartitionsRequest;
using confluent::kafka::test::CreateTopicsEntry;
using confluent::kafka::test::CreateTopicsRequest;
using confluent::kafka::test::CreateTopicsResponse;
using confluent::kafka::test::DeleteRecordsEntry;
using confluent::kafka::test::DeleteRecordsRequest;
using confluent::kafka::test::DeleteRecordsResponse;
using confluent::kafka::test::DeletedRecords;
using confluent::kafka::test::DeleteTopicsRequest;
using confluent::kafka::test::DescribeTopicsEntry;
using confluent::kafka::test::DescribeTopicsRequest;
using confluent::kafka::test::DescribeTopicsResponse;
using confluent::kafka::test::NodeList;
using confluent::kafka::test::ResultKey;
using confluent::kafka::test::TopicDescription;
using confluent::kafka::test::TopicMetadata;
using confluent::kafka::test::TopicMetadataAndConfig;
using confluent::kafka::test::TopicPartitionInfo;
using confluent::kafka::test::VoidKeyedResponse;
using confluent::kafka::test::VoidResultEntry;
// Shared / payload messages.
using confluent::kafka::test::CallbackLogEntry;
using confluent::kafka::test::CallbackLogPartition;
using confluent::kafka::test::CallbackLogResponse;
using confluent::kafka::test::ConsumerRecord;
using confluent::kafka::test::Header;
using confluent::kafka::test::LongOffsetMapEntry;
using confluent::kafka::test::Node;
using confluent::kafka::test::OffsetAndMetadata;
using confluent::kafka::test::OffsetAndTimestamp;
using confluent::kafka::test::OffsetAndTimestampMapEntry;
using confluent::kafka::test::OffsetMapEntry;
using confluent::kafka::test::PartitionInfo;
using confluent::kafka::test::ProducerCallbackLogRequest;
using confluent::kafka::test::StringList;
using confluent::kafka::test::TopicPartition;
using confluent::kafka::test::TopicPartitionInfoEntry;

namespace {

// Mirrors the proto KafkaError.Variant enum. Keep in lockstep with
// producer_service.proto.
constexpr int VARIANT_GENERIC = 0;
constexpr int VARIANT_TOPIC_AUTHORIZATION = 1;
constexpr int VARIANT_INVALID_TOPIC = 2;
constexpr int VARIANT_GROUP_AUTHORIZATION = 3;
constexpr int VARIANT_BUFFER_EXHAUSTED = 4;
constexpr int VARIANT_ILLEGAL_ARGUMENT = 5;
constexpr int VARIANT_ILLEGAL_STATE = 6;
constexpr int VARIANT_TIMEOUT = 7;
constexpr int VARIANT_RECORD_TOO_LARGE = 8;
constexpr int VARIANT_SERIALIZATION = 9;

// Infer the proto KafkaError.Variant from an error message.
//
// The C FFI does not surface the Rust enum discriminator — only the code, the
// message and the retriable/fatal flags — so both servers have to guess, and the
// Rust client matches on the variant it receives. **The guess must be identical
// in both servers**: a differential harness whose two servers translate the same
// error differently manufactures disagreements that are not defects. This
// mirrors `grpc_translate.py`'s `_guess_variant` pattern for pattern and in the
// same order, including the lowercasing.
//
// Slice G1 found this the hard way. This function used to live inside
// ProducerServiceImpl, cover only three of the nine variants, and be reachable
// only from the two `Send` call sites — every other error left `fill_proto_error`
// at its GENERIC default. Python meanwhile guessed for *every* error, so an admin
// timeout crossed as Timeout from Python (retriable) and as
// Generic/UnknownServerError from C (not retriable), failing two scenarios on the
// C backend alone with nothing wrong in the client.
int guess_variant(const char* message) {
  if (message == nullptr) return VARIANT_GENERIC;
  std::string s(message);
  std::transform(s.begin(), s.end(), s.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  if (s.empty()) return VARIANT_GENERIC;
  const auto has = [&s](const char* needle) { return s.find(needle) != std::string::npos; };
  if (has("max.request.size") || has("is larger than") || has("too large")) {
    return VARIANT_RECORD_TOO_LARGE;
  }
  if (has("buffer is full") || has("buffer.memory")) return VARIANT_BUFFER_EXHAUSTED;
  if (has("timed out") || has("expired") || has("not present in metadata")) {
    return VARIANT_TIMEOUT;
  }
  if (has("topic authorization")) return VARIANT_TOPIC_AUTHORIZATION;
  if (has("invalid topic")) return VARIANT_INVALID_TOPIC;
  if (has("group authorization")) return VARIANT_GROUP_AUTHORIZATION;
  if (has("illegal state") || has("already been closed")) return VARIANT_ILLEGAL_STATE;
  if (has("serialization") || has("failed to serialize")) return VARIANT_SERIALIZATION;
  return VARIANT_GENERIC;
}

// Build a proto KafkaError from a C FFI error handle. Takes ownership
// of the handle (destroys it on the way out).
//
// The variant is always inferred from the message, as the Python servers do for
// every error. There is no `variant_hint` parameter any more: its GENERIC default
// was the divergence described on `guess_variant`, and every caller that wanted
// the right answer had to remember to pass the guess explicitly.
void fill_proto_error(KafkaError* dst, kafka_common_KafkaError_t* err) {
  if (err == nullptr) {
    dst->set_variant(static_cast<KafkaError::Variant>(VARIANT_GENERIC));
    dst->set_code(-1);
    dst->set_message("c server: null error handle");
    dst->set_is_retriable(false);
    dst->set_is_fatal(true);
    return;
  }
  const int32_t code = kafka_common_KafkaError_code(err);
  const char* msg = kafka_common_KafkaError_message(err);
  dst->set_variant(static_cast<KafkaError::Variant>(guess_variant(msg)));
  dst->set_code(code);
  dst->set_message(msg ? std::string(msg) : std::string());
  dst->set_is_retriable(kafka_common_KafkaError_is_retriable(err));
  dst->set_is_fatal(kafka_common_KafkaError_is_fatal(err));
  kafka_common_KafkaError_destroy(err);
}

// The normative mock-selection rule of admin_service.proto's
// CreateAdminRequest: an empty config, or one whose every value is empty,
// selects the mock. Slice G1 unified this with both Python servers, which
// already applied the broader predicate; testing only `empty()` made
// {"bootstrap.servers": ""} pick a real client here and a mock there.
template <typename ConfigMap>
bool selects_mock(const ConfigMap& config) {
  if (config.empty()) return true;
  for (const auto& kv : config) {
    if (!kv.second.empty()) return false;
  }
  return true;
}

// Construct a synthetic KafkaError without an underlying FFI handle.
KafkaError make_synthetic_error(int variant, const std::string& message,
                                bool is_fatal = true) {
  KafkaError err;
  err.set_variant(static_cast<KafkaError::Variant>(variant));
  err.set_code(-1);
  err.set_message("c server: " + message);
  err.set_is_retriable(false);
  err.set_is_fatal(is_fatal);
  return err;
}

// Fields from a metadata handle, copied via the convenience callback so
// we get all fields in one FFI hop and the handle is destroyed for us.
struct MetadataFields {
  int64_t offset = -1;
  int32_t partition = -1;
  std::string topic;
  int64_t timestamp = -1;
};

extern "C" void metadata_copy_cb(int64_t offset, int32_t partition,
                                 const char* topic, int64_t timestamp,
                                 void* user_data) {
  auto* out = static_cast<MetadataFields*>(user_data);
  out->offset = offset;
  out->partition = partition;
  out->topic = topic ? std::string(topic) : std::string();
  out->timestamp = timestamp;
}

// Defined in the consumer section below; reused by the producer PartitionsFor.
void node_to_proto(const kafka_common_Node_t* node, Node* dst);
void partition_info_to_proto(const kafka_consumer_PartitionInfo_t* info, PartitionInfo* dst);

// ---------------------------------------------------------------------------
// Callback log
//
// The Rust multilanguage harness cannot hand an in-process callback object to
// this server, so instead it sets the SubscribeRequest.with_listener /
// CommitAsyncRequest.with_callback / SendRequest.with_callback flags, we register
// *real* C callbacks through the FFI, and it reads back what those callbacks
// observed with GetCallbackLog.
//
// The kind strings and field encoding are pinned by producer_service.proto's
// CallbackLogEntry — this server, grpc_server.py and grpc_server_async.py must
// emit them identically or the shared Rust test body cannot compare backends.
// ---------------------------------------------------------------------------

constexpr const char* KIND_ASSIGNED = "assigned";
constexpr const char* KIND_REVOKED = "revoked";
constexpr const char* KIND_LOST = "lost";
constexpr const char* KIND_COMMIT = "commit";
constexpr const char* KIND_DELIVERY = "delivery";

// The CallbackLogEntry.offsets key for a partition.
std::string offset_key(const std::string& topic, int32_t partition) {
  return topic + "-" + std::to_string(partition);
}

// Thread-safe per-client log of user-callback invocations.
//
// The mutex is mandatory, not defensive: every FFI callback here runs on the
// client's own dispatcher thread, while GetCallbackLog is served on a gRPC
// worker thread. It is deliberately a *separate* mutex from the service's
// id-map `mu_`, so a callback firing mid-poll never has to wait behind an
// in-flight CreateX / Close.
class CallbackLog {
 public:
  void append(uint64_t client_id, CallbackLogEntry entry) {
    std::lock_guard<std::mutex> lock(mu_);
    entries_[client_id].push_back(std::move(entry));
  }

  // Entries are never dropped, not even on Close: the callbacks a close()
  // drives (a delivery report from its flush, a commit callback from its final
  // drain, on_partitions_lost) are exactly the ones a test wants to read
  // afterwards. The server's lifetime is one test session. This matches
  // grpc_server.py / grpc_server_async.py, whose service-level CallbackLog is
  // likewise never popped on Close.
  //
  // Post-close reads are *eventually* consistent on this backend, though: an
  // FFI callback is enqueued on the client's dispatcher thread and appended
  // when that job runs, and neither `..._close` nor `..._destroy` joins the
  // dispatcher. The Python servers append synchronously in-process and so have
  // no such window. Callers that assert on entry counts must therefore poll
  // (`wait_for_kind` / `wait_for_kind_settled` / `poll_until_kind` on the Rust
  // side) rather than read once.
  void fill(uint64_t client_id, CallbackLogResponse* resp) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = entries_.find(client_id);
    if (it == entries_.end()) return;
    for (const auto& entry : it->second) *resp->add_entries() = entry;
  }

 private:
  std::mutex mu_;
  std::unordered_map<uint64_t, std::vector<CallbackLogEntry>> entries_;
};

// What a C callback needs in order to find its log: the `user_data` for every
// callback this server registers.
//
// Exactly one heap instance per client, allocated in CreateProducer /
// CreateConsumer and owned by the service's `log_states_` map — NOT by any
// individual callback registration. That is what makes every `user_data_destroy`
// argument below `nullptr`, and it is deliberate:
//
//   - a rebalance listener is released only by a *replacing* subscribe or by
//     consumer destroy (never by unsubscribe), and a later re-subscribe must be
//     able to reuse the same state;
//   - `kafka_producer_Producer_send_with_callback` has no destroy hook at all,
//     so a per-send allocation would have to be freed by the callback itself —
//     and then leak on the validation-failure path where the callback is
//     documented not to fire.
//
// The state is **never freed before the service is destroyed** — it is
// session-lifetime, held by `log_states_` as a `unique_ptr` that `Close` does
// not erase.
//
// It used to be `delete`d in Close right after `..._destroy(client)` returned,
// on the premise that destroying the client drops the listener / commit
// adapters so no callback could still reference it. That premise is false:
// neither destroy *joins* the dispatcher thread that actually runs the C
// callback, both deliberately detach it (`src/ffi/producer.rs`,
// `src/ffi/consumer.rs`), and the Rust-side callback only *enqueues* the C
// callback as a dispatcher job. A queued `log_delivery(..., state)` could
// therefore still dereference `state->log` after the `delete` — a
// use-after-free. Keeping the state alive for the whole session also removes
// the `log_state_for()`-returns-nullptr-after-Close race in Send / CommitAsync
// and the leak for a client that is never Closed (neither service impl has a
// destructor).
struct LogState {
  CallbackLog* log;
  uint64_t client_id;
};

// Shared body of the three rebalance trampolines. The callee owns the delivered
// TopicPartitionList and must destroy it; returning NULL means the listener
// succeeded (a non-null error would fail the rebalance, like a throwing Java
// listener).
kafka_common_KafkaError_t* log_rebalance(kafka_consumer_TopicPartitionList_t* partitions,
                                        void* user_data, const char* kind) {
  auto* state = static_cast<LogState*>(user_data);
  CallbackLogEntry entry;
  entry.set_kind(kind);
  if (partitions != nullptr) {
    int32_t n = kafka_consumer_TopicPartitionList_count(partitions);
    for (int32_t i = 0; i < n; i++) {
      const kafka_consumer_TopicPartition_t* tp =
          kafka_consumer_TopicPartitionList_get(partitions, i);
      CallbackLogPartition* p = entry.add_partitions();
      const char* topic = kafka_consumer_TopicPartition_topic(tp);
      p->set_topic(topic ? topic : "");
      p->set_partition(kafka_consumer_TopicPartition_partition(tp));
    }
    kafka_consumer_TopicPartitionList_destroy(partitions);
  }
  state->log->append(state->client_id, std::move(entry));
  return nullptr;
}

extern "C" kafka_common_KafkaError_t* log_partitions_assigned(
    kafka_consumer_TopicPartitionList_t* partitions, void* user_data) {
  return log_rebalance(partitions, user_data, KIND_ASSIGNED);
}

extern "C" kafka_common_KafkaError_t* log_partitions_revoked(
    kafka_consumer_TopicPartitionList_t* partitions, void* user_data) {
  return log_rebalance(partitions, user_data, KIND_REVOKED);
}

// Passed explicitly rather than left NULL (which would make the adapter
// reproduce Java's "onPartitionsLost delegates to onPartitionsRevoked" default),
// so a lost callback is distinguishable from a revoke in the log.
extern "C" kafka_common_KafkaError_t* log_partitions_lost(
    kafka_consumer_TopicPartitionList_t* partitions, void* user_data) {
  return log_rebalance(partitions, user_data, KIND_LOST);
}

// Move the error message into `entry` and destroy the handle (the callee owns
// every non-null handle delivered to a callback).
void take_error_into(CallbackLogEntry* entry, kafka_common_KafkaError_t* error) {
  if (error == nullptr) return;
  const char* msg = kafka_common_KafkaError_message(error);
  entry->set_error(msg ? std::string(msg) : std::string("c server: unnamed callback error"));
  kafka_common_KafkaError_destroy(error);
}

// Copy an owned OffsetMap into `entry`'s partitions + offsets, then destroy it.
void take_offsets_into(CallbackLogEntry* entry, kafka_consumer_OffsetMap_t* offsets) {
  if (offsets == nullptr) return;
  int32_t n = kafka_consumer_OffsetMap_count(offsets);
  for (int32_t i = 0; i < n; i++) {
    const kafka_consumer_TopicPartition_t* tp = kafka_consumer_OffsetMap_get_key(offsets, i);
    const char* raw_topic = kafka_consumer_TopicPartition_topic(tp);
    const std::string topic = raw_topic ? raw_topic : "";
    const int32_t partition = kafka_consumer_TopicPartition_partition(tp);
    CallbackLogPartition* p = entry->add_partitions();
    p->set_topic(topic);
    p->set_partition(partition);
    const kafka_consumer_OffsetAndMetadata_t* v = kafka_consumer_OffsetMap_get_value(offsets, i);
    (*entry->mutable_offsets())[offset_key(topic, partition)] =
        kafka_consumer_OffsetAndMetadata_offset(v);
  }
  kafka_consumer_OffsetMap_destroy(offsets);
}

extern "C" void log_commit_complete(kafka_consumer_OffsetMap_t* offsets,
                                    kafka_common_KafkaError_t* error, void* user_data) {
  auto* state = static_cast<LogState*>(user_data);
  CallbackLogEntry entry;
  entry.set_kind(KIND_COMMIT);
  take_offsets_into(&entry, offsets);
  take_error_into(&entry, error);
  state->log->append(state->client_id, std::move(entry));
}

// Java's commitAsync(offsets, null) is legal, but
// kafka_consumer_Consumer_commit_async_offsets_with_callback's `callback`
// parameter is not nullable and there is no plain `..._commit_async_offsets`.
// So an explicit-offsets CommitAsync without with_callback gets this no-op,
// which still has to free the handles it is given.
extern "C" void discard_commit_complete(kafka_consumer_OffsetMap_t* offsets,
                                        kafka_common_KafkaError_t* error, void* /*user_data*/) {
  if (offsets != nullptr) kafka_consumer_OffsetMap_destroy(offsets);
  if (error != nullptr) kafka_common_KafkaError_destroy(error);
}

extern "C" void log_delivery(kafka_producer_RecordMetadata_t* metadata,
                             kafka_common_KafkaError_t* error, void* user_data) {
  auto* state = static_cast<LogState*>(user_data);
  CallbackLogEntry entry;
  entry.set_kind(KIND_DELIVERY);
  // Both arguments can be set at once: a real producer rejecting a record
  // before it reaches the accumulator delivers placeholder metadata
  // (offset/partition -1) alongside the error, mirroring Java's
  // callback.onCompletion(nullMetadata, e). Record whichever is present.
  if (metadata != nullptr) {
    MetadataFields fields;
    // Also destroys the handle, which the callee owns.
    kafka_producer_RecordMetadata_copy(metadata, metadata_copy_cb, &fields);
    CallbackLogPartition* p = entry.add_partitions();
    p->set_topic(fields.topic);
    p->set_partition(fields.partition);
    (*entry.mutable_offsets())[offset_key(fields.topic, fields.partition)] = fields.offset;
  }
  take_error_into(&entry, error);
  state->log->append(state->client_id, std::move(entry));
}

class ProducerServiceImpl final : public ProducerService::Service {
 public:
  grpc::Status CreateProducer(grpc::ServerContext*,
                              const CreateProducerRequest* req,
                              CreateProducerResponse* resp) override {
    kafka_producer_Producer_t* producer = nullptr;
    kafka_common_KafkaError_t* err = nullptr;

    if (req->config().empty()) {
      // Empty config selects MockProducer for client-side smoke testing.
      producer = kafka_producer_MockProducer_new(/*auto_complete=*/true);
    } else {
      kafka_producer_ProducerProperties_t* props =
          kafka_producer_ProducerProperties_new();
      for (const auto& kv : req->config()) {
        kafka_producer_ProducerProperties_put(props, kv.first.c_str(),
                                              kv.second.c_str());
      }
      producer = kafka_producer_KafkaProducer_new(props, &err);
      kafka_producer_ProducerProperties_destroy(props);
    }

    if (producer == nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }

    const uint64_t id = next_id_.fetch_add(1);
    {
      std::lock_guard<std::mutex> lock(mu_);
      producers_[id] = producer;
      // One stable LogState per producer — see the struct's comment for why the
      // delivery callback's user_data cannot be per-send, and why it lives for
      // the whole session rather than being freed at Close.
      log_states_[id] = std::unique_ptr<LogState>(new LogState{&callback_log_, id});
    }
    resp->set_producer_id(id);
    std::cerr << "c server: created producer " << id << std::endl;
    return grpc::Status::OK;
  }

  grpc::Status Send(grpc::ServerContext*, const SendRequest* req,
                    SendResponse* resp) override {
    kafka_producer_Producer_t* producer = producer_for(req->producer_id());
    if (producer == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          VARIANT_ILLEGAL_STATE,
          "unknown producer_id " + std::to_string(req->producer_id()));
      return grpc::Status::OK;
    }

    const auto& rec = req->record();
    const int32_t partition = rec.has_partition() ? rec.partition() : -1;
    const int64_t timestamp = rec.has_timestamp() ? rec.timestamp() : -1;
    const uint8_t* key = nullptr;
    int32_t key_len = -1;
    if (rec.has_key()) {
      key = reinterpret_cast<const uint8_t*>(rec.key().data());
      key_len = static_cast<int32_t>(rec.key().size());
    }
    const uint8_t* value = nullptr;
    int32_t value_len = -1;
    if (rec.has_value()) {
      value = reinterpret_cast<const uint8_t*>(rec.value().data());
      value_len = static_cast<int32_t>(rec.value().size());
    }

    // with_callback => register a *real* delivery callback through the FFI, so
    // the Rust harness can assert (via GetCallbackLog) on what the binding's own
    // callback saw. The blocking future path below is unchanged: the FFI gives us
    // both, exactly like Java's send(record, callback).
    kafka_common_KafkaError_t* send_err = nullptr;
    kafka_producer_FutureRecordMetadata_t* future = nullptr;
    if (req->with_callback()) {
      LogState* state = log_state_for(req->producer_id());
      future = kafka_producer_Producer_send_with_callback(
          producer, rec.topic().c_str(), partition, timestamp, key, key_len,
          value, value_len, log_delivery, state, &send_err);
    } else {
      future = kafka_producer_Producer_send(
          producer, rec.topic().c_str(), partition, timestamp, key, key_len,
          value, value_len, &send_err);
    }
    if (future == nullptr) {
      // Synchronous failure (RecordTooLarge, IllegalState, etc.). The
      // FFI returns a non-null error we forward verbatim.
      fill_proto_error(resp->mutable_error(), send_err);
      return grpc::Status::OK;
    }

    // Block this gRPC worker thread waiting for the future to resolve.
    // The FFI's get() blocks on a tokio runtime handle that the producer
    // owns, so it's safe to call from arbitrary threads.
    kafka_common_KafkaError_t* get_err = nullptr;
    kafka_producer_RecordMetadata_t* metadata =
        kafka_producer_FutureRecordMetadata_get(future, &get_err);
    kafka_producer_FutureRecordMetadata_destroy(future);
    if (metadata == nullptr) {
      fill_proto_error(resp->mutable_error(), get_err);
      return grpc::Status::OK;
    }

    MetadataFields fields;
    kafka_producer_RecordMetadata_copy(metadata, metadata_copy_cb, &fields);

    RecordMetadata* m = resp->mutable_metadata();
    m->set_offset(fields.offset);
    m->set_timestamp(fields.timestamp);
    m->set_topic(fields.topic);
    m->set_partition(fields.partition);
    // The C FFI metadata API doesn't expose serialized key/value sizes;
    // surface -1 so the Rust side's RecordMetadata::new still constructs
    // validly. Tests that depend on these specific fields are skipped
    // for the c backend.
    m->set_serialized_key_size(-1);
    m->set_serialized_value_size(-1);
    return grpc::Status::OK;
  }

  grpc::Status Flush(grpc::ServerContext*, const FlushRequest* req,
                     StatusResponse* resp) override {
    kafka_producer_Producer_t* producer = producer_for(req->producer_id());
    if (producer == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          VARIANT_ILLEGAL_STATE,
          "unknown producer_id " + std::to_string(req->producer_id()));
      return grpc::Status::OK;
    }
    kafka_common_KafkaError_t* err = nullptr;
    kafka_producer_Producer_flush(producer, &err);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
    }
    return grpc::Status::OK;
  }

  grpc::Status PartitionsFor(grpc::ServerContext*,
                             const PartitionsForRequest* req,
                             PartitionsForResponse* resp) override {
    kafka_producer_Producer_t* producer = producer_for(req->producer_id());
    if (producer == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          VARIANT_ILLEGAL_STATE,
          "unknown producer_id " + std::to_string(req->producer_id()));
      return grpc::Status::OK;
    }
    kafka_consumer_PartitionInfoList_t* list = nullptr;
    kafka_common_KafkaError_t* err =
        kafka_producer_Producer_partitions_for(producer, req->topic().c_str(), &list);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    int32_t n = kafka_consumer_PartitionInfoList_count(list);
    for (int32_t i = 0; i < n; i++) {
      partition_info_to_proto(kafka_consumer_PartitionInfoList_get(list, i), resp->add_partitions());
    }
    kafka_consumer_PartitionInfoList_destroy(list);
    return grpc::Status::OK;
  }

  grpc::Status Metrics(grpc::ServerContext*, const MetricsRequest* req,
                       MetricsResponse* resp) override {
    kafka_producer_Producer_t* producer = producer_for(req->producer_id());
    if (producer == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          VARIANT_ILLEGAL_STATE,
          "unknown producer_id " + std::to_string(req->producer_id()));
      return grpc::Status::OK;
    }
    kafka_producer_MetricMap_t* map = kafka_producer_Producer_metrics(producer);
    if (map == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          VARIANT_ILLEGAL_STATE, "no metrics for producer " + std::to_string(req->producer_id()));
      return grpc::Status::OK;
    }
    MetricList* out = resp->mutable_metrics();
    int32_t n = kafka_producer_MetricMap_count(map);
    for (int32_t i = 0; i < n; i++) {
      Metric* m = out->add_metrics();
      const char* name = kafka_producer_MetricMap_get_name(map, i);
      const char* group = kafka_producer_MetricMap_get_group(map, i);
      const char* desc = kafka_producer_MetricMap_get_description(map, i);
      m->set_name(name ? name : "");
      m->set_group(group ? group : "");
      m->set_description(desc ? desc : "");
      int32_t tn = kafka_producer_MetricMap_get_tag_count(map, i);
      for (int32_t t = 0; t < tn; t++) {
        const char* k = kafka_producer_MetricMap_get_tag_key(map, i, t);
        const char* v = kafka_producer_MetricMap_get_tag_value(map, i, t);
        (*m->mutable_tags())[k ? k : ""] = v ? v : "";
      }
      // Kind constants mirror the Rust MetricValue variants; see
      // METRIC_VALUE_* in src/ffi/common.rs.
      switch (kafka_producer_MetricMap_get_value_kind(map, i)) {
        case 1: {
          const char* s = kafka_producer_MetricMap_get_value_string(map, i);
          m->set_string_value(s ? s : "");
          break;
        }
        case 2:
          m->set_long_value(kafka_producer_MetricMap_get_value_long(map, i));
          break;
        case 3:
          m->set_int_value(kafka_producer_MetricMap_get_value_int(map, i));
          break;
        default:
          m->set_double_value(kafka_producer_MetricMap_get_value_double(map, i));
          break;
      }
    }
    kafka_producer_MetricMap_destroy(map);
    return grpc::Status::OK;
  }

  grpc::Status Close(grpc::ServerContext*, const CloseRequest* req,
                     StatusResponse* resp) override {
    kafka_producer_Producer_t* producer = nullptr;
    {
      std::lock_guard<std::mutex> lock(mu_);
      auto it = producers_.find(req->producer_id());
      if (it != producers_.end()) {
        producer = it->second;
        producers_.erase(it);
      }
      // log_states_ is deliberately NOT erased — see LogState's comment: a
      // dispatcher job queued by a callback that already fired may still hold
      // the pointer, because destroy detaches the dispatcher instead of
      // joining it.
    }
    if (producer == nullptr) {
      // Idempotent close — silent success on unknown id.
      return grpc::Status::OK;
    }
    kafka_common_KafkaError_t* err = nullptr;
    // close() flushes, so the *Rust* side of every outstanding delivery
    // callback has run by the time this returns — i.e. its C callback has been
    // enqueued on the dispatcher. It does not guarantee the dispatcher has run
    // that job, so a GetCallbackLog issued immediately after Close may still be
    // one entry behind; see CallbackLog's comment.
    kafka_producer_Producer_close(producer, &err);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
    }
    kafka_producer_Producer_destroy(producer);
    // The log entries stay in callback_log_ and the LogState stays in
    // log_states_, so GetCallbackLog still works post-close and a late
    // dispatcher job still has valid user_data.
    return grpc::Status::OK;
  }

  grpc::Status GetCallbackLog(grpc::ServerContext*, const ProducerCallbackLogRequest* req,
                              CallbackLogResponse* resp) override {
    callback_log_.fill(req->producer_id(), resp);
    return grpc::Status::OK;
  }

  grpc::Status CloseTimeout(grpc::ServerContext* ctx,
                            const CloseTimeoutRequest* req,
                            StatusResponse* resp) override {
    // The C FFI's close doesn't take a timeout — best-effort: behaves
    // like Close. Surface this in the message field if anything goes
    // wrong so the Rust side can distinguish from a true close error.
    CloseRequest close_req;
    close_req.set_producer_id(req->producer_id());
    return Close(ctx, &close_req, resp);
  }

 private:
  kafka_producer_Producer_t* producer_for(uint64_t id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = producers_.find(id);
    return it == producers_.end() ? nullptr : it->second;
  }

  LogState* log_state_for(uint64_t id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = log_states_.find(id);
    return it == log_states_.end() ? nullptr : it->second.get();
  }


  std::mutex mu_;
  std::unordered_map<uint64_t, kafka_producer_Producer_t*> producers_;
  // user_data for the delivery callbacks; owned here, one per producer, for the
  // whole session (never erased by Close — see LogState).
  std::unordered_map<uint64_t, std::unique_ptr<LogState>> log_states_;
  // Has its own mutex; see CallbackLog.
  CallbackLog callback_log_;
  std::atomic<uint64_t> next_id_{1};
};

// ---------------------------------------------------------------------------
// Consumer service
// ---------------------------------------------------------------------------

// Build parallel (topics, partitions) C arrays from a repeated TopicPartition.
// The char* point into the proto strings, which outlive the synchronous FFI
// call, and the FFI copies them into owned Rust data before returning.
struct TpArrays {
  std::vector<const char*> topics;
  std::vector<int32_t> partitions;
  int32_t count() const { return static_cast<int32_t>(topics.size()); }
};

TpArrays tp_arrays(
    const ::google::protobuf::RepeatedPtrField<TopicPartition>& tps) {
  TpArrays a;
  a.topics.reserve(tps.size());
  a.partitions.reserve(tps.size());
  for (const auto& tp : tps) {
    a.topics.push_back(tp.topic().c_str());
    a.partitions.push_back(tp.partition());
  }
  return a;
}

// The five parallel arrays the commit_*_offsets FFI entry points take. Same
// pointer-lifetime rule as TpArrays: the char* point into the proto, which
// outlives the synchronous FFI call.
//
// Shared by CommitSync and CommitAsync — the offsets shape is identical (the
// CommitAsyncRequest.offsets field deliberately reuses OffsetMapEntry).
struct OffsetArrays {
  std::vector<const char*> topics;
  std::vector<int32_t> partitions;
  std::vector<int64_t> offsets;
  std::vector<int32_t> leader_epochs;
  std::vector<const char*> metadata;
  int32_t count() const { return static_cast<int32_t>(topics.size()); }
};

OffsetArrays offset_arrays(
    const ::google::protobuf::RepeatedPtrField<OffsetMapEntry>& entries) {
  OffsetArrays a;
  a.topics.reserve(entries.size());
  a.partitions.reserve(entries.size());
  a.offsets.reserve(entries.size());
  a.leader_epochs.reserve(entries.size());
  a.metadata.reserve(entries.size());
  for (const auto& e : entries) {
    a.topics.push_back(e.partition().topic().c_str());
    a.partitions.push_back(e.partition().partition());
    a.offsets.push_back(e.offset().offset());
    a.leader_epochs.push_back(e.offset().has_leader_epoch() ? e.offset().leader_epoch() : -1);
    a.metadata.push_back(e.offset().metadata().c_str());
  }
  return a;
}

void node_to_proto(const kafka_common_Node_t* node, Node* dst) {
  dst->set_id(kafka_common_Node_id(node));
  int32_t host_len = 0;
  const char* host = kafka_common_Node_host(node, &host_len);
  if (host != nullptr) dst->set_host(std::string(host, host_len));
  dst->set_port(kafka_common_Node_port(node));
  int32_t rack_len = 0;
  const char* rack = kafka_common_Node_rack(node, &rack_len);
  if (rack != nullptr) dst->set_rack(std::string(rack, rack_len));
}

void partition_info_to_proto(const kafka_consumer_PartitionInfo_t* info,
                             PartitionInfo* dst) {
  const char* topic = kafka_consumer_PartitionInfo_topic(info);  // NUL-terminated
  dst->set_topic(topic ? topic : "");
  dst->set_partition(kafka_consumer_PartitionInfo_partition(info));
  const kafka_common_Node_t* leader = kafka_consumer_PartitionInfo_leader(info);
  if (leader != nullptr) node_to_proto(leader, dst->mutable_leader());
  int32_t n = kafka_consumer_PartitionInfo_replica_count(info);
  for (int32_t i = 0; i < n; i++) {
    node_to_proto(kafka_consumer_PartitionInfo_replica(info, i), dst->add_replicas());
  }
  n = kafka_consumer_PartitionInfo_in_sync_replica_count(info);
  for (int32_t i = 0; i < n; i++) {
    node_to_proto(kafka_consumer_PartitionInfo_in_sync_replica(info, i),
                  dst->add_in_sync_replicas());
  }
  n = kafka_consumer_PartitionInfo_offline_replica_count(info);
  for (int32_t i = 0; i < n; i++) {
    node_to_proto(kafka_consumer_PartitionInfo_offline_replica(info, i),
                  dst->add_offline_replicas());
  }
}

void tp_to_proto(const kafka_consumer_TopicPartition_t* tp, TopicPartition* dst) {
  const char* topic = kafka_consumer_TopicPartition_topic(tp);
  dst->set_topic(topic ? topic : "");
  dst->set_partition(kafka_consumer_TopicPartition_partition(tp));
}

class ConsumerServiceImpl final : public ConsumerService::Service {
 public:
  grpc::Status CreateConsumer(grpc::ServerContext*,
                              const CreateConsumerRequest* req,
                              CreateConsumerResponse* resp) override {
    kafka_consumer_Consumer_t* consumer = nullptr;
    if (req->config().empty()) {
      consumer = kafka_consumer_MockConsumer_new("earliest");
    } else {
      kafka_consumer_ConsumerProperties_t* props =
          kafka_consumer_ConsumerProperties_new();
      for (const auto& kv : req->config()) {
        kafka_consumer_ConsumerProperties_put(props, kv.first.c_str(), kv.second.c_str());
      }
      kafka_common_KafkaError_t* err = nullptr;
      consumer = kafka_consumer_KafkaConsumer_new(props, &err);
      kafka_consumer_ConsumerProperties_destroy(props);
      if (consumer == nullptr) {
        fill_proto_error(resp->mutable_error(), err);
        return grpc::Status::OK;
      }
    }
    const uint64_t id = next_id_.fetch_add(1);
    {
      std::lock_guard<std::mutex> lock(mu_);
      consumers_[id] = consumer;
      // One stable LogState per consumer, shared by the rebalance listener and
      // every commit callback — see the struct's comment (session-lifetime; not
      // freed at Close).
      log_states_[id] = std::unique_ptr<LogState>(new LogState{&callback_log_, id});
    }
    resp->set_consumer_id(id);
    std::cerr << "c server: created consumer " << id << std::endl;
    return grpc::Status::OK;
  }

  grpc::Status Subscribe(grpc::ServerContext*, const SubscribeRequest* req,
                         StatusResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) return unknown(resp, req->consumer_id());
    std::vector<const char*> topics;
    topics.reserve(req->topics_size());
    for (const auto& t : req->topics()) topics.push_back(t.c_str());
    kafka_common_KafkaError_t* err = nullptr;
    if (req->with_listener()) {
      // A real ConsumerRebalanceListener whose invocations land in the callback
      // log. user_data_destroy is NULL because the LogState is owned by
      // log_states_, not by this registration (which a replacing subscribe
      // releases while the state must survive). subscribe_with_listener consumes
      // the listener handle even when it fails, so there is nothing to free here.
      kafka_consumer_ConsumerRebalanceListener_t* listener =
          kafka_consumer_ConsumerRebalanceListener_new(
              log_partitions_revoked, log_partitions_assigned, log_partitions_lost,
              log_state_for(req->consumer_id()), /*user_data_destroy=*/nullptr);
      err = kafka_consumer_Consumer_subscribe_with_listener(
          c, topics.data(), static_cast<int32_t>(topics.size()), listener);
    } else {
      err = kafka_consumer_Consumer_subscribe(
          c, topics.data(), static_cast<int32_t>(topics.size()));
    }
    if (err != nullptr) fill_proto_error(resp->mutable_error(), err);
    return grpc::Status::OK;
  }

  grpc::Status Unsubscribe(grpc::ServerContext*, const ConsumerIdRequest* req,
                           StatusResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) return unknown(resp, req->consumer_id());
    kafka_common_KafkaError_t* err = kafka_consumer_Consumer_unsubscribe(c);
    if (err != nullptr) fill_proto_error(resp->mutable_error(), err);
    return grpc::Status::OK;
  }

  grpc::Status Assign(grpc::ServerContext*, const AssignRequest* req,
                      StatusResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) return unknown(resp, req->consumer_id());
    TpArrays a = tp_arrays(req->partitions());
    kafka_common_KafkaError_t* err =
        kafka_consumer_Consumer_assign(c, a.topics.data(), a.partitions.data(), a.count());
    if (err != nullptr) fill_proto_error(resp->mutable_error(), err);
    return grpc::Status::OK;
  }

  grpc::Status Poll(grpc::ServerContext*, const PollRequest* req,
                    PollResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          VARIANT_ILLEGAL_STATE, "unknown consumer_id " + std::to_string(req->consumer_id()));
      return grpc::Status::OK;
    }
    kafka_common_KafkaError_t* err = nullptr;
    kafka_consumer_ConsumerRecords_t* records =
        kafka_consumer_Consumer_poll(c, req->timeout_ms(), &err);
    if (records == nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    ConsumerRecordList* list = resp->mutable_records();
    int32_t n = kafka_consumer_ConsumerRecords_count(records);
    for (int32_t i = 0; i < n; i++) {
      const kafka_consumer_ConsumerRecord_t* rec =
          kafka_consumer_ConsumerRecords_get(records, i);
      record_to_proto(rec, list->add_records());
    }
    kafka_consumer_ConsumerRecords_destroy(records);
    return grpc::Status::OK;
  }

  grpc::Status CommitSync(grpc::ServerContext*, const CommitSyncRequest* req,
                          StatusResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) return unknown(resp, req->consumer_id());
    kafka_common_KafkaError_t* err = nullptr;
    if (req->offsets().empty()) {
      err = kafka_consumer_Consumer_commit_sync(c);
    } else {
      OffsetArrays a = offset_arrays(req->offsets());
      err = kafka_consumer_Consumer_commit_sync_offsets(
          c, a.topics.data(), a.partitions.data(), a.offsets.data(), a.leader_epochs.data(),
          a.metadata.data(), a.count());
    }
    if (err != nullptr) fill_proto_error(resp->mutable_error(), err);
    return grpc::Status::OK;
  }

  grpc::Status CommitAsync(grpc::ServerContext*, const CommitAsyncRequest* req,
                           StatusResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) return unknown(resp, req->consumer_id());
    kafka_common_KafkaError_t* err = nullptr;
    LogState* state = log_state_for(req->consumer_id());
    // user_data_destroy is NULL for the same reason as in Subscribe: the
    // LogState belongs to log_states_, not to this one registration.
    if (req->offsets().empty()) {
      err = req->with_callback()
                ? kafka_consumer_Consumer_commit_async_with_callback(
                      c, log_commit_complete, state, /*user_data_destroy=*/nullptr)
                : kafka_consumer_Consumer_commit_async(c);
    } else {
      OffsetArrays a = offset_arrays(req->offsets());
      err = kafka_consumer_Consumer_commit_async_offsets_with_callback(
          c, a.topics.data(), a.partitions.data(), a.offsets.data(), a.leader_epochs.data(),
          a.metadata.data(), a.count(),
          req->with_callback() ? log_commit_complete : discard_commit_complete, state,
          /*user_data_destroy=*/nullptr);
    }
    if (err != nullptr) fill_proto_error(resp->mutable_error(), err);
    return grpc::Status::OK;
  }

  grpc::Status Committed(grpc::ServerContext*, const CommittedRequest* req,
                         CommittedResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          VARIANT_ILLEGAL_STATE, "unknown consumer_id " + std::to_string(req->consumer_id()));
      return grpc::Status::OK;
    }
    TpArrays a = tp_arrays(req->partitions());
    kafka_consumer_OffsetMap_t* map = nullptr;
    kafka_common_KafkaError_t* err = kafka_consumer_Consumer_committed(
        c, a.topics.data(), a.partitions.data(), a.count(), &map);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    OffsetMap* out = resp->mutable_offsets();
    int32_t n = kafka_consumer_OffsetMap_count(map);
    for (int32_t i = 0; i < n; i++) {
      OffsetMapEntry* entry = out->add_entries();
      tp_to_proto(kafka_consumer_OffsetMap_get_key(map, i), entry->mutable_partition());
      const kafka_consumer_OffsetAndMetadata_t* v = kafka_consumer_OffsetMap_get_value(map, i);
      OffsetAndMetadata* oam = entry->mutable_offset();
      oam->set_offset(kafka_consumer_OffsetAndMetadata_offset(v));
      const char* meta = kafka_consumer_OffsetAndMetadata_metadata(v);
      oam->set_metadata(meta ? meta : "");
      int32_t epoch = 0;
      if (kafka_consumer_OffsetAndMetadata_leader_epoch(v, &epoch)) oam->set_leader_epoch(epoch);
    }
    kafka_consumer_OffsetMap_destroy(map);
    return grpc::Status::OK;
  }

  grpc::Status Position(grpc::ServerContext*, const PositionRequest* req,
                        PositionResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          VARIANT_ILLEGAL_STATE, "unknown consumer_id " + std::to_string(req->consumer_id()));
      return grpc::Status::OK;
    }
    int64_t out = 0;
    kafka_common_KafkaError_t* err = kafka_consumer_Consumer_position(
        c, req->partition().topic().c_str(), req->partition().partition(), &out);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
    } else {
      resp->set_offset(out);
    }
    return grpc::Status::OK;
  }

  grpc::Status Seek(grpc::ServerContext*, const SeekRequest* req,
                    StatusResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) return unknown(resp, req->consumer_id());
    kafka_common_KafkaError_t* err = nullptr;
    if (req->has_metadata() || req->has_leader_epoch()) {
      err = kafka_consumer_Consumer_seek_with_metadata(
          c, req->partition().topic().c_str(), req->partition().partition(),
          req->offset(), req->has_leader_epoch() ? req->leader_epoch() : -1,
          req->has_metadata() ? req->metadata().c_str() : "");
    } else {
      err = kafka_consumer_Consumer_seek(
          c, req->partition().topic().c_str(), req->partition().partition(), req->offset());
    }
    if (err != nullptr) fill_proto_error(resp->mutable_error(), err);
    return grpc::Status::OK;
  }

  grpc::Status SeekToBeginning(grpc::ServerContext*, const TopicPartitionListRequest* req,
                               StatusResponse* resp) override {
    return tp_list_op(req, resp, kafka_consumer_Consumer_seek_to_beginning);
  }
  grpc::Status SeekToEnd(grpc::ServerContext*, const TopicPartitionListRequest* req,
                         StatusResponse* resp) override {
    return tp_list_op(req, resp, kafka_consumer_Consumer_seek_to_end);
  }
  grpc::Status Pause(grpc::ServerContext*, const TopicPartitionListRequest* req,
                     StatusResponse* resp) override {
    return tp_list_op(req, resp, kafka_consumer_Consumer_pause);
  }
  grpc::Status Resume(grpc::ServerContext*, const TopicPartitionListRequest* req,
                      StatusResponse* resp) override {
    return tp_list_op(req, resp, kafka_consumer_Consumer_resume);
  }

  grpc::Status BeginningOffsets(grpc::ServerContext*, const TopicPartitionListRequest* req,
                                LongOffsetsResponse* resp) override {
    return long_offsets(req, resp, kafka_consumer_Consumer_beginning_offsets);
  }
  grpc::Status EndOffsets(grpc::ServerContext*, const TopicPartitionListRequest* req,
                          LongOffsetsResponse* resp) override {
    return long_offsets(req, resp, kafka_consumer_Consumer_end_offsets);
  }

  grpc::Status OffsetsForTimes(grpc::ServerContext*, const OffsetsForTimesRequest* req,
                               OffsetAndTimestampResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          VARIANT_ILLEGAL_STATE, "unknown consumer_id " + std::to_string(req->consumer_id()));
      return grpc::Status::OK;
    }
    std::vector<const char*> topics;
    std::vector<int32_t> partitions;
    std::vector<int64_t> timestamps;
    for (const auto& e : req->timestamps()) {
      topics.push_back(e.partition().topic().c_str());
      partitions.push_back(e.partition().partition());
      timestamps.push_back(e.timestamp());
    }
    kafka_consumer_OffsetAndTimestampMap_t* map = nullptr;
    kafka_common_KafkaError_t* err = kafka_consumer_Consumer_offsets_for_times(
        c, topics.data(), partitions.data(), timestamps.data(),
        static_cast<int32_t>(topics.size()), &map);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    OffsetAndTimestampMap* out = resp->mutable_offsets();
    int32_t n = kafka_consumer_OffsetAndTimestampMap_count(map);
    for (int32_t i = 0; i < n; i++) {
      OffsetAndTimestampMapEntry* entry = out->add_entries();
      tp_to_proto(kafka_consumer_OffsetAndTimestampMap_get_key(map, i), entry->mutable_partition());
      const kafka_consumer_OffsetAndTimestamp_t* v =
          kafka_consumer_OffsetAndTimestampMap_get_value(map, i);
      OffsetAndTimestamp* oat = entry->mutable_offset();
      oat->set_offset(kafka_consumer_OffsetAndTimestamp_offset(v));
      oat->set_timestamp(kafka_consumer_OffsetAndTimestamp_timestamp(v));
      int32_t epoch = 0;
      if (kafka_consumer_OffsetAndTimestamp_leader_epoch(v, &epoch)) oat->set_leader_epoch(epoch);
    }
    kafka_consumer_OffsetAndTimestampMap_destroy(map);
    return grpc::Status::OK;
  }

  grpc::Status PartitionsFor(grpc::ServerContext*, const ConsumerPartitionsForRequest* req,
                             PartitionsForResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          VARIANT_ILLEGAL_STATE, "unknown consumer_id " + std::to_string(req->consumer_id()));
      return grpc::Status::OK;
    }
    kafka_consumer_PartitionInfoList_t* infos = nullptr;
    kafka_common_KafkaError_t* err =
        kafka_consumer_Consumer_partitions_for(c, req->topic().c_str(), &infos);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    int32_t n = kafka_consumer_PartitionInfoList_count(infos);
    for (int32_t i = 0; i < n; i++) {
      partition_info_to_proto(kafka_consumer_PartitionInfoList_get(infos, i), resp->add_partitions());
    }
    kafka_consumer_PartitionInfoList_destroy(infos);
    return grpc::Status::OK;
  }

  grpc::Status ListTopics(grpc::ServerContext*, const ConsumerIdRequest* req,
                          ListTopicsResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          VARIANT_ILLEGAL_STATE, "unknown consumer_id " + std::to_string(req->consumer_id()));
      return grpc::Status::OK;
    }
    kafka_consumer_TopicPartitionInfoMap_t* map = nullptr;
    kafka_common_KafkaError_t* err = kafka_consumer_Consumer_list_topics(c, &map);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    TopicListing* listing = resp->mutable_topics();
    int32_t n = kafka_consumer_TopicPartitionInfoMap_count(map);
    for (int32_t i = 0; i < n; i++) {
      TopicPartitionInfoEntry* entry = listing->add_topics();
      const char* topic = kafka_consumer_TopicPartitionInfoMap_get_topic(map, i);
      entry->set_topic(topic ? topic : "");
      const kafka_consumer_PartitionInfoList_t* infos =
          kafka_consumer_TopicPartitionInfoMap_get_partitions(map, i);
      int32_t pn = kafka_consumer_PartitionInfoList_count(infos);
      for (int32_t j = 0; j < pn; j++) {
        partition_info_to_proto(kafka_consumer_PartitionInfoList_get(infos, j), entry->add_partitions());
      }
    }
    kafka_consumer_TopicPartitionInfoMap_destroy(map);
    return grpc::Status::OK;
  }

  grpc::Status Assignment(grpc::ServerContext*, const ConsumerIdRequest* req,
                          TopicPartitionListResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          VARIANT_ILLEGAL_STATE, "unknown consumer_id " + std::to_string(req->consumer_id()));
      return grpc::Status::OK;
    }
    kafka_consumer_TopicPartitionList_t* list = kafka_consumer_Consumer_assignment(c);
    fill_tp_list(list, resp);
    return grpc::Status::OK;
  }

  grpc::Status Paused(grpc::ServerContext*, const ConsumerIdRequest* req,
                      TopicPartitionListResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          VARIANT_ILLEGAL_STATE, "unknown consumer_id " + std::to_string(req->consumer_id()));
      return grpc::Status::OK;
    }
    kafka_consumer_TopicPartitionList_t* list = kafka_consumer_Consumer_paused(c);
    fill_tp_list(list, resp);
    return grpc::Status::OK;
  }

  grpc::Status Metrics(grpc::ServerContext*, const ConsumerIdRequest* req,
                       MetricsResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          VARIANT_ILLEGAL_STATE, "unknown consumer_id " + std::to_string(req->consumer_id()));
      return grpc::Status::OK;
    }
    kafka_consumer_MetricMap_t* map = kafka_consumer_Consumer_metrics(c);
    if (map == nullptr) {
      // Single-owner guard rejected the call (concurrent access).
      *resp->mutable_error() = make_synthetic_error(
          VARIANT_ILLEGAL_STATE, "concurrent access to consumer " + std::to_string(req->consumer_id()));
      return grpc::Status::OK;
    }
    MetricList* out = resp->mutable_metrics();
    int32_t n = kafka_consumer_MetricMap_count(map);
    for (int32_t i = 0; i < n; i++) {
      Metric* m = out->add_metrics();
      const char* name = kafka_consumer_MetricMap_get_name(map, i);
      const char* group = kafka_consumer_MetricMap_get_group(map, i);
      const char* desc = kafka_consumer_MetricMap_get_description(map, i);
      m->set_name(name ? name : "");
      m->set_group(group ? group : "");
      m->set_description(desc ? desc : "");
      int32_t tn = kafka_consumer_MetricMap_get_tag_count(map, i);
      for (int32_t t = 0; t < tn; t++) {
        const char* k = kafka_consumer_MetricMap_get_tag_key(map, i, t);
        const char* v = kafka_consumer_MetricMap_get_tag_value(map, i, t);
        (*m->mutable_tags())[k ? k : ""] = v ? v : "";
      }
      // Kind constants mirror the Rust MetricValue variants; see
      // METRIC_VALUE_* in src/ffi/common.rs.
      switch (kafka_consumer_MetricMap_get_value_kind(map, i)) {
        case 1: {
          const char* s = kafka_consumer_MetricMap_get_value_string(map, i);
          m->set_string_value(s ? s : "");
          break;
        }
        case 2:
          m->set_long_value(kafka_consumer_MetricMap_get_value_long(map, i));
          break;
        case 3:
          m->set_int_value(kafka_consumer_MetricMap_get_value_int(map, i));
          break;
        default:
          m->set_double_value(kafka_consumer_MetricMap_get_value_double(map, i));
          break;
      }
    }
    kafka_consumer_MetricMap_destroy(map);
    return grpc::Status::OK;
  }

  grpc::Status Subscription(grpc::ServerContext*, const ConsumerIdRequest* req,
                            SubscriptionResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          VARIANT_ILLEGAL_STATE, "unknown consumer_id " + std::to_string(req->consumer_id()));
      return grpc::Status::OK;
    }
    kafka_consumer_StringList_t* list = kafka_consumer_Consumer_subscription(c);
    StringList* out = resp->mutable_topics();
    if (list != nullptr) {
      int32_t n = kafka_consumer_StringList_count(list);
      for (int32_t i = 0; i < n; i++) {
        const char* s = kafka_consumer_StringList_get(list, i);
        out->add_values(s ? s : "");
      }
      kafka_consumer_StringList_destroy(list);
    }
    return grpc::Status::OK;
  }

  grpc::Status Wakeup(grpc::ServerContext*, const ConsumerIdRequest* req,
                      StatusResponse*) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c != nullptr) kafka_consumer_Consumer_wakeup(c);
    return grpc::Status::OK;
  }

  grpc::Status Close(grpc::ServerContext*, const ConsumerCloseRequest* req,
                     StatusResponse* resp) override {
    kafka_consumer_Consumer_t* consumer = nullptr;
    {
      std::lock_guard<std::mutex> lock(mu_);
      auto it = consumers_.find(req->consumer_id());
      if (it != consumers_.end()) {
        consumer = it->second;
        consumers_.erase(it);
      }
      // log_states_ is deliberately NOT erased — see LogState's comment.
      // `Consumer_destroy` step 1 is `runtime.shutdown_background()`, which
      // cancels the task awaiting a `dispatch_and_wait` job while leaving the
      // already-queued job to run later.
    }
    if (consumer == nullptr) {
      return grpc::Status::OK;  // idempotent
    }
    // close() drains pending commit callbacks and fires on_partitions_lost, so
    // the Rust side of those callbacks has run by the time this returns — i.e.
    // their C callbacks have been enqueued on the dispatcher. It does not
    // guarantee the dispatcher has run them; see CallbackLog's comment.
    kafka_common_KafkaError_t* err = kafka_consumer_Consumer_close(consumer);
    if (err != nullptr) fill_proto_error(resp->mutable_error(), err);
    // The log entries stay in callback_log_ and the LogState stays in
    // log_states_, so GetCallbackLog still works post-close and a late
    // dispatcher job still has valid user_data.
    kafka_consumer_Consumer_destroy(consumer);
    return grpc::Status::OK;
  }

  grpc::Status GetCallbackLog(grpc::ServerContext*, const CallbackLogRequest* req,
                              CallbackLogResponse* resp) override {
    callback_log_.fill(req->consumer_id(), resp);
    return grpc::Status::OK;
  }

 private:
  kafka_consumer_Consumer_t* consumer_for(uint64_t id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = consumers_.find(id);
    return it == consumers_.end() ? nullptr : it->second;
  }

  LogState* log_state_for(uint64_t id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = log_states_.find(id);
    return it == log_states_.end() ? nullptr : it->second.get();
  }

  grpc::Status unknown(StatusResponse* resp, uint64_t id) {
    *resp->mutable_error() =
        make_synthetic_error(VARIANT_ILLEGAL_STATE, "unknown consumer_id " + std::to_string(id));
    return grpc::Status::OK;
  }

  using TpListFn = kafka_common_KafkaError_t* (*)(const kafka_consumer_Consumer_t*,
                                                  const char* const*, const int32_t*, int32_t);
  grpc::Status tp_list_op(const TopicPartitionListRequest* req, StatusResponse* resp, TpListFn fn) {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) return unknown(resp, req->consumer_id());
    TpArrays a = tp_arrays(req->partitions());
    kafka_common_KafkaError_t* err = fn(c, a.topics.data(), a.partitions.data(), a.count());
    if (err != nullptr) fill_proto_error(resp->mutable_error(), err);
    return grpc::Status::OK;
  }

  using LongOffFn = kafka_common_KafkaError_t* (*)(const kafka_consumer_Consumer_t*,
                                                   const char* const*, const int32_t*, int32_t,
                                                   kafka_consumer_LongOffsetMap_t**);
  grpc::Status long_offsets(const TopicPartitionListRequest* req, LongOffsetsResponse* resp,
                            LongOffFn fn) {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          VARIANT_ILLEGAL_STATE, "unknown consumer_id " + std::to_string(req->consumer_id()));
      return grpc::Status::OK;
    }
    TpArrays a = tp_arrays(req->partitions());
    kafka_consumer_LongOffsetMap_t* map = nullptr;
    kafka_common_KafkaError_t* err = fn(c, a.topics.data(), a.partitions.data(), a.count(), &map);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    LongOffsetMap* out = resp->mutable_offsets();
    int32_t n = kafka_consumer_LongOffsetMap_count(map);
    for (int32_t i = 0; i < n; i++) {
      LongOffsetMapEntry* entry = out->add_entries();
      tp_to_proto(kafka_consumer_LongOffsetMap_get_key(map, i), entry->mutable_partition());
      entry->set_offset(kafka_consumer_LongOffsetMap_get_value(map, i));
    }
    kafka_consumer_LongOffsetMap_destroy(map);
    return grpc::Status::OK;
  }

  void fill_tp_list(kafka_consumer_TopicPartitionList_t* list, TopicPartitionListResponse* resp) {
    TopicPartitionList* out = resp->mutable_partitions();
    if (list != nullptr) {
      int32_t n = kafka_consumer_TopicPartitionList_count(list);
      for (int32_t i = 0; i < n; i++) {
        tp_to_proto(kafka_consumer_TopicPartitionList_get(list, i), out->add_partitions());
      }
      kafka_consumer_TopicPartitionList_destroy(list);
    }
  }

  static void record_to_proto(const kafka_consumer_ConsumerRecord_t* rec, ConsumerRecord* dst) {
    int32_t topic_len = 0;
    const char* topic = kafka_consumer_ConsumerRecord_topic(rec, &topic_len);
    if (topic != nullptr) dst->set_topic(std::string(topic, topic_len));
    dst->set_partition(kafka_consumer_ConsumerRecord_partition(rec));
    dst->set_offset(kafka_consumer_ConsumerRecord_offset(rec));
    dst->set_timestamp(kafka_consumer_ConsumerRecord_timestamp(rec));
    dst->set_timestamp_type(kafka_consumer_ConsumerRecord_timestamp_type(rec));
    int32_t key_len = 0;
    const uint8_t* key = kafka_consumer_ConsumerRecord_key(rec, &key_len);
    if (key != nullptr) dst->set_key(std::string(reinterpret_cast<const char*>(key), key_len));
    int32_t val_len = 0;
    const uint8_t* value = kafka_consumer_ConsumerRecord_value(rec, &val_len);
    if (value != nullptr) dst->set_value(std::string(reinterpret_cast<const char*>(value), val_len));
    int32_t epoch = 0;
    if (kafka_consumer_ConsumerRecord_leader_epoch(rec, &epoch)) dst->set_leader_epoch(epoch);
    int32_t hn = kafka_consumer_ConsumerRecord_header_count(rec);
    for (int32_t i = 0; i < hn; i++) {
      Header* h = dst->add_headers();
      int32_t hk_len = 0;
      const char* hk = kafka_consumer_ConsumerRecord_header_key(rec, i, &hk_len);
      if (hk != nullptr) h->set_key(std::string(hk, hk_len));
      int32_t hv_len = 0;
      const uint8_t* hv = kafka_consumer_ConsumerRecord_header_value(rec, i, &hv_len);
      if (hv != nullptr) h->set_value(std::string(reinterpret_cast<const char*>(hv), hv_len));
    }
  }

  std::mutex mu_;
  std::unordered_map<uint64_t, kafka_consumer_Consumer_t*> consumers_;
  // user_data for the rebalance-listener and commit callbacks; owned here, one
  // per consumer, for the whole session (never erased by Close — see LogState).
  std::unordered_map<uint64_t, std::unique_ptr<LogState>> log_states_;
  // Has its own mutex; see CallbackLog.
  CallbackLog callback_log_;
  std::atomic<uint64_t> next_id_{1};
};

// Drives the **bare sync** admin C entry points (kafka_admin_AdminClient_*),
// as the producer and consumer services above do for theirs. The `_async`
// variants are covered by the committed C unit tests; re-testing them here
// would trade differential coverage for redundancy.
class AdminServiceImpl final : public AdminService::Service {
 public:
  grpc::Status CreateAdmin(grpc::ServerContext*, const CreateAdminRequest* req,
                           CreateAdminResponse* resp) override {
    kafka_admin_AdminClient_t* admin = nullptr;
    if (selects_mock(req->config())) {
      // Absent num_brokers means 1.
      const int32_t num_brokers = req->has_num_brokers() ? req->num_brokers() : 1;
      admin = kafka_admin_MockAdminClient_new(num_brokers);
      if (admin == nullptr) {
        // The constructor reports failure by returning NULL without an error
        // handle (a Rust panic must not unwind across the C boundary), so the
        // message is synthesised here.
        *resp->mutable_error() = make_synthetic_error(
            VARIANT_ILLEGAL_ARGUMENT,
            "MockAdminClient_new returned null for num_brokers " + std::to_string(num_brokers));
        return grpc::Status::OK;
      }
    } else {
      kafka_admin_AdminClientProperties_t* props = kafka_admin_AdminClientProperties_new();
      for (const auto& kv : req->config()) {
        kafka_admin_AdminClientProperties_put(props, kv.first.c_str(), kv.second.c_str());
      }
      kafka_common_KafkaError_t* err = nullptr;
      admin = kafka_admin_AdminClient_new(props, &err);
      kafka_admin_AdminClientProperties_destroy(props);
      if (admin == nullptr) {
        fill_proto_error(resp->mutable_error(), err);
        return grpc::Status::OK;
      }
    }
    const uint64_t id = next_id_.fetch_add(1);
    {
      std::lock_guard<std::mutex> lock(mu_);
      admins_[id] = admin;
    }
    resp->set_admin_id(id);
    std::cerr << "c server: created admin " << id << std::endl;
    return grpc::Status::OK;
  }

  // -- Topics & partitions (slice G1) ---------------------------------------
  //
  // Every handler: resolve the handle, call the **bare sync** entry point
  // (which blocks this gRPC worker thread until every per-key future has
  // resolved, exactly as the FFI contract says), then walk the flattened result
  // handle into the per-key envelope and destroy it.
  //
  // The sync entry points distinguish the two failure levels for us: a non-null
  // return means the request could not be submitted at all, which is the
  // response's top-level `error`; a per-key failure comes back inside the result
  // handle with a null return.

  grpc::Status CreateTopics(grpc::ServerContext*, const CreateTopicsRequest* req,
                            CreateTopicsResponse* resp) override {
    kafka_admin_AdminClient_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }

    // Build the NewTopic handles, keeping them alive until the call returns.
    std::vector<kafka_admin_NewTopic_t*> owned;
    owned.reserve(req->topics_size());
    for (const auto& spec : req->topics()) {
      // -1 is the wire's "absent"; kafka_admin_NewTopic_new reads a negative
      // value as unset, so the sentinel passes straight through.
      kafka_admin_NewTopic_t* topic = kafka_admin_NewTopic_new(
          spec.name().c_str(), spec.num_partitions(),
          static_cast<int16_t>(spec.replication_factor()));
      for (const auto& kv : spec.configs()) {
        kafka_admin_NewTopic_put_config(topic, kv.first.c_str(), kv.second.c_str());
      }
      // Setting any assignment switches the entry to Java's
      // NewTopic(name, Map<Integer, List<Integer>>) form.
      for (const auto& assignment : spec.replicas_assignments()) {
        std::vector<int32_t> brokers(assignment.broker_ids().begin(),
                                     assignment.broker_ids().end());
        kafka_admin_NewTopic_set_replicas_assignment(
            topic, assignment.partition(), brokers.data(),
            static_cast<int32_t>(brokers.size()));
      }
      owned.push_back(topic);
    }
    std::vector<const kafka_admin_NewTopic_t*> topics(owned.begin(), owned.end());

    kafka_admin_CreateTopicsResult_t* result = nullptr;
    kafka_common_KafkaError_t* err = kafka_admin_AdminClient_create_topics(
        admin, topics.data(), static_cast<int32_t>(topics.size()),
        timeout_ms(*req), req->validate_only(), retry_on_quota(*req), &result);
    for (kafka_admin_NewTopic_t* topic : owned) kafka_admin_NewTopic_destroy(topic);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }

    const int32_t count = kafka_admin_CreateTopicsResult_count(result);
    for (int32_t i = 0; i < count; i++) {
      CreateTopicsEntry* entry = resp->add_entries();
      set_name_key(entry->mutable_key(), kafka_admin_CreateTopicsResult_get_key(result, i));
      const kafka_common_KafkaError_t* key_err =
          kafka_admin_CreateTopicsResult_get_error(result, i);
      if (key_err != nullptr) {
        copy_proto_error(entry->mutable_error(), key_err);
      } else {
        // get_value is non-null exactly when get_error is null (the FFI builds
        // the two vectors complementarily), but a null here would be a segfault
        // inside the oracle, so report it as an error instead.
        const kafka_admin_TopicMetadataAndConfig_t* value =
            kafka_admin_CreateTopicsResult_get_value(result, i);
        if (value == nullptr) {
          *entry->mutable_error() = make_synthetic_error(
              VARIANT_ILLEGAL_STATE, "createTopics entry has neither value nor error");
        } else {
          metadata_to_proto(value, entry->mutable_value());
        }
      }
    }
    kafka_admin_CreateTopicsResult_destroy(result);
    return grpc::Status::OK;
  }

  grpc::Status DeleteTopics(grpc::ServerContext*, const DeleteTopicsRequest* req,
                            VoidKeyedResponse* resp) override {
    kafka_admin_AdminClient_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    const bool by_ids = req->has_topic_ids();
    std::vector<std::string> owned(by_ids ? req->topic_ids().values().begin()
                                          : req->names().values().begin(),
                                   by_ids ? req->topic_ids().values().end()
                                          : req->names().values().end());
    std::vector<const char*> keys;
    keys.reserve(owned.size());
    for (const std::string& key : owned) keys.push_back(key.c_str());

    kafka_admin_DeleteTopicsResult_t* result = nullptr;
    kafka_common_KafkaError_t* err =
        by_ids ? kafka_admin_AdminClient_delete_topics_by_ids(
                     admin, keys.data(), static_cast<int32_t>(keys.size()),
                     timeout_ms(*req), retry_on_quota(*req), &result)
               : kafka_admin_AdminClient_delete_topics(
                     admin, keys.data(), static_cast<int32_t>(keys.size()),
                     timeout_ms(*req), retry_on_quota(*req), &result);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }

    const int32_t count = kafka_admin_DeleteTopicsResult_count(result);
    for (int32_t i = 0; i < count; i++) {
      VoidResultEntry* entry = resp->add_entries();
      set_keyed(entry->mutable_key(), kafka_admin_DeleteTopicsResult_get_key(result, i), by_ids);
      // No value for a KafkaFuture<Void>: an absent error is the success signal.
      const kafka_common_KafkaError_t* key_err =
          kafka_admin_DeleteTopicsResult_get_error(result, i);
      if (key_err != nullptr) copy_proto_error(entry->mutable_error(), key_err);
    }
    kafka_admin_DeleteTopicsResult_destroy(result);
    return grpc::Status::OK;
  }

  grpc::Status ListTopics(grpc::ServerContext*, const AdminListTopicsRequest* req,
                          AdminListTopicsResponse* resp) override {
    kafka_admin_AdminClient_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    kafka_admin_ListTopicsResult_t* result = nullptr;
    kafka_common_KafkaError_t* err = kafka_admin_AdminClient_list_topics(
        admin, timeout_ms(*req), req->list_internal(), &result);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    // Whole-value response: Java's ListTopicsResult holds one future for the
    // entire map, so no listing can fail on its own.
    const int32_t count = kafka_admin_ListTopicsResult_count(result);
    for (int32_t i = 0; i < count; i++) {
      const kafka_admin_TopicListing_t* listing =
          kafka_admin_ListTopicsResult_get_value(result, i);
      if (listing == nullptr) continue;  // out of range; cannot happen for i < count
      AdminTopicListing* dst = resp->add_listings();
      dst->set_name(cstr(kafka_admin_TopicListing_name(listing)));
      dst->set_topic_id(cstr(kafka_admin_TopicListing_topic_id(listing)));
      dst->set_is_internal(kafka_admin_TopicListing_is_internal(listing));
    }
    kafka_admin_ListTopicsResult_destroy(result);
    return grpc::Status::OK;
  }

  grpc::Status DescribeTopics(grpc::ServerContext*, const DescribeTopicsRequest* req,
                              DescribeTopicsResponse* resp) override {
    kafka_admin_AdminClient_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    const bool by_ids = req->has_topic_ids();
    std::vector<std::string> owned(by_ids ? req->topic_ids().values().begin()
                                          : req->names().values().begin(),
                                   by_ids ? req->topic_ids().values().end()
                                          : req->names().values().end());
    std::vector<const char*> keys;
    keys.reserve(owned.size());
    for (const std::string& key : owned) keys.push_back(key.c_str());
    // A negative limit leaves Java's default (2000) in place, which is what an
    // absent field means.
    const int32_t limit = req->has_partition_size_limit_per_response()
                              ? req->partition_size_limit_per_response()
                              : -1;

    kafka_admin_DescribeTopicsResult_t* result = nullptr;
    kafka_common_KafkaError_t* err =
        by_ids ? kafka_admin_AdminClient_describe_topics_by_ids(
                     admin, keys.data(), static_cast<int32_t>(keys.size()),
                     timeout_ms(*req), req->include_authorized_operations(), limit, &result)
               : kafka_admin_AdminClient_describe_topics(
                     admin, keys.data(), static_cast<int32_t>(keys.size()),
                     timeout_ms(*req), req->include_authorized_operations(), limit, &result);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }

    const int32_t count = kafka_admin_DescribeTopicsResult_count(result);
    for (int32_t i = 0; i < count; i++) {
      DescribeTopicsEntry* entry = resp->add_entries();
      set_keyed(entry->mutable_key(), kafka_admin_DescribeTopicsResult_get_key(result, i), by_ids);
      const kafka_common_KafkaError_t* key_err =
          kafka_admin_DescribeTopicsResult_get_error(result, i);
      if (key_err != nullptr) {
        copy_proto_error(entry->mutable_error(), key_err);
      } else {
        const kafka_admin_TopicDescription_t* value =
            kafka_admin_DescribeTopicsResult_get_value(result, i);
        if (value == nullptr) {
          *entry->mutable_error() = make_synthetic_error(
              VARIANT_ILLEGAL_STATE, "describeTopics entry has neither value nor error");
        } else {
          description_to_proto(value, entry->mutable_value());
        }
      }
    }
    kafka_admin_DescribeTopicsResult_destroy(result);
    return grpc::Status::OK;
  }

  grpc::Status CreatePartitions(grpc::ServerContext*, const CreatePartitionsRequest* req,
                                VoidKeyedResponse* resp) override {
    kafka_admin_AdminClient_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    std::vector<std::string> topic_names;
    std::vector<kafka_admin_NewPartitions_t*> owned;
    topic_names.reserve(req->partitions_size());
    owned.reserve(req->partitions_size());
    for (const auto& spec : req->partitions()) {
      topic_names.push_back(spec.topic());
      kafka_admin_NewPartitions_t* np = kafka_admin_NewPartitions_new(spec.total_count());
      // Adding any assignment selects Java's
      // increaseTo(int, List<List<Integer>>) — a different broker request from
      // increaseTo(int).
      if (spec.has_new_assignments()) {
        for (const auto& row : spec.new_assignments().assignments()) {
          std::vector<int32_t> brokers(row.broker_ids().begin(), row.broker_ids().end());
          kafka_admin_NewPartitions_add_assignment(
              np, brokers.data(), static_cast<int32_t>(brokers.size()));
        }
      }
      owned.push_back(np);
    }
    std::vector<const char*> topics;
    std::vector<const kafka_admin_NewPartitions_t*> counts(owned.begin(), owned.end());
    topics.reserve(topic_names.size());
    for (const std::string& name : topic_names) topics.push_back(name.c_str());

    kafka_admin_CreatePartitionsResult_t* result = nullptr;
    kafka_common_KafkaError_t* err = kafka_admin_AdminClient_create_partitions(
        admin, topics.data(), counts.data(), static_cast<int32_t>(topics.size()),
        timeout_ms(*req), req->validate_only(), retry_on_quota(*req), &result);
    for (kafka_admin_NewPartitions_t* np : owned) kafka_admin_NewPartitions_destroy(np);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }

    const int32_t count = kafka_admin_CreatePartitionsResult_count(result);
    for (int32_t i = 0; i < count; i++) {
      VoidResultEntry* entry = resp->add_entries();
      set_name_key(entry->mutable_key(), kafka_admin_CreatePartitionsResult_get_key(result, i));
      const kafka_common_KafkaError_t* key_err =
          kafka_admin_CreatePartitionsResult_get_error(result, i);
      if (key_err != nullptr) copy_proto_error(entry->mutable_error(), key_err);
    }
    kafka_admin_CreatePartitionsResult_destroy(result);
    return grpc::Status::OK;
  }

  grpc::Status DeleteRecords(grpc::ServerContext*, const DeleteRecordsRequest* req,
                             DeleteRecordsResponse* resp) override {
    kafka_admin_AdminClient_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    // The entry point takes three parallel arrays rather than a map.
    std::vector<std::string> topic_names;
    std::vector<int32_t> partitions;
    std::vector<int64_t> before_offsets;
    topic_names.reserve(req->records_size());
    for (const auto& spec : req->records()) {
      topic_names.push_back(spec.partition().topic());
      partitions.push_back(spec.partition().partition());
      before_offsets.push_back(spec.before_offset());
    }
    std::vector<const char*> topics;
    topics.reserve(topic_names.size());
    for (const std::string& name : topic_names) topics.push_back(name.c_str());

    kafka_admin_DeleteRecordsResult_t* result = nullptr;
    kafka_common_KafkaError_t* err = kafka_admin_AdminClient_delete_records(
        admin, topics.data(), partitions.data(), before_offsets.data(),
        static_cast<int32_t>(topics.size()), timeout_ms(*req), &result);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }

    const int32_t count = kafka_admin_DeleteRecordsResult_count(result);
    for (int32_t i = 0; i < count; i++) {
      DeleteRecordsEntry* entry = resp->add_entries();
      TopicPartition* tp = entry->mutable_key()->mutable_partition();
      tp->set_topic(cstr(kafka_admin_DeleteRecordsResult_get_topic(result, i)));
      tp->set_partition(kafka_admin_DeleteRecordsResult_get_partition(result, i));
      const kafka_common_KafkaError_t* key_err =
          kafka_admin_DeleteRecordsResult_get_error(result, i);
      if (key_err != nullptr) {
        copy_proto_error(entry->mutable_error(), key_err);
      } else {
        entry->mutable_value()->set_low_watermark(
            kafka_admin_DeleteRecordsResult_get_low_watermark(result, i));
      }
    }
    kafka_admin_DeleteRecordsResult_destroy(result);
    return grpc::Status::OK;
  }

  grpc::Status Close(grpc::ServerContext*, const AdminCloseRequest* req,
                     StatusResponse* resp) override {
    kafka_admin_AdminClient_t* admin = nullptr;
    {
      std::lock_guard<std::mutex> lock(mu_);
      auto it = admins_.find(req->admin_id());
      if (it != admins_.end()) {
        admin = it->second;
        admins_.erase(it);
      }
    }
    if (admin == nullptr) {
      // Idempotent close — silent success on unknown id.
      return grpc::Status::OK;
    }
    // A negative timeout is the C layer's spelling of Java's no-argument
    // close() (wait indefinitely), which is what an absent timeout_ms means.
    const int64_t timeout_ms = req->has_timeout_ms() ? req->timeout_ms() : -1;
    // Returns void: Java's Admin.close(Duration) is void, so a successful
    // close leaves resp->error unset.
    kafka_admin_AdminClient_close(admin, timeout_ms);
    kafka_admin_AdminClient_destroy(admin);
    (void)resp;
    return grpc::Status::OK;
  }

 private:
  kafka_admin_AdminClient_t* admin_for(uint64_t id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = admins_.find(id);
    return it == admins_.end() ? nullptr : it->second;
  }

  static KafkaError unknown_admin(uint64_t id) {
    return make_synthetic_error(VARIANT_ILLEGAL_STATE,
                                "unknown admin_id " + std::to_string(id));
  }

  static std::string cstr(const char* s) { return s ? std::string(s) : std::string(); }

  // A negative timeout is the C layer's "unset", so default.api.timeout.ms
  // applies — which is what an absent timeout_ms means.
  template <typename Req>
  static int32_t timeout_ms(const Req& req) {
    return req.has_timeout_ms() ? req.timeout_ms() : -1;
  }

  // Java's CreateTopicsOptions.retryOnQuotaViolation etc. default to true, so an
  // absent field is true rather than proto3's implicit false.
  template <typename Req>
  static bool retry_on_quota(const Req& req) {
    return req.has_retry_on_quota_violation() ? req.retry_on_quota_violation() : true;
  }

  // Per-key error pointers are *borrowed* from the result handle, so unlike
  // fill_proto_error this must not destroy them.
  static void copy_proto_error(KafkaError* dst, const kafka_common_KafkaError_t* err) {
    const char* msg = kafka_common_KafkaError_message(err);
    dst->set_variant(static_cast<KafkaError::Variant>(guess_variant(msg)));
    dst->set_code(kafka_common_KafkaError_code(err));
    dst->set_message(msg ? std::string(msg) : std::string());
    dst->set_is_retriable(kafka_common_KafkaError_is_retriable(err));
    dst->set_is_fatal(kafka_common_KafkaError_is_fatal(err));
  }

  static void set_name_key(ResultKey* key, const char* name) { key->set_name(cstr(name)); }

  // deleteTopics / describeTopics render their key as a topic name or as the
  // base64 topic id depending on which TopicCollection the request carried.
  static void set_keyed(ResultKey* key, const char* text, bool by_ids) {
    if (by_ids) {
      key->set_topic_id(cstr(text));
    } else {
      key->set_name(cstr(text));
    }
  }

  static void metadata_to_proto(const kafka_admin_TopicMetadataAndConfig_t* mc,
                                TopicMetadataAndConfig* dst) {
    // The value-level error of envelope exception 3: the topic was created but
    // the broker did not return its metadata, so every Java accessor rethrows.
    // It must cross as the error arm, not as a metadata of -1s.
    const kafka_common_KafkaError_t* err = kafka_admin_TopicMetadataAndConfig_error(mc);
    if (err != nullptr) {
      copy_proto_error(dst->mutable_error(), err);
      return;
    }
    TopicMetadata* metadata = dst->mutable_metadata();
    metadata->set_topic_id(cstr(kafka_admin_TopicMetadataAndConfig_topic_id(mc)));
    metadata->set_num_partitions(kafka_admin_TopicMetadataAndConfig_num_partitions(mc));
    metadata->set_replication_factor(
        kafka_admin_TopicMetadataAndConfig_replication_factor(mc));
    const int32_t configs = kafka_admin_TopicMetadataAndConfig_config_count(mc);
    for (int32_t i = 0; i < configs; i++) {
      ConfigEntry* entry = metadata->add_configs();
      entry->set_name(cstr(kafka_admin_TopicMetadataAndConfig_config_name(mc, i)));
      // Java's nullable value(): null when unset or suppressed as sensitive.
      const char* value = kafka_admin_TopicMetadataAndConfig_config_value(mc, i);
      if (value != nullptr) entry->set_value(std::string(value));
      entry->set_is_default(kafka_admin_TopicMetadataAndConfig_config_is_default(mc, i));
      entry->set_is_sensitive(kafka_admin_TopicMetadataAndConfig_config_is_sensitive(mc, i));
      entry->set_is_read_only(kafka_admin_TopicMetadataAndConfig_config_is_read_only(mc, i));
    }
  }

  // Named for its Java type rather than reusing `partition_info_to_proto`,
  // which is the consumer service's free function for `PartitionInfo`; a member
  // of the same name would hide it inside this class.
  static void topic_partition_info_to_proto(const kafka_admin_TopicPartitionInfo_t* info,
                                            TopicPartitionInfo* dst) {
    dst->set_partition(kafka_admin_TopicPartitionInfo_partition(info));
    const kafka_common_Node_t* leader = kafka_admin_TopicPartitionInfo_leader(info);
    if (leader != nullptr) node_to_proto(leader, dst->mutable_leader());
    const int32_t replicas = kafka_admin_TopicPartitionInfo_replica_count(info);
    for (int32_t i = 0; i < replicas; i++) {
      node_to_proto(kafka_admin_TopicPartitionInfo_replica(info, i), dst->add_replicas());
    }
    const int32_t isr = kafka_admin_TopicPartitionInfo_isr_count(info);
    for (int32_t i = 0; i < isr; i++) {
      node_to_proto(kafka_admin_TopicPartitionInfo_isr(info, i), dst->add_isr());
    }
    // elr / last_known_elr are nullable in Java, and an absent list reports the
    // same count 0 as an empty one — hence the dedicated has_* predicates.
    if (kafka_admin_TopicPartitionInfo_has_elr(info)) {
      NodeList* elr = dst->mutable_elr();
      const int32_t n = kafka_admin_TopicPartitionInfo_elr_count(info);
      for (int32_t i = 0; i < n; i++) {
        node_to_proto(kafka_admin_TopicPartitionInfo_elr(info, i), elr->add_nodes());
      }
    }
    if (kafka_admin_TopicPartitionInfo_has_last_known_elr(info)) {
      NodeList* last = dst->mutable_last_known_elr();
      const int32_t n = kafka_admin_TopicPartitionInfo_last_known_elr_count(info);
      for (int32_t i = 0; i < n; i++) {
        node_to_proto(kafka_admin_TopicPartitionInfo_last_known_elr(info, i), last->add_nodes());
      }
    }
  }

  static void description_to_proto(const kafka_admin_TopicDescription_t* description,
                                   TopicDescription* dst) {
    dst->set_name(cstr(kafka_admin_TopicDescription_name(description)));
    dst->set_topic_id(cstr(kafka_admin_TopicDescription_topic_id(description)));
    dst->set_is_internal(kafka_admin_TopicDescription_is_internal(description));
    const int32_t partitions = kafka_admin_TopicDescription_partition_count(description);
    for (int32_t i = 0; i < partitions; i++) {
      topic_partition_info_to_proto(kafka_admin_TopicDescription_partition(description, i),
                                    dst->add_partitions());
    }
    // Absent means the broker did not report the operations, which is not the
    // same as reporting that none are authorized.
    if (kafka_admin_TopicDescription_has_authorized_operations(description)) {
      AclOperationList* ops = dst->mutable_authorized_operations();
      const int32_t n = kafka_admin_TopicDescription_authorized_operation_count(description);
      for (int32_t i = 0; i < n; i++) {
        ops->add_operations(kafka_admin_TopicDescription_authorized_operation(description, i));
      }
    }
  }

  std::mutex mu_;
  std::unordered_map<uint64_t, kafka_admin_AdminClient_t*> admins_;
  std::atomic<uint64_t> next_id_{1};
};

}  // namespace

int main(int /*argc*/, char** /*argv*/) {
  int port = 50052;
  if (const char* env = std::getenv("GRPC_PORT")) {
    port = std::atoi(env);
  }
  const std::string address = "0.0.0.0:" + std::to_string(port);

  grpc::ServerBuilder builder;
  builder.AddListeningPort(address, grpc::InsecureServerCredentials());
  ProducerServiceImpl producer_service;
  ConsumerServiceImpl consumer_service;
  AdminServiceImpl admin_service;
  builder.RegisterService(&producer_service);
  builder.RegisterService(&consumer_service);
  builder.RegisterService(&admin_service);

  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  if (!server) {
    std::cerr << "c server: failed to start gRPC server on " << address
              << std::endl;
    return 1;
  }
  // The Rust BackendPool waits for "listening" on stderr — keep this
  // string in sync with backend_pool.rs's WaitFor::message_on_stderr.
  std::cerr << "c server: listening on " << address << std::endl;
  server->Wait();
  return 0;
}
