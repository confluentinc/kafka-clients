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
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <deque>
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
using confluent::kafka::test::ConsumerGroupMetadata;
using confluent::kafka::test::CreateProducerRequest;
using confluent::kafka::test::CreateProducerResponse;
using confluent::kafka::test::FlushRequest;
using confluent::kafka::test::KafkaError;
using confluent::kafka::test::PartitionsForRequest;
using confluent::kafka::test::PartitionsForResponse;
using confluent::kafka::test::ProducerService;
using confluent::kafka::test::RecordMetadata;
using confluent::kafka::test::SendOffsetsToTransactionRequest;
using confluent::kafka::test::SendRequest;
using confluent::kafka::test::SendResponse;
using confluent::kafka::test::StatusResponse;
using confluent::kafka::test::TransactionRequest;
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
using confluent::kafka::test::GroupMetadataResponse;
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
using confluent::kafka::test::ReleaseGroupMetadataRequest;
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
using confluent::kafka::test::AdminConfig;
using confluent::kafka::test::AdminListTopicsRequest;
using confluent::kafka::test::AdminListTopicsResponse;
using confluent::kafka::test::AdminService;
using confluent::kafka::test::AdminTopicListing;
using confluent::kafka::test::AlterPartitionReassignmentsRequest;
using confluent::kafka::test::AlterReplicaLogDirsRequest;
using confluent::kafka::test::ClusterDescription;
using confluent::kafka::test::ConfigEntry;
using confluent::kafka::test::ConfigResource;
using confluent::kafka::test::ConfigSynonym;
using confluent::kafka::test::ElectLeadersRequest;
using confluent::kafka::test::DescribeClusterRequest;
using confluent::kafka::test::DescribeClusterResponse;
using confluent::kafka::test::DescribeConfigsEntry;
using confluent::kafka::test::DescribeConfigsRequest;
using confluent::kafka::test::DescribeConfigsResponse;
using confluent::kafka::test::DescribeLogDirsEntry;
using confluent::kafka::test::DescribeLogDirsRequest;
using confluent::kafka::test::DescribeLogDirsResponse;
using confluent::kafka::test::DescribeReplicaLogDirsEntry;
using confluent::kafka::test::DescribeReplicaLogDirsRequest;
using confluent::kafka::test::DescribeReplicaLogDirsResponse;
using confluent::kafka::test::IncrementalAlterConfigsRequest;
using confluent::kafka::test::ListConfigResourcesRequest;
using confluent::kafka::test::ListConfigResourcesResponse;
using confluent::kafka::test::ListOffsetsEntry;
using confluent::kafka::test::ListOffsetsRequest;
using confluent::kafka::test::ListOffsetsResponse;
using confluent::kafka::test::ListOffsetsResultInfo;
using confluent::kafka::test::ListPartitionReassignmentsRequest;
using confluent::kafka::test::ListPartitionReassignmentsResponse;
using confluent::kafka::test::LogDirDescription;
using confluent::kafka::test::LogDirDescriptionMap;
using confluent::kafka::test::OffsetSpec;
using confluent::kafka::test::OngoingPartitionReassignment;
using confluent::kafka::test::PartitionReassignment;
using confluent::kafka::test::ReplicaInfoEntry;
using confluent::kafka::test::ReplicaLogDirInfo;
using confluent::kafka::test::TopicPartitionReplica;
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
// AdminService group messages (slice G4). This list does not glob: a name left
// out here makes the handler's request parameter deduce to `int`, and every
// member access then fails with an error pointing at an unrelated template.
using confluent::kafka::test::AlterConsumerGroupOffsetsRequest;
using confluent::kafka::test::ClassicGroupDescription;
using confluent::kafka::test::ConsumerGroupDescription;
using confluent::kafka::test::DeleteConsumerGroupOffsetsRequest;
using confluent::kafka::test::DeleteConsumerGroupsRequest;
using confluent::kafka::test::DescribeClassicGroupsEntry;
using confluent::kafka::test::DescribeClassicGroupsRequest;
using confluent::kafka::test::DescribeClassicGroupsResponse;
using confluent::kafka::test::DescribeConsumerGroupsEntry;
using confluent::kafka::test::DescribeConsumerGroupsRequest;
using confluent::kafka::test::DescribeConsumerGroupsResponse;
using confluent::kafka::test::GroupListing;
using confluent::kafka::test::GroupOffset;
using confluent::kafka::test::GroupOffsets;
using confluent::kafka::test::ListConsumerGroupOffsetsEntry;
using confluent::kafka::test::ListConsumerGroupOffsetsRequest;
using confluent::kafka::test::ListConsumerGroupOffsetsResponse;
using confluent::kafka::test::ListGroupsRequest;
using confluent::kafka::test::ListGroupsResponse;
using confluent::kafka::test::MemberAssignment;
using confluent::kafka::test::MemberDescription;
using confluent::kafka::test::OffsetAndMetadata;
using confluent::kafka::test::RemoveMembersFromConsumerGroupRequest;
using confluent::kafka::test::TopicMetadataAndConfig;
using confluent::kafka::test::TopicPartitionInfo;
using confluent::kafka::test::VoidKeyedResponse;
using confluent::kafka::test::VoidResultEntry;
// Shared / payload messages.
using confluent::kafka::test::CallbackLogEntry;
using confluent::kafka::test::CallbackLogPartition;
using confluent::kafka::test::CallbackLogResponse;
using confluent::kafka::test::AclBinding;
using confluent::kafka::test::AclBindingFilter;
using confluent::kafka::test::AlterClientQuotasRequest;
using confluent::kafka::test::AlterUserScramCredentialsRequest;
using confluent::kafka::test::ClientQuotaAlteration;
using confluent::kafka::test::ClientQuotaEntity;
using confluent::kafka::test::ClientQuotaFilterComponent;
using confluent::kafka::test::CreateAclsRequest;
using confluent::kafka::test::CreateDelegationTokenRequest;
using confluent::kafka::test::CreateDelegationTokenResponse;
using confluent::kafka::test::DelegationToken;
using confluent::kafka::test::DelegationTokenExpiryResponse;
using confluent::kafka::test::DeleteAclsEntry;
using confluent::kafka::test::DeleteAclsRequest;
using confluent::kafka::test::DeleteAclsResponse;
using confluent::kafka::test::DeletedAcl;
using confluent::kafka::test::DescribeAclsRequest;
using confluent::kafka::test::DescribeAclsResponse;
using confluent::kafka::test::DescribeClientQuotasRequest;
using confluent::kafka::test::DescribeClientQuotasResponse;
using confluent::kafka::test::DescribeDelegationTokenRequest;
using confluent::kafka::test::DescribeDelegationTokenResponse;
using confluent::kafka::test::DescribeFeaturesRequest;
using confluent::kafka::test::DescribeFeaturesResponse;
using confluent::kafka::test::DescribeUserScramCredentialsEntry;
using confluent::kafka::test::DescribeUserScramCredentialsRequest;
using confluent::kafka::test::DescribeUserScramCredentialsResponse;
using confluent::kafka::test::EntityQuotas;
using confluent::kafka::test::ExpireDelegationTokenRequest;
using confluent::kafka::test::FeatureMetadata;
using confluent::kafka::test::FilterResults;
using confluent::kafka::test::FinalizedVersionRange;
using confluent::kafka::test::KafkaPrincipal;
using confluent::kafka::test::MATCH_KIND_ANY;
using confluent::kafka::test::MATCH_KIND_DEFAULT;
using confluent::kafka::test::MATCH_KIND_EXACT;
using confluent::kafka::test::QuotaValue;
using confluent::kafka::test::RenewDelegationTokenRequest;
using confluent::kafka::test::ScramCredentialInfo;
using confluent::kafka::test::SupportedVersionRange;
using confluent::kafka::test::TokenInformation;
using confluent::kafka::test::UpdateFeaturesRequest;
using confluent::kafka::test::UserScramCredentialsDescription;
using confluent::kafka::test::AbortTransactionRequest;
using confluent::kafka::test::DescribeProducersEntry;
using confluent::kafka::test::DescribeProducersRequest;
using confluent::kafka::test::DescribeProducersResponse;
using confluent::kafka::test::DescribeTransactionsEntry;
using confluent::kafka::test::DescribeTransactionsRequest;
using confluent::kafka::test::DescribeTransactionsResponse;
using confluent::kafka::test::FenceProducersEntry;
using confluent::kafka::test::FenceProducersRequest;
using confluent::kafka::test::FenceProducersResponse;
using confluent::kafka::test::ForceTerminateTransactionRequest;
using confluent::kafka::test::ListTransactionsEntry;
using confluent::kafka::test::ListTransactionsRequest;
using confluent::kafka::test::ListTransactionsResponse;
using confluent::kafka::test::PartitionProducerState;
using confluent::kafka::test::ProducerIdAndEpoch;
using confluent::kafka::test::ProducerState;
using confluent::kafka::test::TransactionDescription;
using confluent::kafka::test::TransactionListing;
using confluent::kafka::test::TransactionListingList;

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

// Construct a synthetic KafkaError: an error of the server's own making
// ("unknown consumer_id"), with no FFI handle behind it.
//
// The code is a real one taken from the generated header, not a placeholder
// -1. -1 is `UNKNOWN_SERVER_ERROR`, a different class, and since `code` is now
// the only discriminator on the wire (the proto's `Variant` field is gone) the
// Rust client would decode these as an unknown *server* error rather than the
// local failure they are.
//
// It defaults to `LOCAL_ILLEGAL_STATE`, which is what the server's own
// bookkeeping failures ("unknown producer_id") are; the sites that validate a
// request's arguments pass `LOCAL_ILLEGAL_ARGUMENT` instead, mirroring the
// class Java would throw.
KafkaError make_synthetic_error(
    const std::string& message,
    kafka_common_ErrorCode_e code = kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE) {
  KafkaError err;
  err.set_code(code);
  err.set_message("c server: " + message);
  return err;
}

// Build a proto KafkaError from a C FFI error handle. Takes ownership
// of the handle (destroys it on the way out).
//
// `kafka_common_Error_code` identifies the error's class on its own -- the
// codes are injective -- so nothing else has to be inferred here. This is what
// retired `guess_variant_from_message`, which substring-matched the message
// text to recover a class the code could not carry, on the two producer paths
// that bothered; every consumer path shipped an unclassified error.
//
// `is_retriable` and `is_fatal` are gone from the proto: no reader consumed
// them, both are derivable from the code, and this was the server's only
// predicate call.
void fill_proto_error(KafkaError* dst, kafka_common_Error_t* err) {
  if (err == nullptr) {
    // Also the server's own bookkeeping failure, so it takes the same code.
    *dst = make_synthetic_error("null error handle");
    return;
  }
  const char* msg = kafka_common_Error_message(err);
  dst->set_code(kafka_common_Error_code(err));
  dst->set_message(msg ? std::string(msg) : std::string());
  kafka_common_Error_destroy(err);
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

// Fields copied out of a RecordMetadata handle through its accessors. The
// handles this server sees are all BORROWED — from the future a send returned
// (valid until that future is destroyed) or for the duration of a delivery
// callback — so the fields are copied while the handle is alive and the handle
// is never destroyed here.
struct MetadataFields {
  int64_t offset = -1;
  int32_t partition = -1;
  std::string topic;
  int64_t timestamp = -1;
  int32_t serialized_key_size = -1;
  int32_t serialized_value_size = -1;
};

MetadataFields read_metadata(const kafka_producer_RecordMetadata_t* metadata) {
  MetadataFields out;
  // offset() / timestamp() are -1 when unknown, like Java's.
  out.offset = kafka_producer_RecordMetadata_offset(metadata);
  out.partition = kafka_producer_RecordMetadata_partition(metadata);
  const char* topic = kafka_producer_RecordMetadata_topic(metadata);  // borrowed
  out.topic = topic ? std::string(topic) : std::string();
  out.timestamp = kafka_producer_RecordMetadata_timestamp(metadata);
  out.serialized_key_size = kafka_producer_RecordMetadata_serialized_key_size(metadata);
  out.serialized_value_size = kafka_producer_RecordMetadata_serialized_value_size(metadata);
  return out;
}

// Copies an OWNED `metrics()` map of owned `kafka_common_MetricName_t *` ->
// owned `kafka_common_metrics_KafkaMetric_t *` (the shape both the producer
// and the consumer return) into the proto list, then destroys the map, which
// frees both sides of every entry.
void metrics_map_to_proto(kafka_Map_t* map, MetricList* out) {
  const int32_t n = kafka_Map_size(map);
  for (int32_t i = 0; i < n; i++) {
    Metric* m = out->add_metrics();
    const auto* name = static_cast<const kafka_common_MetricName_t*>(kafka_Map_key(map, i));
    const auto* metric =
        static_cast<const kafka_common_metrics_KafkaMetric_t*>(kafka_Map_value(map, i));
    const char* metric_name = kafka_common_MetricName_name(name);
    const char* group = kafka_common_MetricName_group(name);
    const char* desc = kafka_common_MetricName_description(name);
    m->set_name(metric_name ? metric_name : "");
    m->set_group(group ? group : "");
    m->set_description(desc ? desc : "");
    // tags(): an owned map of `char *` -> `char *`.
    kafka_Map_t* tags = kafka_common_MetricName_tags(name);
    const int32_t tn = kafka_Map_size(tags);
    for (int32_t t = 0; t < tn; t++) {
      const char* k = static_cast<const char*>(kafka_Map_key(tags, t));
      const char* v = static_cast<const char*>(kafka_Map_value(tags, t));
      (*m->mutable_tags())[k ? k : ""] = v ? v : "";
    }
    kafka_Map_destroy(tags);
    // metricValue(): an owned snapshot of the reading, taken through the
    // metric's `Metric` view (borrowed from the KafkaMetric handle).
    kafka_common_MetricValue_t* value =
        kafka_common_Metric_metric_value(kafka_common_metrics_KafkaMetric__as_Metric(metric));
    // One typed accessor per variant, selected by `__enum` (each accessor
    // returns a sentinel for the other variants, so the switch comes first).
    switch (kafka_common_MetricValue__enum(value)) {
      case kafka_common_MetricValue_STRING: {
        // Borrowed until the value handle is destroyed below.
        const char* s = kafka_common_MetricValue_as_string(value);
        m->set_string_value(s ? s : "");
        break;
      }
      case kafka_common_MetricValue_LONG:
        m->set_long_value(kafka_common_MetricValue_as_long(value));
        break;
      case kafka_common_MetricValue_INT:
        m->set_int_value(kafka_common_MetricValue_as_int(value));
        break;
      case kafka_common_MetricValue_DOUBLE:
      default:
        m->set_double_value(kafka_common_MetricValue_as_double(value));
        break;
    }
    kafka_common_MetricValue_destroy(value);
  }
  kafka_Map_destroy(map);
}

// Defined in the consumer section below; reused by the producer PartitionsFor.
void node_to_proto(const kafka_common_Node_t* node, Node* dst);
void partition_info_to_proto(const kafka_common_PartitionInfo_t* info, PartitionInfo* dst);

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
// gRPC worker thread that is inside the blocking C call driving it (the
// consumer's poll / close / unsubscribe for the listener and commit callbacks,
// the producer's `execute_callbacks` pump for delivery reports), while
// GetCallbackLog is served on another worker thread. It is deliberately a
// *separate* mutex from the service's id-map `mu_`, so a callback firing
// mid-poll never has to wait behind an in-flight CreateX / Close.
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
  // Reads are consistent with the RPC that drove the callback, for both
  // clients. The consumer uses only the BLOCKING C entry points, which invoke
  // the listener / commit callback directly on the calling thread (CLAUDE.md
  // §4 rule 5) — the entry is appended, and the result reported, before the
  // poll / commit / close RPC returns. (Before the Phase 3 FFI rewrite the
  // consumer's callbacks were jobs on a detached dispatcher thread, and
  // post-close reads had to poll.) The producer's delivery callbacks are queued
  // on the producer's callback vector and this server pumps them on the RPC
  // thread (after a Send's future resolved, in Flush / Close, and in
  // GetCallbackLog itself), so an entry is appended before the RPC that
  // completed the record returns.
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

// What a C callback needs in order to find its log: the `self` of every
// interface registration this server makes (`kafka_producer_Callback_t`,
// `kafka_consumer_ConsumerRebalanceListener_t`,
// `kafka_consumer_OffsetCommitCallback_t`).
//
// Exactly one heap instance per client, allocated in CreateProducer /
// CreateConsumer and owned by the service's `log_states_` map — NOT by any
// individual registration. The C interfaces have no destroy hook for `self`
// (CLAUDE.md §4 rule 3: the caller owns it and keeps it alive until the
// registration is released), and the release points differ per interface:
//
//   - a rebalance listener's `self` must live until the next `subscribe_*`,
//     `unsubscribe` or the consumer's destruction, and a later re-subscribe
//     must be able to reuse the same state;
//   - a commit callback's `self` must live until `onComplete` fired (or the
//     consumer was destroyed);
//   - a delivery callback's `self` must live until the callback fired, so a
//     per-send allocation would have to be freed by the callback itself — and
//     then leak on the validation-failure path where the callback is
//     documented not to fire.
//
// The state is **never freed before the service is destroyed** — it is
// session-lifetime, held by `log_states_` as a `unique_ptr` that `Close` does
// not erase. `_destroy` on either client runs the callbacks still pending
// before returning (so each fires exactly once), which would make freeing the
// state right after Close safe today; keeping it for the whole session also
// removes the `log_state_for()`-returns-nullptr-after-Close race in Send /
// CommitAsync and the leak for a client that is never Closed (neither service
// impl has a destructor).
struct LogState {
  CallbackLog* log;
  uint64_t client_id;
  // The consumer's `Consumer` view, for `kafka_consumer_Consumer__set_callback_result`
  // (informational there — the lookup is by callback_id — but named anyway).
  // nullptr for a producer's state.
  const kafka_consumer_Consumer_t* consumer = nullptr;
};

// Shared body of the three rebalance trampolines, the C implementation of a
// ConsumerRebalanceListener method (CLAUDE.md §4 rule 3). `partitions` is
// BORROWED for the call (a `kafka_List_t` of `kafka_common_TopicPartition_t *`,
// never destroyed here). The method is `async` in Rust, so it returns void and
// MUST report its result exactly once through
// `kafka_consumer_Consumer__set_callback_result`: NULL for success (an owned
// `kafka_common_Error_t *` would fail the rebalance, like a throwing Java
// listener). This server reports synchronously from inside the method, the
// model for a blocking-entry-point caller: the method runs on the gRPC worker
// thread that is inside the consumer's poll() / close() / unsubscribe(), and
// the membership state machine does not advance until the report
// (consumer-threading.md §31).
void log_rebalance(void* self, const kafka_List_t* partitions, int64_t callback_id,
                   const char* kind) {
  auto* state = static_cast<LogState*>(self);
  CallbackLogEntry entry;
  entry.set_kind(kind);
  if (partitions != nullptr) {
    const int32_t n = kafka_List_size(partitions);
    for (int32_t i = 0; i < n; i++) {
      const auto* tp = static_cast<const kafka_common_TopicPartition_t*>(kafka_List_get(partitions, i));
      CallbackLogPartition* p = entry.add_partitions();
      const char* topic = kafka_common_TopicPartition_topic(tp);
      p->set_topic(topic ? topic : "");
      p->set_partition(kafka_common_TopicPartition_partition(tp));
    }
  }
  state->log->append(state->client_id, std::move(entry));
  kafka_consumer_Consumer__set_callback_result(state->consumer, callback_id, nullptr);
}

extern "C" void log_partitions_assigned(void* self, const kafka_List_t* partitions,
                                        int64_t callback_id) {
  log_rebalance(self, partitions, callback_id, KIND_ASSIGNED);
}

extern "C" void log_partitions_revoked(void* self, const kafka_List_t* partitions,
                                       int64_t callback_id) {
  log_rebalance(self, partitions, callback_id, KIND_REVOKED);
}

// Passed explicitly rather than left NULL (which would make the registration
// reproduce Java's "onPartitionsLost delegates to onPartitionsRevoked" default),
// so a lost callback is distinguishable from a revoke in the log.
extern "C" void log_partitions_lost(void* self, const kafka_List_t* partitions,
                                    int64_t callback_id) {
  log_rebalance(self, partitions, callback_id, KIND_LOST);
}

// Copies a BORROWED error's message into `entry`; nothing is destroyed.
void copy_error_into(CallbackLogEntry* entry, const kafka_common_Error_t* error) {
  if (error == nullptr) return;
  const char* msg = kafka_common_Error_message(error);
  entry->set_error(msg ? std::string(msg) : std::string("c server: unnamed callback error"));
}

// Copies a BORROWED offsets map (`kafka_common_TopicPartition_t *` ->
// `kafka_consumer_OffsetAndMetadata_t *`) into `entry`'s partitions + offsets;
// nothing is destroyed.
void copy_offsets_into(CallbackLogEntry* entry, const kafka_Map_t* offsets) {
  if (offsets == nullptr) return;
  const int32_t n = kafka_Map_size(offsets);
  for (int32_t i = 0; i < n; i++) {
    const auto* tp = static_cast<const kafka_common_TopicPartition_t*>(kafka_Map_key(offsets, i));
    const char* raw_topic = kafka_common_TopicPartition_topic(tp);
    const std::string topic = raw_topic ? raw_topic : "";
    const int32_t partition = kafka_common_TopicPartition_partition(tp);
    CallbackLogPartition* p = entry->add_partitions();
    p->set_topic(topic);
    p->set_partition(partition);
    const auto* v = static_cast<const kafka_consumer_OffsetAndMetadata_t*>(kafka_Map_value(offsets, i));
    (*entry->mutable_offsets())[offset_key(topic, partition)] =
        kafka_consumer_OffsetAndMetadata_offset(v);
  }
}

// The `on_complete` of the kafka_consumer_OffsetCommitCallback_t CommitAsync
// registers with with_callback; `self` is the consumer's LogState. Both
// `offsets` and `error` are BORROWED for the call (the FFI frees its copies
// after it returns), so nothing is destroyed here. Java's onComplete is void,
// so the mandatory report is always NULL: the result is ignored, the report
// tells the consumer the callback finished. It runs on the gRPC worker thread
// inside the blocking consumer call that drove the callback (the next poll /
// commit / close after the commit completed).
extern "C" void log_commit_complete(void* self, const kafka_Map_t* offsets,
                                    const kafka_common_Error_t* error, int64_t callback_id) {
  auto* state = static_cast<LogState*>(self);
  CallbackLogEntry entry;
  entry.set_kind(KIND_COMMIT);
  copy_offsets_into(&entry, offsets);
  copy_error_into(&entry, error);
  state->log->append(state->client_id, std::move(entry));
  kafka_consumer_Consumer__set_callback_result(state->consumer, callback_id, nullptr);
}

// Java's commitAsync(offsets, null) is legal, but
// kafka_consumer_Consumer_commit_async_with_offsets_callback's `callback`
// parameter is not nullable and there is no plain `..._commit_async_with_offsets`.
// So an explicit-offsets CommitAsync without with_callback gets this no-op,
// which still has to report completion (an implementation that never reports
// hangs the consumer's next blocking call, as a Java callback that never
// returns would).
extern "C" void discard_commit_complete(void* self, const kafka_Map_t* /*offsets*/,
                                        const kafka_common_Error_t* /*error*/,
                                        int64_t callback_id) {
  auto* state = static_cast<LogState*>(self);
  kafka_consumer_Consumer__set_callback_result(state->consumer, callback_id, nullptr);
}

// The `on_completion` of the kafka_producer_Callback_t registered by Send with
// with_callback; `self` is the producer's LogState. Both arguments are BORROWED
// for the call (the FFI frees them after it returns), so nothing is destroyed
// here. It runs on whichever gRPC worker thread pumps
// kafka_producer_Producer_execute_callbacks (see pump_callbacks below), never
// on a Rust thread.
extern "C" void log_delivery(void* self, const kafka_producer_RecordMetadata_t* metadata,
                             const kafka_common_Error_t* error) {
  auto* state = static_cast<LogState*>(self);
  CallbackLogEntry entry;
  entry.set_kind(KIND_DELIVERY);
  // Both arguments can be set at once: a real producer rejecting a record
  // before it reaches the accumulator delivers placeholder metadata
  // (offset/partition -1) alongside the error, mirroring Java's
  // callback.onCompletion(nullMetadata, e). Record whichever is present.
  if (metadata != nullptr) {
    const MetadataFields fields = read_metadata(metadata);
    CallbackLogPartition* p = entry.add_partitions();
    p->set_topic(fields.topic);
    p->set_partition(fields.partition);
    (*entry.mutable_offsets())[offset_key(fields.topic, fields.partition)] = fields.offset;
  }
  copy_error_into(&entry, error);
  state->log->append(state->client_id, std::move(entry));
}

// Server-side group-metadata handles, by id. ConsumerService.GroupMetadata
// adds the OWNED handle kafka_consumer_Consumer_group_metadata returns, and
// ProducerService.SendOffsetsToTransaction passes it back to the FFI, so the
// producer gets exactly the metadata the consumer handed out (the C API's own
// kafka_consumer_ConsumerGroupMetadata_new builds a C-implemented interface,
// which would make this server, not the consumer, the source of the fields).
// ConsumerService.ReleaseGroupMetadata removes it, when the Rust client drops
// its last reference.
//
// Handles sit in shared_ptrs whose deleter is
// kafka_consumer_ConsumerGroupMetadata_destroy, so a release that races a
// send_offsets_to_transaction destroys the handle only after the FFI call
// returns. The handle caches its own copy of the metadata, so it stays valid
// after its consumer closes or is destroyed.
class GroupMetadataStore {
 public:
  uint64_t add(kafka_consumer_ConsumerGroupMetadata_t* handle) {
    const uint64_t id = next_id_.fetch_add(1);
    std::lock_guard<std::mutex> lock(mu_);
    handles_[id] = std::shared_ptr<kafka_consumer_ConsumerGroupMetadata_t>(
        handle, kafka_consumer_ConsumerGroupMetadata_destroy);
    return id;
  }

  // nullptr for an unknown (or already released) id.
  std::shared_ptr<kafka_consumer_ConsumerGroupMetadata_t> get(uint64_t id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = handles_.find(id);
    return it == handles_.end() ? nullptr : it->second;
  }

  void release(uint64_t id) {
    std::shared_ptr<kafka_consumer_ConsumerGroupMetadata_t> handle;
    {
      std::lock_guard<std::mutex> lock(mu_);
      auto it = handles_.find(id);
      if (it == handles_.end()) return;
      handle = std::move(it->second);
      handles_.erase(it);
    }
    // `handle` goes out of scope here, outside the lock.
  }

 private:
  std::mutex mu_;
  std::unordered_map<uint64_t,
                     std::shared_ptr<kafka_consumer_ConsumerGroupMetadata_t>>
      handles_;
  std::atomic<uint64_t> next_id_{1};
};

// A producer as CreateProducer built it: the owning class handle (exactly one
// of `kafka` / `mock` is set, because `kafka_producer_Producer_t` is the
// `Producer` interface and has no `_new` / `_destroy` of its own) and its
// `__as_Producer` view, borrowed from the class handle and valid until that
// handle is destroyed. Every operation takes the view; only Close touches the
// class handle.
struct ProducerHandle {
  kafka_producer_KafkaProducer_t* kafka = nullptr;
  kafka_producer_MockProducer_t* mock = nullptr;
  const kafka_producer_Producer_t* view = nullptr;

  // Frees the class handle; the view is invalid afterwards. `_destroy` also
  // runs the callbacks still queued on the producer, so each delivery callback
  // fires exactly once even if nothing pumped it before.
  void destroy() {
    if (kafka != nullptr) kafka_producer_KafkaProducer_destroy(kafka);
    if (mock != nullptr) kafka_producer_MockProducer_destroy(mock);
    kafka = nullptr;
    mock = nullptr;
    view = nullptr;
  }
};

// Runs the producer's queued callbacks on this gRPC worker thread. Delivery
// callbacks are always *queued* by the FFI (the producer's background task
// fires them onto the callback vector) and only run when a caller pumps
// `kafka_producer_Producer_execute_callbacks`; this server has no pump thread,
// so every RPC that can complete a record (Send after its future resolved,
// Flush, Close) and GetCallbackLog itself pump here. Pumping is serialized
// inside the FFI, so concurrent RPCs may call this freely.
void pump_callbacks(const kafka_producer_Producer_t* producer) {
  kafka_producer_Producer_execute_callbacks(producer);
}

class ProducerServiceImpl final : public ProducerService::Service {
 public:
  explicit ProducerServiceImpl(GroupMetadataStore* group_metadata)
      : group_metadata_(group_metadata) {}

  grpc::Status CreateProducer(grpc::ServerContext*,
                              const CreateProducerRequest* req,
                              CreateProducerResponse* resp) override {
    ProducerHandle handle;

    if (req->config().empty()) {
      // Empty config selects MockProducer for client-side smoke testing. NULL
      // serializers (the only choice for the mock constructor used here): keys
      // and values are `kafka_Bytes_t *`, see Send.
      handle.mock = kafka_producer_MockProducer_with_auto_complete(/*auto_complete=*/1);
      handle.view = kafka_producer_MockProducer__as_Producer(handle.mock);
    } else {
      // ProducerConfig takes a C-built map of `char *` -> `char *`, borrowed
      // for the call (the config copies and validates them), so the proto
      // strings can be handed over without copying and the map destroyed
      // right after.
      kafka_Map_t* props = kafka_Map_new();
      for (const auto& kv : req->config()) {
        kafka_Map_put(props, const_cast<char*>(kv.first.c_str()),
                      const_cast<char*>(kv.second.c_str()));
      }
      kafka_producer_ProducerConfig_t* config = nullptr;
      kafka_common_Error_t* err = kafka_producer_ProducerConfig_new(props, &config);
      kafka_Map_destroy(props);
      if (err != nullptr) {
        // Validation happens here, not in KafkaProducer_new: an invalid value
        // fails the creation with the ConfigException Java would throw.
        fill_proto_error(resp->mutable_error(), err);
        return grpc::Status::OK;
      }
      // NULL serializers: keys and values cross as `kafka_Bytes_t *`, exactly
      // the bytes the proto carries. The config stays ours and can be destroyed
      // right after the producer was built from it.
      err = kafka_producer_KafkaProducer_new(config, nullptr, nullptr, &handle.kafka);
      kafka_producer_ProducerConfig_destroy(config);
      if (err != nullptr) {
        fill_proto_error(resp->mutable_error(), err);
        return grpc::Status::OK;
      }
      handle.view = kafka_producer_KafkaProducer__as_Producer(handle.kafka);
    }

    const uint64_t id = next_id_.fetch_add(1);
    {
      std::lock_guard<std::mutex> lock(mu_);
      producers_[id] = handle;
      // One stable LogState per producer — see the struct's comment for why the
      // delivery callback's `self` cannot be per-send, and why it lives for
      // the whole session rather than being freed at Close.
      log_states_[id] = std::unique_ptr<LogState>(new LogState{&callback_log_, id});
    }
    resp->set_producer_id(id);
    std::cerr << "c server: created producer " << id << std::endl;
    return grpc::Status::OK;
  }

  grpc::Status Send(grpc::ServerContext*, const SendRequest* req,
                    SendResponse* resp) override {
    const kafka_producer_Producer_t* producer = producer_for(req->producer_id());
    if (producer == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          "unknown producer_id " + std::to_string(req->producer_id()));
      return grpc::Status::OK;
    }

    const auto& rec = req->record();
    // The producers here are built without serializers, so the record's key
    // and value `void *` are `kafka_Bytes_t *` (NULL for Java's null). The
    // record keeps the pointers, not the bytes: the blocking send below reads
    // them during the call, so these locals (and the proto they point into)
    // must outlive it — they do, the RPC is synchronous.
    kafka_Bytes_t key_bytes{nullptr, 0};
    const void* key = nullptr;
    if (rec.has_key()) {
      key_bytes.data = reinterpret_cast<const uint8_t*>(rec.key().data());
      key_bytes.len = static_cast<int32_t>(rec.key().size());
      key = &key_bytes;
    }
    kafka_Bytes_t value_bytes{nullptr, 0};
    const void* value = nullptr;
    if (rec.has_value()) {
      value_bytes.data = reinterpret_cast<const uint8_t*>(rec.value().data());
      value_bytes.len = static_cast<int32_t>(rec.value().size());
      value = &value_bytes;
    }

    // Headers, when the record carries any. `add_with_key_value` copies the
    // bytes; the builder's `set_headers` copies the headers again, so the
    // handle is destroyed together with the builder below.
    kafka_common_header_internals_RecordHeaders_t* headers = nullptr;
    if (rec.headers_size() > 0) {
      headers = kafka_common_header_internals_RecordHeaders_new();
      // Borrowed view of the handle, never destroyed on its own.
      kafka_common_header_Headers_t* headers_view =
          kafka_common_header_internals_RecordHeaders__as_Headers(headers);
      for (const auto& h : rec.headers()) {
        kafka_Bytes_t header_value{reinterpret_cast<const uint8_t*>(h.value().data()),
                                   static_cast<int32_t>(h.value().size())};
        kafka_common_Error_t* header_err = kafka_common_header_Headers_add_with_key_value(
            headers_view, h.key().c_str(), header_value);
        if (header_err != nullptr) {
          kafka_common_header_internals_RecordHeaders_destroy(headers);
          fill_proto_error(resp->mutable_error(), header_err);
          return grpc::Status::OK;
        }
      }
    }

    // Java's six-argument ProducerRecord constructor through its options
    // builder: it takes every optional the proto can carry (partition,
    // timestamp, key, headers), unset meaning Java's null. `set_value` is
    // mandatory even for a null (tombstone) value.
    kafka_producer_ProducerRecordOptionsBuilder_t* builder =
        kafka_producer_ProducerRecordOptionsBuilder_new();
    kafka_producer_ProducerRecordOptionsBuilder_set_topic(builder, rec.topic().c_str());
    if (rec.has_partition()) {
      kafka_producer_ProducerRecordOptionsBuilder_set_partition(builder, rec.partition());
    }
    if (rec.has_timestamp()) {
      kafka_producer_ProducerRecordOptionsBuilder_set_timestamp(builder, rec.timestamp());
    }
    kafka_producer_ProducerRecordOptionsBuilder_set_key(builder, key);
    kafka_producer_ProducerRecordOptionsBuilder_set_value(builder, value);
    if (headers != nullptr) {
      kafka_producer_ProducerRecordOptionsBuilder_set_headers(builder, headers);
    }
    kafka_producer_ProducerRecordOptions_t* options = nullptr;
    kafka_common_Error_t* build_err =
        kafka_producer_ProducerRecordOptionsBuilder_build(builder, &options);
    // `build` consumed the builder's content; the handle itself is still ours.
    kafka_producer_ProducerRecordOptionsBuilder_destroy(builder);
    if (headers != nullptr) kafka_common_header_internals_RecordHeaders_destroy(headers);
    if (build_err != nullptr) {
      fill_proto_error(resp->mutable_error(), build_err);
      return grpc::Status::OK;
    }
    kafka_producer_ProducerRecord_t* record = nullptr;
    kafka_common_Error_t* record_err = kafka_producer_ProducerRecord_with_options(options, &record);
    kafka_producer_ProducerRecordOptions_destroy(options);
    if (record_err != nullptr) {
      // Java's constructor validation (e.g. "Invalid timestamp").
      fill_proto_error(resp->mutable_error(), record_err);
      return grpc::Status::OK;
    }

    // with_callback => register a *real* delivery callback through the FFI, so
    // the Rust harness can assert (via GetCallbackLog) on what the binding's own
    // callback saw. The blocking future path below is unchanged: the FFI gives us
    // both, exactly like Java's send(record, callback). The registration is
    // copied into the Rust closure during the call, so the Callback_t handle is
    // destroyed right after send returned; its `self` (the LogState) lives for
    // the whole session.
    kafka_common_Error_t* send_err = nullptr;
    kafka_common_KafkaFuture_t* future = nullptr;
    if (req->with_callback()) {
      LogState* state = log_state_for(req->producer_id());
      kafka_producer_Callback_t* callback = kafka_producer_Callback_new(state, log_delivery);
      send_err = kafka_producer_Producer_send_with_callback(producer, record, callback, &future);
      kafka_producer_Callback_destroy(callback);
    } else {
      send_err = kafka_producer_Producer_send(producer, record, &future);
    }
    // send cloned the record.
    kafka_producer_ProducerRecord_destroy(record);
    if (send_err != nullptr) {
      // Synchronous failure (RecordTooLarge, LocalIllegalState, etc.). The
      // FFI returns a non-null error we forward verbatim. A pre-accumulator
      // rejection also queued the delivery callback (with placeholder
      // metadata beside the error, as Java does), so pump it into the log.
      pump_callbacks(producer);
      fill_proto_error(resp->mutable_error(), send_err);
      return grpc::Status::OK;
    }

    // Block this gRPC worker thread waiting for the future to resolve. The
    // FFI's get() drives the producer's tokio runtime from the calling thread,
    // so it's safe to call from arbitrary threads.
    //
    // The metadata is BORROWED from the future (valid until the future is
    // destroyed, never passed to RecordMetadata_destroy), so copy the fields
    // out before destroying the future.
    void* raw_metadata = nullptr;
    kafka_common_Error_t* get_err = kafka_common_KafkaFuture_get(future, &raw_metadata);
    if (get_err != nullptr) {
      kafka_common_KafkaFuture_destroy(future);
      // The record's delivery callback was queued before its future resolved.
      pump_callbacks(producer);
      fill_proto_error(resp->mutable_error(), get_err);
      return grpc::Status::OK;
    }
    const MetadataFields fields =
        read_metadata(static_cast<const kafka_producer_RecordMetadata_t*>(raw_metadata));
    kafka_common_KafkaFuture_destroy(future);
    pump_callbacks(producer);

    RecordMetadata* m = resp->mutable_metadata();
    m->set_offset(fields.offset);
    m->set_timestamp(fields.timestamp);
    m->set_topic(fields.topic);
    m->set_partition(fields.partition);
    m->set_serialized_key_size(fields.serialized_key_size);
    m->set_serialized_value_size(fields.serialized_value_size);
    return grpc::Status::OK;
  }

  // ── Transactions (Milestone 11) ──
  //
  // Each maps to the same-named Producer FFI call, which returns a
  // *error handle directly (not via an out-param), and reports it through
  // StatusResponse. fill_proto_error copies the handle's code and message; the
  // code identifies the error class on its own, so nothing is inferred from the
  // message text. init/commit/abort block on the producer's tokio runtime
  // server-side; the unary RPC is the resolved result.
  grpc::Status InitTransactions(grpc::ServerContext*,
                                const TransactionRequest* req,
                                StatusResponse* resp) override {
    const kafka_producer_Producer_t* producer = producer_for(req->producer_id());
    if (producer == nullptr) {
      *resp->mutable_error() = make_synthetic_error("unknown producer_id " + std::to_string(req->producer_id()));
      return grpc::Status::OK;
    }
    kafka_common_Error_t* err =
        kafka_producer_Producer_init_transactions(producer);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
    }
    return grpc::Status::OK;
  }

  grpc::Status BeginTransaction(grpc::ServerContext*,
                                const TransactionRequest* req,
                                StatusResponse* resp) override {
    const kafka_producer_Producer_t* producer = producer_for(req->producer_id());
    if (producer == nullptr) {
      *resp->mutable_error() = make_synthetic_error("unknown producer_id " + std::to_string(req->producer_id()));
      return grpc::Status::OK;
    }
    kafka_common_Error_t* err =
        kafka_producer_Producer_begin_transaction(producer);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
    }
    return grpc::Status::OK;
  }

  grpc::Status CommitTransaction(grpc::ServerContext*,
                                 const TransactionRequest* req,
                                 StatusResponse* resp) override {
    const kafka_producer_Producer_t* producer = producer_for(req->producer_id());
    if (producer == nullptr) {
      *resp->mutable_error() = make_synthetic_error("unknown producer_id " + std::to_string(req->producer_id()));
      return grpc::Status::OK;
    }
    kafka_common_Error_t* err =
        kafka_producer_Producer_commit_transaction(producer);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
    }
    // Committing (or failing to) completes the transaction's records, whose
    // delivery callbacks are now queued.
    pump_callbacks(producer);
    return grpc::Status::OK;
  }

  grpc::Status AbortTransaction(grpc::ServerContext*,
                                const TransactionRequest* req,
                                StatusResponse* resp) override {
    const kafka_producer_Producer_t* producer = producer_for(req->producer_id());
    if (producer == nullptr) {
      *resp->mutable_error() = make_synthetic_error("unknown producer_id " + std::to_string(req->producer_id()));
      return grpc::Status::OK;
    }
    kafka_common_Error_t* err =
        kafka_producer_Producer_abort_transaction(producer);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
    }
    // Aborting fails the transaction's in-flight records, whose delivery
    // callbacks are now queued.
    pump_callbacks(producer);
    return grpc::Status::OK;
  }

  // Java sendOffsetsToTransaction(offsets, groupMetadata): the producer half of
  // consume-transform-produce. Builds the C map of TopicPartition ->
  // OffsetAndMetadata the FFI expects, and passes the group-metadata handle
  // stored under the request's id by ConsumerService.GroupMetadata.
  grpc::Status SendOffsetsToTransaction(
      grpc::ServerContext*, const SendOffsetsToTransactionRequest* req,
      StatusResponse* resp) override {
    const kafka_producer_Producer_t* producer = producer_for(req->producer_id());
    if (producer == nullptr) {
      *resp->mutable_error() = make_synthetic_error("unknown producer_id " + std::to_string(req->producer_id()));
      return grpc::Status::OK;
    }
    // A C-built map holds borrowed elements: the FFI copies what it needs
    // during the call, and we free the entries and the map afterwards. The
    // marshaling matches kafka_consumer_Consumer_commit_sync_with_offsets via
    // read_offset_map: an absent leader_epoch is -1 ("no epoch"), and an
    // absent metadata entry means the empty string.
    kafka_Map_t* offsets = kafka_Map_new();
    std::vector<kafka_common_TopicPartition_t*> partitions;
    std::vector<kafka_consumer_OffsetAndMetadata_t*> offset_values;
    partitions.reserve(req->offsets_size());
    offset_values.reserve(req->offsets_size());
    auto release = [&]() {
      for (auto* tp : partitions) kafka_common_TopicPartition_destroy(tp);
      for (auto* oam : offset_values) kafka_consumer_OffsetAndMetadata_destroy(oam);
      kafka_Map_destroy(offsets);
    };
    for (const auto& e : req->offsets()) {
      kafka_consumer_OffsetAndMetadata_t* oam = nullptr;
      // Java's OffsetAndMetadata constructor validation (negative offset)
      // surfaces here, as the IllegalArgumentException it is.
      kafka_common_Error_t* build_err = kafka_consumer_OffsetAndMetadata_with_leader_epoch_metadata(
          e.offset(), e.has_leader_epoch() ? e.leader_epoch() : -1,
          e.has_metadata() ? e.metadata().c_str() : "", &oam);
      if (build_err != nullptr) {
        release();
        fill_proto_error(resp->mutable_error(), build_err);
        return grpc::Status::OK;
      }
      kafka_common_TopicPartition_t* tp =
          kafka_common_TopicPartition_new(e.topic().c_str(), e.partition());
      partitions.push_back(tp);
      offset_values.push_back(oam);
      kafka_Map_put(offsets, tp, oam);
    }
    // Holding the shared_ptr keeps the handle alive through the FFI call even
    // if a ReleaseGroupMetadata for it arrives meanwhile.
    std::shared_ptr<kafka_consumer_ConsumerGroupMetadata_t> group_meta =
        group_metadata_->get(req->group_metadata_id());
    if (group_meta == nullptr) {
      release();
      *resp->mutable_error() = make_synthetic_error(
          "unknown group_metadata_id " +
          std::to_string(req->group_metadata_id()));
      return grpc::Status::OK;
    }
    kafka_common_Error_t* err =
        kafka_producer_Producer_send_offsets_to_transaction(
            producer, offsets, group_meta.get());
    release();
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
    }
    return grpc::Status::OK;
  }

  grpc::Status Flush(grpc::ServerContext*, const FlushRequest* req,
                     StatusResponse* resp) override {
    const kafka_producer_Producer_t* producer = producer_for(req->producer_id());
    if (producer == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          "unknown producer_id " + std::to_string(req->producer_id()));
      return grpc::Status::OK;
    }
    kafka_common_Error_t* err = kafka_producer_Producer_flush(producer);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
    }
    // flush() completes every outstanding record, so their delivery callbacks
    // are queued by now: run them before answering, as Java's flush returns
    // only after the callbacks ran.
    pump_callbacks(producer);
    return grpc::Status::OK;
  }

  grpc::Status PartitionsFor(grpc::ServerContext*,
                             const PartitionsForRequest* req,
                             PartitionsForResponse* resp) override {
    const kafka_producer_Producer_t* producer = producer_for(req->producer_id());
    if (producer == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          "unknown producer_id " + std::to_string(req->producer_id()));
      return grpc::Status::OK;
    }
    // An owned list of owned PartitionInfo handles: destroying the list frees
    // the elements.
    kafka_List_t* list = nullptr;
    kafka_common_Error_t* err =
        kafka_producer_Producer_partitions_for(producer, req->topic().c_str(), &list);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    const int32_t n = kafka_List_size(list);
    for (int32_t i = 0; i < n; i++) {
      partition_info_to_proto(
          static_cast<const kafka_common_PartitionInfo_t*>(kafka_List_get(list, i)),
          resp->add_partitions());
    }
    kafka_List_destroy(list);
    return grpc::Status::OK;
  }

  grpc::Status Metrics(grpc::ServerContext*, const MetricsRequest* req,
                       MetricsResponse* resp) override {
    const kafka_producer_Producer_t* producer = producer_for(req->producer_id());
    if (producer == nullptr) {
      *resp->mutable_error() = make_synthetic_error("unknown producer_id " + std::to_string(req->producer_id()));
      return grpc::Status::OK;
    }
    // An owned map of owned MetricName -> KafkaMetric handles; destroying the
    // map frees both sides of every entry.
    kafka_Map_t* map = kafka_producer_Producer_metrics(producer);
    if (map == nullptr) {
      *resp->mutable_error() = make_synthetic_error("no metrics for producer " + std::to_string(req->producer_id()));
      return grpc::Status::OK;
    }
    metrics_map_to_proto(map, resp->mutable_metrics());
    return grpc::Status::OK;
  }

  grpc::Status Close(grpc::ServerContext*, const CloseRequest* req,
                     StatusResponse* resp) override {
    return close_producer(req->producer_id(), /*has_timeout=*/false, 0, resp);
  }

  grpc::Status GetCallbackLog(grpc::ServerContext*, const ProducerCallbackLogRequest* req,
                              CallbackLogResponse* resp) override {
    // Pump first, so a delivery callback queued since the last RPC (e.g. by a
    // batch the background task completed after Send returned) is in the log
    // the reader gets. A closed producer has no view any more; its callbacks
    // ran during Close.
    const kafka_producer_Producer_t* producer = producer_for(req->producer_id());
    if (producer != nullptr) pump_callbacks(producer);
    callback_log_.fill(req->producer_id(), resp);
    return grpc::Status::OK;
  }

  grpc::Status CloseTimeout(grpc::ServerContext*,
                            const CloseTimeoutRequest* req,
                            StatusResponse* resp) override {
    return close_producer(req->producer_id(), /*has_timeout=*/true, req->timeout_ms(), resp);
  }

 private:
  // Shared body of Close (Java's close(), i.e. Duration.ofMillis(Long.MAX_VALUE))
  // and CloseTimeout (close(Duration)).
  grpc::Status close_producer(uint64_t producer_id, bool has_timeout, int64_t timeout_ms,
                              StatusResponse* resp) {
    if (has_timeout && timeout_ms < 0) {
      // Java's close(Duration) rejects a negative timeout up front with
      // IllegalArgumentException and leaves the producer open, so do not
      // unregister it: forward the FFI's own validation error and keep it.
      const kafka_producer_Producer_t* producer = producer_for(producer_id);
      if (producer == nullptr) return grpc::Status::OK;
      kafka_common_Error_t* err = kafka_producer_Producer_close_with_timeout(producer, timeout_ms);
      if (err != nullptr) fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    ProducerHandle handle;
    {
      std::lock_guard<std::mutex> lock(mu_);
      auto it = producers_.find(producer_id);
      if (it != producers_.end()) {
        handle = it->second;
        producers_.erase(it);
      }
      // log_states_ is deliberately NOT erased — see LogState's comment.
    }
    if (handle.view == nullptr) {
      // Idempotent close — silent success on unknown id.
      return grpc::Status::OK;
    }
    // close() flushes, so every outstanding delivery callback is queued by the
    // time this returns; pumping them here before destroying the handle makes
    // a GetCallbackLog issued right after Close complete (destroy would run
    // the still-pending ones anyway, so each fires exactly once either way).
    kafka_common_Error_t* err =
        has_timeout ? kafka_producer_Producer_close_with_timeout(handle.view, timeout_ms)
                    : kafka_producer_Producer_close(handle.view);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
    }
    pump_callbacks(handle.view);
    // Destroying the class handle invalidates the view; it does not close the
    // producer, which is why close ran first.
    handle.destroy();
    // The log entries stay in callback_log_ and the LogState stays in
    // log_states_, so GetCallbackLog still works post-close.
    return grpc::Status::OK;
  }

  // The producer's `Producer` view, or nullptr for an unknown (or closed) id.
  const kafka_producer_Producer_t* producer_for(uint64_t id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = producers_.find(id);
    return it == producers_.end() ? nullptr : it->second.view;
  }

  LogState* log_state_for(uint64_t id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = log_states_.find(id);
    return it == log_states_.end() ? nullptr : it->second.get();
  }


  // Shared with ConsumerServiceImpl; owned by main().
  GroupMetadataStore* group_metadata_;
  std::mutex mu_;
  std::unordered_map<uint64_t, ProducerHandle> producers_;
  // `self` for the delivery callbacks; owned here, one per producer, for the
  // whole session (never erased by Close — see LogState).
  std::unordered_map<uint64_t, std::unique_ptr<LogState>> log_states_;
  // Has its own mutex; see CallbackLog.
  CallbackLog callback_log_;
  std::atomic<uint64_t> next_id_{1};
};

// ---------------------------------------------------------------------------
// Consumer service
// ---------------------------------------------------------------------------

// A consumer as CreateConsumer built it. `view` is the `Consumer` interface
// handle (`kafka_consumer_Consumer_t`, Rust's `Box<dyn Consumer>`) every
// operation takes. For a MockConsumer it is the `__as_Consumer` view BORROWED
// from the class handle `mock`, valid until that handle is destroyed and never
// passed to `kafka_consumer_Consumer_destroy`. For a KafkaConsumer there is no
// class handle at all — `kafka_consumer_KafkaConsumer_new` delivers the boxed
// `Consumer` itself — so `view` is OWNED and freed with
// `kafka_consumer_Consumer_destroy`. Exactly one of the two shapes applies,
// selected by `mock != nullptr`.
struct ConsumerEntry {
  kafka_consumer_MockConsumer_t* mock = nullptr;
  kafka_consumer_Consumer_t* view = nullptr;

  // Frees whichever handle owns the consumer; the view is invalid afterwards.
  // Both destroys close the consumer if close() never ran and run the
  // callbacks still queued on it — none here, since this server only uses the
  // blocking entry points, whose listener / commit callbacks fire inline.
  void destroy() {
    if (mock != nullptr) {
      kafka_consumer_MockConsumer_destroy(mock);
    } else if (view != nullptr) {
      kafka_consumer_Consumer_destroy(view);
    }
    mock = nullptr;
    view = nullptr;
  }
};

// C-built containers hold BORROWED elements: the FFI copies what it needs
// during the call and the builder frees the elements and the container
// afterwards (the rule SendOffsetsToTransaction applies by hand). The RAII
// wrappers below do that freeing in their destructors, so every early return
// in a handler releases them.

// A `kafka_List_t` of owned `kafka_common_TopicPartition_t *` built from a
// repeated TopicPartition — the shape `assign`, `seek_to_*`, `pause`,
// `resume`, `committed`, `beginning_offsets` and `end_offsets` take.
struct TpList {
  explicit TpList(const ::google::protobuf::RepeatedPtrField<TopicPartition>& tps)
      : list(kafka_List_new()) {
    handles.reserve(tps.size());
    for (const auto& tp : tps) {
      kafka_common_TopicPartition_t* handle =
          kafka_common_TopicPartition_new(tp.topic().c_str(), tp.partition());
      handles.push_back(handle);
      kafka_List_add(list, handle);
    }
  }
  ~TpList() {
    for (auto* handle : handles) kafka_common_TopicPartition_destroy(handle);
    kafka_List_destroy(list);
  }
  TpList(const TpList&) = delete;
  TpList& operator=(const TpList&) = delete;

  kafka_List_t* list;
  std::vector<kafka_common_TopicPartition_t*> handles;
};

// A `kafka_List_t` of `char *` aliasing a `repeated string` field, for
// `subscribe_with_topics`. The pointers alias the protobuf-owned strings,
// which outlive the synchronous call, and the FFI copies them, so nothing is
// copied here and only the list itself is freed.
struct BorrowedStringList {
  explicit BorrowedStringList(const ::google::protobuf::RepeatedPtrField<std::string>& values)
      : list(kafka_List_new()) {
    for (const std::string& value : values) {
      kafka_List_add(list, const_cast<char*>(value.c_str()));
    }
  }
  ~BorrowedStringList() { kafka_List_destroy(list); }
  BorrowedStringList(const BorrowedStringList&) = delete;
  BorrowedStringList& operator=(const BorrowedStringList&) = delete;

  kafka_List_t* list;
};

// A `kafka_Map_t` of owned `kafka_common_TopicPartition_t *` to owned
// `kafka_consumer_OffsetAndMetadata_t *` built from repeated OffsetMapEntry —
// the offsets `commit_sync_with_offsets` and
// `commit_async_with_offsets_callback` take. Shared by CommitSync and
// CommitAsync (CommitAsyncRequest.offsets deliberately reuses OffsetMapEntry).
// The marshaling matches SendOffsetsToTransaction: an absent leader_epoch is
// -1 ("no epoch"), the metadata string is passed as is.
//
// `error` is non-null when an entry failed Java's OffsetAndMetadata
// constructor validation (negative offset), surfacing the
// IllegalArgumentException it is; the map is then incomplete and must not be
// passed on. The handler takes it with `take_error()`.
struct OffsetMapHandles {
  explicit OffsetMapHandles(const ::google::protobuf::RepeatedPtrField<OffsetMapEntry>& entries)
      : map(kafka_Map_new()) {
    partitions.reserve(entries.size());
    offsets.reserve(entries.size());
    for (const auto& e : entries) {
      kafka_consumer_OffsetAndMetadata_t* oam = nullptr;
      error = kafka_consumer_OffsetAndMetadata_with_leader_epoch_metadata(
          e.offset().offset(), e.offset().has_leader_epoch() ? e.offset().leader_epoch() : -1,
          e.offset().metadata().c_str(), &oam);
      if (error != nullptr) return;
      kafka_common_TopicPartition_t* tp =
          kafka_common_TopicPartition_new(e.partition().topic().c_str(), e.partition().partition());
      partitions.push_back(tp);
      offsets.push_back(oam);
      kafka_Map_put(map, tp, oam);
    }
  }
  ~OffsetMapHandles() {
    for (auto* tp : partitions) kafka_common_TopicPartition_destroy(tp);
    for (auto* oam : offsets) kafka_consumer_OffsetAndMetadata_destroy(oam);
    kafka_Map_destroy(map);
    if (error != nullptr) kafka_common_Error_destroy(error);
  }
  OffsetMapHandles(const OffsetMapHandles&) = delete;
  OffsetMapHandles& operator=(const OffsetMapHandles&) = delete;

  // Hands the construction error (owned) to the caller.
  kafka_common_Error_t* take_error() {
    kafka_common_Error_t* out = error;
    error = nullptr;
    return out;
  }

  kafka_Map_t* map;
  kafka_common_Error_t* error = nullptr;
  std::vector<kafka_common_TopicPartition_t*> partitions;
  std::vector<kafka_consumer_OffsetAndMetadata_t*> offsets;
};

// A `kafka_Map_t` of owned `kafka_common_TopicPartition_t *` to `int64_t *`
// (the timestamps to search) for `offsets_for_times`. `timestamps` is
// reserved up front so the addresses handed to the map stay stable.
struct TimestampMapHandles {
  explicit TimestampMapHandles(
      const ::google::protobuf::RepeatedPtrField<confluent::kafka::test::TimestampSpecEntry>& entries)
      : map(kafka_Map_new()) {
    partitions.reserve(entries.size());
    timestamps.reserve(entries.size());
    for (const auto& e : entries) {
      kafka_common_TopicPartition_t* tp =
          kafka_common_TopicPartition_new(e.partition().topic().c_str(), e.partition().partition());
      partitions.push_back(tp);
      timestamps.push_back(e.timestamp());
      kafka_Map_put(map, tp, &timestamps.back());
    }
  }
  ~TimestampMapHandles() {
    for (auto* tp : partitions) kafka_common_TopicPartition_destroy(tp);
    kafka_Map_destroy(map);
  }
  TimestampMapHandles(const TimestampMapHandles&) = delete;
  TimestampMapHandles& operator=(const TimestampMapHandles&) = delete;

  kafka_Map_t* map;
  std::vector<kafka_common_TopicPartition_t*> partitions;
  std::vector<int64_t> timestamps;
};

void node_to_proto(const kafka_common_Node_t* node, Node* dst) {
  dst->set_id(kafka_common_Node_id(node));
  const char* host = kafka_common_Node_host(node);  // NUL-terminated, borrowed
  if (host != nullptr) dst->set_host(host);
  dst->set_port(kafka_common_Node_port(node));
  const char* rack = kafka_common_Node_rack(node);  // nullptr when Java's rack is null
  if (rack != nullptr) dst->set_rack(rack);
}

// Converts every `kafka_common_Node_t *` of an owned list into the proto node
// returned by `add()`, then frees the list. A null list (Java null) adds nothing.
template <typename Add>
void node_list_to_proto(kafka_List_t* nodes, Add add) {
  if (nodes == nullptr) return;
  const int32_t n = kafka_List_size(nodes);
  for (int32_t i = 0; i < n; i++) {
    node_to_proto(static_cast<const kafka_common_Node_t*>(kafka_List_get(nodes, i)), add());
  }
  kafka_List_destroy(nodes);
}

void partition_info_to_proto(const kafka_common_PartitionInfo_t* info,
                             PartitionInfo* dst) {
  const char* topic = kafka_common_PartitionInfo_topic(info);  // NUL-terminated
  dst->set_topic(topic ? topic : "");
  dst->set_partition(kafka_common_PartitionInfo_partition(info));
  const kafka_common_Node_t* leader = kafka_common_PartitionInfo_leader(info);
  if (leader != nullptr) node_to_proto(leader, dst->mutable_leader());
  node_list_to_proto(kafka_common_PartitionInfo_replicas(info),
                     [dst] { return dst->add_replicas(); });
  node_list_to_proto(kafka_common_PartitionInfo_in_sync_replicas(info),
                     [dst] { return dst->add_in_sync_replicas(); });
  node_list_to_proto(kafka_common_PartitionInfo_offline_replicas(info),
                     [dst] { return dst->add_offline_replicas(); });
}

// Converts every `kafka_common_PartitionInfo_t *` of an owned list into the
// proto returned by `add()`, then frees the list (which frees its elements).
template <typename Add>
void partition_info_list_to_proto(kafka_List_t* infos, Add add) {
  if (infos == nullptr) return;
  const int32_t n = kafka_List_size(infos);
  for (int32_t i = 0; i < n; i++) {
    partition_info_to_proto(
        static_cast<const kafka_common_PartitionInfo_t*>(kafka_List_get(infos, i)), add());
  }
  kafka_List_destroy(infos);
}

void tp_to_proto(const kafka_common_TopicPartition_t* tp, TopicPartition* dst) {
  const char* topic = kafka_common_TopicPartition_topic(tp);
  dst->set_topic(topic ? topic : "");
  dst->set_partition(kafka_common_TopicPartition_partition(tp));
}

// Copies an OffsetAndMetadata handle into its proto. leader_epoch is -1 for
// Java's Optional.empty(), in which case the optional proto field stays unset.
void oam_to_proto(const kafka_consumer_OffsetAndMetadata_t* v, OffsetAndMetadata* dst) {
  dst->set_offset(kafka_consumer_OffsetAndMetadata_offset(v));
  const char* meta = kafka_consumer_OffsetAndMetadata_metadata(v);  // borrowed
  dst->set_metadata(meta ? meta : "");
  const int32_t epoch = kafka_consumer_OffsetAndMetadata_leader_epoch(v);
  if (epoch >= 0) dst->set_leader_epoch(epoch);
}

class ConsumerServiceImpl final : public ConsumerService::Service {
 public:
  explicit ConsumerServiceImpl(GroupMetadataStore* group_metadata)
      : group_metadata_(group_metadata) {}

  grpc::Status CreateConsumer(grpc::ServerContext*,
                              const CreateConsumerRequest* req,
                              CreateConsumerResponse* resp) override {
    ConsumerEntry entry;
    if (req->config().empty()) {
      // Empty config selects MockConsumer for client-side smoke testing. The
      // mock has no deserializers: a record's key / value `void *` are the
      // pointers add_record was given (this server never adds any).
      kafka_common_Error_t* err = kafka_consumer_MockConsumer_new("earliest", &entry.mock);
      if (err != nullptr) {
        fill_proto_error(resp->mutable_error(), err);
        return grpc::Status::OK;
      }
      entry.view = kafka_consumer_MockConsumer__as_Consumer(entry.mock);
    } else {
      // ConsumerConfig takes a C-built map of `char *` -> `char *`, borrowed
      // for the call (the config copies and validates them), so the proto
      // strings can be handed over without copying and the map destroyed
      // right after.
      kafka_Map_t* props = kafka_Map_new();
      for (const auto& kv : req->config()) {
        kafka_Map_put(props, const_cast<char*>(kv.first.c_str()),
                      const_cast<char*>(kv.second.c_str()));
      }
      kafka_consumer_ConsumerConfig_t* config = nullptr;
      kafka_common_Error_t* err = kafka_consumer_ConsumerConfig_new(props, &config);
      kafka_Map_destroy(props);
      if (err != nullptr) {
        // Validation happens here, not in KafkaConsumer_new: an invalid value
        // fails the creation with the ConfigException Java would throw.
        fill_proto_error(resp->mutable_error(), err);
        return grpc::Status::OK;
      }
      // NULL deserializers: keys and values cross as `kafka_Bytes_t *` owned by
      // the records that delivered them (see record_to_proto). The config stays
      // ours and can be destroyed right after the consumer was built from it.
      err = kafka_consumer_KafkaConsumer_new(config, nullptr, nullptr, &entry.view);
      kafka_consumer_ConsumerConfig_destroy(config);
      if (err != nullptr) {
        fill_proto_error(resp->mutable_error(), err);
        return grpc::Status::OK;
      }
    }
    const uint64_t id = next_id_.fetch_add(1);
    {
      std::lock_guard<std::mutex> lock(mu_);
      consumers_[id] = entry;
      // One stable LogState per consumer, shared by the rebalance listener and
      // every commit callback — see the struct's comment (session-lifetime; not
      // freed at Close). It carries the consumer view the callbacks report to.
      log_states_[id] = std::unique_ptr<LogState>(new LogState{&callback_log_, id, entry.view});
    }
    resp->set_consumer_id(id);
    std::cerr << "c server: created consumer " << id << std::endl;
    return grpc::Status::OK;
  }

  grpc::Status Subscribe(grpc::ServerContext*, const SubscribeRequest* req,
                         StatusResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) return unknown(resp, req->consumer_id());
    BorrowedStringList topics(req->topics());
    kafka_common_Error_t* err = nullptr;
    if (req->with_listener()) {
      // A real ConsumerRebalanceListener whose invocations land in the callback
      // log. Its `self` is the consumer's LogState, which lives for the whole
      // session and so outlives every release point of the registration (the
      // next subscribe_*, unsubscribe, or the consumer's destruction). The
      // registration is COPIED by subscribe, success or failure, so the handle
      // is destroyed right after.
      kafka_consumer_ConsumerRebalanceListener_t* listener =
          kafka_consumer_ConsumerRebalanceListener_new(
              log_state_for(req->consumer_id()), log_partitions_revoked,
              log_partitions_assigned, log_partitions_lost);
      err = kafka_consumer_Consumer_subscribe_with_topics_listener(c, topics.list, listener);
      kafka_consumer_ConsumerRebalanceListener_destroy(listener);
    } else {
      err = kafka_consumer_Consumer_subscribe_with_topics(c, topics.list);
    }
    if (err != nullptr) fill_proto_error(resp->mutable_error(), err);
    return grpc::Status::OK;
  }

  grpc::Status Unsubscribe(grpc::ServerContext*, const ConsumerIdRequest* req,
                           StatusResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) return unknown(resp, req->consumer_id());
    kafka_common_Error_t* err = kafka_consumer_Consumer_unsubscribe(c);
    if (err != nullptr) fill_proto_error(resp->mutable_error(), err);
    return grpc::Status::OK;
  }

  grpc::Status Assign(grpc::ServerContext*, const AssignRequest* req,
                      StatusResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) return unknown(resp, req->consumer_id());
    TpList partitions(req->partitions());
    kafka_common_Error_t* err = kafka_consumer_Consumer_assign(c, partitions.list);
    if (err != nullptr) fill_proto_error(resp->mutable_error(), err);
    return grpc::Status::OK;
  }

  grpc::Status Poll(grpc::ServerContext*, const PollRequest* req,
                    PollResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          "unknown consumer_id " + std::to_string(req->consumer_id()));
      return grpc::Status::OK;
    }
    // Blocking poll: it runs on this gRPC worker thread, and any rebalance
    // listener callback it drives is invoked inline (see log_rebalance), so
    // the log entry is appended before this RPC answers.
    kafka_consumer_ConsumerRecords_t* records = nullptr;
    kafka_common_Error_t* err = kafka_consumer_Consumer_poll(c, req->timeout_ms(), &records);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    ConsumerRecordList* list = resp->mutable_records();
    // Java's iterator order: partition by partition, in fetch order.
    // partitions() is an owned list of owned TopicPartition handles;
    // records(tp) an owned list of records BORROWED from the records handle
    // (valid until it is destroyed, never passed to ConsumerRecord_destroy).
    kafka_List_t* partitions = kafka_consumer_ConsumerRecords_partitions(records);
    const int32_t pn = kafka_List_size(partitions);
    for (int32_t p = 0; p < pn; p++) {
      const auto* tp =
          static_cast<const kafka_common_TopicPartition_t*>(kafka_List_get(partitions, p));
      kafka_List_t* recs = kafka_consumer_ConsumerRecords_records_with_partition(records, tp);
      const int32_t n = kafka_List_size(recs);
      for (int32_t i = 0; i < n; i++) {
        record_to_proto(static_cast<const kafka_consumer_ConsumerRecord_t*>(kafka_List_get(recs, i)),
                        list->add_records());
      }
      kafka_List_destroy(recs);
    }
    kafka_List_destroy(partitions);
    // Also releases the fetch buffer the records' key / value bytes point into,
    // which is why every record was copied out above.
    kafka_consumer_ConsumerRecords_destroy(records);
    return grpc::Status::OK;
  }

  grpc::Status CommitSync(grpc::ServerContext*, const CommitSyncRequest* req,
                          StatusResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) return unknown(resp, req->consumer_id());
    kafka_common_Error_t* err = nullptr;
    if (req->offsets().empty()) {
      err = kafka_consumer_Consumer_commit_sync(c);
    } else {
      OffsetMapHandles offsets(req->offsets());
      if (offsets.error != nullptr) {
        fill_proto_error(resp->mutable_error(), offsets.take_error());
        return grpc::Status::OK;
      }
      err = kafka_consumer_Consumer_commit_sync_with_offsets(c, offsets.map);
    }
    if (err != nullptr) fill_proto_error(resp->mutable_error(), err);
    return grpc::Status::OK;
  }

  grpc::Status CommitAsync(grpc::ServerContext*, const CommitAsyncRequest* req,
                           StatusResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) return unknown(resp, req->consumer_id());
    kafka_common_Error_t* err = nullptr;
    if (req->offsets().empty() && !req->with_callback()) {
      err = kafka_consumer_Consumer_commit_async(c);
    } else {
      // A real OffsetCommitCallback (or the reporting no-op, see
      // discard_commit_complete) whose `self` is the consumer's LogState: it
      // lives for the whole session, as required until onComplete fired. The
      // registration is COPIED by commit_async_*, so the handle is destroyed
      // right after the call, success or failure.
      kafka_consumer_OffsetCommitCallback_t* callback = kafka_consumer_OffsetCommitCallback_new(
          log_state_for(req->consumer_id()),
          req->with_callback() ? log_commit_complete : discard_commit_complete);
      if (req->offsets().empty()) {
        err = kafka_consumer_Consumer_commit_async_with_callback(c, callback);
      } else {
        OffsetMapHandles offsets(req->offsets());
        if (offsets.error != nullptr) {
          kafka_consumer_OffsetCommitCallback_destroy(callback);
          fill_proto_error(resp->mutable_error(), offsets.take_error());
          return grpc::Status::OK;
        }
        err = kafka_consumer_Consumer_commit_async_with_offsets_callback(c, offsets.map, callback);
      }
      kafka_consumer_OffsetCommitCallback_destroy(callback);
    }
    if (err != nullptr) fill_proto_error(resp->mutable_error(), err);
    return grpc::Status::OK;
  }

  grpc::Status Committed(grpc::ServerContext*, const CommittedRequest* req,
                         CommittedResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          "unknown consumer_id " + std::to_string(req->consumer_id()));
      return grpc::Status::OK;
    }
    TpList partitions(req->partitions());
    // An owned map of owned TopicPartition -> owned OffsetAndMetadata handles;
    // destroying the map frees both sides of every entry.
    kafka_Map_t* map = nullptr;
    kafka_common_Error_t* err = kafka_consumer_Consumer_committed(c, partitions.list, &map);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    OffsetMap* out = resp->mutable_offsets();
    const int32_t n = kafka_Map_size(map);
    for (int32_t i = 0; i < n; i++) {
      OffsetMapEntry* entry = out->add_entries();
      tp_to_proto(static_cast<const kafka_common_TopicPartition_t*>(kafka_Map_key(map, i)),
                  entry->mutable_partition());
      oam_to_proto(static_cast<const kafka_consumer_OffsetAndMetadata_t*>(kafka_Map_value(map, i)),
                   entry->mutable_offset());
    }
    kafka_Map_destroy(map);
    return grpc::Status::OK;
  }

  grpc::Status Position(grpc::ServerContext*, const PositionRequest* req,
                        PositionResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          "unknown consumer_id " + std::to_string(req->consumer_id()));
      return grpc::Status::OK;
    }
    kafka_common_TopicPartition_t* tp = kafka_common_TopicPartition_new(
        req->partition().topic().c_str(), req->partition().partition());
    int64_t out = 0;
    kafka_common_Error_t* err = kafka_consumer_Consumer_position(c, tp, &out);
    kafka_common_TopicPartition_destroy(tp);
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
    kafka_common_TopicPartition_t* tp = kafka_common_TopicPartition_new(
        req->partition().topic().c_str(), req->partition().partition());
    kafka_common_Error_t* err = nullptr;
    if (req->has_metadata() || req->has_leader_epoch()) {
      // seek(TopicPartition, OffsetAndMetadata): the handle is borrowed for the
      // call. Its constructor validation (negative offset) surfaces as the
      // IllegalArgumentException it is; an absent leader_epoch is -1.
      kafka_consumer_OffsetAndMetadata_t* oam = nullptr;
      err = kafka_consumer_OffsetAndMetadata_with_leader_epoch_metadata(
          req->offset(), req->has_leader_epoch() ? req->leader_epoch() : -1,
          req->has_metadata() ? req->metadata().c_str() : "", &oam);
      if (err == nullptr) {
        err = kafka_consumer_Consumer_seek_with_offset_and_metadata(c, tp, oam);
        kafka_consumer_OffsetAndMetadata_destroy(oam);
      }
    } else {
      err = kafka_consumer_Consumer_seek_with_offset(c, tp, req->offset());
    }
    kafka_common_TopicPartition_destroy(tp);
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
          "unknown consumer_id " + std::to_string(req->consumer_id()));
      return grpc::Status::OK;
    }
    TimestampMapHandles timestamps(req->timestamps());
    // An owned map of owned TopicPartition -> owned OffsetAndTimestamp handles;
    // partitions the broker could not resolve are absent (Java's null values).
    kafka_Map_t* map = nullptr;
    kafka_common_Error_t* err =
        kafka_consumer_Consumer_offsets_for_times(c, timestamps.map, &map);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    OffsetAndTimestampMap* out = resp->mutable_offsets();
    const int32_t n = kafka_Map_size(map);
    for (int32_t i = 0; i < n; i++) {
      OffsetAndTimestampMapEntry* entry = out->add_entries();
      tp_to_proto(static_cast<const kafka_common_TopicPartition_t*>(kafka_Map_key(map, i)),
                  entry->mutable_partition());
      const auto* v = static_cast<const kafka_consumer_OffsetAndTimestamp_t*>(kafka_Map_value(map, i));
      OffsetAndTimestamp* oat = entry->mutable_offset();
      oat->set_offset(kafka_consumer_OffsetAndTimestamp_offset(v));
      oat->set_timestamp(kafka_consumer_OffsetAndTimestamp_timestamp(v));
      // -1 for Java's Optional.empty(): the optional proto field stays unset.
      const int32_t epoch = kafka_consumer_OffsetAndTimestamp_leader_epoch(v);
      if (epoch >= 0) oat->set_leader_epoch(epoch);
    }
    kafka_Map_destroy(map);
    return grpc::Status::OK;
  }

  grpc::Status PartitionsFor(grpc::ServerContext*, const ConsumerPartitionsForRequest* req,
                             PartitionsForResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          "unknown consumer_id " + std::to_string(req->consumer_id()));
      return grpc::Status::OK;
    }
    // An owned list of owned PartitionInfo handles: destroying the list frees
    // the elements.
    kafka_List_t* infos = nullptr;
    kafka_common_Error_t* err =
        kafka_consumer_Consumer_partitions_for(c, req->topic().c_str(), &infos);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    partition_info_list_to_proto(infos, [resp] { return resp->add_partitions(); });
    return grpc::Status::OK;
  }

  grpc::Status ListTopics(grpc::ServerContext*, const ConsumerIdRequest* req,
                          ListTopicsResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          "unknown consumer_id " + std::to_string(req->consumer_id()));
      return grpc::Status::OK;
    }
    // An owned map of owned `char *` topic -> owned `kafka_List_t *` of owned
    // PartitionInfo handles. Destroying the map frees the keys and the lists
    // (with their elements), so the inner lists are read in place, not
    // destroyed on their own.
    kafka_Map_t* map = nullptr;
    kafka_common_Error_t* err = kafka_consumer_Consumer_list_topics(c, &map);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    TopicListing* listing = resp->mutable_topics();
    const int32_t n = kafka_Map_size(map);
    for (int32_t i = 0; i < n; i++) {
      TopicPartitionInfoEntry* entry = listing->add_topics();
      const char* topic = static_cast<const char*>(kafka_Map_key(map, i));
      entry->set_topic(topic ? topic : "");
      const auto* infos = static_cast<const kafka_List_t*>(kafka_Map_value(map, i));
      const int32_t pn = infos == nullptr ? 0 : kafka_List_size(infos);
      for (int32_t j = 0; j < pn; j++) {
        partition_info_to_proto(
            static_cast<const kafka_common_PartitionInfo_t*>(kafka_List_get(infos, j)),
            entry->add_partitions());
      }
    }
    kafka_Map_destroy(map);
    return grpc::Status::OK;
  }

  grpc::Status Assignment(grpc::ServerContext*, const ConsumerIdRequest* req,
                          TopicPartitionListResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          "unknown consumer_id " + std::to_string(req->consumer_id()));
      return grpc::Status::OK;
    }
    fill_tp_list(kafka_consumer_Consumer_assignment(c), resp);
    return grpc::Status::OK;
  }

  grpc::Status Paused(grpc::ServerContext*, const ConsumerIdRequest* req,
                      TopicPartitionListResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          "unknown consumer_id " + std::to_string(req->consumer_id()));
      return grpc::Status::OK;
    }
    fill_tp_list(kafka_consumer_Consumer_paused(c), resp);
    return grpc::Status::OK;
  }

  grpc::Status Metrics(grpc::ServerContext*, const ConsumerIdRequest* req,
                       MetricsResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          "unknown consumer_id " + std::to_string(req->consumer_id()));
      return grpc::Status::OK;
    }
    // An owned map of owned MetricName -> KafkaMetric handles, the same shape
    // the producer returns; EMPTY (not null) while another operation holds the
    // consumer's single-owner flag.
    kafka_Map_t* map = kafka_consumer_Consumer_metrics(c);
    if (map == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          "no metrics for consumer " + std::to_string(req->consumer_id()));
      return grpc::Status::OK;
    }
    metrics_map_to_proto(map, resp->mutable_metrics());
    return grpc::Status::OK;
  }

  grpc::Status Subscription(grpc::ServerContext*, const ConsumerIdRequest* req,
                            SubscriptionResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          "unknown consumer_id " + std::to_string(req->consumer_id()));
      return grpc::Status::OK;
    }
    // An owned, sorted list of owned `char *`; destroying the list frees them.
    kafka_List_t* list = kafka_consumer_Consumer_subscription(c);
    StringList* out = resp->mutable_topics();
    if (list != nullptr) {
      const int32_t n = kafka_List_size(list);
      for (int32_t i = 0; i < n; i++) {
        const char* s = static_cast<const char*>(kafka_List_get(list, i));
        out->add_values(s ? s : "");
      }
      kafka_List_destroy(list);
    }
    return grpc::Status::OK;
  }

  // Java groupMetadata(): store the handle and return its id together with the
  // fields read through the accessors (group_instance_id is null for a dynamic
  // member).
  grpc::Status GroupMetadata(grpc::ServerContext*, const ConsumerIdRequest* req,
                             GroupMetadataResponse* resp) override {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          "unknown consumer_id " + std::to_string(req->consumer_id()));
      return grpc::Status::OK;
    }
    // An OWNED handle (its deleter is the GroupMetadataStore's), or NULL while
    // another operation holds the consumer's single-owner flag — the
    // ConcurrentModificationException Java would throw, which the sync getter
    // has no error slot to return.
    kafka_consumer_ConsumerGroupMetadata_t* handle =
        kafka_consumer_Consumer_group_metadata(c);
    if (handle == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          "KafkaConsumer is not safe for multi-threaded access (consumer " +
              std::to_string(req->consumer_id()) + " is busy)",
          kafka_common_ErrorCode_LOCAL_CONCURRENT_MODIFICATION);
      return grpc::Status::OK;
    }
    ConsumerGroupMetadata* fields = resp->mutable_group_metadata();
    const char* group_id = kafka_consumer_ConsumerGroupMetadata_group_id(handle);
    fields->set_group_id(group_id ? group_id : "");
    fields->set_generation_id(
        kafka_consumer_ConsumerGroupMetadata_generation_id(handle));
    const char* member_id = kafka_consumer_ConsumerGroupMetadata_member_id(handle);
    fields->set_member_id(member_id ? member_id : "");
    if (const char* instance =
            kafka_consumer_ConsumerGroupMetadata_group_instance_id(handle)) {
      fields->set_group_instance_id(instance);
    }
    resp->set_group_metadata_id(group_metadata_->add(handle));
    return grpc::Status::OK;
  }

  grpc::Status ReleaseGroupMetadata(grpc::ServerContext*,
                                    const ReleaseGroupMetadataRequest* req,
                                    StatusResponse*) override {
    group_metadata_->release(req->group_metadata_id());
    return grpc::Status::OK;
  }

  grpc::Status Wakeup(grpc::ServerContext*, const ConsumerIdRequest* req,
                      StatusResponse*) override {
    // Always allowed, even while a blocking poll holds the consumer on another
    // worker thread — that is its purpose.
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c != nullptr) kafka_consumer_Consumer_wakeup(c);
    return grpc::Status::OK;
  }

  grpc::Status Close(grpc::ServerContext*, const ConsumerCloseRequest* req,
                     StatusResponse* resp) override {
    ConsumerEntry entry;
    {
      std::lock_guard<std::mutex> lock(mu_);
      auto it = consumers_.find(req->consumer_id());
      if (it != consumers_.end()) {
        entry = it->second;
        consumers_.erase(it);
      }
      // log_states_ is deliberately NOT erased — see LogState's comment.
    }
    if (entry.view == nullptr) {
      return grpc::Status::OK;  // idempotent
    }
    // close() is a blocking entry point: the commit callbacks it drains and
    // the on_partitions_lost / revoked it fires are invoked inline on this
    // thread (reporting synchronously, see log_rebalance), so their log
    // entries are appended before this returns and GetCallbackLog is
    // consistent right after Close.
    kafka_common_Error_t* err = kafka_consumer_Consumer_close(entry.view);
    if (err != nullptr) fill_proto_error(resp->mutable_error(), err);
    // Frees the class handle (mock) or the owned Consumer (KafkaConsumer); the
    // view is invalid afterwards. The log entries stay in callback_log_ and the
    // LogState stays in log_states_, so GetCallbackLog still works post-close.
    entry.destroy();
    return grpc::Status::OK;
  }

  grpc::Status GetCallbackLog(grpc::ServerContext*, const CallbackLogRequest* req,
                              CallbackLogResponse* resp) override {
    // Nothing to pump: the consumer's callbacks all fired inline on the RPC
    // thread that drove them (blocking entry points only on this server).
    callback_log_.fill(req->consumer_id(), resp);
    return grpc::Status::OK;
  }

 private:
  // The consumer's `Consumer` view, or nullptr for an unknown (or closed) id.
  kafka_consumer_Consumer_t* consumer_for(uint64_t id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = consumers_.find(id);
    return it == consumers_.end() ? nullptr : it->second.view;
  }

  LogState* log_state_for(uint64_t id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = log_states_.find(id);
    return it == log_states_.end() ? nullptr : it->second.get();
  }

  grpc::Status unknown(StatusResponse* resp, uint64_t id) {
    *resp->mutable_error() =
        make_synthetic_error("unknown consumer_id " + std::to_string(id));
    return grpc::Status::OK;
  }

  // The void operations over a partition list (seek_to_*, pause, resume).
  using TpListFn = kafka_common_Error_t* (*)(kafka_consumer_Consumer_t*, const kafka_List_t*);
  grpc::Status tp_list_op(const TopicPartitionListRequest* req, StatusResponse* resp, TpListFn fn) {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) return unknown(resp, req->consumer_id());
    TpList partitions(req->partitions());
    kafka_common_Error_t* err = fn(c, partitions.list);
    if (err != nullptr) fill_proto_error(resp->mutable_error(), err);
    return grpc::Status::OK;
  }

  // beginning_offsets / end_offsets: an owned map of owned TopicPartition ->
  // owned `int64_t *`; destroying the map frees both sides of every entry.
  using LongOffFn = kafka_common_Error_t* (*)(kafka_consumer_Consumer_t*, const kafka_List_t*,
                                              kafka_Map_t**);
  grpc::Status long_offsets(const TopicPartitionListRequest* req, LongOffsetsResponse* resp,
                            LongOffFn fn) {
    kafka_consumer_Consumer_t* c = consumer_for(req->consumer_id());
    if (c == nullptr) {
      *resp->mutable_error() = make_synthetic_error(
          "unknown consumer_id " + std::to_string(req->consumer_id()));
      return grpc::Status::OK;
    }
    TpList partitions(req->partitions());
    kafka_Map_t* map = nullptr;
    kafka_common_Error_t* err = fn(c, partitions.list, &map);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    LongOffsetMap* out = resp->mutable_offsets();
    const int32_t n = kafka_Map_size(map);
    for (int32_t i = 0; i < n; i++) {
      LongOffsetMapEntry* entry = out->add_entries();
      tp_to_proto(static_cast<const kafka_common_TopicPartition_t*>(kafka_Map_key(map, i)),
                  entry->mutable_partition());
      entry->set_offset(*static_cast<const int64_t*>(kafka_Map_value(map, i)));
    }
    kafka_Map_destroy(map);
    return grpc::Status::OK;
  }

  // Copies an owned list of owned TopicPartition handles (assignment() /
  // paused(): sorted, EMPTY while another operation holds the consumer) into
  // the response and frees it.
  void fill_tp_list(kafka_List_t* list, TopicPartitionListResponse* resp) {
    TopicPartitionList* out = resp->mutable_partitions();
    if (list != nullptr) {
      const int32_t n = kafka_List_size(list);
      for (int32_t i = 0; i < n; i++) {
        tp_to_proto(static_cast<const kafka_common_TopicPartition_t*>(kafka_List_get(list, i)),
                    out->add_partitions());
      }
      kafka_List_destroy(list);
    }
  }

  // The record is BORROWED from the records handle (see Poll). The consumers
  // here are built with NULL deserializers, so key() / value() are
  // `kafka_Bytes_t *` owned by that handle and pointing into its fetch buffer
  // (zero-copy); NULL is Java's null. Every byte is copied into the proto
  // while the handle is alive, since destroying it releases the buffer.
  static void record_to_proto(const kafka_consumer_ConsumerRecord_t* rec, ConsumerRecord* dst) {
    const char* topic = kafka_consumer_ConsumerRecord_topic(rec);  // borrowed, NUL-terminated
    dst->set_topic(topic ? topic : "");
    dst->set_partition(kafka_consumer_ConsumerRecord_partition(rec));
    dst->set_offset(kafka_consumer_ConsumerRecord_offset(rec));
    dst->set_timestamp(kafka_consumer_ConsumerRecord_timestamp(rec));
    // The proto carries Java's TimestampType.id (-1 none, 0 create, 1 append);
    // the accessor returns the borrowed enum singleton.
    dst->set_timestamp_type(
        kafka_common_record_TimestampType_id(kafka_consumer_ConsumerRecord_timestamp_type(rec)));
    const auto* key = static_cast<const kafka_Bytes_t*>(kafka_consumer_ConsumerRecord_key(rec));
    if (key != nullptr && key->data != nullptr) {
      dst->set_key(std::string(reinterpret_cast<const char*>(key->data), key->len));
    }
    const auto* value = static_cast<const kafka_Bytes_t*>(kafka_consumer_ConsumerRecord_value(rec));
    if (value != nullptr && value->data != nullptr) {
      dst->set_value(std::string(reinterpret_cast<const char*>(value->data), value->len));
    }
    // -1 for Java's Optional.empty(): the optional proto field stays unset.
    const int32_t epoch = kafka_consumer_ConsumerRecord_leader_epoch(rec);
    if (epoch >= 0) dst->set_leader_epoch(epoch);
    // headers(): BORROWED from the record. The RecordHeaders handle is only
    // enumerable through its `Headers` view, whose accessor takes a mutable
    // pointer although nothing below writes, hence the const_cast; the view
    // itself is borrowed and never destroyed. toArray() is an owned list of
    // BORROWED RecordHeader handles (never destroyed on their own).
    const kafka_common_header_internals_RecordHeaders_t* headers =
        kafka_consumer_ConsumerRecord_headers(rec);
    if (headers != nullptr) {
      kafka_common_header_Headers_t* headers_view =
          kafka_common_header_internals_RecordHeaders__as_Headers(
              const_cast<kafka_common_header_internals_RecordHeaders_t*>(headers));
      kafka_List_t* all = kafka_common_header_Headers_to_array(headers_view);
      const int32_t hn = kafka_List_size(all);
      for (int32_t i = 0; i < hn; i++) {
        const auto* record_header =
            static_cast<const kafka_common_header_internals_RecordHeader_t*>(kafka_List_get(all, i));
        const kafka_common_header_Header_t* header =
            kafka_common_header_internals_RecordHeader__as_Header(record_header);
        Header* h = dst->add_headers();
        const char* hk = kafka_common_header_Header_key(header);  // borrowed
        h->set_key(hk ? hk : "");
        // A view over the header's bytes; `data` is null for Java's null value.
        const kafka_Bytes_t hv = kafka_common_header_Header_value(header);
        if (hv.data != nullptr) {
          h->set_value(std::string(reinterpret_cast<const char*>(hv.data), hv.len));
        }
      }
      kafka_List_destroy(all);
    }
  }

  // Shared with ProducerServiceImpl; owned by main().
  GroupMetadataStore* group_metadata_;
  std::mutex mu_;
  std::unordered_map<uint64_t, ConsumerEntry> consumers_;
  // `self` for the rebalance-listener and commit callbacks; owned here, one
  // per consumer, for the whole session (never erased by Close — see LogState).
  std::unordered_map<uint64_t, std::unique_ptr<LogState>> log_states_;
  // Has its own mutex; see CallbackLog.
  CallbackLog callback_log_;
  std::atomic<uint64_t> next_id_{1};
};

// ---------------------------------------------------------------------------
// Admin service.
//
// Drives the sync admin C API (kafka_admin_Admin_*): every RPC returns a
// `*Result_t` immediately, holding one `kafka_common_KafkaFuture_t` per key,
// and the server blocks on those futures the way the Java tests call
// `KafkaFuture.get()`. Values delivered by `kafka_common_KafkaFuture_get` are
// borrowed from the future and die with it, so each handler copies them into
// the proto before the future is destroyed.
// ---------------------------------------------------------------------------

// Deleter that forwards to a C `_destroy` function, tolerating null.
template <auto Destroy>
struct CDeleter {
  template <typename T>
  void operator()(T* p) const {
    if (p != nullptr) Destroy(p);
  }
};

template <typename T, auto Destroy>
using Owned = std::unique_ptr<T, CDeleter<Destroy>>;

// Rust-built containers own their elements: destroying them frees the
// elements too (futures, keys, data classes).
using OwnedFuture = Owned<kafka_common_KafkaFuture_t, kafka_common_KafkaFuture_destroy>;
using OwnedMap = Owned<kafka_Map_t, kafka_Map_destroy>;
using OwnedList = Owned<kafka_List_t, kafka_List_destroy>;
using OwnedUuid = Owned<kafka_common_Uuid_t, kafka_common_Uuid_destroy>;
using OwnedCString = Owned<char, kafka_string_destroy>;
using OwnedError = Owned<kafka_common_Error_t, kafka_common_Error_destroy>;

// Handles built by the server for an input container; freed when the
// container is no longer needed (the FFI copies what it keeps).
template <typename T, auto Destroy>
struct OwnedHandles {
  OwnedHandles() = default;
  ~OwnedHandles() {
    for (T* h : handles) {
      if (h != nullptr) Destroy(h);
    }
  }
  OwnedHandles(const OwnedHandles&) = delete;
  OwnedHandles& operator=(const OwnedHandles&) = delete;

  T* add(T* h) {
    handles.push_back(h);
    return h;
  }
  std::vector<T*> handles;
};

// A C-built `kafka_List_t`: the list owns nothing, elements are borrowed.
struct CList {
  CList() : list(kafka_List_new()) {}
  ~CList() { kafka_List_destroy(list); }
  CList(const CList&) = delete;
  CList& operator=(const CList&) = delete;

  void add(const void* value) { kafka_List_add(list, const_cast<void*>(value)); }
  kafka_List_t* list;
};

// A C-built `kafka_Map_t`: the map owns nothing, keys and values are borrowed.
struct CMap {
  CMap() : map(kafka_Map_new()) {}
  ~CMap() { kafka_Map_destroy(map); }
  CMap(const CMap&) = delete;
  CMap& operator=(const CMap&) = delete;

  void put(const void* key, const void* value) {
    kafka_Map_put(map, const_cast<void*>(key), const_cast<void*>(value));
  }
  kafka_Map_t* map;
};

// Stable storage for boxed scalars passed through `kafka_List_t` /
// `kafka_Map_t` (`int32_t *` broker ids, `int64_t *` producer ids, ...).
// A deque never relocates, so the pointers stay valid while it lives.
struct ScalarArena {
  int32_t* i32(int32_t v) {
    i32s.push_back(v);
    return &i32s.back();
  }
  int64_t* i64(int64_t v) {
    i64s.push_back(v);
    return &i64s.back();
  }
  std::deque<int32_t> i32s;
  std::deque<int64_t> i64s;
};

// Lists of boxed `int32_t *` nested in a `kafka_List_t` / `kafka_Map_t`
// (replica assignments). Each inner list is C-built and borrows its ints.
struct Int32ListArena {
  ~Int32ListArena() {
    for (kafka_List_t* l : lists) kafka_List_destroy(l);
  }
  kafka_List_t* list_of(const ::google::protobuf::RepeatedField<int32_t>& ids) {
    kafka_List_t* l = kafka_List_new();
    for (int32_t id : ids) kafka_List_add(l, scalars.i32(id));
    lists.push_back(l);
    return l;
  }
  ScalarArena scalars;
  std::vector<kafka_List_t*> lists;
};

std::string cstr(const char* s) { return s == nullptr ? std::string() : std::string(s); }

// Copies a borrowed error handle into its proto without destroying it.
void copy_proto_error(KafkaError* dst, const kafka_common_Error_t* err) {
  if (err == nullptr) {
    *dst = make_synthetic_error("null error handle");
    return;
  }
  dst->set_code(kafka_common_Error_code(err));
  dst->set_message(cstr(kafka_common_Error_message(err)));
}

// Awaits a future; on success `*out` is the borrowed value (null for a Void
// future). The returned error is owned by the caller.
template <typename T>
kafka_common_Error_t* future_get(const kafka_common_KafkaFuture_t* future, const T** out) {
  void* value = nullptr;
  kafka_common_Error_t* err = kafka_common_KafkaFuture_get(future, &value);
  *out = static_cast<const T*>(value);
  return err;
}

kafka_common_Error_t* future_await(const kafka_common_KafkaFuture_t* future) {
  void* value = nullptr;
  return kafka_common_KafkaFuture_get(future, &value);
}

// Awaits a Void future and reports its failure on the proto's error slot.
template <typename HasError>
void await_into(const kafka_common_KafkaFuture_t* future, HasError* dst) {
  kafka_common_Error_t* err = future_await(future);
  if (err != nullptr) fill_proto_error(dst->mutable_error(), err);
}

// ---- ResultKey setters -----------------------------------------------------

void set_name_key(ResultKey* key, const char* name) { key->set_name(cstr(name)); }

void set_topic_id_key(ResultKey* key, const kafka_common_Uuid_t* id) {
  OwnedCString s(kafka_common_Uuid_to_string(id));
  key->set_topic_id(cstr(s.get()));
}

void set_partition_key(ResultKey* key, const kafka_common_TopicPartition_t* tp) {
  tp_to_proto(tp, key->mutable_partition());
}

void set_broker_key(ResultKey* key, const int32_t* broker) { key->set_broker_id(*broker); }

void config_resource_to_proto(const kafka_common_config_ConfigResource_t* r,
                              ConfigResource* dst) {
  dst->set_resource_type(kafka_common_config_ConfigResource_Type_id(
      kafka_common_config_ConfigResource_type(r)));
  dst->set_name(cstr(kafka_common_config_ConfigResource_name(r)));
}

void set_config_resource_key(ResultKey* key, const kafka_common_config_ConfigResource_t* r) {
  config_resource_to_proto(r, key->mutable_config_resource());
}

void replica_to_proto(const kafka_common_TopicPartitionReplica_t* r, TopicPartitionReplica* dst) {
  dst->set_topic(cstr(kafka_common_TopicPartitionReplica_topic(r)));
  dst->set_partition(kafka_common_TopicPartitionReplica_partition(r));
  dst->set_broker_id(kafka_common_TopicPartitionReplica_broker_id(r));
}

void set_replica_key(ResultKey* key, const kafka_common_TopicPartitionReplica_t* r) {
  replica_to_proto(r, key->mutable_replica());
}

void acl_binding_to_proto(const kafka_common_acl_AclBinding_t* b, AclBinding* dst) {
  const kafka_common_resource_ResourcePattern_t* pattern = kafka_common_acl_AclBinding_pattern(b);
  const kafka_common_acl_AccessControlEntry_t* entry = kafka_common_acl_AclBinding_entry(b);
  dst->set_resource_type(kafka_common_resource_ResourceType_code(
      kafka_common_resource_ResourcePattern_resource_type(pattern)));
  dst->set_resource_name(cstr(kafka_common_resource_ResourcePattern_name(pattern)));
  dst->set_pattern_type(kafka_common_resource_PatternType_code(
      kafka_common_resource_ResourcePattern_pattern_type(pattern)));
  dst->set_principal(cstr(kafka_common_acl_AccessControlEntry_principal(entry)));
  dst->set_host(cstr(kafka_common_acl_AccessControlEntry_host(entry)));
  dst->set_operation(kafka_common_acl_AclOperation_code(
      kafka_common_acl_AccessControlEntry_operation(entry)));
  dst->set_permission_type(kafka_common_acl_AclPermissionType_code(
      kafka_common_acl_AccessControlEntry_permission_type(entry)));
}

void set_acl_binding_key(ResultKey* key, const kafka_common_acl_AclBinding_t* b) {
  acl_binding_to_proto(b, key->mutable_acl_binding());
}

void acl_filter_to_proto(const kafka_common_acl_AclBindingFilter_t* f, AclBindingFilter* dst) {
  const kafka_common_resource_ResourcePatternFilter_t* pf =
      kafka_common_acl_AclBindingFilter_pattern_filter(f);
  const kafka_common_acl_AccessControlEntryFilter_t* ef =
      kafka_common_acl_AclBindingFilter_entry_filter(f);
  dst->set_resource_type(kafka_common_resource_ResourceType_code(
      kafka_common_resource_ResourcePatternFilter_resource_type(pf)));
  const char* name = kafka_common_resource_ResourcePatternFilter_name(pf);
  if (name != nullptr) dst->set_resource_name(name);
  dst->set_pattern_type(kafka_common_resource_PatternType_code(
      kafka_common_resource_ResourcePatternFilter_pattern_type(pf)));
  const char* principal = kafka_common_acl_AccessControlEntryFilter_principal(ef);
  if (principal != nullptr) dst->set_principal(principal);
  const char* host = kafka_common_acl_AccessControlEntryFilter_host(ef);
  if (host != nullptr) dst->set_host(host);
  dst->set_operation(kafka_common_acl_AclOperation_code(
      kafka_common_acl_AccessControlEntryFilter_operation(ef)));
  dst->set_permission_type(kafka_common_acl_AclPermissionType_code(
      kafka_common_acl_AccessControlEntryFilter_permission_type(ef)));
}

void set_acl_filter_key(ResultKey* key, const kafka_common_acl_AclBindingFilter_t* f) {
  acl_filter_to_proto(f, key->mutable_acl_binding_filter());
}

void quota_entity_to_proto(const kafka_common_quota_ClientQuotaEntity_t* e, ClientQuotaEntity* dst) {
  OwnedMap entries(kafka_common_quota_ClientQuotaEntity_entries(e));
  const int32_t n = kafka_Map_size(entries.get());
  for (int32_t i = 0; i < n; i++) {
    auto* entry = dst->add_entries();
    entry->set_entity_type(cstr(static_cast<const char*>(kafka_Map_key(entries.get(), i))));
    const char* name = static_cast<const char*>(kafka_Map_value(entries.get(), i));
    if (name != nullptr) entry->set_entity_name(name);
  }
}

void set_quota_entity_key(ResultKey* key, const kafka_common_quota_ClientQuotaEntity_t* e) {
  quota_entity_to_proto(e, key->mutable_client_quota_entity());
}

// ---- Keyed-result walkers --------------------------------------------------

// Walks an owned `kafka_Map_t` of key -> `kafka_common_KafkaFuture_t *`: for
// each pair adds an entry, sets its key, awaits the future and either fills
// the entry's error or hands the borrowed value to `fill`.
template <typename K, typename V, typename AddEntry, typename SetKey, typename Fill>
void keyed_futures_to_proto(const kafka_Map_t* map, AddEntry add_entry, SetKey set_key,
                            Fill fill) {
  if (map == nullptr) return;
  const int32_t n = kafka_Map_size(map);
  for (int32_t i = 0; i < n; i++) {
    auto* entry = add_entry();
    set_key(entry->mutable_key(), static_cast<const K*>(kafka_Map_key(map, i)));
    const auto* future = static_cast<const kafka_common_KafkaFuture_t*>(kafka_Map_value(map, i));
    const V* value = nullptr;
    kafka_common_Error_t* err = future_get<V>(future, &value);
    if (err != nullptr) {
      fill_proto_error(entry->mutable_error(), err);
    } else {
      fill(entry, value);
    }
  }
}

// Same for Void futures: each entry carries only its key and optional error.
template <typename K, typename AddEntry, typename SetKey>
void void_futures_to_proto(const kafka_Map_t* map, AddEntry add_entry, SetKey set_key) {
  if (map == nullptr) return;
  const int32_t n = kafka_Map_size(map);
  for (int32_t i = 0; i < n; i++) {
    auto* entry = add_entry();
    set_key(entry->mutable_key(), static_cast<const K*>(kafka_Map_key(map, i)));
    await_into(static_cast<const kafka_common_KafkaFuture_t*>(kafka_Map_value(map, i)), entry);
  }
}

// ---- Enum name mappings (the proto carries Java enum constant names) -------

const char* config_source_name(const kafka_admin_ConfigEntry_ConfigSource_t* s) {
  switch (kafka_admin_ConfigEntry_ConfigSource__enum(s)) {
    case kafka_admin_ConfigEntry_ConfigSource_DEFAULT_CONFIG: return "DEFAULT_CONFIG";
    case kafka_admin_ConfigEntry_ConfigSource_DYNAMIC_BROKER_CONFIG: return "DYNAMIC_BROKER_CONFIG";
    case kafka_admin_ConfigEntry_ConfigSource_DYNAMIC_BROKER_LOGGER_CONFIG:
      return "DYNAMIC_BROKER_LOGGER_CONFIG";
    case kafka_admin_ConfigEntry_ConfigSource_DYNAMIC_CLIENT_METRICS_CONFIG:
      return "DYNAMIC_CLIENT_METRICS_CONFIG";
    case kafka_admin_ConfigEntry_ConfigSource_DYNAMIC_DEFAULT_BROKER_CONFIG:
      return "DYNAMIC_DEFAULT_BROKER_CONFIG";
    case kafka_admin_ConfigEntry_ConfigSource_DYNAMIC_GROUP_CONFIG: return "DYNAMIC_GROUP_CONFIG";
    case kafka_admin_ConfigEntry_ConfigSource_DYNAMIC_TOPIC_CONFIG: return "DYNAMIC_TOPIC_CONFIG";
    case kafka_admin_ConfigEntry_ConfigSource_STATIC_BROKER_CONFIG: return "STATIC_BROKER_CONFIG";
    case kafka_admin_ConfigEntry_ConfigSource_UNKNOWN: return "UNKNOWN";
  }
  return "UNKNOWN";
}

const char* config_type_name(const kafka_admin_ConfigEntry_ConfigType_t* t) {
  switch (kafka_admin_ConfigEntry_ConfigType__enum(t)) {
    case kafka_admin_ConfigEntry_ConfigType_BOOLEAN: return "BOOLEAN";
    case kafka_admin_ConfigEntry_ConfigType_CLASS: return "CLASS";
    case kafka_admin_ConfigEntry_ConfigType_DOUBLE: return "DOUBLE";
    case kafka_admin_ConfigEntry_ConfigType_INT: return "INT";
    case kafka_admin_ConfigEntry_ConfigType_LIST: return "LIST";
    case kafka_admin_ConfigEntry_ConfigType_LONG: return "LONG";
    case kafka_admin_ConfigEntry_ConfigType_PASSWORD: return "PASSWORD";
    case kafka_admin_ConfigEntry_ConfigType_SHORT: return "SHORT";
    case kafka_admin_ConfigEntry_ConfigType_STRING: return "STRING";
    case kafka_admin_ConfigEntry_ConfigType_UNKNOWN: return "UNKNOWN";
  }
  return "UNKNOWN";
}

std::string transaction_state_name(const kafka_admin_TransactionState_t* s) {
  OwnedCString name(kafka_admin_TransactionState_to_string(s));
  return cstr(name.get());
}

// ---- Data-class converters -------------------------------------------------

void config_entry_to_proto(const kafka_admin_ConfigEntry_t* e, ConfigEntry* dst) {
  dst->set_name(cstr(kafka_admin_ConfigEntry_name(e)));
  const char* value = kafka_admin_ConfigEntry_value(e);
  if (value != nullptr) dst->set_value(value);
  dst->set_is_default(kafka_admin_ConfigEntry_is_default(e) != 0);
  dst->set_is_sensitive(kafka_admin_ConfigEntry_is_sensitive(e) != 0);
  dst->set_is_read_only(kafka_admin_ConfigEntry_is_read_only(e) != 0);
  dst->set_source(config_source_name(kafka_admin_ConfigEntry_source(e)));
  dst->set_config_type(config_type_name(kafka_admin_ConfigEntry_type(e)));
  const char* doc = kafka_admin_ConfigEntry_documentation(e);
  if (doc != nullptr) dst->set_documentation(doc);
  OwnedList synonyms(kafka_admin_ConfigEntry_synonyms(e));
  if (synonyms) {
    const int32_t n = kafka_List_size(synonyms.get());
    for (int32_t i = 0; i < n; i++) {
      const auto* s = static_cast<const kafka_admin_ConfigEntry_ConfigSynonym_t*>(
          kafka_List_get(synonyms.get(), i));
      ConfigSynonym* ps = dst->add_synonyms();
      ps->set_name(cstr(kafka_admin_ConfigEntry_ConfigSynonym_name(s)));
      const char* sv = kafka_admin_ConfigEntry_ConfigSynonym_value(s);
      if (sv != nullptr) ps->set_value(sv);
      ps->set_source(config_source_name(kafka_admin_ConfigEntry_ConfigSynonym_source(s)));
    }
  }
}

template <typename AddEntry>
void config_to_proto(const kafka_admin_Config_t* config, AddEntry add_entry) {
  OwnedList entries(kafka_admin_Config_entries(config));
  if (!entries) return;
  const int32_t n = kafka_List_size(entries.get());
  for (int32_t i = 0; i < n; i++) {
    config_entry_to_proto(
        static_cast<const kafka_admin_ConfigEntry_t*>(kafka_List_get(entries.get(), i)),
        add_entry());
  }
}

// Calls `f(element)` for every element of a borrowed list (null = empty).
template <typename T, typename F>
void for_each_in_list(const kafka_List_t* list, F f) {
  if (list == nullptr) return;
  const int32_t n = kafka_List_size(list);
  for (int32_t i = 0; i < n; i++) f(static_cast<const T*>(kafka_List_get(list, i)));
}

// Fills an AclOperationList from a borrowed list of AclOperation singletons.
// Callers decide the proto field stays unset when the list is Java null.
void acl_operations_to_proto(const kafka_List_t* ops, AclOperationList* dst) {
  for_each_in_list<kafka_common_acl_AclOperation_t>(ops, [dst](const kafka_common_acl_AclOperation_t* op) {
    dst->add_operations(kafka_common_acl_AclOperation_code(op));
  });
}

// Copies a borrowed list of TopicPartition handles into the protos `add()` returns.
template <typename Add>
void tp_list_to_proto(const kafka_List_t* tps, Add add) {
  for_each_in_list<kafka_common_TopicPartition_t>(
      tps, [&add](const kafka_common_TopicPartition_t* tp) { tp_to_proto(tp, add()); });
}

void topic_partition_info_to_proto(const kafka_common_TopicPartitionInfo_t* info,
                                   TopicPartitionInfo* dst) {
  dst->set_partition(kafka_common_TopicPartitionInfo_partition(info));
  const kafka_common_Node_t* leader = kafka_common_TopicPartitionInfo_leader(info);
  if (leader != nullptr) node_to_proto(leader, dst->mutable_leader());
  node_list_to_proto(kafka_common_TopicPartitionInfo_replicas(info),
                     [dst] { return dst->add_replicas(); });
  node_list_to_proto(kafka_common_TopicPartitionInfo_isr(info), [dst] { return dst->add_isr(); });
  kafka_List_t* elr = kafka_common_TopicPartitionInfo_elr(info);
  if (elr != nullptr) {
    NodeList* out = dst->mutable_elr();
    node_list_to_proto(elr, [out] { return out->add_nodes(); });
  }
  kafka_List_t* last_known_elr = kafka_common_TopicPartitionInfo_last_known_elr(info);
  if (last_known_elr != nullptr) {
    NodeList* out = dst->mutable_last_known_elr();
    node_list_to_proto(last_known_elr, [out] { return out->add_nodes(); });
  }
}

void topic_description_to_proto(const kafka_admin_TopicDescription_t* d, TopicDescription* dst) {
  dst->set_name(cstr(kafka_admin_TopicDescription_name(d)));
  OwnedUuid id(kafka_admin_TopicDescription_topic_id(d));
  OwnedCString id_str(kafka_common_Uuid_to_string(id.get()));
  dst->set_topic_id(cstr(id_str.get()));
  dst->set_is_internal(kafka_admin_TopicDescription_is_internal(d) != 0);
  OwnedList partitions(kafka_admin_TopicDescription_partitions(d));
  if (partitions) {
    const int32_t n = kafka_List_size(partitions.get());
    for (int32_t i = 0; i < n; i++) {
      topic_partition_info_to_proto(static_cast<const kafka_common_TopicPartitionInfo_t*>(
                                        kafka_List_get(partitions.get(), i)),
                                    dst->add_partitions());
    }
  }
  OwnedList ops(kafka_admin_TopicDescription_authorized_operations(d));
  if (ops) acl_operations_to_proto(ops.get(), dst->mutable_authorized_operations());
}

void member_description_to_proto(const kafka_admin_MemberDescription_t* m, MemberDescription* dst) {
  dst->set_consumer_id(cstr(kafka_admin_MemberDescription_consumer_id(m)));
  const char* instance = kafka_admin_MemberDescription_group_instance_id(m);
  if (instance != nullptr) dst->set_group_instance_id(instance);
  const char* rack = kafka_admin_MemberDescription_rack_id(m);
  if (rack != nullptr) dst->set_rack_id(rack);
  dst->set_client_id(cstr(kafka_admin_MemberDescription_client_id(m)));
  dst->set_host(cstr(kafka_admin_MemberDescription_host(m)));
  const kafka_admin_MemberAssignment_t* assignment = kafka_admin_MemberDescription_assignment(m);
  MemberAssignment* pa = dst->mutable_assignment();
  if (assignment != nullptr) {
    OwnedList tps(kafka_admin_MemberAssignment_topic_partitions(assignment));
    tp_list_to_proto(tps.get(), [pa] { return pa->add_topic_partitions(); });
  }
  const kafka_admin_MemberAssignment_t* target = kafka_admin_MemberDescription_target_assignment(m);
  if (target != nullptr) {
    MemberAssignment* pt = dst->mutable_target_assignment();
    OwnedList tps(kafka_admin_MemberAssignment_topic_partitions(target));
    tp_list_to_proto(tps.get(), [pt] { return pt->add_topic_partitions(); });
  }
  const int32_t epoch = kafka_admin_MemberDescription_member_epoch(m);
  if (epoch >= 0) dst->set_member_epoch(epoch);
  const int8_t upgraded = kafka_admin_MemberDescription_upgraded(m);
  if (upgraded >= 0) dst->set_upgraded(upgraded != 0);
}

template <typename Add>
void member_list_to_proto(kafka_List_t* members, Add add) {
  OwnedList owned(members);
  if (!owned) return;
  const int32_t n = kafka_List_size(owned.get());
  for (int32_t i = 0; i < n; i++) {
    member_description_to_proto(
        static_cast<const kafka_admin_MemberDescription_t*>(kafka_List_get(owned.get(), i)), add());
  }
}

void consumer_group_description_to_proto(const kafka_admin_ConsumerGroupDescription_t* d,
                                         ConsumerGroupDescription* dst) {
  dst->set_group_id(cstr(kafka_admin_ConsumerGroupDescription_group_id(d)));
  dst->set_is_simple_consumer_group(kafka_admin_ConsumerGroupDescription_is_simple_consumer_group(d) != 0);
  member_list_to_proto(kafka_admin_ConsumerGroupDescription_members(d),
                       [dst] { return dst->add_members(); });
  dst->set_partition_assignor(cstr(kafka_admin_ConsumerGroupDescription_partition_assignor(d)));
  dst->set_group_type(cstr(kafka_common_GroupType_name(kafka_admin_ConsumerGroupDescription_type(d))));
  dst->set_group_state(
      cstr(kafka_common_GroupState_name(kafka_admin_ConsumerGroupDescription_group_state(d))));
  const kafka_common_Node_t* coordinator = kafka_admin_ConsumerGroupDescription_coordinator(d);
  if (coordinator != nullptr) node_to_proto(coordinator, dst->mutable_coordinator());
  OwnedList ops(kafka_admin_ConsumerGroupDescription_authorized_operations(d));
  if (ops) acl_operations_to_proto(ops.get(), dst->mutable_authorized_operations());
  const int32_t group_epoch = kafka_admin_ConsumerGroupDescription_group_epoch(d);
  if (group_epoch >= 0) dst->set_group_epoch(group_epoch);
  const int32_t target_epoch = kafka_admin_ConsumerGroupDescription_target_assignment_epoch(d);
  if (target_epoch >= 0) dst->set_target_assignment_epoch(target_epoch);
}

void classic_group_description_to_proto(const kafka_admin_ClassicGroupDescription_t* d,
                                        ClassicGroupDescription* dst) {
  dst->set_group_id(cstr(kafka_admin_ClassicGroupDescription_group_id(d)));
  dst->set_protocol(cstr(kafka_admin_ClassicGroupDescription_protocol(d)));
  dst->set_protocol_data(cstr(kafka_admin_ClassicGroupDescription_protocol_data(d)));
  dst->set_is_simple_consumer_group(kafka_admin_ClassicGroupDescription_is_simple_consumer_group(d) != 0);
  member_list_to_proto(kafka_admin_ClassicGroupDescription_members(d),
                       [dst] { return dst->add_members(); });
  dst->set_state(
      cstr(kafka_common_ClassicGroupState_name(kafka_admin_ClassicGroupDescription_state(d))));
  const kafka_common_Node_t* coordinator = kafka_admin_ClassicGroupDescription_coordinator(d);
  if (coordinator != nullptr) node_to_proto(coordinator, dst->mutable_coordinator());
  OwnedList ops(kafka_admin_ClassicGroupDescription_authorized_operations(d));
  if (ops) acl_operations_to_proto(ops.get(), dst->mutable_authorized_operations());
}

void principal_to_proto(const kafka_common_security_auth_KafkaPrincipal_t* p, KafkaPrincipal* dst) {
  dst->set_principal_type(cstr(kafka_common_security_auth_KafkaPrincipal_principal_type(p)));
  dst->set_name(cstr(kafka_common_security_auth_KafkaPrincipal_name(p)));
  dst->set_token_authenticated(kafka_common_security_auth_KafkaPrincipal_token_authenticated(p) != 0);
}

void delegation_token_to_proto(const kafka_common_security_token_delegation_DelegationToken_t* t,
                               DelegationToken* dst) {
  const kafka_common_security_token_delegation_TokenInformation_t* info =
      kafka_common_security_token_delegation_DelegationToken_token_info(t);
  TokenInformation* pi = dst->mutable_token_information();
  pi->set_token_id(cstr(kafka_common_security_token_delegation_TokenInformation_token_id(info)));
  const kafka_common_security_auth_KafkaPrincipal_t* owner =
      kafka_common_security_token_delegation_TokenInformation_owner(info);
  if (owner != nullptr) principal_to_proto(owner, pi->mutable_owner());
  const kafka_common_security_auth_KafkaPrincipal_t* requester =
      kafka_common_security_token_delegation_TokenInformation_token_requester(info);
  if (requester != nullptr) principal_to_proto(requester, pi->mutable_token_requester());
  OwnedList renewers(kafka_common_security_token_delegation_TokenInformation_renewers(info));
  if (renewers) {
    const int32_t n = kafka_List_size(renewers.get());
    for (int32_t i = 0; i < n; i++) {
      principal_to_proto(static_cast<const kafka_common_security_auth_KafkaPrincipal_t*>(
                             kafka_List_get(renewers.get(), i)),
                         pi->add_renewers());
    }
  }
  pi->set_issue_timestamp(
      kafka_common_security_token_delegation_TokenInformation_issue_timestamp(info));
  pi->set_max_timestamp(kafka_common_security_token_delegation_TokenInformation_max_timestamp(info));
  pi->set_expiry_timestamp(
      kafka_common_security_token_delegation_TokenInformation_expiry_timestamp(info));
  const kafka_Bytes_t hmac = kafka_common_security_token_delegation_DelegationToken_hmac(t);
  dst->set_hmac(hmac.data, hmac.len > 0 ? static_cast<size_t>(hmac.len) : 0);
  OwnedCString b64(kafka_common_security_token_delegation_DelegationToken_hmac_as_base64_string(t));
  dst->set_hmac_as_base64(cstr(b64.get()));
}

// Applies the proto's optional timeout to any `*Options_t` handle; an
// absent field leaves Java's default (the client's request timeout).
template <typename Req, typename Opts, typename SetTimeout>
void apply_timeout(const Req& req, Opts* opts, SetTimeout set_timeout) {
  if (req.has_timeout_ms()) set_timeout(opts, req.timeout_ms());
}

// Java's `Optional<Boolean> retryOnQuotaViolation` defaults to true.
template <typename Req>
bool retry_on_quota(const Req& req) {
  return req.has_retry_on_quota_violation() ? req.retry_on_quota_violation() : true;
}

// Builds a TopicCollection from the request's `oneof topics`: topic ids are
// parsed into owned Uuid handles; names alias the proto strings.
struct TopicCollectionArg {
  template <typename Req>
  explicit TopicCollectionArg(const Req& req) {
    by_ids = req.has_topic_ids();
    if (by_ids) {
      for (const std::string& id : req.topic_ids().values()) {
        kafka_common_Uuid_t* uuid = nullptr;
        kafka_common_Error_t* err = kafka_common_Uuid_from_string(id.c_str(), &uuid);
        if (err != nullptr) {
          error.reset(err);
          return;
        }
        ids.add(uuid);
        list.add(uuid);
      }
      collection.reset(kafka_common_TopicCollection_of_topic_ids(list.list));
    } else {
      for (const std::string& name : req.names().values()) list.add(name.c_str());
      collection.reset(kafka_common_TopicCollection_of_topic_names(list.list));
    }
  }

  bool by_ids = false;
  OwnedError error;
  OwnedHandles<kafka_common_Uuid_t, kafka_common_Uuid_destroy> ids;
  CList list;
  Owned<kafka_common_TopicCollection_t, kafka_common_TopicCollection_destroy> collection;
};

// Either a real `AdminClient` (owned) or a `MockAdminClient` seen through
// its `__as_Admin` view, as the request's config selects.
struct AdminEntry {
  kafka_admin_MockAdminClient_t* mock = nullptr;
  kafka_admin_Admin_t* owned = nullptr;
  const kafka_admin_Admin_t* view = nullptr;

  void destroy() {
    if (mock != nullptr) {
      kafka_admin_MockAdminClient_destroy(mock);
    } else if (owned != nullptr) {
      kafka_admin_Admin_destroy(owned);
    }
    mock = nullptr;
    owned = nullptr;
    view = nullptr;
  }
};

class AdminServiceImpl final : public AdminService::Service {
 public:
  grpc::Status CreateAdmin(grpc::ServerContext*, const CreateAdminRequest* req,
                           CreateAdminResponse* resp) override {
    AdminEntry entry;
    if (selects_mock(req->config())) {
      const int32_t num_brokers = req->has_num_brokers() ? req->num_brokers() : 1;
      kafka_common_Error_t* err = kafka_admin_MockAdminClient_create(num_brokers, &entry.mock);
      if (err != nullptr) {
        fill_proto_error(resp->mutable_error(), err);
        return grpc::Status::OK;
      }
      entry.view = kafka_admin_MockAdminClient__as_Admin(entry.mock);
    } else {
      // The map aliases the proto strings; AdminClientConfig_new copies them.
      CMap props;
      for (const auto& kv : req->config()) props.put(kv.first.c_str(), kv.second.c_str());
      kafka_admin_AdminClientConfig_t* config = nullptr;
      kafka_common_Error_t* err = kafka_admin_AdminClientConfig_new(props.map, &config);
      if (err != nullptr) {
        fill_proto_error(resp->mutable_error(), err);
        return grpc::Status::OK;
      }
      err = kafka_admin_AdminClient_create(config, &entry.owned);
      kafka_admin_AdminClientConfig_destroy(config);
      if (err != nullptr) {
        fill_proto_error(resp->mutable_error(), err);
        return grpc::Status::OK;
      }
      entry.view = entry.owned;
    }
    const uint64_t id = next_id_.fetch_add(1);
    {
      std::lock_guard<std::mutex> lock(mu_);
      admins_[id] = entry;
    }
    resp->set_admin_id(id);
    std::cerr << "c server: created admin " << id << std::endl;
    return grpc::Status::OK;
  }

  grpc::Status CreateTopics(grpc::ServerContext*, const CreateTopicsRequest* req,
                            CreateTopicsResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }

    // Build the NewTopic handles, kept alive until the call returns.
    OwnedHandles<kafka_admin_NewTopic_t, kafka_admin_NewTopic_destroy> topics;
    CList topic_list;
    Int32ListArena assignments;
    std::vector<std::unique_ptr<CMap>> maps;
    for (const auto& spec : req->topics()) {
      kafka_admin_NewTopic_t* topic = nullptr;
      if (spec.replicas_assignments_size() > 0) {
        // Any assignment selects Java's NewTopic(name, Map<Integer, List<Integer>>).
        maps.push_back(std::make_unique<CMap>());
        CMap& replicas = *maps.back();
        for (const auto& assignment : spec.replicas_assignments()) {
          replicas.put(assignments.scalars.i32(assignment.partition()),
                       assignments.list_of(assignment.broker_ids()));
        }
        topic = kafka_admin_NewTopic_with_replicas_assignments(spec.name().c_str(), replicas.map);
      } else {
        // -1 is the wire's "absent" and the C constructor's empty Optional.
        topic = kafka_admin_NewTopic_with_num_partitions_replication_factor(
            spec.name().c_str(), spec.num_partitions(),
            static_cast<int16_t>(spec.replication_factor()));
      }
      if (!spec.configs().empty()) {
        maps.push_back(std::make_unique<CMap>());
        CMap& configs = *maps.back();
        for (const auto& kv : spec.configs()) configs.put(kv.first.c_str(), kv.second.c_str());
        kafka_admin_NewTopic_set_configs(topic, configs.map);
      }
      topic_list.add(topics.add(topic));
    }

    Owned<kafka_admin_CreateTopicsOptions_t, kafka_admin_CreateTopicsOptions_destroy> opts(
        kafka_admin_CreateTopicsOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_CreateTopicsOptions_set_timeout_ms);
    kafka_admin_CreateTopicsOptions_set_validate_only(opts.get(), req->validate_only() ? 1 : 0);
    kafka_admin_CreateTopicsOptions_set_retry_on_quota_violation(opts.get(),
                                                                 retry_on_quota(*req) ? 1 : 0);
    Owned<kafka_admin_CreateTopicsResult_t, kafka_admin_CreateTopicsResult_destroy> result(
        kafka_admin_Admin_create_topics_with_options(admin, topic_list.list, opts.get()));

    OwnedMap values(kafka_admin_CreateTopicsResult_values(result.get()));
    const int32_t n = kafka_Map_size(values.get());
    for (int32_t i = 0; i < n; i++) {
      const char* topic = static_cast<const char*>(kafka_Map_key(values.get(), i));
      CreateTopicsEntry* entry = resp->add_entries();
      set_name_key(entry->mutable_key(), topic);
      kafka_common_Error_t* err = future_await(
          static_cast<const kafka_common_KafkaFuture_t*>(kafka_Map_value(values.get(), i)));
      if (err != nullptr) {
        fill_proto_error(entry->mutable_error(), err);
        continue;
      }
      metadata_to_proto(result.get(), topic, entry->mutable_value());
    }
    return grpc::Status::OK;
  }

  grpc::Status DeleteTopics(grpc::ServerContext*, const DeleteTopicsRequest* req,
                            VoidKeyedResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    TopicCollectionArg topics(*req);
    if (topics.error) {
      fill_proto_error(resp->mutable_error(), topics.error.release());
      return grpc::Status::OK;
    }
    Owned<kafka_admin_DeleteTopicsOptions_t, kafka_admin_DeleteTopicsOptions_destroy> opts(
        kafka_admin_DeleteTopicsOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_DeleteTopicsOptions_set_timeout_ms);
    kafka_admin_DeleteTopicsOptions_set_retry_on_quota_violation(opts.get(),
                                                                 retry_on_quota(*req) ? 1 : 0);
    Owned<kafka_admin_DeleteTopicsResult_t, kafka_admin_DeleteTopicsResult_destroy> result(
        kafka_admin_Admin_delete_topics_with_options(admin, topics.collection.get(), opts.get()));
    if (topics.by_ids) {
      OwnedMap values(kafka_admin_DeleteTopicsResult_topic_id_values(result.get()));
      void_futures_to_proto<kafka_common_Uuid_t>(
          values.get(), [resp] { return resp->add_entries(); }, set_topic_id_key);
    } else {
      OwnedMap values(kafka_admin_DeleteTopicsResult_topic_name_values(result.get()));
      void_futures_to_proto<char>(values.get(), [resp] { return resp->add_entries(); }, set_name_key);
    }
    return grpc::Status::OK;
  }

  grpc::Status ListTopics(grpc::ServerContext*, const AdminListTopicsRequest* req,
                          AdminListTopicsResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    Owned<kafka_admin_ListTopicsOptions_t, kafka_admin_ListTopicsOptions_destroy> opts(
        kafka_admin_ListTopicsOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_ListTopicsOptions_set_timeout_ms);
    kafka_admin_ListTopicsOptions_set_list_internal(opts.get(), req->list_internal() ? 1 : 0);
    Owned<kafka_admin_ListTopicsResult_t, kafka_admin_ListTopicsResult_destroy> result(
        kafka_admin_Admin_list_topics_with_options(admin, opts.get()));

    OwnedFuture listings(kafka_admin_ListTopicsResult_listings(result.get()));
    const kafka_List_t* list = nullptr;
    kafka_common_Error_t* err = future_get(listings.get(), &list);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    for_each_in_list<kafka_admin_TopicListing_t>(list, [resp](const kafka_admin_TopicListing_t* l) {
      AdminTopicListing* dst = resp->add_listings();
      dst->set_name(cstr(kafka_admin_TopicListing_name(l)));
      OwnedUuid id(kafka_admin_TopicListing_topic_id(l));
      OwnedCString id_str(kafka_common_Uuid_to_string(id.get()));
      dst->set_topic_id(cstr(id_str.get()));
      dst->set_is_internal(kafka_admin_TopicListing_is_internal(l) != 0);
    });
    return grpc::Status::OK;
  }

  grpc::Status DescribeTopics(grpc::ServerContext*, const DescribeTopicsRequest* req,
                              DescribeTopicsResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    TopicCollectionArg topics(*req);
    if (topics.error) {
      fill_proto_error(resp->mutable_error(), topics.error.release());
      return grpc::Status::OK;
    }
    Owned<kafka_admin_DescribeTopicsOptions_t, kafka_admin_DescribeTopicsOptions_destroy> opts(
        kafka_admin_DescribeTopicsOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_DescribeTopicsOptions_set_timeout_ms);
    kafka_admin_DescribeTopicsOptions_set_include_authorized_operations(
        opts.get(), req->include_authorized_operations() ? 1 : 0);
    // An absent limit leaves Java's default (2000) in place.
    if (req->has_partition_size_limit_per_response()) {
      kafka_admin_DescribeTopicsOptions_set_partition_size_limit_per_response(
          opts.get(), req->partition_size_limit_per_response());
    }
    Owned<kafka_admin_DescribeTopicsResult_t, kafka_admin_DescribeTopicsResult_destroy> result(
        kafka_admin_Admin_describe_topics_with_topics_options(admin, topics.collection.get(),
                                                              opts.get()));
    auto fill = [](DescribeTopicsEntry* entry, const kafka_admin_TopicDescription_t* d) {
      topic_description_to_proto(d, entry->mutable_value());
    };
    if (topics.by_ids) {
      OwnedMap values(kafka_admin_DescribeTopicsResult_topic_id_values(result.get()));
      keyed_futures_to_proto<kafka_common_Uuid_t, kafka_admin_TopicDescription_t>(
          values.get(), [resp] { return resp->add_entries(); }, set_topic_id_key, fill);
    } else {
      OwnedMap values(kafka_admin_DescribeTopicsResult_topic_name_values(result.get()));
      keyed_futures_to_proto<char, kafka_admin_TopicDescription_t>(
          values.get(), [resp] { return resp->add_entries(); }, set_name_key, fill);
    }
    return grpc::Status::OK;
  }

  grpc::Status CreatePartitions(grpc::ServerContext*, const CreatePartitionsRequest* req,
                                VoidKeyedResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    OwnedHandles<kafka_admin_NewPartitions_t, kafka_admin_NewPartitions_destroy> handles;
    Int32ListArena assignments;
    std::vector<std::unique_ptr<CList>> outer_lists;
    CMap new_partitions;
    for (const auto& spec : req->partitions()) {
      kafka_admin_NewPartitions_t* np = nullptr;
      if (spec.has_new_assignments()) {
        outer_lists.push_back(std::make_unique<CList>());
        CList& outer = *outer_lists.back();
        for (const auto& brokers : spec.new_assignments().assignments()) {
          outer.add(assignments.list_of(brokers.broker_ids()));
        }
        np = kafka_admin_NewPartitions_increase_to_with_new_assignments(spec.total_count(),
                                                                        outer.list);
      } else {
        np = kafka_admin_NewPartitions_increase_to(spec.total_count());
      }
      new_partitions.put(spec.topic().c_str(), handles.add(np));
    }
    Owned<kafka_admin_CreatePartitionsOptions_t, kafka_admin_CreatePartitionsOptions_destroy> opts(
        kafka_admin_CreatePartitionsOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_CreatePartitionsOptions_set_timeout_ms);
    kafka_admin_CreatePartitionsOptions_set_validate_only(opts.get(), req->validate_only() ? 1 : 0);
    kafka_admin_CreatePartitionsOptions_set_retry_on_quota_violation(
        opts.get(), retry_on_quota(*req) ? 1 : 0);
    Owned<kafka_admin_CreatePartitionsResult_t, kafka_admin_CreatePartitionsResult_destroy> result(
        kafka_admin_Admin_create_partitions_with_options(admin, new_partitions.map, opts.get()));
    OwnedMap values(kafka_admin_CreatePartitionsResult_values(result.get()));
    void_futures_to_proto<char>(values.get(), [resp] { return resp->add_entries(); }, set_name_key);
    return grpc::Status::OK;
  }

  grpc::Status DeleteRecords(grpc::ServerContext*, const DeleteRecordsRequest* req,
                             DeleteRecordsResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    OwnedHandles<kafka_common_TopicPartition_t, kafka_common_TopicPartition_destroy> tps;
    OwnedHandles<kafka_admin_RecordsToDelete_t, kafka_admin_RecordsToDelete_destroy> records;
    CMap records_to_delete;
    for (const auto& spec : req->records()) {
      records_to_delete.put(
          tps.add(kafka_common_TopicPartition_new(spec.partition().topic().c_str(),
                                                  spec.partition().partition())),
          records.add(kafka_admin_RecordsToDelete_with_offset(spec.before_offset())));
    }
    Owned<kafka_admin_DeleteRecordsOptions_t, kafka_admin_DeleteRecordsOptions_destroy> opts(
        kafka_admin_DeleteRecordsOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_DeleteRecordsOptions_set_timeout_ms);
    Owned<kafka_admin_DeleteRecordsResult_t, kafka_admin_DeleteRecordsResult_destroy> result(
        kafka_admin_Admin_delete_records_with_options(admin, records_to_delete.map, opts.get()));
    OwnedMap values(kafka_admin_DeleteRecordsResult_low_watermarks(result.get()));
    keyed_futures_to_proto<kafka_common_TopicPartition_t, kafka_admin_DeletedRecords_t>(
        values.get(), [resp] { return resp->add_entries(); }, set_partition_key,
        [](DeleteRecordsEntry* entry, const kafka_admin_DeletedRecords_t* d) {
          entry->mutable_value()->set_low_watermark(kafka_admin_DeletedRecords_low_watermark(d));
        });
    return grpc::Status::OK;
  }

  grpc::Status DescribeCluster(grpc::ServerContext*, const DescribeClusterRequest* req,
                               DescribeClusterResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    Owned<kafka_admin_DescribeClusterOptions_t, kafka_admin_DescribeClusterOptions_destroy> opts(
        kafka_admin_DescribeClusterOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_DescribeClusterOptions_set_timeout_ms);
    kafka_admin_DescribeClusterOptions_set_include_authorized_operations(
        opts.get(), req->include_authorized_operations() ? 1 : 0);
    kafka_admin_DescribeClusterOptions_set_include_fenced_brokers(
        opts.get(), req->include_fenced_brokers() ? 1 : 0);
    Owned<kafka_admin_DescribeClusterResult_t, kafka_admin_DescribeClusterResult_destroy> result(
        kafka_admin_Admin_describe_cluster_with_options(admin, opts.get()));

    ClusterDescription* dst = resp->mutable_description();
    {
      OwnedFuture f(kafka_admin_DescribeClusterResult_cluster_id(result.get()));
      const char* cluster_id = nullptr;
      kafka_common_Error_t* err = future_get(f.get(), &cluster_id);
      if (err != nullptr) {
        resp->clear_description();
        fill_proto_error(resp->mutable_error(), err);
        return grpc::Status::OK;
      }
      dst->set_cluster_id(cstr(cluster_id));
    }
    {
      OwnedFuture f(kafka_admin_DescribeClusterResult_nodes(result.get()));
      const kafka_List_t* nodes = nullptr;
      kafka_common_Error_t* err = future_get(f.get(), &nodes);
      if (err != nullptr) {
        resp->clear_description();
        fill_proto_error(resp->mutable_error(), err);
        return grpc::Status::OK;
      }
      for_each_in_list<kafka_common_Node_t>(
          nodes, [dst](const kafka_common_Node_t* n) { node_to_proto(n, dst->add_nodes()); });
    }
    {
      // Java's controller() is nullable: a null handle means no current
      // controller, and must not become a fabricated Node.
      OwnedFuture f(kafka_admin_DescribeClusterResult_controller(result.get()));
      const kafka_common_Node_t* controller = nullptr;
      kafka_common_Error_t* err = future_get(f.get(), &controller);
      if (err != nullptr) {
        resp->clear_description();
        fill_proto_error(resp->mutable_error(), err);
        return grpc::Status::OK;
      }
      if (controller != nullptr) node_to_proto(controller, dst->mutable_controller());
    }
    {
      // A null list is Java's null: the operations were not requested, which
      // is not the same as none being authorized.
      OwnedFuture f(kafka_admin_DescribeClusterResult_authorized_operations(result.get()));
      const kafka_List_t* ops = nullptr;
      kafka_common_Error_t* err = future_get(f.get(), &ops);
      if (err != nullptr) {
        resp->clear_description();
        fill_proto_error(resp->mutable_error(), err);
        return grpc::Status::OK;
      }
      if (ops != nullptr) acl_operations_to_proto(ops, dst->mutable_authorized_operations());
    }
    return grpc::Status::OK;
  }

  grpc::Status DescribeConfigs(grpc::ServerContext*, const DescribeConfigsRequest* req,
                               DescribeConfigsResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    OwnedHandles<kafka_common_config_ConfigResource_t, kafka_common_config_ConfigResource_destroy>
        resources;
    CList resource_list;
    for (const auto& r : req->resources()) {
      resource_list.add(resources.add(kafka_common_config_ConfigResource_new(
          kafka_common_config_ConfigResource_Type_for_id(static_cast<int8_t>(r.resource_type())),
          r.name().c_str())));
    }
    Owned<kafka_admin_DescribeConfigsOptions_t, kafka_admin_DescribeConfigsOptions_destroy> opts(
        kafka_admin_DescribeConfigsOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_DescribeConfigsOptions_set_timeout_ms);
    kafka_admin_DescribeConfigsOptions_set_include_synonyms(opts.get(),
                                                            req->include_synonyms() ? 1 : 0);
    kafka_admin_DescribeConfigsOptions_set_include_documentation(
        opts.get(), req->include_documentation() ? 1 : 0);
    Owned<kafka_admin_DescribeConfigsResult_t, kafka_admin_DescribeConfigsResult_destroy> result(
        kafka_admin_Admin_describe_configs_with_options(admin, resource_list.list, opts.get()));
    OwnedMap values(kafka_admin_DescribeConfigsResult_values(result.get()));
    keyed_futures_to_proto<kafka_common_config_ConfigResource_t, kafka_admin_Config_t>(
        values.get(), [resp] { return resp->add_entries(); }, set_config_resource_key,
        [](DescribeConfigsEntry* entry, const kafka_admin_Config_t* config) {
          AdminConfig* dst = entry->mutable_value();
          config_to_proto(config, [dst] { return dst->add_entries(); });
        });
    return grpc::Status::OK;
  }

  grpc::Status IncrementalAlterConfigs(grpc::ServerContext*,
                                       const IncrementalAlterConfigsRequest* req,
                                       VoidKeyedResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    OwnedHandles<kafka_common_config_ConfigResource_t, kafka_common_config_ConfigResource_destroy>
        resources;
    OwnedHandles<kafka_admin_ConfigEntry_t, kafka_admin_ConfigEntry_destroy> entries;
    OwnedHandles<kafka_admin_AlterConfigOp_t, kafka_admin_AlterConfigOp_destroy> ops;
    std::vector<std::unique_ptr<CList>> op_lists;
    CMap configs;
    for (const auto& spec : req->configs()) {
      op_lists.push_back(std::make_unique<CList>());
      CList& op_list = *op_lists.back();
      for (const auto& op : spec.ops()) {
        const kafka_admin_AlterConfigOp_OpType_t* type =
            kafka_admin_AlterConfigOp_OpType_for_id(static_cast<int8_t>(op.op_type()));
        if (type == nullptr) {
          *resp->mutable_error() =
              make_synthetic_error("unknown AlterConfigOp.OpType id " + std::to_string(op.op_type()),
                                   kafka_common_ErrorCode_LOCAL_ILLEGAL_ARGUMENT);
          return grpc::Status::OK;
        }
        kafka_admin_ConfigEntry_t* entry = entries.add(
            kafka_admin_ConfigEntry_new(op.name().c_str(), op.has_value() ? op.value().c_str() : nullptr));
        op_list.add(ops.add(kafka_admin_AlterConfigOp_new(entry, type)));
      }
      configs.put(resources.add(kafka_common_config_ConfigResource_new(
                      kafka_common_config_ConfigResource_Type_for_id(
                          static_cast<int8_t>(spec.resource().resource_type())),
                      spec.resource().name().c_str())),
                  op_list.list);
    }
    Owned<kafka_admin_AlterConfigsOptions_t, kafka_admin_AlterConfigsOptions_destroy> opts(
        kafka_admin_AlterConfigsOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_AlterConfigsOptions_set_timeout_ms);
    kafka_admin_AlterConfigsOptions_set_validate_only(opts.get(), req->validate_only() ? 1 : 0);
    Owned<kafka_admin_AlterConfigsResult_t, kafka_admin_AlterConfigsResult_destroy> result(
        kafka_admin_Admin_incremental_alter_configs_with_options(admin, configs.map, opts.get()));
    OwnedMap values(kafka_admin_AlterConfigsResult_values(result.get()));
    void_futures_to_proto<kafka_common_config_ConfigResource_t>(
        values.get(), [resp] { return resp->add_entries(); }, set_config_resource_key);
    return grpc::Status::OK;
  }

  grpc::Status ListConfigResources(grpc::ServerContext*, const ListConfigResourcesRequest* req,
                                   ListConfigResourcesResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    CList types;  // borrowed Type singletons
    for (int32_t t : req->resource_types()) {
      types.add(kafka_common_config_ConfigResource_Type_for_id(static_cast<int8_t>(t)));
    }
    Owned<kafka_admin_ListConfigResourcesOptions_t, kafka_admin_ListConfigResourcesOptions_destroy>
        opts(kafka_admin_ListConfigResourcesOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_ListConfigResourcesOptions_set_timeout_ms);
    Owned<kafka_admin_ListConfigResourcesResult_t, kafka_admin_ListConfigResourcesResult_destroy>
        result(kafka_admin_Admin_list_config_resources_with_options(admin, types.list, opts.get()));
    OwnedFuture all(kafka_admin_ListConfigResourcesResult_all(result.get()));
    const kafka_List_t* list = nullptr;
    kafka_common_Error_t* err = future_get(all.get(), &list);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    for_each_in_list<kafka_common_config_ConfigResource_t>(
        list, [resp](const kafka_common_config_ConfigResource_t* r) {
          config_resource_to_proto(r, resp->add_resources());
        });
    return grpc::Status::OK;
  }

  grpc::Status DescribeLogDirs(grpc::ServerContext*, const DescribeLogDirsRequest* req,
                               DescribeLogDirsResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    ScalarArena scalars;
    CList brokers;
    for (int32_t b : req->brokers()) brokers.add(scalars.i32(b));
    Owned<kafka_admin_DescribeLogDirsOptions_t, kafka_admin_DescribeLogDirsOptions_destroy> opts(
        kafka_admin_DescribeLogDirsOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_DescribeLogDirsOptions_set_timeout_ms);
    Owned<kafka_admin_DescribeLogDirsResult_t, kafka_admin_DescribeLogDirsResult_destroy> result(
        kafka_admin_Admin_describe_log_dirs_with_options(admin, brokers.list, opts.get()));
    // Broker id -> future of Map<String, LogDirDescription>.
    OwnedMap descriptions(kafka_admin_DescribeLogDirsResult_descriptions(result.get()));
    keyed_futures_to_proto<int32_t, kafka_Map_t>(
        descriptions.get(), [resp] { return resp->add_entries(); }, set_broker_key,
        [](DescribeLogDirsEntry* entry, const kafka_Map_t* dirs) {
          // The nested level: one description per log-dir path, borrowed
          // from the future together with the map.
          LogDirDescriptionMap* dst = entry->mutable_value();
          const int32_t n = kafka_Map_size(dirs);
          for (int32_t d = 0; d < n; d++) {
            const char* path = static_cast<const char*>(kafka_Map_key(dirs, d));
            log_dir_description_to_proto(
                static_cast<const kafka_admin_LogDirDescription_t*>(kafka_Map_value(dirs, d)),
                &(*dst->mutable_log_dirs())[cstr(path)]);
          }
        });
    return grpc::Status::OK;
  }

  grpc::Status AlterReplicaLogDirs(grpc::ServerContext*, const AlterReplicaLogDirsRequest* req,
                                   VoidKeyedResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    OwnedHandles<kafka_common_TopicPartitionReplica_t, kafka_common_TopicPartitionReplica_destroy>
        replicas;
    CMap assignment;  // replica -> borrowed log dir string
    for (const auto& a : req->assignments()) {
      assignment.put(replicas.add(kafka_common_TopicPartitionReplica_new(
                         a.replica().topic().c_str(), a.replica().partition(),
                         a.replica().broker_id())),
                     a.log_dir().c_str());
    }
    Owned<kafka_admin_AlterReplicaLogDirsOptions_t, kafka_admin_AlterReplicaLogDirsOptions_destroy>
        opts(kafka_admin_AlterReplicaLogDirsOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_AlterReplicaLogDirsOptions_set_timeout_ms);
    Owned<kafka_admin_AlterReplicaLogDirsResult_t, kafka_admin_AlterReplicaLogDirsResult_destroy>
        result(kafka_admin_Admin_alter_replica_log_dirs_with_options(admin, assignment.map,
                                                                     opts.get()));
    OwnedMap values(kafka_admin_AlterReplicaLogDirsResult_values(result.get()));
    void_futures_to_proto<kafka_common_TopicPartitionReplica_t>(
        values.get(), [resp] { return resp->add_entries(); }, set_replica_key);
    return grpc::Status::OK;
  }

  grpc::Status DescribeReplicaLogDirs(grpc::ServerContext*,
                                      const DescribeReplicaLogDirsRequest* req,
                                      DescribeReplicaLogDirsResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    OwnedHandles<kafka_common_TopicPartitionReplica_t, kafka_common_TopicPartitionReplica_destroy>
        replicas;
    CList replica_list;
    for (const auto& r : req->replicas()) {
      replica_list.add(replicas.add(
          kafka_common_TopicPartitionReplica_new(r.topic().c_str(), r.partition(), r.broker_id())));
    }
    Owned<kafka_admin_DescribeReplicaLogDirsOptions_t,
          kafka_admin_DescribeReplicaLogDirsOptions_destroy>
        opts(kafka_admin_DescribeReplicaLogDirsOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_DescribeReplicaLogDirsOptions_set_timeout_ms);
    Owned<kafka_admin_DescribeReplicaLogDirsResult_t,
          kafka_admin_DescribeReplicaLogDirsResult_destroy>
        result(kafka_admin_Admin_describe_replica_log_dirs_with_options(admin, replica_list.list,
                                                                        opts.get()));
    OwnedMap values(kafka_admin_DescribeReplicaLogDirsResult_values(result.get()));
    keyed_futures_to_proto<kafka_common_TopicPartitionReplica_t,
                           kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t>(
        values.get(), [resp] { return resp->add_entries(); }, set_replica_key,
        [](DescribeReplicaLogDirsEntry* entry,
           const kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t* info) {
          ReplicaLogDirInfo* dst = entry->mutable_value();
          // Java's current/futureReplicaLogDir are nullable strings.
          const char* current =
              kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_current_replica_log_dir(info);
          if (current != nullptr) dst->set_current_replica_log_dir(current);
          dst->set_current_replica_offset_lag(
              kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_current_replica_offset_lag(
                  info));
          const char* future =
              kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_future_replica_log_dir(info);
          if (future != nullptr) dst->set_future_replica_log_dir(future);
          dst->set_future_replica_offset_lag(
              kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_future_replica_offset_lag(
                  info));
        });
    return grpc::Status::OK;
  }

  grpc::Status ElectLeaders(grpc::ServerContext*, const ElectLeadersRequest* req,
                            VoidKeyedResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    const kafka_common_ElectionType_t* election_type = nullptr;
    kafka_common_Error_t* err = kafka_common_ElectionType_value_of(
        static_cast<int8_t>(req->election_type()), &election_type);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    // An absent partition set is Java's **null** Set: elect a leader for every
    // partition in the cluster. It crosses as a NULL list, which can never be
    // confused with the present-but-empty case (an empty selection). Testing
    // `partitions().partitions_size() == 0` instead would collapse the two.
    std::unique_ptr<TpList> partitions;
    if (req->has_partitions()) partitions = std::make_unique<TpList>(req->partitions().partitions());
    Owned<kafka_admin_ElectLeadersOptions_t, kafka_admin_ElectLeadersOptions_destroy> opts(
        kafka_admin_ElectLeadersOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_ElectLeadersOptions_set_timeout_ms);
    Owned<kafka_admin_ElectLeadersResult_t, kafka_admin_ElectLeadersResult_destroy> result(
        kafka_admin_Admin_elect_leaders_with_options(
            admin, election_type, partitions ? partitions->list : nullptr, opts.get()));

    OwnedFuture f(kafka_admin_ElectLeadersResult_partitions(result.get()));
    const kafka_Map_t* outcomes = nullptr;
    err = future_get(f.get(), &outcomes);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    const int32_t n = outcomes == nullptr ? 0 : kafka_Map_size(outcomes);
    for (int32_t i = 0; i < n; i++) {
      VoidResultEntry* entry = resp->add_entries();
      set_partition_key(entry->mutable_key(),
                        static_cast<const kafka_common_TopicPartition_t*>(kafka_Map_key(outcomes, i)));
      // Java's Optional<Throwable> per partition: a null handle means the
      // election succeeded for that partition, which is the absent error.
      const auto* key_err = static_cast<const kafka_common_Error_t*>(kafka_Map_value(outcomes, i));
      if (key_err != nullptr) copy_proto_error(entry->mutable_error(), key_err);
    }
    return grpc::Status::OK;
  }

  grpc::Status AlterPartitionReassignments(grpc::ServerContext*,
                                           const AlterPartitionReassignmentsRequest* req,
                                           VoidKeyedResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    OwnedHandles<kafka_common_TopicPartition_t, kafka_common_TopicPartition_destroy> tps;
    OwnedHandles<kafka_admin_NewPartitionReassignment_t, kafka_admin_NewPartitionReassignment_destroy>
        reassignments;
    Int32ListArena replicas;
    CMap map;  // partition -> NewPartitionReassignment, or NULL to cancel
    for (const auto& spec : req->reassignments()) {
      kafka_admin_NewPartitionReassignment_t* reassignment = nullptr;
      // An absent reassignment is Java's Optional.empty(): cancel the ongoing one.
      if (spec.has_reassignment()) {
        kafka_common_Error_t* err = kafka_admin_NewPartitionReassignment_new(
            replicas.list_of(spec.reassignment().target_replicas()), &reassignment);
        if (err != nullptr) {
          fill_proto_error(resp->mutable_error(), err);
          return grpc::Status::OK;
        }
        reassignments.add(reassignment);
      }
      map.put(tps.add(kafka_common_TopicPartition_new(spec.partition().topic().c_str(),
                                                      spec.partition().partition())),
              reassignment);
    }
    Owned<kafka_admin_AlterPartitionReassignmentsOptions_t,
          kafka_admin_AlterPartitionReassignmentsOptions_destroy>
        opts(kafka_admin_AlterPartitionReassignmentsOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_AlterPartitionReassignmentsOptions_set_timeout_ms);
    if (req->has_allow_replication_factor_change()) {
      kafka_admin_AlterPartitionReassignmentsOptions_set_allow_replication_factor_change(
          opts.get(), req->allow_replication_factor_change() ? 1 : 0);
    }
    Owned<kafka_admin_AlterPartitionReassignmentsResult_t,
          kafka_admin_AlterPartitionReassignmentsResult_destroy>
        result(kafka_admin_Admin_alter_partition_reassignments_with_options(admin, map.map,
                                                                            opts.get()));
    OwnedMap values(kafka_admin_AlterPartitionReassignmentsResult_values(result.get()));
    void_futures_to_proto<kafka_common_TopicPartition_t>(
        values.get(), [resp] { return resp->add_entries(); }, set_partition_key);
    return grpc::Status::OK;
  }

  grpc::Status ListPartitionReassignments(grpc::ServerContext*,
                                          const ListPartitionReassignmentsRequest* req,
                                          ListPartitionReassignmentsResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    Owned<kafka_admin_ListPartitionReassignmentsOptions_t,
          kafka_admin_ListPartitionReassignmentsOptions_destroy>
        opts(kafka_admin_ListPartitionReassignmentsOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_ListPartitionReassignmentsOptions_set_timeout_ms);
    // An absent set is Java's zero-arg overload (every reassignment); a
    // present one, even empty, is the Set<TopicPartition> overload.
    Owned<kafka_admin_ListPartitionReassignmentsResult_t,
          kafka_admin_ListPartitionReassignmentsResult_destroy>
        result;
    if (req->has_partitions()) {
      TpList partitions(req->partitions().partitions());
      result.reset(kafka_admin_Admin_list_partition_reassignments_with_partitions_options(
          admin, partitions.list, opts.get()));
    } else {
      result.reset(kafka_admin_Admin_list_partition_reassignments_with_options(admin, opts.get()));
    }
    OwnedFuture f(kafka_admin_ListPartitionReassignmentsResult_reassignments(result.get()));
    const kafka_Map_t* reassignments = nullptr;
    kafka_common_Error_t* err = future_get(f.get(), &reassignments);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    const int32_t n = reassignments == nullptr ? 0 : kafka_Map_size(reassignments);
    for (int32_t i = 0; i < n; i++) {
      OngoingPartitionReassignment* dst = resp->add_reassignments();
      tp_to_proto(static_cast<const kafka_common_TopicPartition_t*>(kafka_Map_key(reassignments, i)),
                  dst->mutable_partition());
      const auto* r =
          static_cast<const kafka_admin_PartitionReassignment_t*>(kafka_Map_value(reassignments, i));
      PartitionReassignment* pr = dst->mutable_reassignment();
      int32_list_to_proto(kafka_admin_PartitionReassignment_replicas(r),
                          [pr](int32_t id) { pr->add_replicas(id); });
      int32_list_to_proto(kafka_admin_PartitionReassignment_adding_replicas(r),
                          [pr](int32_t id) { pr->add_adding_replicas(id); });
      int32_list_to_proto(kafka_admin_PartitionReassignment_removing_replicas(r),
                          [pr](int32_t id) { pr->add_removing_replicas(id); });
    }
    return grpc::Status::OK;
  }

  grpc::Status ListOffsets(grpc::ServerContext*, const ListOffsetsRequest* req,
                           ListOffsetsResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    OwnedHandles<kafka_common_TopicPartition_t, kafka_common_TopicPartition_destroy> tps;
    OwnedHandles<kafka_admin_OffsetSpec_t, kafka_admin_OffsetSpec_destroy> timestamp_specs;
    CMap specs;  // partition -> borrowed OffsetSpec (singleton or owned above)
    for (const auto& spec : req->specs()) {
      const kafka_admin_OffsetSpec_t* offset_spec = offset_spec_for(spec.spec(), &timestamp_specs);
      if (offset_spec == nullptr) {
        // A KIND_UNSPECIFIED or an unknown kind, or FOR_TIMESTAMP with no
        // timestamp: a protocol error, never a defaulted variant. Whole-call,
        // with the LOCAL_ILLEGAL_ARGUMENT code, which is what the Python
        // servers' AdminRequestError maps to for the same condition.
        *resp->mutable_error() = make_synthetic_error(
            "OffsetSpec for " + spec.partition().topic() + "-" +
                std::to_string(spec.partition().partition()) + " has no usable kind (" +
                std::to_string(spec.spec().kind()) + ")",
            kafka_common_ErrorCode_LOCAL_ILLEGAL_ARGUMENT);
        return grpc::Status::OK;
      }
      specs.put(tps.add(kafka_common_TopicPartition_new(spec.partition().topic().c_str(),
                                                        spec.partition().partition())),
                offset_spec);
    }
    const kafka_common_IsolationLevel_t* isolation_level = nullptr;
    kafka_common_Error_t* err = kafka_common_IsolationLevel_for_id(
        static_cast<int8_t>(req->isolation_level()), &isolation_level);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    Owned<kafka_admin_ListOffsetsOptions_t, kafka_admin_ListOffsetsOptions_destroy> opts(
        kafka_admin_ListOffsetsOptions_with_isolation_level(isolation_level));
    apply_timeout(*req, opts.get(), kafka_admin_ListOffsetsOptions_set_timeout_ms);
    Owned<kafka_admin_ListOffsetsResult_t, kafka_admin_ListOffsetsResult_destroy> result(
        kafka_admin_Admin_list_offsets_with_options(admin, specs.map, opts.get()));

    // One entry per requested partition, through the per-key future so a
    // partition's own failure stays on its entry.
    for (kafka_common_TopicPartition_t* tp : tps.handles) {
      ListOffsetsEntry* entry = resp->add_entries();
      set_partition_key(entry->mutable_key(), tp);
      kafka_common_KafkaFuture_t* raw = nullptr;
      err = kafka_admin_ListOffsetsResult_partition_result(result.get(), tp, &raw);
      if (err != nullptr) {
        fill_proto_error(entry->mutable_error(), err);
        continue;
      }
      OwnedFuture f(raw);
      const kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t* info = nullptr;
      err = future_get(f.get(), &info);
      if (err != nullptr) {
        fill_proto_error(entry->mutable_error(), err);
        continue;
      }
      ListOffsetsResultInfo* dst = entry->mutable_value();
      dst->set_offset(kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_offset(info));
      dst->set_timestamp(kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_timestamp(info));
      // Java's Optional<Integer> leaderEpoch: -1 is empty.
      const int32_t epoch = kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_leader_epoch(info);
      if (epoch >= 0) dst->set_leader_epoch(epoch);
    }
    return grpc::Status::OK;
  }

  grpc::Status ListGroups(grpc::ServerContext*, const ListGroupsRequest* req,
                          ListGroupsResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    Owned<kafka_admin_ListGroupsOptions_t, kafka_admin_ListGroupsOptions_destroy> opts(
        kafka_admin_ListGroupsOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_ListGroupsOptions_set_timeout_ms);
    // The three filters cross as the Java enums' names; an empty list is
    // Java's empty set, i.e. the filter left unset.
    CList states;  // borrowed GroupState singletons
    for (const std::string& name : req->group_states()) {
      states.add(kafka_common_GroupState_parse(name.c_str()));
    }
    if (req->group_states_size() > 0) kafka_admin_ListGroupsOptions_in_group_states(opts.get(), states.list);
    BorrowedStringList protocols(req->protocol_types());
    if (req->protocol_types_size() > 0) {
      kafka_admin_ListGroupsOptions_with_protocol_types(opts.get(), protocols.list);
    }
    CList types;  // borrowed GroupType singletons
    for (const std::string& name : req->types()) types.add(kafka_common_GroupType_parse(name.c_str()));
    if (req->types_size() > 0) kafka_admin_ListGroupsOptions_with_types(opts.get(), types.list);

    Owned<kafka_admin_ListGroupsResult_t, kafka_admin_ListGroupsResult_destroy> result(
        kafka_admin_Admin_list_groups_with_options(admin, opts.get()));
    {
      OwnedFuture f(kafka_admin_ListGroupsResult_valid(result.get()));
      const kafka_List_t* valid = nullptr;
      kafka_common_Error_t* err = future_get(f.get(), &valid);
      if (err != nullptr) {
        fill_proto_error(resp->mutable_error(), err);
        return grpc::Status::OK;
      }
      for_each_in_list<kafka_admin_GroupListing_t>(valid, [resp](const kafka_admin_GroupListing_t* l) {
        GroupListing* dst = resp->add_valid();
        dst->set_group_id(cstr(kafka_admin_GroupListing_group_id(l)));
        dst->set_protocol(cstr(kafka_admin_GroupListing_protocol(l)));
        dst->set_is_simple_consumer_group(kafka_admin_GroupListing_is_simple_consumer_group(l) != 0);
        // type / groupState are Java Optionals: NULL is an empty one and
        // must stay absent rather than becoming "".
        const kafka_common_GroupType_t* type = kafka_admin_GroupListing_type(l);
        if (type != nullptr) dst->set_group_type(cstr(kafka_common_GroupType_name(type)));
        const kafka_common_GroupState_t* state = kafka_admin_GroupListing_group_state(l);
        if (state != nullptr) dst->set_group_state(cstr(kafka_common_GroupState_name(state)));
      });
    }
    {
      OwnedFuture f(kafka_admin_ListGroupsResult_errors(result.get()));
      const kafka_List_t* errors = nullptr;
      kafka_common_Error_t* err = future_get(f.get(), &errors);
      if (err != nullptr) {
        resp->clear_valid();
        fill_proto_error(resp->mutable_error(), err);
        return grpc::Status::OK;
      }
      for_each_in_list<kafka_common_Error_t>(errors, [resp](const kafka_common_Error_t* e) {
        copy_proto_error(resp->add_listing_errors(), e);
      });
    }
    return grpc::Status::OK;
  }

  grpc::Status DescribeConsumerGroups(grpc::ServerContext*, const DescribeConsumerGroupsRequest* req,
                                      DescribeConsumerGroupsResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    BorrowedStringList group_ids(req->group_ids());
    Owned<kafka_admin_DescribeConsumerGroupsOptions_t, kafka_admin_DescribeConsumerGroupsOptions_destroy>
        opts(kafka_admin_DescribeConsumerGroupsOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_DescribeConsumerGroupsOptions_set_timeout_ms);
    kafka_admin_DescribeConsumerGroupsOptions_set_include_authorized_operations(
        opts.get(), req->include_authorized_operations() ? 1 : 0);
    Owned<kafka_admin_DescribeConsumerGroupsResult_t, kafka_admin_DescribeConsumerGroupsResult_destroy>
        result(kafka_admin_Admin_describe_consumer_groups_with_options(admin, group_ids.list,
                                                                       opts.get()));
    OwnedMap groups(kafka_admin_DescribeConsumerGroupsResult_described_groups(result.get()));
    keyed_futures_to_proto<char, kafka_admin_ConsumerGroupDescription_t>(
        groups.get(), [resp] { return resp->add_entries(); }, set_name_key,
        [](DescribeConsumerGroupsEntry* entry, const kafka_admin_ConsumerGroupDescription_t* d) {
          consumer_group_description_to_proto(d, entry->mutable_value());
        });
    return grpc::Status::OK;
  }

  grpc::Status DescribeClassicGroups(grpc::ServerContext*, const DescribeClassicGroupsRequest* req,
                                     DescribeClassicGroupsResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    BorrowedStringList group_ids(req->group_ids());
    Owned<kafka_admin_DescribeClassicGroupsOptions_t, kafka_admin_DescribeClassicGroupsOptions_destroy>
        opts(kafka_admin_DescribeClassicGroupsOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_DescribeClassicGroupsOptions_set_timeout_ms);
    kafka_admin_DescribeClassicGroupsOptions_set_include_authorized_operations(
        opts.get(), req->include_authorized_operations() ? 1 : 0);
    Owned<kafka_admin_DescribeClassicGroupsResult_t, kafka_admin_DescribeClassicGroupsResult_destroy>
        result(kafka_admin_Admin_describe_classic_groups_with_options(admin, group_ids.list,
                                                                      opts.get()));
    OwnedMap groups(kafka_admin_DescribeClassicGroupsResult_described_groups(result.get()));
    keyed_futures_to_proto<char, kafka_admin_ClassicGroupDescription_t>(
        groups.get(), [resp] { return resp->add_entries(); }, set_name_key,
        [](DescribeClassicGroupsEntry* entry, const kafka_admin_ClassicGroupDescription_t* d) {
          classic_group_description_to_proto(d, entry->mutable_value());
        });
    return grpc::Status::OK;
  }

  grpc::Status ListConsumerGroupOffsets(grpc::ServerContext*,
                                        const ListConsumerGroupOffsetsRequest* req,
                                        ListConsumerGroupOffsetsResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    OwnedHandles<kafka_admin_ListConsumerGroupOffsetsSpec_t,
                 kafka_admin_ListConsumerGroupOffsetsSpec_destroy>
        specs;
    std::vector<std::unique_ptr<TpList>> partition_lists;
    CMap group_specs;  // borrowed group id -> spec
    for (const auto& spec : req->group_specs()) {
      kafka_admin_ListConsumerGroupOffsetsSpec_t* s =
          specs.add(kafka_admin_ListConsumerGroupOffsetsSpec_new());
      // An absent list is Java's null (all partitions of the group); a
      // present one, even empty, is an explicit selection.
      if (spec.has_topic_partitions()) {
        partition_lists.push_back(std::make_unique<TpList>(spec.topic_partitions().partitions()));
        kafka_admin_ListConsumerGroupOffsetsSpec_set_topic_partitions(s, partition_lists.back()->list);
      }
      group_specs.put(spec.group_id().c_str(), s);
    }
    Owned<kafka_admin_ListConsumerGroupOffsetsOptions_t,
          kafka_admin_ListConsumerGroupOffsetsOptions_destroy>
        opts(kafka_admin_ListConsumerGroupOffsetsOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_ListConsumerGroupOffsetsOptions_set_timeout_ms);
    kafka_admin_ListConsumerGroupOffsetsOptions_set_require_stable(opts.get(),
                                                                   req->require_stable() ? 1 : 0);
    Owned<kafka_admin_ListConsumerGroupOffsetsResult_t,
          kafka_admin_ListConsumerGroupOffsetsResult_destroy>
        result(kafka_admin_Admin_list_consumer_group_offsets_with_group_specs_options(
            admin, group_specs.map, opts.get()));

    // One entry per requested group, through the per-group future so a
    // group's own failure stays on its entry.
    for (const auto& spec : req->group_specs()) {
      ListConsumerGroupOffsetsEntry* entry = resp->add_entries();
      set_name_key(entry->mutable_key(), spec.group_id().c_str());
      kafka_common_KafkaFuture_t* raw = nullptr;
      kafka_common_Error_t* err =
          kafka_admin_ListConsumerGroupOffsetsResult_partitions_to_offset_and_metadata_with_group_id(
              result.get(), spec.group_id().c_str(), &raw);
      if (err != nullptr) {
        fill_proto_error(entry->mutable_error(), err);
        continue;
      }
      OwnedFuture f(raw);
      const kafka_Map_t* offsets = nullptr;
      err = future_get(f.get(), &offsets);
      if (err != nullptr) {
        fill_proto_error(entry->mutable_error(), err);
        continue;
      }
      // The nested level: one committed offset per partition, each nullable.
      GroupOffsets* dst = entry->mutable_value();
      const int32_t n = offsets == nullptr ? 0 : kafka_Map_size(offsets);
      for (int32_t p = 0; p < n; p++) {
        GroupOffset* pair = dst->add_offsets();
        tp_to_proto(static_cast<const kafka_common_TopicPartition_t*>(kafka_Map_key(offsets, p)),
                    pair->mutable_partition());
        // Java's map value is nullable: a null means the group has no
        // committed offset for this partition, which is not offset 0, so
        // the whole OffsetAndMetadata stays absent.
        const auto* oam =
            static_cast<const kafka_consumer_OffsetAndMetadata_t*>(kafka_Map_value(offsets, p));
        if (oam != nullptr) oam_to_proto(oam, pair->mutable_offset());
      }
    }
    return grpc::Status::OK;
  }

  grpc::Status AlterConsumerGroupOffsets(grpc::ServerContext*,
                                         const AlterConsumerGroupOffsetsRequest* req,
                                         VoidKeyedResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    OwnedHandles<kafka_common_TopicPartition_t, kafka_common_TopicPartition_destroy> tps;
    OwnedHandles<kafka_consumer_OffsetAndMetadata_t, kafka_consumer_OffsetAndMetadata_destroy> oams;
    CMap offsets;
    for (const auto& commit : req->offsets()) {
      const OffsetAndMetadata& o = commit.offset();
      kafka_consumer_OffsetAndMetadata_t* oam = nullptr;
      // An absent leader_epoch is Java's Optional.empty(); the constructor
      // validates the offset (negative -> IllegalArgumentException).
      kafka_common_Error_t* err =
          o.has_leader_epoch()
              ? kafka_consumer_OffsetAndMetadata_with_leader_epoch_metadata(
                    o.offset(), o.leader_epoch(), o.metadata().c_str(), &oam)
              : kafka_consumer_OffsetAndMetadata_with_metadata(o.offset(), o.metadata().c_str(), &oam);
      if (err != nullptr) {
        fill_proto_error(resp->mutable_error(), err);
        return grpc::Status::OK;
      }
      offsets.put(tps.add(kafka_common_TopicPartition_new(commit.partition().topic().c_str(),
                                                          commit.partition().partition())),
                  oams.add(oam));
    }
    Owned<kafka_admin_AlterConsumerGroupOffsetsOptions_t,
          kafka_admin_AlterConsumerGroupOffsetsOptions_destroy>
        opts(kafka_admin_AlterConsumerGroupOffsetsOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_AlterConsumerGroupOffsetsOptions_set_timeout_ms);
    Owned<kafka_admin_AlterConsumerGroupOffsetsResult_t,
          kafka_admin_AlterConsumerGroupOffsetsResult_destroy>
        result(kafka_admin_Admin_alter_consumer_group_offsets_with_options(
            admin, req->group_id().c_str(), offsets.map, opts.get()));
    // One entry per requested partition through its own future.
    for (kafka_common_TopicPartition_t* tp : tps.handles) {
      VoidResultEntry* entry = resp->add_entries();
      set_partition_key(entry->mutable_key(), tp);
      OwnedFuture f(kafka_admin_AlterConsumerGroupOffsetsResult_partition_result(result.get(), tp));
      await_into(f.get(), entry);
    }
    return grpc::Status::OK;
  }

  grpc::Status DeleteConsumerGroupOffsets(grpc::ServerContext*,
                                          const DeleteConsumerGroupOffsetsRequest* req,
                                          VoidKeyedResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    TpList partitions(req->partitions());
    Owned<kafka_admin_DeleteConsumerGroupOffsetsOptions_t,
          kafka_admin_DeleteConsumerGroupOffsetsOptions_destroy>
        opts(kafka_admin_DeleteConsumerGroupOffsetsOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_DeleteConsumerGroupOffsetsOptions_set_timeout_ms);
    Owned<kafka_admin_DeleteConsumerGroupOffsetsResult_t,
          kafka_admin_DeleteConsumerGroupOffsetsResult_destroy>
        result(kafka_admin_Admin_delete_consumer_group_offsets_with_options(
            admin, req->group_id().c_str(), partitions.list, opts.get()));
    for (kafka_common_TopicPartition_t* tp : partitions.handles) {
      VoidResultEntry* entry = resp->add_entries();
      set_partition_key(entry->mutable_key(), tp);
      kafka_common_KafkaFuture_t* raw = nullptr;
      kafka_common_Error_t* err =
          kafka_admin_DeleteConsumerGroupOffsetsResult_partition_result(result.get(), tp, &raw);
      if (err != nullptr) {
        fill_proto_error(entry->mutable_error(), err);
        continue;
      }
      OwnedFuture f(raw);
      await_into(f.get(), entry);
    }
    return grpc::Status::OK;
  }

  grpc::Status DeleteConsumerGroups(grpc::ServerContext*, const DeleteConsumerGroupsRequest* req,
                                    VoidKeyedResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    BorrowedStringList group_ids(req->group_ids());
    Owned<kafka_admin_DeleteConsumerGroupsOptions_t, kafka_admin_DeleteConsumerGroupsOptions_destroy>
        opts(kafka_admin_DeleteConsumerGroupsOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_DeleteConsumerGroupsOptions_set_timeout_ms);
    Owned<kafka_admin_DeleteConsumerGroupsResult_t, kafka_admin_DeleteConsumerGroupsResult_destroy>
        result(kafka_admin_Admin_delete_consumer_groups_with_options(admin, group_ids.list,
                                                                     opts.get()));
    OwnedMap deleted(kafka_admin_DeleteConsumerGroupsResult_deleted_groups(result.get()));
    void_futures_to_proto<char>(deleted.get(), [resp] { return resp->add_entries(); }, set_name_key);
    return grpc::Status::OK;
  }

  grpc::Status RemoveMembersFromConsumerGroup(grpc::ServerContext*,
                                              const RemoveMembersFromConsumerGroupRequest* req,
                                              VoidKeyedResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    // An absent member list is Java's removeAll constructor
    // (`new RemoveMembersFromConsumerGroupOptions()`), which removes every
    // member of the group; a present-but-empty one is rejected by Java with
    // "Invalid empty members has been provided".
    const bool remove_all = !req->has_members();
    OwnedHandles<kafka_admin_MemberToRemove_t, kafka_admin_MemberToRemove_destroy> members;
    kafka_admin_RemoveMembersFromConsumerGroupOptions_t* raw_opts = nullptr;
    kafka_common_Error_t* err = nullptr;
    if (remove_all) {
      raw_opts = kafka_admin_RemoveMembersFromConsumerGroupOptions_new();
    } else {
      CList member_list;
      for (const auto& m : req->members().members()) {
        member_list.add(members.add(kafka_admin_MemberToRemove_new(m.group_instance_id().c_str())));
      }
      err = kafka_admin_RemoveMembersFromConsumerGroupOptions_with_members(member_list.list, &raw_opts);
    }
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    Owned<kafka_admin_RemoveMembersFromConsumerGroupOptions_t,
          kafka_admin_RemoveMembersFromConsumerGroupOptions_destroy>
        opts(raw_opts);
    apply_timeout(*req, opts.get(), kafka_admin_RemoveMembersFromConsumerGroupOptions_set_timeout_ms);
    if (req->has_reason()) {
      kafka_admin_RemoveMembersFromConsumerGroupOptions_set_reason(opts.get(), req->reason().c_str());
    }
    Owned<kafka_admin_RemoveMembersFromConsumerGroupResult_t,
          kafka_admin_RemoveMembersFromConsumerGroupResult_destroy>
        result(kafka_admin_Admin_remove_members_from_consumer_group_with_options(
            admin, req->group_id().c_str(), opts.get()));
    if (remove_all) {
      // In removeAll mode Java's memberResult is not applicable (the result
      // carries no keys), so `entries` stays empty and the only outcome is
      // `all()`, reported on the response.
      OwnedFuture all(kafka_admin_RemoveMembersFromConsumerGroupResult_all(result.get()));
      await_into(all.get(), resp);
      return grpc::Status::OK;
    }
    for (kafka_admin_MemberToRemove_t* member : members.handles) {
      VoidResultEntry* entry = resp->add_entries();
      set_name_key(entry->mutable_key(), kafka_admin_MemberToRemove_group_instance_id(member));
      kafka_common_KafkaFuture_t* raw = nullptr;
      err = kafka_admin_RemoveMembersFromConsumerGroupResult_member_result(result.get(), member, &raw);
      if (err != nullptr) {
        fill_proto_error(entry->mutable_error(), err);
        continue;
      }
      OwnedFuture f(raw);
      await_into(f.get(), entry);
    }
    return grpc::Status::OK;
  }

  grpc::Status CreateAcls(grpc::ServerContext*, const CreateAclsRequest* req,
                          VoidKeyedResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    AclBindingArgs bindings;
    CList acl_list;
    for (const AclBinding& b : req->acls()) {
      kafka_common_acl_AclBinding_t* binding = nullptr;
      kafka_common_Error_t* err = bindings.build(b, &binding);
      if (err != nullptr) {
        fill_proto_error(resp->mutable_error(), err);
        return grpc::Status::OK;
      }
      acl_list.add(binding);
    }
    Owned<kafka_admin_CreateAclsOptions_t, kafka_admin_CreateAclsOptions_destroy> opts(
        kafka_admin_CreateAclsOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_CreateAclsOptions_set_timeout_ms);
    Owned<kafka_admin_CreateAclsResult_t, kafka_admin_CreateAclsResult_destroy> result(
        kafka_admin_Admin_create_acls_with_options(admin, acl_list.list, opts.get()));
    OwnedMap values(kafka_admin_CreateAclsResult_values(result.get()));
    void_futures_to_proto<kafka_common_acl_AclBinding_t>(
        values.get(), [resp] { return resp->add_entries(); }, set_acl_binding_key);
    return grpc::Status::OK;
  }

  grpc::Status DescribeAcls(grpc::ServerContext*, const DescribeAclsRequest* req,
                            DescribeAclsResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    AclFilterArgs filters;
    const kafka_common_acl_AclBindingFilter_t* filter = filters.build(req->filter());
    Owned<kafka_admin_DescribeAclsOptions_t, kafka_admin_DescribeAclsOptions_destroy> opts(
        kafka_admin_DescribeAclsOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_DescribeAclsOptions_set_timeout_ms);
    Owned<kafka_admin_DescribeAclsResult_t, kafka_admin_DescribeAclsResult_destroy> result(
        kafka_admin_Admin_describe_acls_with_options(admin, filter, opts.get()));
    // The values future is borrowed from the result handle, never destroyed.
    const kafka_List_t* acls = nullptr;
    kafka_common_Error_t* err =
        future_get(kafka_admin_DescribeAclsResult_values(result.get()), &acls);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    for_each_in_list<kafka_common_acl_AclBinding_t>(acls, [resp](const kafka_common_acl_AclBinding_t* b) {
      acl_binding_to_proto(b, resp->add_acls());
    });
    return grpc::Status::OK;
  }

  grpc::Status DeleteAcls(grpc::ServerContext*, const DeleteAclsRequest* req,
                          DeleteAclsResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    AclFilterArgs filters;
    CList filter_list;
    for (const AclBindingFilter& f : req->filters()) filter_list.add(filters.build(f));
    Owned<kafka_admin_DeleteAclsOptions_t, kafka_admin_DeleteAclsOptions_destroy> opts(
        kafka_admin_DeleteAclsOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_DeleteAclsOptions_set_timeout_ms);
    Owned<kafka_admin_DeleteAclsResult_t, kafka_admin_DeleteAclsResult_destroy> result(
        kafka_admin_Admin_delete_acls_with_options(admin, filter_list.list, opts.get()));
    OwnedMap values(kafka_admin_DeleteAclsResult_values(result.get()));
    keyed_futures_to_proto<kafka_common_acl_AclBindingFilter_t, kafka_admin_DeleteAclsResult_FilterResults_t>(
        values.get(), [resp] { return resp->add_entries(); }, set_acl_filter_key,
        [](DeleteAclsEntry* entry, const kafka_admin_DeleteAclsResult_FilterResults_t* results) {
          // The inner level: one result per ACL the filter matched, each
          // carrying either the deleted binding or its own failure.
          FilterResults* dst = entry->mutable_value();
          OwnedList list(kafka_admin_DeleteAclsResult_FilterResults_values(results));
          for_each_in_list<kafka_admin_DeleteAclsResult_FilterResult_t>(
              list.get(), [dst](const kafka_admin_DeleteAclsResult_FilterResult_t* r) {
                DeletedAcl* deleted = dst->add_values();
                const kafka_common_acl_AclBinding_t* binding =
                    kafka_admin_DeleteAclsResult_FilterResult_binding(r);
                if (binding != nullptr) acl_binding_to_proto(binding, deleted->mutable_binding());
                const kafka_common_Error_t* err = kafka_admin_DeleteAclsResult_FilterResult_error(r);
                if (err != nullptr) copy_proto_error(deleted->mutable_exception(), err);
              });
        });
    return grpc::Status::OK;
  }

  grpc::Status DescribeClientQuotas(grpc::ServerContext*, const DescribeClientQuotasRequest* req,
                                    DescribeClientQuotasResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    // The three match kinds are the three Java factories; MATCH_KIND_UNSPECIFIED
    // is rejected rather than defaulted.
    OwnedHandles<kafka_common_quota_ClientQuotaFilterComponent_t,
                 kafka_common_quota_ClientQuotaFilterComponent_destroy>
        components;
    CList component_list;
    for (const auto& c : req->components()) {
      kafka_common_quota_ClientQuotaFilterComponent_t* component = nullptr;
      switch (c.match_kind()) {
        case MATCH_KIND_EXACT:
          if (!c.has_match_name()) {
            *resp->mutable_error() = make_synthetic_error(
                "ClientQuotaFilterComponent with MATCH_KIND_EXACT carries no match_name",
                kafka_common_ErrorCode_LOCAL_ILLEGAL_ARGUMENT);
            return grpc::Status::OK;
          }
          component = kafka_common_quota_ClientQuotaFilterComponent_of_entity(
              c.entity_type().c_str(), c.match_name().c_str());
          break;
        case MATCH_KIND_DEFAULT:
          component =
              kafka_common_quota_ClientQuotaFilterComponent_of_default_entity(c.entity_type().c_str());
          break;
        case MATCH_KIND_ANY:
          component =
              kafka_common_quota_ClientQuotaFilterComponent_of_entity_type(c.entity_type().c_str());
          break;
        default:
          *resp->mutable_error() = make_synthetic_error(
              "ClientQuotaFilterComponent has no match_kind (got " +
                  std::to_string(static_cast<int>(c.match_kind())) + ")",
              kafka_common_ErrorCode_LOCAL_ILLEGAL_ARGUMENT);
          return grpc::Status::OK;
      }
      component_list.add(components.add(component));
    }
    // Java's ClientQuotaFilter.containsOnly is the strict form of contains.
    Owned<kafka_common_quota_ClientQuotaFilter_t, kafka_common_quota_ClientQuotaFilter_destroy> filter(
        req->strict() ? kafka_common_quota_ClientQuotaFilter_contains_only(component_list.list)
                      : kafka_common_quota_ClientQuotaFilter_contains(component_list.list));
    Owned<kafka_admin_DescribeClientQuotasOptions_t, kafka_admin_DescribeClientQuotasOptions_destroy>
        opts(kafka_admin_DescribeClientQuotasOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_DescribeClientQuotasOptions_set_timeout_ms);
    Owned<kafka_admin_DescribeClientQuotasResult_t, kafka_admin_DescribeClientQuotasResult_destroy>
        result(kafka_admin_Admin_describe_client_quotas_with_options(admin, filter.get(), opts.get()));
    // The entities future is borrowed from the result handle, never destroyed.
    const kafka_Map_t* entities = nullptr;
    kafka_common_Error_t* err =
        future_get(kafka_admin_DescribeClientQuotasResult_entities(result.get()), &entities);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    const int32_t n = entities == nullptr ? 0 : kafka_Map_size(entities);
    for (int32_t i = 0; i < n; i++) {
      EntityQuotas* reported = resp->add_entities();
      quota_entity_to_proto(
          static_cast<const kafka_common_quota_ClientQuotaEntity_t*>(kafka_Map_key(entities, i)),
          reported->mutable_entity());
      const auto* quotas = static_cast<const kafka_Map_t*>(kafka_Map_value(entities, i));
      const int32_t m = quotas == nullptr ? 0 : kafka_Map_size(quotas);
      for (int32_t j = 0; j < m; j++) {
        QuotaValue* pair = reported->add_values();
        pair->set_key(cstr(static_cast<const char*>(kafka_Map_key(quotas, j))));
        pair->set_value(*static_cast<const double*>(kafka_Map_value(quotas, j)));
      }
    }
    return grpc::Status::OK;
  }

  grpc::Status AlterClientQuotas(grpc::ServerContext*, const AlterClientQuotasRequest* req,
                                 VoidKeyedResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    OwnedHandles<kafka_common_quota_ClientQuotaEntity_t, kafka_common_quota_ClientQuotaEntity_destroy>
        entities;
    OwnedHandles<kafka_common_quota_ClientQuotaAlteration_Op_t,
                 kafka_common_quota_ClientQuotaAlteration_Op_destroy>
        ops;
    OwnedHandles<kafka_common_quota_ClientQuotaAlteration_t, kafka_common_quota_ClientQuotaAlteration_destroy>
        alterations;
    std::vector<std::unique_ptr<CList>> op_lists;
    CList alteration_list;
    for (const ClientQuotaAlteration& a : req->entries()) {
      kafka_common_quota_ClientQuotaEntity_t* entity = entities.add(quota_entity_from_proto(a.entity()));
      op_lists.push_back(std::make_unique<CList>());
      CList& op_list = *op_lists.back();
      for (const auto& op : a.ops()) {
        // An absent value is Java's null Double: remove the quota, which
        // the C constructor spells NAN.
        op_list.add(ops.add(kafka_common_quota_ClientQuotaAlteration_Op_new(
            op.key().c_str(), op.has_value() ? op.value() : NAN)));
      }
      alteration_list.add(
          alterations.add(kafka_common_quota_ClientQuotaAlteration_new(entity, op_list.list)));
    }
    Owned<kafka_admin_AlterClientQuotasOptions_t, kafka_admin_AlterClientQuotasOptions_destroy> opts(
        kafka_admin_AlterClientQuotasOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_AlterClientQuotasOptions_set_timeout_ms);
    kafka_admin_AlterClientQuotasOptions_set_validate_only(opts.get(), req->validate_only() ? 1 : 0);
    Owned<kafka_admin_AlterClientQuotasResult_t, kafka_admin_AlterClientQuotasResult_destroy> result(
        kafka_admin_Admin_alter_client_quotas_with_options(admin, alteration_list.list, opts.get()));
    OwnedMap values(kafka_admin_AlterClientQuotasResult_values(result.get()));
    void_futures_to_proto<kafka_common_quota_ClientQuotaEntity_t>(
        values.get(), [resp] { return resp->add_entries(); }, set_quota_entity_key);
    return grpc::Status::OK;
  }

  grpc::Status DescribeUserScramCredentials(grpc::ServerContext*,
                                            const DescribeUserScramCredentialsRequest* req,
                                            DescribeUserScramCredentialsResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    // An empty user list is Java's no-arg overload: describe every user.
    const bool all_users = req->users_size() == 0;
    BorrowedStringList users(req->users());
    Owned<kafka_admin_DescribeUserScramCredentialsOptions_t,
          kafka_admin_DescribeUserScramCredentialsOptions_destroy>
        opts(kafka_admin_DescribeUserScramCredentialsOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_DescribeUserScramCredentialsOptions_set_timeout_ms);
    Owned<kafka_admin_DescribeUserScramCredentialsResult_t,
          kafka_admin_DescribeUserScramCredentialsResult_destroy>
        result(kafka_admin_Admin_describe_user_scram_credentials_with_users_options(
            admin, all_users ? nullptr : users.list, opts.get()));

    std::vector<std::string> names;
    if (all_users) {
      // The users the broker reported, then one description each.
      OwnedFuture f(kafka_admin_DescribeUserScramCredentialsResult_users(result.get()));
      const kafka_List_t* found = nullptr;
      kafka_common_Error_t* err = future_get(f.get(), &found);
      if (err != nullptr) {
        fill_proto_error(resp->mutable_error(), err);
        return grpc::Status::OK;
      }
      for_each_in_list<char>(found, [&names](const char* user) { names.push_back(cstr(user)); });
    } else {
      names.assign(req->users().begin(), req->users().end());
    }
    for (const std::string& user : names) {
      DescribeUserScramCredentialsEntry* entry = resp->add_entries();
      set_name_key(entry->mutable_key(), user.c_str());
      OwnedFuture f(kafka_admin_DescribeUserScramCredentialsResult_description(result.get(), user.c_str()));
      const kafka_admin_UserScramCredentialsDescription_t* d = nullptr;
      kafka_common_Error_t* err = future_get(f.get(), &d);
      if (err != nullptr) {
        fill_proto_error(entry->mutable_error(), err);
        continue;
      }
      UserScramCredentialsDescription* dst = entry->mutable_value();
      dst->set_name(cstr(kafka_admin_UserScramCredentialsDescription_name(d)));
      OwnedList infos(kafka_admin_UserScramCredentialsDescription_credential_infos(d));
      for_each_in_list<kafka_admin_ScramCredentialInfo_t>(
          infos.get(), [dst](const kafka_admin_ScramCredentialInfo_t* info) {
            ScramCredentialInfo* pi = dst->add_credential_infos();
            pi->set_mechanism(kafka_admin_ScramMechanism_type(kafka_admin_ScramCredentialInfo_mechanism(info)));
            pi->set_iterations(kafka_admin_ScramCredentialInfo_iterations(info));
          });
    }
    return grpc::Status::OK;
  }

  grpc::Status AlterUserScramCredentials(grpc::ServerContext*,
                                         const AlterUserScramCredentialsRequest* req,
                                         VoidKeyedResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    OwnedHandles<kafka_admin_ScramCredentialInfo_t, kafka_admin_ScramCredentialInfo_destroy> infos;
    OwnedHandles<kafka_admin_UserScramCredentialUpsertion_t, kafka_admin_UserScramCredentialUpsertion_destroy>
        upsertions;
    OwnedHandles<kafka_admin_UserScramCredentialDeletion_t, kafka_admin_UserScramCredentialDeletion_destroy>
        deletions;
    OwnedHandles<kafka_admin_UserScramCredentialAlteration_t, kafka_admin_UserScramCredentialAlteration_destroy>
        alterations;
    CList alteration_list;
    for (const UserScramCredentialAlteration& a : req->alterations()) {
      const kafka_admin_ScramMechanism_t* mechanism =
          kafka_admin_ScramMechanism_from_type(static_cast<int8_t>(a.mechanism()));
      kafka_admin_UserScramCredentialAlteration_t* alteration = nullptr;
      if (a.is_deletion()) {
        alteration = kafka_admin_UserScramCredentialAlteration_deletion(
            deletions.add(kafka_admin_UserScramCredentialDeletion_new(a.user().c_str(), mechanism)));
      } else {
        kafka_admin_ScramCredentialInfo_t* info =
            infos.add(kafka_admin_ScramCredentialInfo_new(mechanism, a.iterations()));
        const kafka_Bytes_t password = bytes_of(a.has_password() ? a.password() : std::string());
        kafka_admin_UserScramCredentialUpsertion_t* upsertion =
            a.has_salt() ? kafka_admin_UserScramCredentialUpsertion_with_salt(
                               a.user().c_str(), info, password, bytes_of(a.salt()))
                         : kafka_admin_UserScramCredentialUpsertion_with_bytes(a.user().c_str(), info,
                                                                               password);
        alteration = kafka_admin_UserScramCredentialAlteration_upsertion(upsertions.add(upsertion));
      }
      alteration_list.add(alterations.add(alteration));
    }
    Owned<kafka_admin_AlterUserScramCredentialsOptions_t,
          kafka_admin_AlterUserScramCredentialsOptions_destroy>
        opts(kafka_admin_AlterUserScramCredentialsOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_AlterUserScramCredentialsOptions_set_timeout_ms);
    Owned<kafka_admin_AlterUserScramCredentialsResult_t, kafka_admin_AlterUserScramCredentialsResult_destroy>
        result(kafka_admin_Admin_alter_user_scram_credentials_with_options(admin, alteration_list.list,
                                                                           opts.get()));
    OwnedMap values(kafka_admin_AlterUserScramCredentialsResult_values(result.get()));
    void_futures_to_proto<char>(values.get(), [resp] { return resp->add_entries(); }, set_name_key);
    return grpc::Status::OK;
  }

  grpc::Status CreateDelegationToken(grpc::ServerContext*, const CreateDelegationTokenRequest* req,
                                     CreateDelegationTokenResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    OwnedHandles<kafka_common_security_auth_KafkaPrincipal_t, kafka_common_security_auth_KafkaPrincipal_destroy>
        principals;
    CList renewers;
    for (const KafkaPrincipal& p : req->renewers()) {
      renewers.add(principals.add(
          kafka_common_security_auth_KafkaPrincipal_new(p.principal_type().c_str(), p.name().c_str())));
    }
    Owned<kafka_admin_CreateDelegationTokenOptions_t, kafka_admin_CreateDelegationTokenOptions_destroy>
        opts(kafka_admin_CreateDelegationTokenOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_CreateDelegationTokenOptions_set_timeout_ms);
    kafka_admin_CreateDelegationTokenOptions_set_renewers(opts.get(), renewers.list);
    // An absent owner is Java's null: the token is created for the caller.
    if (req->has_owner()) {
      kafka_admin_CreateDelegationTokenOptions_set_owner(
          opts.get(), principals.add(kafka_common_security_auth_KafkaPrincipal_new(
                          req->owner().principal_type().c_str(), req->owner().name().c_str())));
    }
    kafka_admin_CreateDelegationTokenOptions_set_max_lifetime_ms(opts.get(), req->max_lifetime_ms());
    Owned<kafka_admin_CreateDelegationTokenResult_t, kafka_admin_CreateDelegationTokenResult_destroy>
        result(kafka_admin_Admin_create_delegation_token_with_options(admin, opts.get()));
    // Borrowed future, never destroyed.
    const kafka_common_security_token_delegation_DelegationToken_t* token = nullptr;
    kafka_common_Error_t* err =
        future_get(kafka_admin_CreateDelegationTokenResult_delegation_token(result.get()), &token);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    if (token != nullptr) delegation_token_to_proto(token, resp->mutable_token());
    return grpc::Status::OK;
  }

  grpc::Status RenewDelegationToken(grpc::ServerContext*, const RenewDelegationTokenRequest* req,
                                    DelegationTokenExpiryResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    Owned<kafka_admin_RenewDelegationTokenOptions_t, kafka_admin_RenewDelegationTokenOptions_destroy>
        opts(kafka_admin_RenewDelegationTokenOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_RenewDelegationTokenOptions_set_timeout_ms);
    kafka_admin_RenewDelegationTokenOptions_set_renew_time_period_ms(opts.get(),
                                                                     req->renew_time_period_ms());
    Owned<kafka_admin_RenewDelegationTokenResult_t, kafka_admin_RenewDelegationTokenResult_destroy>
        result(kafka_admin_Admin_renew_delegation_token_with_options(admin, bytes_of(req->hmac()),
                                                                     opts.get()));
    const int64_t* expiry = nullptr;
    kafka_common_Error_t* err =
        future_get(kafka_admin_RenewDelegationTokenResult_expiry_timestamp(result.get()), &expiry);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    resp->set_expiry_timestamp_ms(expiry != nullptr ? *expiry : 0);
    return grpc::Status::OK;
  }

  grpc::Status ExpireDelegationToken(grpc::ServerContext*, const ExpireDelegationTokenRequest* req,
                                     DelegationTokenExpiryResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    Owned<kafka_admin_ExpireDelegationTokenOptions_t, kafka_admin_ExpireDelegationTokenOptions_destroy>
        opts(kafka_admin_ExpireDelegationTokenOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_ExpireDelegationTokenOptions_set_timeout_ms);
    kafka_admin_ExpireDelegationTokenOptions_set_expiry_time_period_ms(opts.get(),
                                                                       req->expiry_time_period_ms());
    Owned<kafka_admin_ExpireDelegationTokenResult_t, kafka_admin_ExpireDelegationTokenResult_destroy>
        result(kafka_admin_Admin_expire_delegation_token_with_options(admin, bytes_of(req->hmac()),
                                                                      opts.get()));
    const int64_t* expiry = nullptr;
    kafka_common_Error_t* err =
        future_get(kafka_admin_ExpireDelegationTokenResult_expiry_timestamp(result.get()), &expiry);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    resp->set_expiry_timestamp_ms(expiry != nullptr ? *expiry : 0);
    return grpc::Status::OK;
  }

  grpc::Status DescribeDelegationToken(grpc::ServerContext*,
                                       const DescribeDelegationTokenRequest* req,
                                       DescribeDelegationTokenResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    Owned<kafka_admin_DescribeDelegationTokenOptions_t, kafka_admin_DescribeDelegationTokenOptions_destroy>
        opts(kafka_admin_DescribeDelegationTokenOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_DescribeDelegationTokenOptions_set_timeout_ms);
    // An absent owner list is Java's null (every token); a present one, even
    // empty, is an explicit selection.
    OwnedHandles<kafka_common_security_auth_KafkaPrincipal_t, kafka_common_security_auth_KafkaPrincipal_destroy>
        principals;
    CList owners;
    if (req->has_owners()) {
      for (const KafkaPrincipal& p : req->owners().principals()) {
        owners.add(principals.add(kafka_common_security_auth_KafkaPrincipal_new(
            p.principal_type().c_str(), p.name().c_str())));
      }
      kafka_admin_DescribeDelegationTokenOptions_set_owners(opts.get(), owners.list);
    }
    Owned<kafka_admin_DescribeDelegationTokenResult_t, kafka_admin_DescribeDelegationTokenResult_destroy>
        result(kafka_admin_Admin_describe_delegation_token_with_options(admin, opts.get()));
    const kafka_List_t* tokens = nullptr;
    kafka_common_Error_t* err =
        future_get(kafka_admin_DescribeDelegationTokenResult_delegation_tokens(result.get()), &tokens);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    for_each_in_list<kafka_common_security_token_delegation_DelegationToken_t>(
        tokens, [resp](const kafka_common_security_token_delegation_DelegationToken_t* t) {
          delegation_token_to_proto(t, resp->add_tokens());
        });
    return grpc::Status::OK;
  }

  grpc::Status DescribeFeatures(grpc::ServerContext*, const DescribeFeaturesRequest* req,
                                DescribeFeaturesResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    Owned<kafka_admin_DescribeFeaturesOptions_t, kafka_admin_DescribeFeaturesOptions_destroy> opts(
        kafka_admin_DescribeFeaturesOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_DescribeFeaturesOptions_set_timeout_ms);
    if (req->has_node_id()) kafka_admin_DescribeFeaturesOptions_set_node_id(opts.get(), req->node_id());
    Owned<kafka_admin_DescribeFeaturesResult_t, kafka_admin_DescribeFeaturesResult_destroy> result(
        kafka_admin_Admin_describe_features_with_options(admin, opts.get()));
    OwnedFuture f(kafka_admin_DescribeFeaturesResult_feature_metadata(result.get()));
    const kafka_admin_FeatureMetadata_t* metadata = nullptr;
    kafka_common_Error_t* err = future_get(f.get(), &metadata);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    FeatureMetadata* dst = resp->mutable_metadata();
    {
      OwnedMap finalized(kafka_admin_FeatureMetadata_finalized_features(metadata));
      const int32_t n = finalized ? kafka_Map_size(finalized.get()) : 0;
      for (int32_t i = 0; i < n; i++) {
        const char* name = static_cast<const char*>(kafka_Map_key(finalized.get(), i));
        const auto* range =
            static_cast<const kafka_admin_FinalizedVersionRange_t*>(kafka_Map_value(finalized.get(), i));
        FinalizedVersionRange* pr = &(*dst->mutable_finalized_features())[cstr(name)];
        pr->set_min_version_level(kafka_admin_FinalizedVersionRange_min_version_level(range));
        pr->set_max_version_level(kafka_admin_FinalizedVersionRange_max_version_level(range));
      }
    }
    // Java's Optional<Long> finalizedFeaturesEpoch: -1 is empty.
    const int64_t epoch = kafka_admin_FeatureMetadata_finalized_features_epoch(metadata);
    if (epoch >= 0) dst->set_finalized_features_epoch(epoch);
    {
      OwnedMap supported(kafka_admin_FeatureMetadata_supported_features(metadata));
      const int32_t n = supported ? kafka_Map_size(supported.get()) : 0;
      for (int32_t i = 0; i < n; i++) {
        const char* name = static_cast<const char*>(kafka_Map_key(supported.get(), i));
        const auto* range =
            static_cast<const kafka_admin_SupportedVersionRange_t*>(kafka_Map_value(supported.get(), i));
        SupportedVersionRange* pr = &(*dst->mutable_supported_features())[cstr(name)];
        pr->set_min_version(kafka_admin_SupportedVersionRange_min_version(range));
        pr->set_max_version(kafka_admin_SupportedVersionRange_max_version(range));
      }
    }
    return grpc::Status::OK;
  }

  grpc::Status UpdateFeatures(grpc::ServerContext*, const UpdateFeaturesRequest* req,
                              VoidKeyedResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    OwnedHandles<kafka_admin_FeatureUpdate_t, kafka_admin_FeatureUpdate_destroy> updates;
    CMap update_map;
    for (const auto& kv : req->feature_updates()) {
      kafka_admin_FeatureUpdate_t* update = nullptr;
      // FeatureUpdate's constructor validates the pair (IllegalArgumentException
      // for a bad level or an UNKNOWN upgrade type).
      kafka_common_Error_t* err = kafka_admin_FeatureUpdate_new(
          static_cast<int16_t>(kv.second.max_version_level()),
          kafka_admin_FeatureUpdate_UpgradeType_from_code(kv.second.upgrade_type()), &update);
      if (err != nullptr) {
        fill_proto_error(resp->mutable_error(), err);
        return grpc::Status::OK;
      }
      update_map.put(kv.first.c_str(), updates.add(update));
    }
    Owned<kafka_admin_UpdateFeaturesOptions_t, kafka_admin_UpdateFeaturesOptions_destroy> opts(
        kafka_admin_UpdateFeaturesOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_UpdateFeaturesOptions_set_timeout_ms);
    kafka_admin_UpdateFeaturesOptions_set_validate_only(opts.get(), req->validate_only() ? 1 : 0);
    // The only RPC whose Java method validates its arguments up front
    // (IllegalArgumentException on an empty map), hence the error return.
    kafka_admin_UpdateFeaturesResult_t* raw = nullptr;
    kafka_common_Error_t* err =
        kafka_admin_Admin_update_features_with_options(admin, update_map.map, opts.get(), &raw);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    Owned<kafka_admin_UpdateFeaturesResult_t, kafka_admin_UpdateFeaturesResult_destroy> result(raw);
    OwnedMap values(kafka_admin_UpdateFeaturesResult_values(result.get()));
    void_futures_to_proto<char>(values.get(), [resp] { return resp->add_entries(); }, set_name_key);
    return grpc::Status::OK;
  }

  grpc::Status DescribeProducers(grpc::ServerContext*, const DescribeProducersRequest* req,
                                 DescribeProducersResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    TpList partitions(req->partitions());
    Owned<kafka_admin_DescribeProducersOptions_t, kafka_admin_DescribeProducersOptions_destroy> opts(
        kafka_admin_DescribeProducersOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_DescribeProducersOptions_set_timeout_ms);
    // An absent broker id is Java's empty Optional: ask each partition leader.
    if (req->has_broker_id()) kafka_admin_DescribeProducersOptions_set_broker_id(opts.get(), req->broker_id());
    Owned<kafka_admin_DescribeProducersResult_t, kafka_admin_DescribeProducersResult_destroy> result(
        kafka_admin_Admin_describe_producers_with_options(admin, partitions.list, opts.get()));
    for (kafka_common_TopicPartition_t* tp : partitions.handles) {
      DescribeProducersEntry* entry = resp->add_entries();
      set_partition_key(entry->mutable_key(), tp);
      kafka_common_KafkaFuture_t* raw = nullptr;
      kafka_common_Error_t* err = kafka_admin_DescribeProducersResult_partition_result(result.get(), tp, &raw);
      if (err != nullptr) {
        fill_proto_error(entry->mutable_error(), err);
        continue;
      }
      OwnedFuture f(raw);
      const kafka_admin_DescribeProducersResult_PartitionProducerState_t* state = nullptr;
      err = future_get(f.get(), &state);
      if (err != nullptr) {
        fill_proto_error(entry->mutable_error(), err);
        continue;
      }
      PartitionProducerState* dst = entry->mutable_value();
      OwnedList producers(kafka_admin_DescribeProducersResult_PartitionProducerState_active_producers(state));
      for_each_in_list<kafka_admin_ProducerState_t>(producers.get(), [dst](const kafka_admin_ProducerState_t* p) {
        ProducerState* ps = dst->add_active_producers();
        ps->set_producer_id(kafka_admin_ProducerState_producer_id(p));
        ps->set_producer_epoch(kafka_admin_ProducerState_producer_epoch(p));
        ps->set_last_sequence(kafka_admin_ProducerState_last_sequence(p));
        ps->set_last_timestamp(kafka_admin_ProducerState_last_timestamp(p));
        // Java's OptionalInt / OptionalLong: -1 is empty.
        const int32_t coordinator_epoch = kafka_admin_ProducerState_coordinator_epoch(p);
        if (coordinator_epoch >= 0) ps->set_coordinator_epoch(coordinator_epoch);
        const int64_t start = kafka_admin_ProducerState_current_transaction_start_offset(p);
        if (start >= 0) ps->set_current_transaction_start_offset(start);
      });
    }
    return grpc::Status::OK;
  }

  grpc::Status DescribeTransactions(grpc::ServerContext*, const DescribeTransactionsRequest* req,
                                    DescribeTransactionsResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    BorrowedStringList ids(req->transactional_ids());
    Owned<kafka_admin_DescribeTransactionsOptions_t, kafka_admin_DescribeTransactionsOptions_destroy>
        opts(kafka_admin_DescribeTransactionsOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_DescribeTransactionsOptions_set_timeout_ms);
    Owned<kafka_admin_DescribeTransactionsResult_t, kafka_admin_DescribeTransactionsResult_destroy>
        result(kafka_admin_Admin_describe_transactions_with_options(admin, ids.list, opts.get()));
    for (const std::string& id : req->transactional_ids()) {
      DescribeTransactionsEntry* entry = resp->add_entries();
      set_name_key(entry->mutable_key(), id.c_str());
      kafka_common_KafkaFuture_t* raw = nullptr;
      kafka_common_Error_t* err =
          kafka_admin_DescribeTransactionsResult_description(result.get(), id.c_str(), &raw);
      if (err != nullptr) {
        fill_proto_error(entry->mutable_error(), err);
        continue;
      }
      OwnedFuture f(raw);
      const kafka_admin_TransactionDescription_t* d = nullptr;
      err = future_get(f.get(), &d);
      if (err != nullptr) {
        fill_proto_error(entry->mutable_error(), err);
        continue;
      }
      TransactionDescription* dst = entry->mutable_value();
      dst->set_coordinator_id(kafka_admin_TransactionDescription_coordinator_id(d));
      dst->set_state(transaction_state_name(kafka_admin_TransactionDescription_state(d)));
      dst->set_producer_id(kafka_admin_TransactionDescription_producer_id(d));
      dst->set_producer_epoch(kafka_admin_TransactionDescription_producer_epoch(d));
      dst->set_transaction_timeout_ms(kafka_admin_TransactionDescription_transaction_timeout_ms(d));
      // Java's OptionalLong transactionStartTimeMs: -1 is empty.
      const int64_t start = kafka_admin_TransactionDescription_transaction_start_time_ms(d);
      if (start >= 0) dst->set_transaction_start_time_ms(start);
      OwnedList tps(kafka_admin_TransactionDescription_topic_partitions(d));
      tp_list_to_proto(tps.get(), [dst] { return dst->add_topic_partitions(); });
    }
    return grpc::Status::OK;
  }

  grpc::Status AbortTransaction(grpc::ServerContext*, const AbortTransactionRequest* req,
                                StatusResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    Owned<kafka_common_TopicPartition_t, kafka_common_TopicPartition_destroy> tp(
        kafka_common_TopicPartition_new(req->topic_partition().topic().c_str(),
                                        req->topic_partition().partition()));
    Owned<kafka_admin_AbortTransactionSpec_t, kafka_admin_AbortTransactionSpec_destroy> spec(
        kafka_admin_AbortTransactionSpec_new(tp.get(), req->producer_id(),
                                             static_cast<int16_t>(req->producer_epoch()),
                                             req->coordinator_epoch()));
    Owned<kafka_admin_AbortTransactionOptions_t, kafka_admin_AbortTransactionOptions_destroy> opts(
        kafka_admin_AbortTransactionOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_AbortTransactionOptions_set_timeout_ms);
    Owned<kafka_admin_AbortTransactionResult_t, kafka_admin_AbortTransactionResult_destroy> result(
        kafka_admin_Admin_abort_transaction_with_options(admin, spec.get(), opts.get()));
    OwnedFuture f(kafka_admin_AbortTransactionResult_all(result.get()));
    await_into(f.get(), resp);
    return grpc::Status::OK;
  }

  grpc::Status ForceTerminateTransaction(grpc::ServerContext*,
                                         const ForceTerminateTransactionRequest* req,
                                         StatusResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    Owned<kafka_admin_TerminateTransactionOptions_t, kafka_admin_TerminateTransactionOptions_destroy>
        opts(kafka_admin_TerminateTransactionOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_TerminateTransactionOptions_set_timeout_ms);
    Owned<kafka_admin_TerminateTransactionResult_t, kafka_admin_TerminateTransactionResult_destroy>
        result(kafka_admin_Admin_force_terminate_transaction_with_options(
            admin, req->transactional_id().c_str(), opts.get()));
    OwnedFuture f(kafka_admin_TerminateTransactionResult_result(result.get()));
    await_into(f.get(), resp);
    return grpc::Status::OK;
  }

  grpc::Status ListTransactions(grpc::ServerContext*, const ListTransactionsRequest* req,
                                ListTransactionsResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    Owned<kafka_admin_ListTransactionsOptions_t, kafka_admin_ListTransactionsOptions_destroy> opts(
        kafka_admin_ListTransactionsOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_ListTransactionsOptions_set_timeout_ms);
    // The state filter crosses as TransactionState names; an empty list is
    // Java's empty set, i.e. the filter left unset.
    CList states;  // borrowed TransactionState singletons
    for (const std::string& name : req->states()) states.add(kafka_admin_TransactionState_parse(name.c_str()));
    if (req->states_size() > 0) kafka_admin_ListTransactionsOptions_filter_states(opts.get(), states.list);
    ScalarArena scalars;
    CList producer_ids;
    for (int64_t id : req->producer_ids()) producer_ids.add(scalars.i64(id));
    if (req->producer_ids_size() > 0) {
      kafka_admin_ListTransactionsOptions_filter_producer_ids(opts.get(), producer_ids.list);
    }
    // Passed straight through, as Java does: a negative duration disables the filter.
    kafka_admin_ListTransactionsOptions_filter_on_duration(opts.get(), req->duration_ms());
    // Presence decides: an absent pattern is Java's null (no filter), a
    // present one is passed as is, even when empty.
    if (req->has_transactional_id_pattern()) {
      kafka_admin_ListTransactionsOptions_filter_on_transactional_id_pattern(
          opts.get(), req->transactional_id_pattern().c_str());
    }
    Owned<kafka_admin_ListTransactionsResult_t, kafka_admin_ListTransactionsResult_destroy> result(
        kafka_admin_Admin_list_transactions_with_options(admin, opts.get()));
    OwnedFuture by_broker(kafka_admin_ListTransactionsResult_by_broker_id(result.get()));
    const kafka_Map_t* brokers = nullptr;
    kafka_common_Error_t* err = future_get(by_broker.get(), &brokers);
    if (err != nullptr) {
      fill_proto_error(resp->mutable_error(), err);
      return grpc::Status::OK;
    }
    // The inner futures are owned by the map, which the outer future owns.
    const int32_t n = brokers == nullptr ? 0 : kafka_Map_size(brokers);
    for (int32_t i = 0; i < n; i++) {
      ListTransactionsEntry* entry = resp->add_entries();
      set_broker_key(entry->mutable_key(), static_cast<const int32_t*>(kafka_Map_key(brokers, i)));
      const kafka_List_t* listings = nullptr;
      err = future_get(static_cast<const kafka_common_KafkaFuture_t*>(kafka_Map_value(brokers, i)), &listings);
      if (err != nullptr) {
        fill_proto_error(entry->mutable_error(), err);
        continue;
      }
      // An empty listing list is a successful "this broker has no transactions",
      // so the wrapper value is set even when empty.
      TransactionListingList* value = entry->mutable_value();
      for_each_in_list<kafka_admin_TransactionListing_t>(listings, [value](const kafka_admin_TransactionListing_t* l) {
        TransactionListing* dst = value->add_listings();
        dst->set_transactional_id(cstr(kafka_admin_TransactionListing_transactional_id(l)));
        dst->set_producer_id(kafka_admin_TransactionListing_producer_id(l));
        dst->set_state(transaction_state_name(kafka_admin_TransactionListing_state(l)));
      });
    }
    return grpc::Status::OK;
  }

  grpc::Status FenceProducers(grpc::ServerContext*, const FenceProducersRequest* req,
                              FenceProducersResponse* resp) override {
    const kafka_admin_Admin_t* admin = admin_for(req->admin_id());
    if (admin == nullptr) {
      *resp->mutable_error() = unknown_admin(req->admin_id());
      return grpc::Status::OK;
    }
    BorrowedStringList ids(req->transactional_ids());
    Owned<kafka_admin_FenceProducersOptions_t, kafka_admin_FenceProducersOptions_destroy> opts(
        kafka_admin_FenceProducersOptions_new());
    apply_timeout(*req, opts.get(), kafka_admin_FenceProducersOptions_set_timeout_ms);
    Owned<kafka_admin_FenceProducersResult_t, kafka_admin_FenceProducersResult_destroy> result(
        kafka_admin_Admin_fence_producers_with_options(admin, ids.list, opts.get()));
    for (const std::string& id : req->transactional_ids()) {
      FenceProducersEntry* entry = resp->add_entries();
      set_name_key(entry->mutable_key(), id.c_str());
      kafka_common_KafkaFuture_t* raw_pid = nullptr;
      kafka_common_Error_t* err = kafka_admin_FenceProducersResult_producer_id(result.get(), id.c_str(), &raw_pid);
      if (err != nullptr) {
        fill_proto_error(entry->mutable_error(), err);
        continue;
      }
      OwnedFuture pid_future(raw_pid);
      const int64_t* producer_id = nullptr;
      err = future_get(pid_future.get(), &producer_id);
      if (err != nullptr) {
        fill_proto_error(entry->mutable_error(), err);
        continue;
      }
      kafka_common_KafkaFuture_t* raw_epoch = nullptr;
      err = kafka_admin_FenceProducersResult_epoch_id(result.get(), id.c_str(), &raw_epoch);
      if (err != nullptr) {
        fill_proto_error(entry->mutable_error(), err);
        continue;
      }
      OwnedFuture epoch_future(raw_epoch);
      const int16_t* epoch = nullptr;
      err = future_get(epoch_future.get(), &epoch);
      if (err != nullptr) {
        fill_proto_error(entry->mutable_error(), err);
        continue;
      }
      ProducerIdAndEpoch* dst = entry->mutable_value();
      dst->set_producer_id(producer_id != nullptr ? *producer_id : -1);
      dst->set_epoch(epoch != nullptr ? *epoch : -1);
    }
    return grpc::Status::OK;
  }

  grpc::Status Close(grpc::ServerContext*, const AdminCloseRequest* req,
                     StatusResponse* resp) override {
    AdminEntry entry;
    {
      std::lock_guard<std::mutex> lock(mu_);
      auto it = admins_.find(req->admin_id());
      if (it == admins_.end()) {
        *resp->mutable_error() = unknown_admin(req->admin_id());
        return grpc::Status::OK;
      }
      entry = it->second;
      admins_.erase(it);
    }
    // Java's close(Duration) joins the background thread; the C call blocks
    // the same way and has no error slot. An absent timeout is Java's
    // close() (Long.MAX_VALUE ms).
    if (req->has_timeout_ms()) {
      kafka_admin_Admin_close_with_timeout(entry.view, req->timeout_ms());
    } else {
      kafka_admin_Admin_close(entry.view);
    }
    entry.destroy();
    std::cerr << "c server: closed admin " << req->admin_id() << std::endl;
    return grpc::Status::OK;
  }

 private:
  const kafka_admin_Admin_t* admin_for(uint64_t id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = admins_.find(id);
    return it == admins_.end() ? nullptr : it->second.view;
  }

  static KafkaError unknown_admin(uint64_t id) {
    return make_synthetic_error("unknown admin_id " + std::to_string(id));
  }

  static kafka_Bytes_t bytes_of(const std::string& s) {
    kafka_Bytes_t bytes;
    bytes.data = reinterpret_cast<const uint8_t*>(s.data());
    bytes.len = static_cast<int32_t>(s.size());
    return bytes;
  }

  // CreateTopicsResult's per-topic refinements (`topicId`, `numPartitions`,
  // `replicationFactor`, `config`): each is a future that fails when the
  // topic was created but the broker returned no metadata (Java's
  // `ensureSuccess()` rethrow), which must cross as the error arm, not as a
  // metadata of -1s.
  static void metadata_to_proto(const kafka_admin_CreateTopicsResult_t* result, const char* topic,
                                TopicMetadataAndConfig* dst) {
    TopicMetadata* metadata = dst->mutable_metadata();
    {
      OwnedFuture f(kafka_admin_CreateTopicsResult_topic_id(result, topic));
      const kafka_common_Uuid_t* id = nullptr;
      kafka_common_Error_t* err = future_get(f.get(), &id);
      if (err != nullptr) {
        dst->clear_metadata();
        fill_proto_error(dst->mutable_error(), err);
        return;
      }
      OwnedCString id_str(kafka_common_Uuid_to_string(id));
      metadata->set_topic_id(cstr(id_str.get()));
    }
    {
      OwnedFuture f(kafka_admin_CreateTopicsResult_num_partitions(result, topic));
      const int32_t* n = nullptr;
      kafka_common_Error_t* err = future_get(f.get(), &n);
      if (err != nullptr) {
        dst->clear_metadata();
        fill_proto_error(dst->mutable_error(), err);
        return;
      }
      metadata->set_num_partitions(n != nullptr ? *n : -1);
    }
    {
      OwnedFuture f(kafka_admin_CreateTopicsResult_replication_factor(result, topic));
      const int32_t* rf = nullptr;
      kafka_common_Error_t* err = future_get(f.get(), &rf);
      if (err != nullptr) {
        dst->clear_metadata();
        fill_proto_error(dst->mutable_error(), err);
        return;
      }
      metadata->set_replication_factor(rf != nullptr ? *rf : -1);
    }
    {
      OwnedFuture f(kafka_admin_CreateTopicsResult_config(result, topic));
      const kafka_admin_Config_t* config = nullptr;
      kafka_common_Error_t* err = future_get(f.get(), &config);
      if (err != nullptr) {
        dst->clear_metadata();
        fill_proto_error(dst->mutable_error(), err);
        return;
      }
      if (config != nullptr) config_to_proto(config, [metadata] { return metadata->add_configs(); });
    }
  }

  static void log_dir_description_to_proto(const kafka_admin_LogDirDescription_t* description,
                                           LogDirDescription* dst) {
    // The log dir's own error: the broker answered, but this directory is
    // offline or unreadable. Not the per-broker error.
    const kafka_common_Error_t* err = kafka_admin_LogDirDescription_error(description);
    if (err != nullptr) copy_proto_error(dst->mutable_error(), err);
    // Java's totalBytes() / usableBytes() are OptionalLong; the C surface spells
    // an empty one -1 (DescribeLogDirsResponse.UNKNOWN_VOLUME_BYTES), so a
    // negative stays absent on the wire.
    const int64_t total = kafka_admin_LogDirDescription_total_bytes(description);
    if (total >= 0) dst->set_total_bytes(total);
    const int64_t usable = kafka_admin_LogDirDescription_usable_bytes(description);
    if (usable >= 0) dst->set_usable_bytes(usable);
    OwnedMap replicas(kafka_admin_LogDirDescription_replica_infos(description));
    const int32_t n = replicas ? kafka_Map_size(replicas.get()) : 0;
    for (int32_t i = 0; i < n; i++) {
      ReplicaInfoEntry* replica = dst->add_replica_infos();
      tp_to_proto(static_cast<const kafka_common_TopicPartition_t*>(kafka_Map_key(replicas.get(), i)),
                  replica->mutable_partition());
      const auto* info = static_cast<const kafka_admin_ReplicaInfo_t*>(kafka_Map_value(replicas.get(), i));
      replica->set_size(kafka_admin_ReplicaInfo_size(info));
      replica->set_offset_lag(kafka_admin_ReplicaInfo_offset_lag(info));
      replica->set_is_future(kafka_admin_ReplicaInfo_is_future(info) != 0);
    }
  }

  // Copies an owned list of boxed `int32_t *` through `add`, then frees it.
  template <typename Add>
  static void int32_list_to_proto(kafka_List_t* ids, Add add) {
    OwnedList owned(ids);
    for_each_in_list<int32_t>(owned.get(), [&add](const int32_t* id) { add(*id); });
  }

  // Maps the proto OffsetSpec onto the Java OffsetSpec factories: the five
  // constant kinds are singletons, FOR_TIMESTAMP is an owned handle kept in
  // `owned`. Null for KIND_UNSPECIFIED, an unknown kind, or FOR_TIMESTAMP
  // without a timestamp.
  static const kafka_admin_OffsetSpec_t* offset_spec_for(
      const OffsetSpec& spec, OwnedHandles<kafka_admin_OffsetSpec_t, kafka_admin_OffsetSpec_destroy>* owned) {
    switch (spec.kind()) {
      case OffsetSpec::EARLIEST:
        return kafka_admin_OffsetSpec_earliest();
      case OffsetSpec::LATEST:
        return kafka_admin_OffsetSpec_latest();
      case OffsetSpec::MAX_TIMESTAMP:
        return kafka_admin_OffsetSpec_max_timestamp();
      case OffsetSpec::EARLIEST_LOCAL:
        return kafka_admin_OffsetSpec_earliest_local();
      case OffsetSpec::LATEST_TIERED:
        return kafka_admin_OffsetSpec_latest_tiered();
      case OffsetSpec::EARLIEST_PENDING_UPLOAD:
        return kafka_admin_OffsetSpec_earliest_pending_upload();
      case OffsetSpec::FOR_TIMESTAMP:
        if (!spec.has_timestamp()) return nullptr;
        return owned->add(kafka_admin_OffsetSpec_for_timestamp(spec.timestamp()));
      default:
        return nullptr;
    }
  }

  // Builds owned `kafka_common_acl_AclBinding_t` handles from proto bindings,
  // keeping the pattern and entry handles they were built from alive.
  struct AclBindingArgs {
    kafka_common_Error_t* build(const AclBinding& b, kafka_common_acl_AclBinding_t** out) {
      kafka_common_resource_ResourcePattern_t* pattern = nullptr;
      kafka_common_Error_t* err = kafka_common_resource_ResourcePattern_new(
          kafka_common_resource_ResourceType_from_code(static_cast<int8_t>(b.resource_type())),
          b.resource_name().c_str(),
          kafka_common_resource_PatternType_from_code(static_cast<int8_t>(b.pattern_type())), &pattern);
      if (err != nullptr) return err;
      patterns.add(pattern);
      kafka_common_acl_AccessControlEntry_t* entry = nullptr;
      err = kafka_common_acl_AccessControlEntry_new(
          b.principal().c_str(), b.host().c_str(),
          kafka_common_acl_AclOperation_from_code(static_cast<int8_t>(b.operation())),
          kafka_common_acl_AclPermissionType_from_code(static_cast<int8_t>(b.permission_type())), &entry);
      if (err != nullptr) return err;
      entries.add(entry);
      *out = bindings.add(kafka_common_acl_AclBinding_new(pattern, entry));
      return nullptr;
    }
    OwnedHandles<kafka_common_resource_ResourcePattern_t, kafka_common_resource_ResourcePattern_destroy> patterns;
    OwnedHandles<kafka_common_acl_AccessControlEntry_t, kafka_common_acl_AccessControlEntry_destroy> entries;
    OwnedHandles<kafka_common_acl_AclBinding_t, kafka_common_acl_AclBinding_destroy> bindings;
  };

  // Same for `kafka_common_acl_AclBindingFilter_t`: the optional proto
  // fields are Java's nullable "match any" filter values.
  struct AclFilterArgs {
    const kafka_common_acl_AclBindingFilter_t* build(const AclBindingFilter& f) {
      kafka_common_resource_ResourcePatternFilter_t* pattern = patterns.add(
          kafka_common_resource_ResourcePatternFilter_new(
              kafka_common_resource_ResourceType_from_code(static_cast<int8_t>(f.resource_type())),
              f.has_resource_name() ? f.resource_name().c_str() : nullptr,
              kafka_common_resource_PatternType_from_code(static_cast<int8_t>(f.pattern_type()))));
      kafka_common_acl_AccessControlEntryFilter_t* entry = entries.add(
          kafka_common_acl_AccessControlEntryFilter_new(
              f.has_principal() ? f.principal().c_str() : nullptr,
              f.has_host() ? f.host().c_str() : nullptr,
              kafka_common_acl_AclOperation_from_code(static_cast<int8_t>(f.operation())),
              kafka_common_acl_AclPermissionType_from_code(static_cast<int8_t>(f.permission_type()))));
      return filters.add(kafka_common_acl_AclBindingFilter_new(pattern, entry));
    }
    OwnedHandles<kafka_common_resource_ResourcePatternFilter_t, kafka_common_resource_ResourcePatternFilter_destroy>
        patterns;
    OwnedHandles<kafka_common_acl_AccessControlEntryFilter_t, kafka_common_acl_AccessControlEntryFilter_destroy>
        entries;
    OwnedHandles<kafka_common_acl_AclBindingFilter_t, kafka_common_acl_AclBindingFilter_destroy> filters;
  };

  // `new ClientQuotaEntity(Map<String, String>)`: an absent entity_name is
  // Java's null, naming the built-in default entity for its type.
  static kafka_common_quota_ClientQuotaEntity_t* quota_entity_from_proto(const ClientQuotaEntity& e) {
    CMap entries;
    for (const auto& entry : e.entries()) {
      entries.put(entry.entity_type().c_str(), entry.has_entity_name() ? entry.entity_name().c_str() : nullptr);
    }
    return kafka_common_quota_ClientQuotaEntity_new(entries.map);
  }

  std::mutex mu_;
  std::unordered_map<uint64_t, AdminEntry> admins_;
  std::atomic<uint64_t> next_id_{1};
};

}  // namespace

int main(int /*argc*/, char** /*argv*/) {
  // GRPC_PORT=0 binds an ephemeral port; the bound port is reported on the
  // "listening" line below.
  long port = 50052;
  if (const char* env = std::getenv("GRPC_PORT")) {
    char* end = nullptr;
    errno = 0;
    port = std::strtol(env, &end, 10);
    if (*env == '\0' || *end != '\0' || errno != 0 || port < 0 || port > 65535) {
      std::cerr << "c server: invalid GRPC_PORT '" << env
                << "': expected an integer in 0-65535" << std::endl;
      return 1;
    }
  }
  // Defaults to 127.0.0.1 because the server is unauthenticated. The Docker
  // image sets GRPC_HOST=0.0.0.0 so it is reachable from outside the container.
  std::string host = "127.0.0.1";
  if (const char* env = std::getenv("GRPC_HOST")) {
    host = env;
  }
  const std::string address = host + ":" + std::to_string(port);

  grpc::ServerBuilder builder;
  int selected_port = 0;
  builder.AddListeningPort(address, grpc::InsecureServerCredentials(),
                           &selected_port);
  // Outlives both services, which share it.
  GroupMetadataStore group_metadata;
  ProducerServiceImpl producer_service(&group_metadata);
  ConsumerServiceImpl consumer_service(&group_metadata);
  AdminServiceImpl admin_service;
  builder.RegisterService(&producer_service);
  builder.RegisterService(&consumer_service);
  builder.RegisterService(&admin_service);

  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  // selected_port stays 0 if the address could not be bound.
  if (!server || selected_port == 0) {
    std::cerr << "c server: failed to start gRPC server on " << address
              << std::endl;
    return 1;
  }
  // The Rust BackendPool waits for "listening" on stderr and parses the bound
  // port from the last ':'-separated field — keep this format in sync with
  // backend_pool.rs.
  std::cerr << "c server: listening on " << host << ":" << selected_port
            << std::endl;
  server->Wait();
  return 0;
}
