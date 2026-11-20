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

//! The result of `Admin::delete_share_group_offsets_with_options`.

use std::collections::{HashMap, HashSet};

use crate::common::{Error, KafkaFuture};

/// The per-topic errors carried by the underlying future: `None` for a topic
/// whose offsets were deleted (Java's `null` value).
type TopicErrors = HashMap<String, Option<Error>>;

/// The result of the `Admin::delete_share_group_offsets_with_options` call.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.admin.DeleteShareGroupOffsetsResult")]
pub struct DeleteShareGroupOffsetsResult {
    future: KafkaFuture<TopicErrors>,
    topics: HashSet<String>,
}

impl DeleteShareGroupOffsetsResult {
    /// Creates a result wrapping the single group's per-topic-error future and
    /// the topics of the original request.
    #[doc(alias = "org.apache.kafka.clients.admin.DeleteShareGroupOffsetsResult#DeleteShareGroupOffsetsResult")]
    pub(crate) fn new(future: KafkaFuture<TopicErrors>, topics: HashSet<String>) -> Self {
        Self { future, topics }
    }

    /// Returns a future which succeeds only if all the deletions succeed. If not, the first topic
    /// error shall be returned.
    #[doc(alias = "org.apache.kafka.clients.admin.DeleteShareGroupOffsetsResult#all")]
    pub fn all(&self) -> KafkaFuture<()> {
        // Java walks the topic set in its iteration order, which is unspecified;
        // sorting keeps "the first topic error" stable.
        let mut topics: Vec<String> = self.topics.iter().cloned().collect();
        topics.sort();
        self.future.then_apply(move |topic_results: TopicErrors| {
            for topic in &topics {
                maybe_complete_with_error(&topic_results, topic)?;
            }
            Ok(())
        })
    }

    /// Returns a future which can be used to check the result for a given topic.
    ///
    /// # Arguments
    ///
    /// - `topic` - the topic to check.
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` if the topic was not included in the original
    ///   request.
    #[doc(alias = "org.apache.kafka.clients.admin.DeleteShareGroupOffsetsResult#topicResult")]
    pub fn topic_result(&self, topic: &str) -> Result<KafkaFuture<()>, Error> {
        if !self.topics.contains(topic) {
            return Err(Error::local_illegal_argument(format!(
                "Topic {topic} was not included in the original request"
            )));
        }
        let topic = topic.to_string();
        Ok(self
            .future
            .then_apply(move |topic_results: TopicErrors| maybe_complete_with_error(&topic_results, &topic)))
    }
}

/// The outcome for `topic`: an absent topic fails with the "not included in
/// the response" `IllegalArgumentException`, a present one with its error, and
/// a topic that succeeded is `Ok`.
///
/// Java's version completes the caller's future with the error and returns
/// whether it did; the Rust callers complete theirs from this `Err` instead.
#[doc(alias = "org.apache.kafka.clients.admin.DeleteShareGroupOffsetsResult#maybeCompleteExceptionally")]
fn maybe_complete_with_error(topic_level_errors: &TopicErrors, topic: &str) -> Result<(), Error> {
    match topic_level_errors.get(topic) {
        None => Err(Error::local_illegal_argument(format!(
            "Offset deletion result for topic \"{topic}\" was not included in the response"
        ))),
        Some(Some(error)) => Err(error.clone()),
        Some(None) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::internals::KafkaFutureImpl;
    use crate::common::protocol::Errors;

    fn topics() -> HashSet<String> {
        HashSet::from(["foo".to_string(), "bar".to_string()])
    }

    fn result(errors: TopicErrors, topics: HashSet<String>) -> DeleteShareGroupOffsetsResult {
        let handle: KafkaFutureImpl<TopicErrors> = KafkaFutureImpl::new();
        handle.complete(errors);
        DeleteShareGroupOffsetsResult::new(handle.future(), topics)
    }

    /// New test, no Java original (Java has no `DeleteShareGroupOffsetsResultTest`):
    /// no topic errors, so `all()` and every topic succeed.
    #[tokio::test]
    async fn succeeds_when_no_topic_failed() {
        let result = result(HashMap::from([("foo".to_string(), None), ("bar".to_string(), None)]), topics());
        assert_eq!(result.all().get().await.unwrap(), ());
        assert_eq!(result.topic_result("foo").unwrap().get().await.unwrap(), ());
        assert_eq!(result.topic_result("bar").unwrap().get().await.unwrap(), ());
    }

    /// New test, no Java original: a topic outside the request fails at once
    /// with Java's message.
    #[test]
    fn topic_not_in_request_is_illegal_argument() {
        let result = result(HashMap::new(), topics());
        let error = result.topic_result("zoo").unwrap_err();
        assert!(error.is_local_illegal_argument_error(), "{error:?}");
        assert_eq!(error.message(), "Topic zoo was not included in the original request");
    }

    /// New test, no Java original: a topic error fails that topic and `all()`,
    /// and leaves the other topic successful.
    #[tokio::test]
    async fn topic_error_surfaces_in_topic_result_and_all() {
        let failure = Errors::KafkaStorageError.error_with_optional_message(Some("disk"));
        let result = result(
            HashMap::from([("foo".to_string(), Some(failure)), ("bar".to_string(), None)]),
            topics(),
        );
        let error = result.all().get().await.unwrap_err();
        assert_eq!(error.error(), Errors::KafkaStorageError);
        assert_eq!(error.message(), "disk");
        assert_eq!(
            result.topic_result("foo").unwrap().get().await.unwrap_err().error(),
            Errors::KafkaStorageError
        );
        assert_eq!(result.topic_result("bar").unwrap().get().await.unwrap(), ());
    }

    /// New test, no Java original: a requested topic missing from the response
    /// fails with Java's "not included in the response" message.
    #[tokio::test]
    async fn topic_missing_in_response_is_illegal_argument() {
        let result = result(HashMap::from([("foo".to_string(), None)]), topics());
        let error = result.all().get().await.unwrap_err();
        assert!(error.is_local_illegal_argument_error(), "{error:?}");
        assert_eq!(
            error.message(),
            "Offset deletion result for topic \"bar\" was not included in the response"
        );
        assert_eq!(result.topic_result("foo").unwrap().get().await.unwrap(), ());
        let error = result.topic_result("bar").unwrap().get().await.unwrap_err();
        assert_eq!(
            error.message(),
            "Offset deletion result for topic \"bar\" was not included in the response"
        );
    }

    /// New test, no Java original: a failed request fails `all()` and every
    /// topic with the request's error.
    #[tokio::test]
    async fn request_error_fails_everything() {
        let handle: KafkaFutureImpl<TopicErrors> = KafkaFutureImpl::new();
        handle.complete_with_error(Error::group_authorization("group"));
        let result = DeleteShareGroupOffsetsResult::new(handle.future(), topics());
        assert!(result.all().get().await.unwrap_err().is_group_authorization_error());
        assert!(
            result
                .topic_result("foo")
                .unwrap()
                .get()
                .await
                .unwrap_err()
                .is_group_authorization_error()
        );
    }
}
