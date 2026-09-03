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

//! The result of the `Admin::describe_user_scram_credentials` call.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult`.

use std::collections::HashMap;

use crate::common::protocol::Errors;
use crate::common::{Error, KafkaFuture};
use crate::describe_user_scram_credentials_response_data::{
    DescribeUserScramCredentialsResponseData, DescribeUserScramCredentialsResult as WireUserResult,
};

use super::{ScramCredentialInfo, ScramMechanism, UserScramCredentialsDescription};

/// The result of the `Admin::describe_user_scram_credentials` call.
///
/// Corresponds to
/// `org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult`.
///
/// Holds a single future over the raw response data; `all()`, `users()` and
/// `description()` are per-view refinements (Java uses `whenComplete` on the
/// data future to complete new `KafkaFutureImpl`s — here `then_apply` /
/// `then_apply_try` express the same transform-on-completion).
#[derive(Debug, Clone)]
pub struct DescribeUserScramCredentialsResult {
    data_future: KafkaFuture<DescribeUserScramCredentialsResponseData>,
}

impl DescribeUserScramCredentialsResult {
    /// Creates a new result from the future indicating response data from the
    /// call.
    pub fn new(data_future: KafkaFuture<DescribeUserScramCredentialsResponseData>) -> Self {
        Self { data_future }
    }

    /// Returns a future for the results of all described users, keyed by user
    /// name. The future completes successfully only if all such user
    /// descriptions complete successfully.
    ///
    /// Mirrors `DescribeUserScramCredentialsResult.all()`. A successfully
    /// described user is one with *either* a `NONE` or a `RESOURCE_NOT_FOUND`
    /// error code; the first user with any other error code fails the whole
    /// future.
    pub fn all(&self) -> KafkaFuture<HashMap<String, UserScramCredentialsDescription>> {
        self.data_future.then_apply_try(|data| {
            if let Some(first_failed) = data.results.iter().find(|result| {
                result.error_code != Errors::None.code() && result.error_code != Errors::ResourceNotFound.code()
            }) {
                return Err(api_error(first_failed.error_code, &first_failed.error_message));
            }
            let mut map = HashMap::with_capacity(data.results.len());
            for user_result in &data.results {
                map.insert(
                    user_result.user.clone(),
                    UserScramCredentialsDescription::new(
                        user_result.user.clone(),
                        scram_credential_infos_for(user_result),
                    ),
                );
            }
            Ok(map)
        })
    }

    /// Returns a future indicating the distinct users that meet the request
    /// criteria and that have at least one credential.
    ///
    /// Mirrors `DescribeUserScramCredentialsResult.users()`. Users that do not
    /// exist / have no credential (`RESOURCE_NOT_FOUND`) are excluded; users that
    /// have a credential but could not be described are included.
    pub fn users(&self) -> KafkaFuture<Vec<String>> {
        self.data_future.then_apply(|data| {
            data.results
                .iter()
                .filter(|result| result.error_code != Errors::ResourceNotFound.code())
                .map(|result| result.user.clone())
                .collect()
        })
    }

    /// Returns a future indicating the description results for the given user.
    ///
    /// Mirrors `DescribeUserScramCredentialsResult.description(String)`. If the
    /// given user is not present in the described users, the future completes
    /// exceptionally with a `RESOURCE_NOT_FOUND` error ("No such user: ...").
    pub fn description(&self, user_name: &str) -> KafkaFuture<UserScramCredentialsDescription> {
        let user_name = user_name.to_string();
        self.data_future.then_apply_try(move |data| {
            match data.results.iter().find(|result| result.user == user_name) {
                None => Err(Error::with_message(
                    Errors::ResourceNotFound,
                    format!("No such user: {user_name}"),
                )),
                Some(user_result) => {
                    if user_result.error_code != Errors::None.code() {
                        // RESOURCE_NOT_FOUND is included here.
                        Err(api_error(user_result.error_code, &user_result.error_message))
                    } else {
                        Ok(UserScramCredentialsDescription::new(
                            user_result.user.clone(),
                            scram_credential_infos_for(user_result),
                        ))
                    }
                },
            }
        })
    }
}

/// Mirrors `DescribeUserScramCredentialsResult.getScramCredentialInfosFor`.
fn scram_credential_infos_for(user_result: &WireUserResult) -> Vec<ScramCredentialInfo> {
    user_result
        .credential_infos
        .iter()
        .map(|info| ScramCredentialInfo::new(ScramMechanism::from_type(info.mechanism), info.iterations))
        .collect()
}

/// Builds a [`Error`] from a wire error code and optional message, mirroring
/// `Errors.forCode(code).exception(message)`.
fn api_error(code: i16, message: &Option<String>) -> Error {
    let error = Errors::for_code(code);
    // `Errors.exception(String)` falls back to the code's default text only when
    // the message is **null** (`Errors.java:461-468`); a non-null empty string is
    // passed straight through to the builder. Treating `Some("")` as absent would
    // substitute the default where the broker deliberately sent none.
    match message {
        Some(m) => Error::with_message(error, m.clone()),
        None => Error::new(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::describe_user_scram_credentials_response_data::CredentialInfo;

    fn user_result(user: &str, error_code: i16, infos: Vec<(i8, i32)>) -> WireUserResult {
        let mut r = WireUserResult::new();
        r.set_user(user.to_string()).set_error_code(error_code);
        r.set_credential_infos(
            infos
                .into_iter()
                .map(|(mech, it)| {
                    let mut ci = CredentialInfo::new();
                    ci.set_mechanism(mech).set_iterations(it);
                    ci
                })
                .collect(),
        );
        r
    }

    fn data_future_completed(results: Vec<WireUserResult>) -> KafkaFuture<DescribeUserScramCredentialsResponseData> {
        let mut data = DescribeUserScramCredentialsResponseData::new();
        data.set_error_code(Errors::None.code()).set_results(results);
        KafkaFuture::completed(Ok(data))
    }

    // Mirrors `DescribeUserScramCredentialsResultTest.testTopLevelError`.
    #[tokio::test]
    async fn test_top_level_error() {
        let data_future: KafkaFuture<DescribeUserScramCredentialsResponseData> =
            KafkaFuture::completed(Err(Error::new(Errors::UnknownServerError)));
        let results = DescribeUserScramCredentialsResult::new(data_future);
        assert!(results.all().get().await.is_err());
        assert!(results.users().get().await.is_err());
        assert!(results.description("whatever").get().await.is_err());
    }

    // Mirrors `DescribeUserScramCredentialsResultTest.testUserLevelErrors`.
    #[tokio::test]
    async fn test_user_level_errors() {
        let good_user = "goodUser";
        let unknown_user = "unknownUser";
        let failed_user = "failedUser";
        let iterations = 4096;
        let sha256 = ScramMechanism::ScramSha256;
        let data_future = data_future_completed(vec![
            user_result(good_user, Errors::None.code(), vec![(sha256.r#type(), iterations)]),
            user_result(unknown_user, Errors::ResourceNotFound.code(), vec![]),
            user_result(failed_user, Errors::DuplicateResource.code(), vec![]),
        ]);
        let results = DescribeUserScramCredentialsResult::new(data_future);

        assert!(
            results.all().get().await.is_err(),
            "expected all() to fail on a user-level error"
        );

        let users = results.users().get().await.unwrap();
        assert_eq!(
            users,
            vec![good_user.to_string(), failed_user.to_string()],
            "Expected 2 users with credentials"
        );

        let good_description = results.description(good_user).get().await.unwrap();
        assert_eq!(
            good_description,
            UserScramCredentialsDescription::new(good_user, vec![ScramCredentialInfo::new(sha256, iterations)])
        );
        assert!(results.description(failed_user).get().await.is_err());
        assert!(results.description(unknown_user).get().await.is_err());
    }

    // Mirrors `DescribeUserScramCredentialsResultTest.testSuccessfulDescription`.
    #[tokio::test]
    async fn test_successful_description() {
        let good_user = "goodUser";
        let unknown_user = "unknownUser";
        let iterations = 4096;
        let sha256 = ScramMechanism::ScramSha256;
        let data_future = data_future_completed(vec![user_result(
            good_user,
            Errors::None.code(),
            vec![(sha256.r#type(), iterations)],
        )]);
        let results = DescribeUserScramCredentialsResult::new(data_future);

        assert_eq!(results.users().get().await.unwrap(), vec![good_user.to_string()]);
        let all_results = results.all().get().await.unwrap();
        assert_eq!(all_results.len(), 1);
        let via_all = all_results.get(good_user).unwrap().clone();
        assert_eq!(
            via_all,
            UserScramCredentialsDescription::new(good_user, vec![ScramCredentialInfo::new(sha256, iterations)])
        );
        assert_eq!(via_all, results.description(good_user).get().await.unwrap());
        assert!(results.description(unknown_user).get().await.is_err());
    }
}
