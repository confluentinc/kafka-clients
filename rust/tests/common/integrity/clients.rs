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

//! The clients a run checks: the Rust client in process, or a binding's (the
//! Python sync and asyncio clients, the C client) driven through the gRPC
//! multi-language test servers (`multilanguage-test-server`). Either way the
//! records pass through the client's own send and poll, and the checker sees
//! exactly what the client handed out.
//!
//! Over gRPC a send returns only once the record is acknowledged, so a writer
//! has one record in flight: a batch holds one record, and the binding is
//! checked for what it carries (bytes, headers, nulls, timestamps, offsets,
//! transactions), not for batching. A listener cannot cross the wire, so a
//! member subscribes with the server's own logging listener, which also makes
//! a revocation wait for the member's next poll.

use std::fmt;
use std::sync::Arc;

use confluent_kafka::common::Error;
use confluent_kafka::common::serialization::{ByteArrayDeserializer, ByteArraySerializer};
use confluent_kafka::consumer::{
    ConsumerConfig, ConsumerHandle, ConsumerRebalanceListener, DynConsumer, KafkaConsumer,
};
use confluent_kafka::producer::{DynProducer, KafkaProducer, ProducerConfig};

use super::workload::Props;

pub type BoxProducer = Box<dyn DynProducer<Vec<u8>, Vec<u8>>>;
pub type BoxConsumer = Box<dyn DynConsumer<Vec<u8>, Vec<u8>>>;

#[derive(Clone)]
enum Backend {
    Rust,
    #[cfg(feature = "multilanguage-tests")]
    Grpc {
        channel: tonic::transport::Channel,
        name: &'static str,
        /// Keeps the server running while the run uses it.
        _server: Arc<crate::common::backend_pool::BackendHandle>,
    },
}

/// Creates the producers and consumers of a run.
#[derive(Clone)]
pub struct Clients {
    backend: Backend,
    /// The bootstrap and security properties of these clients, when they
    /// differ from the run's (a server in a container reaches the brokers at
    /// other addresses). The run's own admin and setup clients keep the run's.
    props: Option<Arc<Props>>,
}

impl fmt::Debug for Clients {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl Default for Clients {
    fn default() -> Self {
        Self::rust()
    }
}

impl Clients {
    /// The Rust client, in process.
    pub fn rust() -> Self {
        Self { backend: Backend::Rust, props: None }
    }

    /// A binding's client behind the gRPC server of `server`, connecting with
    /// `props` (bootstrap servers and security).
    #[cfg(feature = "multilanguage-tests")]
    pub async fn grpc(
        server: Arc<crate::common::backend_pool::BackendHandle>,
        name: &'static str,
        props: Props,
    ) -> Self {
        let channel = server.channel().await;
        Self {
            backend: Backend::Grpc { channel, name, _server: server },
            props: Some(Arc::new(props)),
        }
    }

    /// `rust`, or the binding's label (`python`, `python_async`, `c`).
    pub fn name(&self) -> &'static str {
        match &self.backend {
            Backend::Rust => "rust",
            #[cfg(feature = "multilanguage-tests")]
            Backend::Grpc { name, .. } => name,
        }
    }

    /// What these clients' properties start from: their own, or the run's.
    pub fn base(&self, run: &Arc<Props>) -> Arc<Props> {
        self.props.clone().unwrap_or_else(|| run.clone())
    }

    pub async fn producer(&self, props: Props) -> Result<BoxProducer, Error> {
        match &self.backend {
            Backend::Rust => Ok(Box::new(KafkaProducer::new(
                ProducerConfig::new(&props)?,
                ByteArraySerializer::default(),
                ByteArraySerializer::default(),
            )?)),
            #[cfg(feature = "multilanguage-tests")]
            Backend::Grpc { channel, name, .. } => {
                // One record is in flight at a time (see the module docs):
                // lingering would only delay each one.
                let mut props = props;
                props.insert("linger.ms".to_string(), "0".to_string());
                Ok(Box::new(
                    crate::common::multilanguage_producer::MultilanguageProducer::new(channel.clone(), props, name)
                        .await?,
                ))
            },
        }
    }

    pub async fn consumer(&self, props: Props) -> Result<BoxConsumer, Error> {
        match &self.backend {
            Backend::Rust => Ok(Box::new(KafkaConsumer::new(
                ConsumerConfig::new(&props)?,
                ByteArrayDeserializer::default(),
                ByteArrayDeserializer::default(),
            )?)),
            #[cfg(feature = "multilanguage-tests")]
            Backend::Grpc { channel, name, .. } => Ok(Box::new(
                crate::common::multilanguage_consumer::MultilanguageConsumer::new(channel.clone(), props, name).await?,
            )),
        }
    }

    /// A consumer subscribed to `topics` with a rebalance listener: the Rust
    /// client's is `listener`, made from the consumer's handle; a binding's is
    /// the server's logging listener. A consumer that fails to subscribe is
    /// closed.
    pub async fn subscribed_consumer(
        &self,
        props: Props,
        topics: Vec<String>,
        listener: impl FnOnce(ConsumerHandle) -> Arc<dyn ConsumerRebalanceListener>,
    ) -> Result<BoxConsumer, Error> {
        match &self.backend {
            Backend::Rust => {
                let mut consumer = self.consumer(props).await?;
                let listener = listener(consumer.handle());
                match consumer.subscribe_with_topics_listener(topics, listener).await {
                    Ok(()) => Ok(consumer),
                    Err(e) => {
                        let _ = consumer.close().await;
                        Err(e)
                    },
                }
            },
            #[cfg(feature = "multilanguage-tests")]
            Backend::Grpc { channel, name, .. } => {
                let mut consumer =
                    crate::common::multilanguage_consumer::MultilanguageConsumer::new(channel.clone(), props, name)
                        .await?;
                let log =
                    crate::common::callback_log::grpc::ConsumerLog::new(channel.clone(), consumer.consumer_id(), name);
                match log.subscribe_with_listener(topics).await {
                    Ok(()) => Ok(Box::new(consumer)),
                    Err(e) => {
                        let _ = consumer.close().await;
                        Err(e)
                    },
                }
            },
        }
    }

    /// Whether a member's listener can commit from `on_partitions_revoked`:
    /// only the Rust client's, since a binding's listener runs in its server.
    pub fn commits_on_revoke(&self) -> bool {
        matches!(self.backend, Backend::Rust)
    }
}
