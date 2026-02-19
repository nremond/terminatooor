//! Hand-written tonic gRPC types for the Jito ShredStream proxy service.
//!
//! This avoids pulling in `solana-entry` (heavy, pulls GPU/CUDA deps) and
//! `jito-protos` (unpublished on crates.io). The types are wire-compatible
//! with the ShredstreamProxy gRPC service.

use solana_sdk::hash::Hash;
use solana_sdk::transaction::VersionedTransaction;

/// gRPC message: a batch of entries for a given slot.
/// Wire format matches `shredstream.Entry` in the proxy's proto definition.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EntryMessage {
    #[prost(uint64, tag = "1")]
    pub slot: u64,
    #[prost(bytes = "vec", tag = "2")]
    pub entries: ::prost::alloc::vec::Vec<u8>,
}

/// gRPC request for SubscribeEntries (empty message).
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SubscribeEntriesRequest {}

/// Solana entry struct for bincode deserialization.
/// Binary-compatible with `solana_entry::entry::Entry` without the heavy dependency.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct SolanaEntry {
    pub num_hashes: u64,
    pub hash: Hash,
    pub transactions: Vec<VersionedTransaction>,
}

/// Minimal tonic gRPC client for the ShredstreamProxy service.
pub mod shredstream_proxy_client {
    /// Client for the `shredstream.ShredstreamProxy` gRPC service.
    /// Only supports connecting via URL (tonic Channel transport).
    #[derive(Debug, Clone)]
    pub struct ShredstreamProxyClient {
        inner: tonic::client::Grpc<tonic::transport::Channel>,
    }

    impl ShredstreamProxyClient {
        /// Connect to the ShredstreamProxy gRPC endpoint.
        pub async fn connect(dst: String) -> Result<Self, tonic::transport::Error> {
            let conn = tonic::transport::Endpoint::from_shared(dst)?
                .connect()
                .await?;
            Ok(Self {
                inner: tonic::client::Grpc::new(conn),
            })
        }

        /// Subscribe to decoded entries from the ShredStream proxy.
        pub async fn subscribe_entries(
            &mut self,
            request: impl tonic::IntoRequest<super::SubscribeEntriesRequest>,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::EntryMessage>>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service not ready: {}", e),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path: tonic::codegen::http::uri::PathAndQuery =
                "/shredstream.ShredstreamProxy/SubscribeEntries"
                    .parse()
                    .unwrap();
            self.inner
                .server_streaming(request.into_request(), path, codec)
                .await
        }
    }
}
