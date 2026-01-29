use std::collections::HashMap;
use std::time::Duration;

use anchor_lang::prelude::Pubkey;
use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use solana_sdk::address_lookup_table::AddressLookupTableAccount;
use solana_sdk::instruction::{AccountMeta, Instruction};
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, info, warn};

/// Configuration for Titan API connection
#[derive(Clone)]
pub struct TitanConfig {
    /// WebSocket endpoint (e.g., wss://your-endpoint.rpcpool.com/your-token/titan/api/v1/ws)
    pub endpoint: String,
    /// API token for authentication (JWT)
    pub api_token: Option<String>,
}

impl TitanConfig {
    pub fn new(endpoint: String, api_token: Option<String>) -> Self {
        // Ensure endpoint ends with /ws
        let endpoint = if endpoint.ends_with("/ws") {
            endpoint
        } else {
            format!("{}/ws", endpoint.trim_end_matches('/'))
        };
        Self { endpoint, api_token }
    }

    pub fn from_env() -> Result<Self> {
        let endpoint = std::env::var("TITAN_ENDPOINT")
            .map_err(|_| anyhow!("TITAN_ENDPOINT environment variable not set"))?;
        let api_token = std::env::var("TITAN_API_TOKEN").ok();
        Ok(Self::new(endpoint, api_token))
    }
}

/// Decompiled transaction with instructions and lookup tables
#[derive(Debug, Clone)]
pub struct DecompiledVersionedTx {
    pub instructions: Vec<Instruction>,
    pub lookup_tables: Option<Vec<AddressLookupTableAccount>>,
}

/// Swap route information
#[derive(Debug, Clone)]
pub struct SwapRoute {
    pub in_amount: u64,
    pub out_amount: u64,
    pub slippage_bps: u16,
    pub price_impact_pct: String,
    pub instructions: Vec<Instruction>,
    pub address_lookup_tables: Vec<Pubkey>,
}

/// Price information for a token
#[derive(Debug, Clone)]
pub struct SwapPrice {
    pub price: f32,
    pub mint: String,
}

/// Error types for Titan operations
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("No valid route found")]
    NoValidRoute,
    #[error("Price impact too high: {0}%")]
    PriceImpactTooHigh(f32),
    #[error("Response type conversion error")]
    ResponseTypeConversionError,
    #[error("Connection error: {0}")]
    ConnectionError(String),
    #[error("Request error: {0}")]
    RequestError(String),
    #[error("Configuration error: {0}")]
    ConfigError(String),
    #[error("Timeout waiting for response")]
    Timeout,
}

pub type TitanResult<T> = std::result::Result<T, Error>;

// ============================================================================
// Titan API Protocol Types (MessagePack encoded with camelCase)
// ============================================================================

#[derive(Debug)]
enum TitanRequest {
    NewSwapQuoteStream(SwapQuoteStreamRequest),
    GetSwapPrice(SwapPriceRequest),
    StopStream { id: String },
}

#[derive(Debug)]
struct SwapQuoteStreamRequest {
    #[allow(dead_code)]
    request_id: String,
    swap: SwapParams,
    transaction: TransactionParams,
    update: Option<QuoteUpdateParams>,
}

#[derive(Debug)]
struct SwapPriceRequest {
    input_mint: Pubkey,
    output_mint: Pubkey,
    amount: u64,
}

#[derive(Debug)]
struct SwapParams {
    input_mint: Pubkey,
    output_mint: Pubkey,
    amount: u64,
    swap_mode: Option<String>,
    slippage_bps: Option<u16>,
    only_direct_routes: Option<bool>,
    max_accounts: Option<u8>,
}

#[derive(Debug)]
struct TransactionParams {
    user_public_key: Pubkey,
    #[allow(dead_code)]
    fee_bps: Option<u16>,
}

#[derive(Debug)]
struct QuoteUpdateParams {
    interval_ms: Option<u32>,
    num_quotes: Option<u32>,
}

// Note: These struct types are kept for reference but response parsing
// is done directly from msgpack values for flexibility.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TitanStreamData {
    id: String,
    seq: u64,
    payload: SwapQuotesPayload,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SwapQuotesPayload {
    input_mint: String,
    output_mint: String,
    amount: u64,
    quotes: HashMap<String, SwapRouteResponse>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SwapRouteResponse {
    in_amount: u64,
    out_amount: u64,
    slippage_bps: u16,
    #[serde(default)]
    price_impact_pct: Option<String>,
    #[serde(default)]
    instructions: Vec<InstructionData>,
    #[serde(default)]
    address_lookup_tables: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InstructionData {
    program_id: String,
    data: String,
    accounts: Vec<AccountData>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AccountData {
    pubkey: String,
    is_signer: bool,
    is_writable: bool,
}

// ============================================================================
// Titan Client Implementation
// ============================================================================

/// Titan swap client using WebSocket API
pub struct TitanClient {
    config: TitanConfig,
}

impl TitanClient {
    pub fn new(config: TitanConfig) -> Self {
        Self { config }
    }

    /// Helper to create WebSocket connection with Titan subprotocol
    async fn connect(&self) -> TitanResult<(
        futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>, Message>,
        futures_util::stream::SplitStream<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>
    )> {
        let url = &self.config.endpoint;
        debug!("Connecting to Titan WebSocket: {}", url);

        let ws_url = url::Url::parse(url)
            .map_err(|e| Error::ConfigError(format!("Invalid URL: {}", e)))?;

        let ws_request = tokio_tungstenite::tungstenite::http::Request::builder()
            .uri(url)
            .header("Sec-WebSocket-Protocol", "v1.api.titan.ag")
            .header("Host", ws_url.host_str().unwrap_or(""))
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header("Sec-WebSocket-Key", tokio_tungstenite::tungstenite::handshake::client::generate_key())
            .body(())
            .map_err(|e| Error::ConfigError(format!("Failed to build request: {}", e)))?;

        let (ws_stream, _) = timeout(Duration::from_secs(10), connect_async(ws_request))
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|e| Error::ConnectionError(e.to_string()))?;

        Ok(ws_stream.split())
    }

    /// Get simple swap price (no transaction data, just price)
    pub async fn get_swap_price(
        &self,
        input_mint: &Pubkey,
        output_mint: &Pubkey,
        amount: u64,
    ) -> TitanResult<(u64, u64)> {
        if self.config.endpoint.is_empty() {
            return Err(Error::ConfigError("TITAN_ENDPOINT not configured".into()));
        }

        let (mut write, mut read) = self.connect().await?;

        let request = TitanRequest::GetSwapPrice(SwapPriceRequest {
            input_mint: *input_mint,
            output_mint: *output_mint,
            amount,
        });

        let msg_value = request_to_msgpack_value(&request);
        let mut msg_bytes = Vec::new();
        rmpv::encode::write_value(&mut msg_bytes, &msg_value)
            .map_err(|e| Error::RequestError(format!("Failed to encode request: {}", e)))?;

        write
            .send(Message::Binary(msg_bytes))
            .await
            .map_err(|e| Error::RequestError(e.to_string()))?;

        let response_timeout = Duration::from_secs(10);
        let msg = timeout(response_timeout, read.next())
            .await
            .map_err(|_| Error::Timeout)?
            .ok_or(Error::ConnectionError("Connection closed".into()))?
            .map_err(|e| Error::ConnectionError(e.to_string()))?;

        let _ = write.send(Message::Close(None)).await;

        let data = match msg {
            Message::Binary(data) => data,
            _ => return Err(Error::RequestError("Unexpected message type".into())),
        };

        if let Ok(value) = rmpv::decode::read_value(&mut data.as_slice()) {
            // Check for Error response
            if let Some(error) = extract_error_from_response(&value) {
                return Err(Error::RequestError(error));
            }

            // Parse Response > data > GetSwapPrice
            if let Some(price_data) = extract_response_data(&value, "GetSwapPrice") {
                let amount_in = get_u64_field(&price_data, "amountIn").unwrap_or(amount);
                let amount_out = get_u64_field(&price_data, "amountOut").unwrap_or(0);
                if amount_out > 0 {
                    return Ok((amount_in, amount_out));
                }
            }
        }

        Err(Error::NoValidRoute)
    }

    /// Connect to Titan WebSocket and get a swap quote
    pub async fn get_quote(
        &self,
        input_mint: &Pubkey,
        output_mint: &Pubkey,
        amount: u64,
        slippage_bps: Option<u16>,
        only_direct_routes: bool,
        max_accounts: Option<u8>,
        user_public_key: Pubkey,
    ) -> TitanResult<(SwapRoute, Vec<Instruction>)> {
        if self.config.endpoint.is_empty() {
            return Err(Error::ConfigError(
                "TITAN_ENDPOINT not configured".into(),
            ));
        }

        let request_id = uuid::Uuid::new_v4().to_string();

        // Build the request
        let request = TitanRequest::NewSwapQuoteStream(SwapQuoteStreamRequest {
            request_id: request_id.clone(),
            swap: SwapParams {
                input_mint: *input_mint,
                output_mint: *output_mint,
                amount,
                swap_mode: Some("ExactIn".to_string()),
                slippage_bps,
                only_direct_routes: Some(only_direct_routes),
                max_accounts,
            },
            transaction: TransactionParams {
                user_public_key,
                fee_bps: None,
            },
            update: Some(QuoteUpdateParams {
                interval_ms: Some(200), // Titan minimum is 200ms
                num_quotes: Some(1),
            }),
        });

        // Connect using helper
        let (mut write, mut read) = self.connect().await?;

        // Encode request as MessagePack
        let msg_value = request_to_msgpack_value(&request);
        let mut msg_bytes = Vec::new();
        rmpv::encode::write_value(&mut msg_bytes, &msg_value)
            .map_err(|e| Error::RequestError(format!("Failed to encode request: {}", e)))?;

        debug!("Sending request ({} bytes)", msg_bytes.len());

        // Send request as binary
        write
            .send(Message::Binary(msg_bytes))
            .await
            .map_err(|e| Error::RequestError(e.to_string()))?;

        debug!("Sent swap quote request: {}", request_id);

        // Wait for response with timeout
        let response_timeout = Duration::from_secs(30);
        let mut best_route: Option<SwapRouteResponse> = None;
        let mut _current_stream_id: Option<u32> = None;

        loop {
            let msg = timeout(response_timeout, read.next())
                .await
                .map_err(|_| Error::Timeout)?
                .ok_or(Error::ConnectionError("Connection closed".into()))?
                .map_err(|e| Error::ConnectionError(e.to_string()))?;

            let data = match msg {
                Message::Binary(data) => data,
                Message::Text(text) => {
                    debug!("Received text: {}", &text[..text.len().min(500)]);
                    continue;
                }
                Message::Close(_) => {
                    debug!("WebSocket closed");
                    break;
                }
                _ => continue,
            };

            debug!("Received binary message ({} bytes)", data.len());

            // Parse MessagePack response
            if let Ok(value) = rmpv::decode::read_value(&mut data.as_slice()) {
                // Check for error response
                if let Some(error) = extract_error_from_response(&value) {
                    warn!("Titan error: {}", error);
                    return Err(Error::RequestError(error));
                }

                // Check for initial Response (contains stream info)
                if extract_response_data(&value, "NewSwapQuoteStream").is_some() {
                    // Initial response - stream is starting
                    debug!("Stream started");
                    continue;
                }

                // Check for StreamData
                if let Some((stream_id, seq, payload)) = extract_stream_data(&value) {
                    debug!("Stream data id={} seq={}", stream_id, seq);
                    _current_stream_id = Some(stream_id);

                    // Extract SwapQuotes from payload
                    if let Some(swap_quotes) = extract_swap_quotes_from_payload(payload) {
                        if let Some(route) = parse_quotes_from_msgpack(&swap_quotes) {
                            best_route = Some(route);

                            // Stop stream after getting first quote
                            let stop_request = TitanRequest::StopStream { id: stream_id.to_string() };
                            let stop_value = request_to_msgpack_value(&stop_request);
                            let mut stop_bytes = Vec::new();
                            if rmpv::encode::write_value(&mut stop_bytes, &stop_value).is_ok() {
                                let _ = write.send(Message::Binary(stop_bytes)).await;
                            }
                            break;
                        }
                    }
                }
            }
        }

        // Close WebSocket
        let _ = write.send(Message::Close(None)).await;

        // Convert response to our types
        let route = best_route.ok_or(Error::NoValidRoute)?;
        let instructions = parse_instructions(&route.instructions)?;

        Ok((
            SwapRoute {
                in_amount: route.in_amount,
                out_amount: route.out_amount,
                slippage_bps: route.slippage_bps,
                price_impact_pct: route.price_impact_pct.unwrap_or_default(),
                instructions: instructions.clone(),
                address_lookup_tables: route
                    .address_lookup_tables
                    .iter()
                    .filter_map(|s| s.parse().ok())
                    .collect(),
            },
            instructions,
        ))
    }
}

/// Extract error message from response if it's an Error envelope
fn extract_error_from_response(value: &rmpv::Value) -> Option<String> {
    let map = value.as_map()?;
    for (k, v) in map {
        if let rmpv::Value::String(key) = k {
            if key.as_str() == Some("Error") {
                // Error envelope: {Error: {requestId, code, message}}
                if let Some(err_map) = v.as_map() {
                    for (ek, ev) in err_map {
                        if let rmpv::Value::String(ekey) = ek {
                            if ekey.as_str() == Some("message") {
                                return ev.as_str().map(String::from);
                            }
                        }
                    }
                }
            }
        }
    }
    None
}

/// Extract response data for a specific request type
fn extract_response_data<'a>(value: &'a rmpv::Value, request_type: &str) -> Option<&'a rmpv::Value> {
    let map = value.as_map()?;
    for (k, v) in map {
        if let rmpv::Value::String(key) = k {
            if key.as_str() == Some("Response") {
                // Response envelope: {Response: {requestId, data: {RequestType: ...}}}
                if let Some(resp_map) = v.as_map() {
                    for (rk, rv) in resp_map {
                        if let rmpv::Value::String(rkey) = rk {
                            if rkey.as_str() == Some("data") {
                                if let Some(data_map) = rv.as_map() {
                                    for (dk, dv) in data_map {
                                        if let rmpv::Value::String(dkey) = dk {
                                            if dkey.as_str() == Some(request_type) {
                                                return Some(dv);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    None
}

/// Extract StreamData payload
fn extract_stream_data<'a>(value: &'a rmpv::Value) -> Option<(u32, u64, &'a rmpv::Value)> {
    let map = value.as_map()?;
    for (k, v) in map {
        if let rmpv::Value::String(key) = k {
            if key.as_str() == Some("StreamData") {
                // StreamData: {id, seq, payload: {SwapQuotes: ...}}
                if let Some(sd_map) = v.as_map() {
                    let mut stream_id: Option<u32> = None;
                    let mut seq: Option<u64> = None;
                    let mut payload: Option<&rmpv::Value> = None;

                    for (sk, sv) in sd_map {
                        if let rmpv::Value::String(skey) = sk {
                            match skey.as_str().unwrap_or("") {
                                "id" => stream_id = sv.as_u64().map(|x| x as u32),
                                "seq" => seq = sv.as_u64(),
                                "payload" => payload = Some(sv),
                                _ => {}
                            }
                        }
                    }

                    if let (Some(id), Some(s), Some(p)) = (stream_id, seq, payload) {
                        return Some((id, s, p));
                    }
                }
            }
        }
    }
    None
}

/// Get u64 field from msgpack map
fn get_u64_field(value: &rmpv::Value, field: &str) -> Option<u64> {
    let map = value.as_map()?;
    for (k, v) in map {
        if let rmpv::Value::String(key) = k {
            if key.as_str() == Some(field) {
                return v.as_u64();
            }
        }
    }
    None
}

/// Extract SwapQuotes from StreamData payload
fn extract_swap_quotes_from_payload(payload: &rmpv::Value) -> Option<rmpv::Value> {
    let map = payload.as_map()?;
    for (k, v) in map {
        if let rmpv::Value::String(key) = k {
            if key.as_str() == Some("SwapQuotes") {
                return Some(v.clone());
            }
        }
    }
    None
}

/// Parse quotes from MessagePack payload
fn parse_quotes_from_msgpack(payload: &rmpv::Value) -> Option<SwapRouteResponse> {
    let map = payload.as_map()?;

    let mut quotes_value: Option<&rmpv::Value> = None;
    for (k, v) in map {
        if let rmpv::Value::String(key) = k {
            if key.as_str() == Some("quotes") {
                quotes_value = Some(v);
                break;
            }
        }
    }

    let quotes_map = quotes_value?.as_map()?;

    // Get first quote from any provider
    for (_provider_key, quote_val) in quotes_map {
        let quote_map = quote_val.as_map()?;

        let mut in_amount: u64 = 0;
        let mut out_amount: u64 = 0;
        let mut slippage_bps: u16 = 0;
        let mut price_impact: Option<String> = None;
        let mut instructions: Vec<InstructionData> = vec![];
        let mut address_lookup_tables: Vec<String> = vec![];

        for (k, v) in quote_map {
            if let rmpv::Value::String(key) = k {
                match key.as_str().unwrap_or("") {
                    "inAmount" => in_amount = v.as_u64().unwrap_or(0),
                    "outAmount" => out_amount = v.as_u64().unwrap_or(0),
                    "slippageBps" => slippage_bps = v.as_u64().unwrap_or(0) as u16,
                    "priceImpactPct" => price_impact = v.as_str().map(String::from),
                    "instructions" => {
                        if let Some(ixs) = v.as_array() {
                            for ix in ixs {
                                if let Some(parsed) = parse_instruction_from_msgpack(ix) {
                                    instructions.push(parsed);
                                }
                            }
                        }
                    }
                    "addressLookupTables" => {
                        if let Some(tables) = v.as_array() {
                            for t in tables {
                                if let Some(s) = t.as_str() {
                                    address_lookup_tables.push(s.to_string());
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        if out_amount > 0 {
            return Some(SwapRouteResponse {
                in_amount,
                out_amount,
                slippage_bps,
                price_impact_pct: price_impact,
                instructions,
                address_lookup_tables,
            });
        }
    }

    None
}

/// Parse a single instruction from MessagePack
fn parse_instruction_from_msgpack(value: &rmpv::Value) -> Option<InstructionData> {
    let map = value.as_map()?;

    let mut program_id: Option<String> = None;
    let mut data: Option<String> = None;
    let mut accounts: Vec<AccountData> = vec![];

    for (k, v) in map {
        if let rmpv::Value::String(key) = k {
            match key.as_str().unwrap_or("") {
                "programId" => program_id = v.as_str().map(String::from),
                "data" => data = v.as_str().map(String::from),
                "accounts" => {
                    if let Some(accs) = v.as_array() {
                        for acc in accs {
                            if let Some(acc_map) = acc.as_map() {
                                let mut pubkey: Option<String> = None;
                                let mut is_signer = false;
                                let mut is_writable = false;

                                for (ak, av) in acc_map {
                                    if let rmpv::Value::String(akey) = ak {
                                        match akey.as_str().unwrap_or("") {
                                            "pubkey" => pubkey = av.as_str().map(String::from),
                                            "isSigner" => is_signer = av.as_bool().unwrap_or(false),
                                            "isWritable" => is_writable = av.as_bool().unwrap_or(false),
                                            _ => {}
                                        }
                                    }
                                }

                                if let Some(pk) = pubkey {
                                    accounts.push(AccountData {
                                        pubkey: pk,
                                        is_signer,
                                        is_writable,
                                    });
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    Some(InstructionData {
        program_id: program_id?,
        data: data?,
        accounts,
    })
}

/// Helper to convert Pubkey to raw bytes for MessagePack
fn pubkey_to_msgpack_bytes(pubkey: &Pubkey) -> rmpv::Value {
    rmpv::Value::Binary(pubkey.to_bytes().to_vec())
}

/// Counter for request IDs
static REQUEST_ID_COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);

fn next_request_id() -> u32 {
    REQUEST_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
}

/// Convert TitanRequest to MessagePack Value using correct format:
/// {id: u32, data: {RequestType: params}}
fn request_to_msgpack_value(request: &TitanRequest) -> rmpv::Value {
    match request {
        TitanRequest::NewSwapQuoteStream(req) => {
            // Swap params with raw bytes for pubkeys
            let mut swap_map = vec![
                (rmpv::Value::String("inputMint".into()), pubkey_to_msgpack_bytes(&req.swap.input_mint)),
                (rmpv::Value::String("outputMint".into()), pubkey_to_msgpack_bytes(&req.swap.output_mint)),
                (rmpv::Value::String("amount".into()), rmpv::Value::Integer(req.swap.amount.into())),
            ];
            if let Some(ref mode) = req.swap.swap_mode {
                swap_map.push((rmpv::Value::String("swapMode".into()), rmpv::Value::String(mode.clone().into())));
            }
            if let Some(slippage) = req.swap.slippage_bps {
                swap_map.push((rmpv::Value::String("slippageBps".into()), rmpv::Value::Integer(slippage.into())));
            }
            if let Some(only_direct) = req.swap.only_direct_routes {
                swap_map.push((rmpv::Value::String("onlyDirectRoutes".into()), rmpv::Value::Boolean(only_direct)));
            }
            if let Some(max_acc) = req.swap.max_accounts {
                swap_map.push((rmpv::Value::String("maxAccounts".into()), rmpv::Value::Integer(max_acc.into())));
            }

            // Transaction params with raw bytes for pubkey
            let tx_map = vec![
                (rmpv::Value::String("userPublicKey".into()), pubkey_to_msgpack_bytes(&req.transaction.user_public_key)),
            ];

            // Update params (optional)
            let mut inner_map = vec![
                (rmpv::Value::String("swap".into()), rmpv::Value::Map(swap_map)),
                (rmpv::Value::String("transaction".into()), rmpv::Value::Map(tx_map)),
            ];
            if let Some(ref update) = req.update {
                let mut update_map = vec![];
                if let Some(interval) = update.interval_ms {
                    update_map.push((rmpv::Value::String("intervalMs".into()), rmpv::Value::Integer(interval.into())));
                }
                if let Some(num) = update.num_quotes {
                    update_map.push((rmpv::Value::String("numQuotes".into()), rmpv::Value::Integer(num.into())));
                }
                inner_map.push((rmpv::Value::String("update".into()), rmpv::Value::Map(update_map)));
            }

            // Wrap in {id, data: {NewSwapQuoteStream: {...}}}
            let data_map = vec![
                (rmpv::Value::String("NewSwapQuoteStream".into()), rmpv::Value::Map(inner_map)),
            ];

            let outer_map = vec![
                (rmpv::Value::String("id".into()), rmpv::Value::Integer(next_request_id().into())),
                (rmpv::Value::String("data".into()), rmpv::Value::Map(data_map)),
            ];

            rmpv::Value::Map(outer_map)
        }
        TitanRequest::GetSwapPrice(req) => {
            // GetSwapPrice request with raw bytes for pubkeys
            let inner_map = vec![
                (rmpv::Value::String("inputMint".into()), pubkey_to_msgpack_bytes(&req.input_mint)),
                (rmpv::Value::String("outputMint".into()), pubkey_to_msgpack_bytes(&req.output_mint)),
                (rmpv::Value::String("amount".into()), rmpv::Value::Integer(req.amount.into())),
            ];

            let data_map = vec![
                (rmpv::Value::String("GetSwapPrice".into()), rmpv::Value::Map(inner_map)),
            ];

            let outer_map = vec![
                (rmpv::Value::String("id".into()), rmpv::Value::Integer(next_request_id().into())),
                (rmpv::Value::String("data".into()), rmpv::Value::Map(data_map)),
            ];

            rmpv::Value::Map(outer_map)
        }
        TitanRequest::StopStream { id } => {
            // StopStream uses stream ID as number, not string
            let stream_id: u32 = id.parse().unwrap_or(0);
            let data_map = vec![
                (rmpv::Value::String("StopStream".into()), rmpv::Value::Map(vec![
                    (rmpv::Value::String("id".into()), rmpv::Value::Integer(stream_id.into())),
                ])),
            ];

            let outer_map = vec![
                (rmpv::Value::String("id".into()), rmpv::Value::Integer(next_request_id().into())),
                (rmpv::Value::String("data".into()), rmpv::Value::Map(data_map)),
            ];

            rmpv::Value::Map(outer_map)
        }
    }
}

/// Parse instructions from Titan response
fn parse_instructions(ixs: &[InstructionData]) -> TitanResult<Vec<Instruction>> {
    use base64::Engine;

    let mut instructions = Vec::new();

    for ix in ixs {
        let program_id: Pubkey = ix
            .program_id
            .parse()
            .map_err(|_| Error::ResponseTypeConversionError)?;

        let data = base64::engine::general_purpose::STANDARD
            .decode(&ix.data)
            .map_err(|_| Error::ResponseTypeConversionError)?;

        let accounts: Vec<AccountMeta> = ix
            .accounts
            .iter()
            .filter_map(|acc| {
                let pubkey: Pubkey = acc.pubkey.parse().ok()?;
                Some(if acc.is_writable {
                    AccountMeta::new(pubkey, acc.is_signer)
                } else {
                    AccountMeta::new_readonly(pubkey, acc.is_signer)
                })
            })
            .collect();

        instructions.push(Instruction {
            program_id,
            accounts,
            data,
        });
    }

    Ok(instructions)
}

// ============================================================================
// Global Client & Convenience Functions
// ============================================================================

static TITAN_CLIENT: once_cell::sync::OnceCell<TitanClient> = once_cell::sync::OnceCell::new();

/// Initialize the global Titan client
pub fn init_titan_client(config: TitanConfig) {
    let _ = TITAN_CLIENT.set(TitanClient::new(config));
}

/// Get the global Titan client
pub fn get_titan_client() -> &'static TitanClient {
    TITAN_CLIENT.get_or_init(|| {
        let config = TitanConfig::from_env().unwrap_or_else(|_| {
            warn!("TITAN_ENDPOINT not set - Titan API calls will fail");
            TitanConfig::new(String::new(), None)
        });
        TitanClient::new(config)
    })
}

/// Get quote (convenience function)
pub async fn get_quote(
    input_mint: &Pubkey,
    output_mint: &Pubkey,
    amount: u64,
    only_direct_routes: bool,
    slippage_bps: Option<u16>,
    max_accounts: Option<u8>,
) -> TitanResult<SwapRoute> {
    // Use a dummy user key for quote-only requests
    let dummy_user = Pubkey::default();
    let (route, _) = get_titan_client()
        .get_quote(
            input_mint,
            output_mint,
            amount,
            slippage_bps,
            only_direct_routes,
            max_accounts,
            dummy_user,
        )
        .await?;
    Ok(route)
}

/// Get swap instructions (convenience function)
pub async fn get_swap_instructions(
    input_mint: &Pubkey,
    output_mint: &Pubkey,
    amount: u64,
    slippage_bps: Option<u16>,
    user_public_key: Pubkey,
) -> TitanResult<DecompiledVersionedTx> {
    let (_route, instructions) = get_titan_client()
        .get_quote(
            input_mint,
            output_mint,
            amount,
            slippage_bps,
            false,
            None,
            user_public_key,
        )
        .await?;

    Ok(DecompiledVersionedTx {
        instructions,
        lookup_tables: None,
    })
}

/// Fetch prices using GetSwapPrice from Titan (simpler than full quote)
pub async fn get_prices(
    input_mints: &[Pubkey],
    vs_token: &Pubkey,
    _amount: f32,
) -> Result<HashMap<String, SwapPrice>> {
    let client = get_titan_client();
    let mut prices = HashMap::new();

    // Add the vs_token itself with price 1.0
    prices.insert(
        vs_token.to_string(),
        SwapPrice {
            price: 1.0,
            mint: vs_token.to_string(),
        },
    );

    // For each input mint, get a price estimate
    for mint in input_mints {
        if mint == vs_token {
            continue;
        }

        // Use a small amount to get price estimate (1 USDC = 1_000_000 lamports)
        let quote_amount = 1_000_000u64;

        match client.get_swap_price(vs_token, mint, quote_amount).await {
            Ok((amount_in, amount_out)) => {
                if amount_out > 0 {
                    let price = amount_in as f32 / amount_out as f32;
                    prices.insert(
                        mint.to_string(),
                        SwapPrice {
                            price,
                            mint: mint.to_string(),
                        },
                    );
                    info!(
                        "Price for {}: {} (in={}, out={})",
                        mint, price, amount_in, amount_out
                    );
                }
            }
            Err(e) => {
                warn!("Failed to get price for {}: {:?}", mint, e);
            }
        }
    }

    Ok(prices)
}
