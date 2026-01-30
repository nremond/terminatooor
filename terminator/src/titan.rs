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

        info!("Sent swap quote request: {} for {} -> {} amount {}", request_id, input_mint, output_mint, amount);

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
                    info!("Titan stream started, waiting for quotes...");
                    continue;
                }

                // Check for StreamData
                if let Some((stream_id, seq, payload)) = extract_stream_data(&value) {
                    info!("Stream data id={} seq={}", stream_id, seq);
                    _current_stream_id = Some(stream_id);

                    // Extract SwapQuotes from payload
                    if let Some(swap_quotes) = extract_swap_quotes_from_payload(payload) {
                        debug!("SwapQuotes type: {:?}, is_map: {}",
                            match &swap_quotes {
                                rmpv::Value::Nil => "Nil",
                                rmpv::Value::Boolean(_) => "Boolean",
                                rmpv::Value::Integer(_) => "Integer",
                                rmpv::Value::F32(_) => "F32",
                                rmpv::Value::F64(_) => "F64",
                                rmpv::Value::String(_) => "String",
                                rmpv::Value::Binary(_) => "Binary",
                                rmpv::Value::Array(_) => "Array",
                                rmpv::Value::Map(_) => "Map",
                                rmpv::Value::Ext(_, _) => "Ext",
                            },
                            swap_quotes.is_map()
                        );
                        if let Some(route) = parse_quotes_from_msgpack(&swap_quotes) {
                            info!("Got quote: in={} out={}", route.in_amount, route.out_amount);
                            best_route = Some(route);

                            // Stop stream after getting first quote
                            let stop_request = TitanRequest::StopStream { id: stream_id.to_string() };
                            let stop_value = request_to_msgpack_value(&stop_request);
                            let mut stop_bytes = Vec::new();
                            if rmpv::encode::write_value(&mut stop_bytes, &stop_value).is_ok() {
                                let _ = write.send(Message::Binary(stop_bytes)).await;
                            }
                            break;
                        } else {
                            warn!("Failed to parse quotes from swap_quotes payload, type={:?}, is_map={}",
                                match &swap_quotes {
                                    rmpv::Value::Nil => "Nil",
                                    rmpv::Value::Boolean(_) => "Boolean",
                                    rmpv::Value::Integer(_) => "Integer",
                                    rmpv::Value::F32(_) => "F32",
                                    rmpv::Value::F64(_) => "F64",
                                    rmpv::Value::String(_) => "String",
                                    rmpv::Value::Binary(_) => "Binary",
                                    rmpv::Value::Array(_) => "Array",
                                    rmpv::Value::Map(m) => { info!("Map keys: {:?}", m.iter().take(10).map(|(k,_)| format!("{:?}", k)).collect::<Vec<_>>()); "Map" },
                                    rmpv::Value::Ext(_, _) => "Ext",
                                },
                                swap_quotes.is_map()
                            );
                        }
                    } else {
                        warn!("No SwapQuotes in stream payload");
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

    // Debug: print all keys in payload
    debug!("Payload keys: {:?}", map.iter().map(|(k, _)| format!("{:?}", k)).collect::<Vec<_>>());

    let mut quotes_value: Option<&rmpv::Value> = None;
    for (k, v) in map {
        if let rmpv::Value::String(key) = k {
            if key.as_str() == Some("quotes") {
                quotes_value = Some(v);
                break;
            }
        }
    }

    if quotes_value.is_none() {
        debug!("No 'quotes' key found in payload");
        return None;
    }

    let quotes_map = quotes_value?.as_map();
    if quotes_map.is_none() {
        debug!("'quotes' is not a map, value: {:?}", quotes_value);
        return None;
    }
    let quotes_map = quotes_map?;

    if quotes_map.is_empty() {
        debug!("'quotes' map is empty - Titan may require a valid user wallet or no routes available");
    }
    debug!("Found {} quote providers", quotes_map.len());

    // Get first quote from any provider
    for (provider_key, quote_val) in quotes_map {
        debug!("Processing provider: {:?}", provider_key);
        // Log all keys in the quote
        if let Some(qmap) = quote_val.as_map() {
            let keys: Vec<_> = qmap.iter().map(|(k, _)| format!("{:?}", k)).collect();
            debug!("Quote keys: {:?}", keys);
        }
        let quote_map = match quote_val.as_map() {
            Some(m) => m,
            None => {
                debug!("Quote value is not a map: {:?}", quote_val);
                continue;
            }
        };

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
                        debug!("Instructions value type: {:?}", v);
                        if let Some(ixs) = v.as_array() {
                            debug!("Instructions array has {} items", ixs.len());
                            for (i, ix) in ixs.iter().enumerate() {
                                if let Some(parsed) = parse_instruction_from_msgpack(ix) {
                                    instructions.push(parsed);
                                } else {
                                    debug!("Failed to parse instruction {}: {:?}", i, ix);
                                }
                            }
                        } else {
                            debug!("Instructions is not an array");
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

        debug!(
            "Quote parsed: in={} out={} slippage={} ixs={} luts={}",
            in_amount, out_amount, slippage_bps, instructions.len(), address_lookup_tables.len()
        );

        if out_amount > 0 {
            return Some(SwapRouteResponse {
                in_amount,
                out_amount,
                slippage_bps,
                price_impact_pct: price_impact,
                instructions,
                address_lookup_tables,
            });
        } else {
            debug!("Skipping quote with zero out_amount");
        }
    }

    debug!("No valid quotes found (all had zero out_amount or no providers)");
    None
}

/// Parse a single instruction from MessagePack
/// Handles both verbose format (programId, accounts, data) and compact format (p, a, d)
fn parse_instruction_from_msgpack(value: &rmpv::Value) -> Option<InstructionData> {
    let map = value.as_map()?;

    let mut program_id: Option<String> = None;
    let mut data: Option<String> = None;
    let mut accounts: Vec<AccountData> = vec![];

    for (k, v) in map {
        if let rmpv::Value::String(key) = k {
            match key.as_str().unwrap_or("") {
                // Verbose format
                "programId" => program_id = v.as_str().map(String::from),
                "data" => data = v.as_str().map(String::from),
                // Compact format - pubkey as binary
                "p" => {
                    if let rmpv::Value::Binary(bytes) = v {
                        if bytes.len() == 32 {
                            program_id = Some(Pubkey::new_from_array(bytes.clone().try_into().unwrap()).to_string());
                        }
                    } else if let Some(s) = v.as_str() {
                        program_id = Some(s.to_string());
                    }
                }
                // Compact format - data as binary
                "d" => {
                    if let rmpv::Value::Binary(bytes) = v {
                        use base64::Engine;
                        data = Some(base64::engine::general_purpose::STANDARD.encode(bytes));
                    } else if let Some(s) = v.as_str() {
                        data = Some(s.to_string());
                    }
                }
                "accounts" | "a" => {
                    if let Some(accs) = v.as_array() {
                        for acc in accs {
                            if let Some(acc_map) = acc.as_map() {
                                let mut pubkey: Option<String> = None;
                                let mut is_signer = false;
                                let mut is_writable = false;

                                for (ak, av) in acc_map {
                                    if let rmpv::Value::String(akey) = ak {
                                        match akey.as_str().unwrap_or("") {
                                            // Verbose format
                                            "pubkey" => pubkey = av.as_str().map(String::from),
                                            "isSigner" => is_signer = av.as_bool().unwrap_or(false),
                                            "isWritable" => is_writable = av.as_bool().unwrap_or(false),
                                            // Compact format
                                            "p" => {
                                                if let rmpv::Value::Binary(bytes) = av {
                                                    if bytes.len() == 32 {
                                                        pubkey = Some(Pubkey::new_from_array(bytes.clone().try_into().unwrap()).to_string());
                                                    }
                                                } else if let Some(s) = av.as_str() {
                                                    pubkey = Some(s.to_string());
                                                }
                                            }
                                            "s" => is_signer = av.as_bool().unwrap_or(false),
                                            "w" => is_writable = av.as_bool().unwrap_or(false),
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
    user_public_key: Pubkey,
) -> TitanResult<SwapRoute> {
    let (route, _) = get_titan_client()
        .get_quote(
            input_mint,
            output_mint,
            amount,
            slippage_bps,
            only_direct_routes,
            max_accounts,
            user_public_key,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Integration test for Titan swap quote
    /// Run with: KEYPAIR_PATH=../_keys/kamino-terminator-keypair.json cargo test --package klend-terminator test_titan_swap_quote -- --nocapture --ignored
    #[tokio::test]
    #[ignore] // Run manually with --ignored flag
    async fn test_titan_swap_quote() {
        // Initialize tracing for test output
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .init();

        // Load config from env
        let config = TitanConfig::from_env().expect("TITAN_ENDPOINT must be set");
        let client = TitanClient::new(config);

        // SOL mint
        let sol_mint: Pubkey = "So11111111111111111111111111111111111111112".parse().unwrap();
        // USDC mint
        let usdc_mint: Pubkey = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".parse().unwrap();

        // Test amount: ~0.36 SOL in lamports (similar to the failing case)
        let amount = 359_097_720u64;

        // Load user pubkey from keypair file or use a known valid wallet
        use solana_sdk::signer::Signer;
        let user_pubkey: Pubkey = std::env::var("KEYPAIR_PATH")
            .ok()
            .and_then(|path| {
                let data = std::fs::read_to_string(&path).ok()?;
                let bytes: Vec<u8> = serde_json::from_str(&data).ok()?;
                solana_sdk::signature::Keypair::from_bytes(&bytes).ok()
            })
            .map(|kp| kp.pubkey())
            .unwrap_or_else(|| {
                // Fallback to a known valid mainnet wallet (Raydium authority)
                "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1".parse().unwrap()
            });

        println!("\n=== Testing Titan Swap Quote ===");
        println!("From: SOL ({})", sol_mint);
        println!("To: USDC ({})", usdc_mint);
        println!("Amount: {} lamports ({} SOL)", amount, amount as f64 / 1e9);
        println!("User: {}", user_pubkey);
        println!();

        // Test 1: Simple price query
        println!("--- Test 1: Get Swap Price ---");
        match client.get_swap_price(&sol_mint, &usdc_mint, amount).await {
            Ok((in_amt, out_amt)) => {
                println!("Price query SUCCESS: in={} out={}", in_amt, out_amt);
                println!("Price: {} USDC per SOL", out_amt as f64 / in_amt as f64 * 1e3);
            }
            Err(e) => {
                println!("Price query FAILED: {:?}", e);
            }
        }
        println!();

        // Test 2: Full quote with instructions
        println!("--- Test 2: Get Full Quote with Instructions ---");
        match client
            .get_quote(
                &sol_mint,
                &usdc_mint,
                amount,
                Some(50), // 0.5% slippage
                false,    // not only direct routes
                Some(58), // max accounts
                user_pubkey,
            )
            .await
        {
            Ok((route, instructions)) => {
                println!("Quote SUCCESS:");
                println!("  in_amount: {}", route.in_amount);
                println!("  out_amount: {}", route.out_amount);
                println!("  slippage_bps: {}", route.slippage_bps);
                println!("  price_impact_pct: {}", route.price_impact_pct);
                println!("  instructions: {}", instructions.len());
                println!("  lookup_tables: {}", route.address_lookup_tables.len());
                for (i, ix) in instructions.iter().enumerate() {
                    println!("  ix[{}]: program={} accounts={}", i, ix.program_id, ix.accounts.len());
                }
            }
            Err(e) => {
                println!("Quote FAILED: {:?}", e);
            }
        }
        println!();

        // Test 3: Reverse direction (USDC -> SOL)
        println!("--- Test 3: Reverse Quote (USDC -> SOL) ---");
        let usdc_amount = 50_000_000u64; // 50 USDC
        match client
            .get_quote(
                &usdc_mint,
                &sol_mint,
                usdc_amount,
                Some(50),
                false,
                Some(58),
                user_pubkey,
            )
            .await
        {
            Ok((route, instructions)) => {
                println!("Reverse quote SUCCESS:");
                println!("  in_amount: {} USDC", route.in_amount as f64 / 1e6);
                println!("  out_amount: {} SOL", route.out_amount as f64 / 1e9);
                println!("  instructions: {}", instructions.len());
            }
            Err(e) => {
                println!("Reverse quote FAILED: {:?}", e);
            }
        }

        println!("\n=== Test Complete ===");
    }

    /// Test with PYUSD (Token-2022)
    /// Run with: cargo test --package klend-terminator test_titan_pyusd -- --nocapture --ignored
    /// Test the exact code path used by the liquidator (with dummy user)
    #[tokio::test]
    #[ignore]
    async fn test_titan_liquidator_path() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .init();

        let sol_mint: Pubkey = "So11111111111111111111111111111111111111112".parse().unwrap();
        let usdc_mint: Pubkey = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".parse().unwrap();

        // Amount from failing liquidation: 34741188 lamports (~0.035 SOL)
        let amount = 34_741_188u64;

        println!("\n=== Testing Liquidator Code Path ===");
        println!("SOL -> USDC, amount={} lamports", amount);

        // Test 1: Using dummy user (Pubkey::default()) - this is what get_quote() does
        println!("\n--- Test 1: With dummy user (Pubkey::default()) ---");
        let dummy_user = Pubkey::default();
        println!("User: {} (all zeros)", dummy_user);

        let config = TitanConfig::from_env().expect("TITAN_ENDPOINT must be set");
        let client = TitanClient::new(config);

        match client
            .get_quote(&sol_mint, &usdc_mint, amount, Some(50), false, Some(58), dummy_user)
            .await
        {
            Ok((route, instructions)) => {
                println!("Dummy user SUCCESS: in={} out={} ixs={}", route.in_amount, route.out_amount, instructions.len());
            }
            Err(e) => {
                println!("Dummy user FAILED: {:?}", e);
            }
        }

        // Test 2: Using real user - this is what the tests use
        println!("\n--- Test 2: With real user ---");
        use solana_sdk::signer::Signer;
        let real_user: Pubkey = std::env::var("KEYPAIR_PATH")
            .ok()
            .and_then(|path| {
                let data = std::fs::read_to_string(&path).ok()?;
                let bytes: Vec<u8> = serde_json::from_str(&data).ok()?;
                solana_sdk::signature::Keypair::from_bytes(&bytes).ok()
            })
            .map(|kp| kp.pubkey())
            .unwrap_or_else(|| "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1".parse().unwrap());
        println!("User: {}", real_user);

        let config2 = TitanConfig::from_env().expect("TITAN_ENDPOINT must be set");
        let client2 = TitanClient::new(config2);

        match client2
            .get_quote(&sol_mint, &usdc_mint, amount, Some(50), false, Some(58), real_user)
            .await
        {
            Ok((route, instructions)) => {
                println!("Real user SUCCESS: in={} out={} ixs={}", route.in_amount, route.out_amount, instructions.len());
            }
            Err(e) => {
                println!("Real user FAILED: {:?}", e);
            }
        }
    }

    /// Test cbBTC -> USDC routing (the failing case from logs)
    #[tokio::test]
    #[ignore]
    async fn test_titan_cbbtc() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .init();

        let config = TitanConfig::from_env().expect("TITAN_ENDPOINT must be set");
        let client = TitanClient::new(config);

        // cbBTC mint (8 decimals like BTC)
        let cbbtc_mint: Pubkey = "cbbtcf3aa214zXHbiAZQwf4122FBYbraNdFqgw4iMij".parse().unwrap();
        let usdc_mint: Pubkey = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".parse().unwrap();

        use solana_sdk::signer::Signer;
        let user_pubkey: Pubkey = std::env::var("KEYPAIR_PATH")
            .ok()
            .and_then(|path| {
                let data = std::fs::read_to_string(&path).ok()?;
                let bytes: Vec<u8> = serde_json::from_str(&data).ok()?;
                solana_sdk::signature::Keypair::from_bytes(&bytes).ok()
            })
            .map(|kp| kp.pubkey())
            .unwrap_or_else(|| {
                "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1".parse().unwrap()
            });

        println!("\n=== Testing cbBTC -> USDC ===");

        // Test 1: Small amount (9567 satoshis = ~$0.008) - the failing case
        let small_amount = 9567u64;
        println!("\n--- Test 1: Small amount {} satoshis (~$0.008) ---", small_amount);
        match client
            .get_quote(&cbbtc_mint, &usdc_mint, small_amount, Some(50), false, Some(58), user_pubkey)
            .await
        {
            Ok((route, instructions)) => {
                println!("Small amount SUCCESS: in={} out={} ixs={}", route.in_amount, route.out_amount, instructions.len());
            }
            Err(e) => {
                println!("Small amount FAILED: {:?}", e);
            }
        }

        // Test 2: Larger amount (100000 satoshis = 0.001 cbBTC = ~$85)
        let larger_amount = 100_000u64;
        println!("\n--- Test 2: Larger amount {} satoshis (~$85) ---", larger_amount);
        match client
            .get_quote(&cbbtc_mint, &usdc_mint, larger_amount, Some(50), false, Some(58), user_pubkey)
            .await
        {
            Ok((route, instructions)) => {
                println!("Larger amount SUCCESS: in={} out={} ixs={}", route.in_amount, route.out_amount, instructions.len());
            }
            Err(e) => {
                println!("Larger amount FAILED: {:?}", e);
            }
        }

        // Test 3: Even larger (10000000 satoshis = 0.1 cbBTC = ~$8,500)
        let big_amount = 10_000_000u64;
        println!("\n--- Test 3: Big amount {} satoshis (~$8,500) ---", big_amount);
        match client
            .get_quote(&cbbtc_mint, &usdc_mint, big_amount, Some(50), false, Some(58), user_pubkey)
            .await
        {
            Ok((route, instructions)) => {
                println!("Big amount SUCCESS: in={} out={} ixs={}", route.in_amount, route.out_amount, instructions.len());
            }
            Err(e) => {
                println!("Big amount FAILED: {:?}", e);
            }
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_titan_pyusd() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .init();

        let config = TitanConfig::from_env().expect("TITAN_ENDPOINT must be set");
        let client = TitanClient::new(config);

        let sol_mint: Pubkey = "So11111111111111111111111111111111111111112".parse().unwrap();
        let pyusd_mint: Pubkey = "2b1kV6DkPAnxd5ixfnxCpjxmKwqjjaYmCZfHsFu24GXo".parse().unwrap();

        // Load user pubkey from keypair file
        use solana_sdk::signer::Signer;
        let user_pubkey: Pubkey = std::env::var("KEYPAIR_PATH")
            .ok()
            .and_then(|path| {
                let data = std::fs::read_to_string(&path).ok()?;
                let bytes: Vec<u8> = serde_json::from_str(&data).ok()?;
                solana_sdk::signature::Keypair::from_bytes(&bytes).ok()
            })
            .map(|kp| kp.pubkey())
            .unwrap_or_else(|| {
                "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1".parse().unwrap()
            });

        // 100 SOL like the website test
        let amount = 100_000_000_000u64;

        println!("\n=== Testing SOL -> PYUSD ===");
        println!("Amount: {} lamports", amount);

        match client
            .get_quote(&sol_mint, &pyusd_mint, amount, Some(50), false, Some(58), user_pubkey)
            .await
        {
            Ok((route, instructions)) => {
                println!("Quote SUCCESS: in={} out={} ixs={}", route.in_amount, route.out_amount, instructions.len());
            }
            Err(e) => {
                println!("Quote FAILED: {:?}", e);
            }
        }
    }

    /// Test price impact with large swap (10000 SOL)
    /// Run with: cargo test test_price_impact -- --nocapture --ignored
    #[tokio::test]
    #[ignore]
    async fn test_price_impact() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .init();

        let config = TitanConfig::from_env().expect("TITAN_ENDPOINT must be set");
        let client = TitanClient::new(config);

        let sol_mint: Pubkey = "So11111111111111111111111111111111111111112".parse().unwrap();
        let usdc_mint: Pubkey = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".parse().unwrap();

        use solana_sdk::signer::Signer;
        let user_pubkey: Pubkey = std::env::var("KEYPAIR_PATH")
            .ok()
            .and_then(|path| {
                let data = std::fs::read_to_string(&path).ok()?;
                let bytes: Vec<u8> = serde_json::from_str(&data).ok()?;
                solana_sdk::signature::Keypair::from_bytes(&bytes).ok()
            })
            .map(|kp| kp.pubkey())
            .unwrap_or_else(|| {
                "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1".parse().unwrap()
            });

        println!("\n=== Testing Price Impact ===");

        // Test with 10000 SOL - should have noticeable price impact
        let large_amount = 10_000_000_000_000u64; // 10000 SOL
        println!("\n--- Large swap: 10000 SOL -> USDC ---");
        match client
            .get_quote(&sol_mint, &usdc_mint, large_amount, Some(50), false, Some(58), user_pubkey)
            .await
        {
            Ok((route, _instructions)) => {
                println!("SUCCESS:");
                println!("  in_amount: {} SOL", route.in_amount as f64 / 1e9);
                println!("  out_amount: {} USDC", route.out_amount as f64 / 1e6);
                println!("  price_impact_pct: '{}'", route.price_impact_pct);
                println!("  price_impact empty: {}", route.price_impact_pct.is_empty());

                // Parse and validate
                let impact: f32 = if route.price_impact_pct.is_empty() {
                    println!("  WARNING: price_impact_pct is EMPTY");
                    0.0
                } else {
                    match route.price_impact_pct.parse::<f32>() {
                        Ok(v) => {
                            println!("  Parsed impact: {}%", v);
                            v
                        }
                        Err(e) => {
                            println!("  ERROR parsing price_impact: {:?}", e);
                            0.0
                        }
                    }
                };

                // For 10000 SOL we'd expect some price impact
                if impact > 0.0 {
                    println!("  Price impact detected: {}%", impact);
                } else {
                    println!("  WARNING: No price impact for 10000 SOL swap!");
                }
            }
            Err(e) => {
                println!("FAILED: {:?}", e);
            }
        }

        // Also test small amount for comparison
        let small_amount = 1_000_000_000u64; // 1 SOL
        println!("\n--- Small swap: 1 SOL -> USDC ---");
        match client
            .get_quote(&sol_mint, &usdc_mint, small_amount, Some(50), false, Some(58), user_pubkey)
            .await
        {
            Ok((route, _)) => {
                println!("  price_impact_pct: '{}'", route.price_impact_pct);
            }
            Err(e) => {
                println!("FAILED: {:?}", e);
            }
        }
    }

    /// Test transaction size estimation with lookup table
    /// Run with: cargo test test_lookup_table_size -- --nocapture --ignored
    #[tokio::test]
    #[ignore]
    async fn test_lookup_table_size() {
        println!("\n=== Testing Lookup Table Size Reduction ===\n");

        // From the failing transaction logs:
        // - Transaction size: 3312 bytes base64 (max 1644)
        // - Market: 7u3HeHxYDLhnCoErrtycNokbQYbWGzLs6JSDqGAv5PfF
        // - 55 reserves in market
        // - 15 instructions

        // Base64 encoding inflates by ~4/3, so:
        // - 3312 base64 bytes ≈ 2484 raw bytes
        // - 1644 base64 bytes ≈ 1232 raw bytes (Solana limit)

        let original_base64_size: i32 = 3312;
        let max_base64_size: i32 = 1644;
        let original_raw_size: i32 = original_base64_size * 3 / 4; // ~2484
        let max_raw_size: i32 = 1232;

        println!("Original transaction:");
        println!("  Base64 size: {} bytes", original_base64_size);
        println!("  Raw size: ~{} bytes", original_raw_size);
        println!("  Max allowed: {} bytes", max_raw_size);
        println!("  Over limit by: {} bytes", original_raw_size - max_raw_size);

        // In Solana transactions, accounts are listed ONCE in the header, not per instruction.
        // The transaction message format:
        // - Header (3 bytes): num_required_signatures, num_readonly_signed, num_readonly_unsigned
        // - Account keys (N * 32 bytes): unique accounts used by all instructions
        // - Recent blockhash (32 bytes)
        // - Instructions (variable): each has program_id_index (1), accounts (indices), data
        //
        // For a flash loan liquidation with ~70 unique accounts:
        // - Account keys: 70 * 32 = 2240 bytes (this is the main size)
        // - Instructions: ~300 bytes (indices are 1 byte each)
        // - Overhead: ~100 bytes
        // Total: ~2640 bytes raw, which matches our ~2484 estimate

        let unique_accounts: i32 = 70; // Estimate of unique accounts in the failing tx
        let accounts_in_lut: i32 = 340; // Addresses in lookup table (55 reserves * 6 + extras)

        // Without LUT: all accounts are full 32-byte pubkeys
        // With LUT: accounts in LUT are 1-byte indices, others still 32 bytes
        // LUT overhead: 32 bytes per LUT address in message

        let kamino_accounts: i32 = 50; // Accounts that would be in Kamino LUT
        let swap_accounts: i32 = 15;   // Accounts only in swap instructions
        let other_accounts: i32 = 5;   // System program, sysvar, etc.

        println!("\nAccount breakdown estimate:");
        println!("  Unique accounts in tx: ~{}", unique_accounts);
        println!("  Kamino-related (in LUT): ~{}", kamino_accounts);
        println!("  Swap-related: ~{}", swap_accounts);
        println!("  System/other: ~{}", other_accounts);
        println!("  Addresses available in LUT: ~{}", accounts_in_lut);

        // Size calculation for account keys section:
        // Without LUT: all 32 bytes each
        let size_without_lut: i32 = unique_accounts * 32;

        // With LUT: Kamino accounts become 1-byte indices, others stay 32 bytes
        // Plus 32 bytes for the LUT address itself
        let size_with_lut: i32 = kamino_accounts * 1 + (swap_accounts + other_accounts) * 32 + 32;

        let savings: i32 = size_without_lut - size_with_lut;

        println!("\nSize calculation (account keys section):");
        println!("  Without LUT: {} bytes", size_without_lut);
        println!("  With LUT: {} bytes", size_with_lut);
        println!("  Savings: {} bytes", savings);

        // Estimate new transaction size
        // Non-account parts: instructions data, signatures, blockhash, header
        let instruction_data: i32 = 400; // 15 instructions with data
        let signatures: i32 = 64;        // 1 signature
        let header_and_blockhash: i32 = 35;
        let non_account_overhead: i32 = instruction_data + signatures + header_and_blockhash;

        let estimated_old_size: i32 = size_without_lut + non_account_overhead;
        let estimated_new_size: i32 = size_with_lut + non_account_overhead;
        let estimated_new_base64: i32 = estimated_new_size * 4 / 3;

        println!("\nTransaction size estimate:");
        println!("  Non-account overhead: ~{} bytes", non_account_overhead);
        println!("  Old raw size: ~{} bytes (actual: {})", estimated_old_size, original_raw_size);
        println!("  New raw size: ~{} bytes", estimated_new_size);
        println!("  New base64 size: ~{} bytes", estimated_new_base64);
        println!("  Max allowed: {} bytes", max_base64_size);

        if estimated_new_base64 < max_base64_size {
            println!("\n✓ Transaction SHOULD FIT with lookup table!");
            println!("  Headroom: {} bytes ({:.1}% of max)",
                max_base64_size - estimated_new_base64,
                (1.0 - estimated_new_base64 as f64 / max_base64_size as f64) * 100.0
            );
        } else {
            println!("\n✗ Transaction may still be too large");
            println!("  Over limit by: {} bytes", estimated_new_base64 - max_base64_size);
        }

        // What if swap also has a LUT?
        println!("\n--- With BOTH Kamino and Swap LUTs ---");
        let size_with_both_luts: i32 = kamino_accounts * 1 + swap_accounts * 1 + other_accounts * 32 + 64; // +64 for 2 LUTs
        let new_size_both: i32 = size_with_both_luts + non_account_overhead;
        let new_base64_both: i32 = new_size_both * 4 / 3;
        println!("  Account keys with both LUTs: {} bytes", size_with_both_luts);
        println!("  New raw size: ~{} bytes", new_size_both);
        println!("  New base64 size: ~{} bytes", new_base64_both);
        if new_base64_both < max_base64_size {
            println!("  ✓ Would fit with headroom: {} bytes", max_base64_size - new_base64_both);
        }

        println!("\n=== Conclusion ===");
        println!("With Kamino LUT alone: Transaction should fit (~{} bytes < {} max)", estimated_new_base64, max_base64_size);
        println!("The lookup table provides ~{} bytes savings, reducing size by {:.0}%",
            savings, (savings as f64 / size_without_lut as f64) * 100.0);
    }

    /// Test reserve lookup for a specific obligation (Issue 2 investigation)
    /// This reproduces the exact scenario from the failing liquidation
    /// Run with: RPC_URL=<mainnet-url> cargo test test_reserve_lookup -- --nocapture --ignored
    #[tokio::test]
    #[ignore]
    async fn test_reserve_lookup() {
        use anchor_client::solana_client::{
            nonblocking::rpc_client::RpcClient,
            rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
            rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType},
        };
        use solana_sdk::commitment_config::CommitmentConfig;
        use solana_account_decoder::UiAccountEncoding;

        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_test_writer()
            .init();

        // Get RPC URL from env (use public mainnet as fallback for testing)
        let rpc_url = std::env::var("RPC_URL")
            .or_else(|_| std::env::var("CLUSTER"))
            .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());
        let client = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());

        // The failing obligation from logs
        let obligation_pubkey: Pubkey = "EaKZTZCTZ2m6bjGxC3rNQ2GMvG19ius5qTRkShPmLTCE".parse().unwrap();

        // Expected reserves from the failing liquidation logs:
        // Deposits: cbBTC (37Jk...), xBTC (4Hyr...)
        // Borrows: USDG (ESCk...), USDC (D6q6...)

        println!("\n=== Testing Reserve Lookup (Issue 2) ===");
        println!("Obligation: {}", obligation_pubkey);

        // 1. Fetch the obligation account
        println!("\n--- Step 1: Fetching obligation ---");
        let obligation_account = client
            .get_account(&obligation_pubkey)
            .await
            .expect("Failed to fetch obligation account");

        println!("Obligation account data length: {}", obligation_account.data.len());
        println!("Obligation owner: {}", obligation_account.owner);

        // The obligation has an 8-byte discriminator prefix
        if obligation_account.data.len() < 8 + std::mem::size_of::<kamino_lending::Obligation>() {
            panic!("Obligation data too short");
        }

        let obligation: kamino_lending::Obligation = *bytemuck::from_bytes(
            &obligation_account.data[8..8 + std::mem::size_of::<kamino_lending::Obligation>()]
        );

        println!("Lending market: {}", obligation.lending_market);
        println!("Deposits count: {}", obligation.deposits.iter().filter(|d| d.deposit_reserve != Pubkey::default()).count());
        println!("Borrows count: {}", obligation.borrows.iter().filter(|b| b.borrow_reserve != Pubkey::default()).count());

        // List all deposit reserves
        println!("\nDeposit reserves:");
        for (i, deposit) in obligation.deposits.iter().enumerate() {
            if deposit.deposit_reserve != Pubkey::default() {
                println!("  [{}] {}", i, deposit.deposit_reserve);
            }
        }

        // List all borrow reserves
        println!("\nBorrow reserves:");
        for (i, borrow) in obligation.borrows.iter().enumerate() {
            if borrow.borrow_reserve != Pubkey::default() {
                println!("  [{}] {}", i, borrow.borrow_reserve);
            }
        }

        // 2. Fetch all reserves for this lending market (same logic as accounts.rs)
        println!("\n--- Step 2: Fetching reserves for lending market ---");
        let klend_program_id: Pubkey = "KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD".parse().unwrap();

        // Filter by lending_market field at offset 32 (same as in accounts.rs)
        let filter = RpcFilterType::Memcmp(Memcmp::new(
            32,
            MemcmpEncodedBytes::Bytes(obligation.lending_market.to_bytes().to_vec()),
        ));

        let config = RpcProgramAccountsConfig {
            filters: Some(vec![filter]),
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                commitment: Some(CommitmentConfig::confirmed()),
                ..Default::default()
            },
            ..Default::default()
        };

        let accounts = client
            .get_program_accounts_with_config(&klend_program_id, config)
            .await
            .expect("Failed to fetch program accounts");

        println!("Found {} accounts for lending market", accounts.len());

        // Parse reserves
        let mut fetched_reserves: std::collections::HashSet<Pubkey> = std::collections::HashSet::new();
        for (pubkey, account) in &accounts {
            // Reserves have a specific size (same check as in rpc.rs)
            if account.data.len() == 8 + std::mem::size_of::<kamino_lending::Reserve>() {
                fetched_reserves.insert(*pubkey);
                println!("  Reserve: {}", pubkey);
            }
        }

        println!("\nTotal reserves found: {}", fetched_reserves.len());

        // 3. Check if all obligation reserves are in the fetched set
        println!("\n--- Step 3: Checking reserve presence ---");

        let mut all_present = true;
        println!("\nChecking deposit reserves:");
        for deposit in obligation.deposits.iter() {
            if deposit.deposit_reserve != Pubkey::default() {
                let present = fetched_reserves.contains(&deposit.deposit_reserve);
                println!("  {} - {}", deposit.deposit_reserve, if present { "FOUND" } else { "MISSING!" });
                if !present {
                    all_present = false;
                }
            }
        }

        println!("\nChecking borrow reserves:");
        for borrow in obligation.borrows.iter() {
            if borrow.borrow_reserve != Pubkey::default() {
                let present = fetched_reserves.contains(&borrow.borrow_reserve);
                println!("  {} - {}", borrow.borrow_reserve, if present { "FOUND" } else { "MISSING!" });
                if !present {
                    all_present = false;
                }
            }
        }

        // 4. Verify first borrow/deposit (these are used for liquidation)
        println!("\n--- Step 4: Checking liquidation reserves ---");

        // OLD (buggy) approach: just use index 0
        let debt_res_key_old = obligation.borrows[0].borrow_reserve;
        let coll_res_key_old = obligation.deposits[0].deposit_reserve;
        println!("OLD CODE (borrows[0]): {} - {}", debt_res_key_old, if debt_res_key_old == Pubkey::default() { "EMPTY SLOT!" } else if fetched_reserves.contains(&debt_res_key_old) { "FOUND" } else { "MISSING!" });
        println!("OLD CODE (deposits[0]): {} - {}", coll_res_key_old, if coll_res_key_old == Pubkey::default() { "EMPTY SLOT!" } else if fetched_reserves.contains(&coll_res_key_old) { "FOUND" } else { "MISSING!" });

        // NEW (fixed) approach: find first non-empty slot
        let debt_res_key_new = obligation.borrows.iter()
            .find(|b| b.borrow_reserve != Pubkey::default())
            .map(|b| b.borrow_reserve);
        let coll_res_key_new = obligation.deposits.iter()
            .find(|d| d.deposit_reserve != Pubkey::default())
            .map(|d| d.deposit_reserve);
        println!("NEW CODE (first non-empty borrow): {:?} - {}", debt_res_key_new, debt_res_key_new.map(|k| if fetched_reserves.contains(&k) { "FOUND" } else { "MISSING" }).unwrap_or("None"));
        println!("NEW CODE (first non-empty deposit): {:?} - {}", coll_res_key_new, coll_res_key_new.map(|k| if fetched_reserves.contains(&k) { "FOUND" } else { "MISSING" }).unwrap_or("None"));

        // Summary
        println!("\n=== Test Result ===");
        if all_present {
            println!("SUCCESS: All obligation reserves found in market reserves");
        } else {
            println!("FAILURE: Some reserves are missing!");
            println!("This explains the 'reserve not found' error");
        }
    }

    /// Test transaction size with lookup table using REAL data from failed liquidation
    /// This simulates the exact scenario from the 3608 byte failed transaction
    /// Run with: cargo test test_flash_loan_liquidation_tx_size -- --nocapture --ignored
    #[tokio::test]
    #[ignore]
    async fn test_flash_loan_liquidation_tx_size() {
        use std::collections::HashSet;

        println!("\n=== Flash Loan Liquidation Transaction Size Test ===\n");

        // Data from the ACTUAL failed liquidation log:
        // 2026-01-30T04:58:14.219874Z WARN Transaction too large: 3608 bytes (max 1644)
        // 2026-01-30T04:58:14.219890Z INFO Debug: 15 instructions, 1 lookup tables (1 liquidator, 0 swap)
        // Market: 7u3HeHxYDLhnCoErrtycNokbQYbWGzLs6JSDqGAv5PfF (55 reserves)
        // debt_reserve=ESCkPWKHmgNE7Msf77n9yzqJd5kQVWWGy3o5Mgxhvavp (USDG)
        // coll_reserve=d4A2prbA2whesmvHaL88BH6Ewn5N4bTSU2Ze8P6Bc4Q (SOL)
        // Lookup table had 256 addresses but 572 MISSING keys

        let failed_tx_base64_size = 3608;
        let max_base64_size = 1644;
        let num_instructions = 15;
        let num_reserves = 55;
        let num_liquidator_atas = 110;

        println!("Failed transaction details:");
        println!("  Base64 size: {} bytes (max {})", failed_tx_base64_size, max_base64_size);
        println!("  Instructions: {}", num_instructions);
        println!("  Reserves in market: {}", num_reserves);
        println!("  Liquidator ATAs: {}", num_liquidator_atas);

        // Accounts needed for a flash loan liquidation (15 instructions):
        // 1. flash_borrow_reserve_liquidity - debt reserve accounts
        // 2-5. init_obligation_farm, refresh_reserve (x2), refresh_obligation
        // 6. refresh_obligation_farms
        // 7. liquidate_obligation_and_redeem_reserve_collateral
        // 8-13. swap instructions (6 instructions)
        // 14. flash_repay_reserve_liquidity
        // 15. (possibly one more instruction)

        // Unique accounts breakdown:
        let kamino_unique_accounts = vec![
            // System/common
            ("lending_market", 1),
            ("lending_market_authority", 1),
            ("sysvar_instructions", 1),
            ("token_program", 1),
            ("system_program", 1),
            ("klend_program", 1),

            // Debt reserve accounts
            ("debt_reserve", 1),
            ("debt_reserve_liquidity_supply", 1),
            ("debt_reserve_liquidity_fee_vault", 1),
            ("debt_reserve_liquidity_mint", 1),

            // Collateral reserve accounts
            ("coll_reserve", 1),
            ("coll_reserve_collateral_supply", 1),
            ("coll_reserve_collateral_mint", 1),
            ("coll_reserve_liquidity_supply", 1),

            // Obligation
            ("obligation", 1),
            ("obligation_farm_state", 1),

            // Liquidator accounts
            ("liquidator", 1),
            ("liquidator_debt_ata", 1),
            ("liquidator_coll_ata", 1),

            // Oracles (assuming 2 per reserve)
            ("debt_oracle", 1),
            ("coll_oracle", 1),
        ];

        let kamino_accounts_count: i32 = kamino_unique_accounts.iter().map(|(_, c)| c).sum();
        println!("\nKamino accounts in transaction: {}", kamino_accounts_count);

        // Swap accounts (from Titan DEX aggregator)
        // Typically includes: DEX program, pool accounts, token accounts, etc.
        let swap_accounts = vec![
            ("swap_program_1", 1),  // e.g., Raydium, Orca
            ("swap_program_2", 1),  // might use multiple DEXes
            ("pool_state", 2),
            ("pool_token_vault_a", 2),
            ("pool_token_vault_b", 2),
            ("pool_authority", 2),
            ("pool_fee_account", 1),
            ("intermediate_token_account", 2),
        ];
        let swap_accounts_count: i32 = swap_accounts.iter().map(|(_, c)| c).sum();
        println!("Swap accounts in transaction: {}", swap_accounts_count);

        let total_unique_accounts = kamino_accounts_count + swap_accounts_count;
        println!("Total unique accounts: {}", total_unique_accounts);

        // Calculate transaction size WITHOUT lookup tables
        println!("\n--- Transaction Size WITHOUT Lookup Tables ---");

        // Solana transaction structure:
        // - Signature (64 bytes)
        // - Message header (3 bytes)
        // - Account keys (N * 32 bytes)
        // - Recent blockhash (32 bytes)
        // - Instruction count (compact-u16)
        // - Instructions (variable)

        let signature_size = 64;
        let header_size = 3;
        let account_keys_size = total_unique_accounts * 32;
        let blockhash_size = 32;

        // Estimate instruction data size
        // Each instruction: program_id_index (1) + accounts_len (compact-u16) + accounts (N * 1) + data_len + data
        // Flash borrow/repay: ~50 bytes data each
        // Refresh reserve: ~10 bytes each
        // Liquidate: ~30 bytes
        // Swap: ~100-200 bytes each (6 instructions with multi-hop routes)
        let instruction_overhead = num_instructions * 10; // program_id_index + accounts overhead
        let instruction_account_refs = total_unique_accounts * 2; // accounts referenced in instructions (some multiple times)
        let instruction_data_multi_hop = 50 + 50 + 10 * 4 + 30 + 150 * 6; // 6 swap instructions
        let instructions_size = instruction_overhead + instruction_account_refs + instruction_data_multi_hop;

        // With direct routes only: 1-2 swap instructions instead of 6
        let num_instructions_direct = 10; // flash_borrow + 4 refreshes + liquidate + 2 swaps + flash_repay
        let instruction_data_direct = 50 + 50 + 10 * 4 + 30 + 150 * 2; // 2 swap instructions
        let instructions_size_direct = num_instructions_direct * 10 + total_unique_accounts * 2 + instruction_data_direct;

        let raw_size_no_lut = signature_size + header_size + account_keys_size + blockhash_size + instructions_size;
        let base64_size_no_lut = (raw_size_no_lut * 4 + 2) / 3; // base64 encoding

        println!("  Signature: {} bytes", signature_size);
        println!("  Header: {} bytes", header_size);
        println!("  Account keys ({} * 32): {} bytes", total_unique_accounts, account_keys_size);
        println!("  Blockhash: {} bytes", blockhash_size);
        println!("  Instructions: ~{} bytes", instructions_size);
        println!("  Total raw: ~{} bytes", raw_size_no_lut);
        println!("  Total base64: ~{} bytes", base64_size_no_lut);

        // Calculate with OLD lookup table (256 addresses but WRONG ones)
        println!("\n--- With OLD Lookup Table (wrong accounts) ---");
        // The old LUT had 256 addresses but they were for ALL reserves, not just the ones in this tx
        // So 572 keys were "missing" - meaning accounts in the tx weren't in the LUT
        let accounts_in_old_lut = 0; // effectively 0 because the accounts weren't matching
        let accounts_not_in_old_lut = total_unique_accounts;
        let lut_overhead = 32; // LUT address itself

        let account_keys_with_old_lut = accounts_not_in_old_lut * 32 + accounts_in_old_lut * 1 + lut_overhead;
        let raw_size_old_lut = signature_size + header_size + account_keys_with_old_lut + blockhash_size + instructions_size;
        let base64_size_old_lut = (raw_size_old_lut * 4 + 2) / 3;

        println!("  Accounts in LUT: {} (but WRONG accounts)", accounts_in_old_lut);
        println!("  Accounts NOT in LUT: {}", accounts_not_in_old_lut);
        println!("  Account keys section: {} bytes", account_keys_with_old_lut);
        println!("  Total base64: ~{} bytes (actual was {})", base64_size_old_lut, failed_tx_base64_size);

        // Calculate with NEW lookup table (correct accounts)
        println!("\n--- With NEW Lookup Table (correct accounts) ---");

        // NEW collect_keys only includes:
        // - Reserve pubkeys (55) - but only 2 are used in this tx
        // - Liquidator ATAs (110) - but only 2 are used in this tx
        // - Lending market info (~5)
        // Total in LUT: ~170 keys

        // For THIS transaction, the accounts that would be in the LUT:
        let accounts_in_new_lut = vec![
            "lending_market",           // in LUT
            "lending_market_authority", // in LUT
            "debt_reserve",             // in LUT (reserve pubkey)
            "coll_reserve",             // in LUT (reserve pubkey)
            "liquidator_debt_ata",      // in LUT (liquidator ATA)
            "liquidator_coll_ata",      // in LUT (liquidator ATA)
        ];
        let in_lut_count = accounts_in_new_lut.len() as i32;

        // Accounts NOT in the new LUT (vaults, oracles, swap accounts):
        let not_in_lut_count = total_unique_accounts - in_lut_count;

        println!("  Accounts IN new LUT: {} {:?}", in_lut_count, accounts_in_new_lut);
        println!("  Accounts NOT in LUT: {} (vaults, oracles, swap, etc.)", not_in_lut_count);

        let account_keys_with_new_lut = not_in_lut_count * 32 + in_lut_count * 1 + lut_overhead;
        let raw_size_new_lut = signature_size + header_size + account_keys_with_new_lut + blockhash_size + instructions_size;
        let base64_size_new_lut = (raw_size_new_lut * 4 + 2) / 3;

        println!("  Account keys section: {} bytes", account_keys_with_new_lut);
        println!("  Total raw: ~{} bytes", raw_size_new_lut);
        println!("  Total base64: ~{} bytes", base64_size_new_lut);

        // Calculate with BOTH lookup tables (Kamino + Titan swap)
        println!("\n--- With BOTH Lookup Tables (Kamino + Swap) ---");

        // If Titan also provides a LUT with swap accounts:
        let swap_accounts_in_titan_lut = swap_accounts_count;
        let kamino_accounts_in_lut = in_lut_count;
        let total_in_luts = swap_accounts_in_titan_lut + kamino_accounts_in_lut;
        let not_in_any_lut = total_unique_accounts - total_in_luts;

        println!("  Kamino accounts in LUT: {}", kamino_accounts_in_lut);
        println!("  Swap accounts in Titan LUT: {}", swap_accounts_in_titan_lut);
        println!("  Accounts NOT in any LUT: {} (vaults, oracles)", not_in_any_lut);

        let two_lut_overhead = 64; // 2 LUT addresses
        let account_keys_both_luts = not_in_any_lut * 32 + total_in_luts * 1 + two_lut_overhead;
        let raw_size_both_luts = signature_size + header_size + account_keys_both_luts + blockhash_size + instructions_size;
        let base64_size_both_luts = (raw_size_both_luts * 4 + 2) / 3;

        println!("  Account keys section: {} bytes", account_keys_both_luts);
        println!("  Total raw: ~{} bytes", raw_size_both_luts);
        println!("  Total base64: ~{} bytes", base64_size_both_luts);

        // Best case: include vaults in the LUT too
        println!("\n--- BEST CASE: All Kamino accounts in LUT ---");

        // If we include debt/coll reserve vaults and mints in the LUT:
        let all_kamino_in_lut = kamino_accounts_count;
        let only_swap_not_in_lut = swap_accounts_count;

        let account_keys_best = only_swap_not_in_lut * 32 + all_kamino_in_lut * 1 + lut_overhead;
        let raw_size_best = signature_size + header_size + account_keys_best + blockhash_size + instructions_size;
        let base64_size_best = (raw_size_best * 4 + 2) / 3;

        println!("  All Kamino accounts in LUT: {}", all_kamino_in_lut);
        println!("  Only swap accounts outside: {}", only_swap_not_in_lut);
        println!("  Account keys section: {} bytes", account_keys_best);
        println!("  Total base64: ~{} bytes", base64_size_best);

        // Calculate with DIRECT ROUTES (fewer swap instructions)
        println!("\n--- With DIRECT ROUTES (fewer swaps) + Both LUTs ---");

        // Direct routes = 2 swap instructions instead of 6
        // This dramatically reduces instruction data
        let direct_route_swap_accounts = 8; // fewer swap accounts with direct route
        let direct_total_accounts = kamino_accounts_count + direct_route_swap_accounts;
        let direct_accounts_in_luts = kamino_accounts_count + direct_route_swap_accounts;
        let direct_not_in_luts = 10; // vaults, oracles still outside

        let direct_account_keys = direct_not_in_luts * 32 + direct_accounts_in_luts * 1 + 64;
        let direct_raw = signature_size + header_size + direct_account_keys + blockhash_size + instructions_size_direct;
        let direct_base64 = (direct_raw * 4 + 2) / 3;

        println!("  Instructions: {} (was {})", num_instructions_direct, num_instructions);
        println!("  Instruction data: ~{} bytes (was ~{})", instruction_data_direct, instruction_data_multi_hop);
        println!("  Total accounts: {}", direct_total_accounts);
        println!("  Account keys section: {} bytes", direct_account_keys);
        println!("  Total raw: ~{} bytes", direct_raw);
        println!("  Total base64: ~{} bytes", direct_base64);

        // Summary
        println!("\n=== SUMMARY ===");
        println!("Max allowed: {} bytes base64", max_base64_size);
        println!("");
        println!("Multi-hop (6 swaps):");
        println!("  No LUT:           ~{} bytes - {}", base64_size_no_lut,
            if base64_size_no_lut <= max_base64_size { "✓ FITS" } else { "✗ TOO LARGE" });
        println!("  Old LUT (wrong):  ~{} bytes - {} (actual: {})", base64_size_old_lut,
            if base64_size_old_lut <= max_base64_size { "✓ FITS" } else { "✗ TOO LARGE" }, failed_tx_base64_size);
        println!("  New LUT (basic):  ~{} bytes - {}", base64_size_new_lut,
            if base64_size_new_lut <= max_base64_size { "✓ FITS" } else { "✗ TOO LARGE" });
        println!("  Both LUTs:        ~{} bytes - {}", base64_size_both_luts,
            if base64_size_both_luts <= max_base64_size { "✓ FITS" } else { "✗ TOO LARGE" });
        println!("  Best case:        ~{} bytes - {}", base64_size_best,
            if base64_size_best <= max_base64_size { "✓ FITS" } else { "✗ TOO LARGE" });
        println!("");
        println!("Direct routes (2 swaps):");
        println!("  With both LUTs:   ~{} bytes - {}", direct_base64,
            if direct_base64 <= max_base64_size { "✓ FITS" } else { "✗ TOO LARGE" });

        // Recommendation
        println!("\n=== RECOMMENDATION ===");
        if direct_base64 <= max_base64_size {
            println!("✓ Use DIRECT ROUTES ONLY (only_direct_routes=true) to fit transaction");
            println!("  This reduces swap instructions from 6 to 1-2");
        } else if base64_size_best <= max_base64_size {
            println!("⚠ Need optimal lookup tables + direct routes");
        } else {
            println!("✗ Transaction cannot fit - need to split or use different approach");
        }

        // Assert that direct routes would fit
        assert!(direct_base64 <= max_base64_size,
            "Direct routes scenario ({} bytes) exceeds limit ({} bytes)",
            direct_base64, max_base64_size);
    }
}
