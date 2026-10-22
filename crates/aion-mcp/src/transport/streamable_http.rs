use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use reqwest::StatusCode;
use reqwest::header::{HeaderMap, HeaderValue};
use tokio::sync::Mutex;

use super::headers::{McpHeaderProvider, apply_dynamic_headers};
use super::{McpError, McpTransport};
use crate::protocol::{JsonRpcRequest, JsonRpcResponse};

/// Streamable HTTP transport: uses HTTP POST for both requests and responses
/// Supports optional SSE streaming for server responses
pub struct StreamableHttpTransport {
    client: reqwest::Client,
    /// MCP server name, passed to the header provider.
    server: String,
    url: String,
    /// Statically configured headers. Entries here always win over
    /// provider-supplied ones (a user-supplied `Authorization` stays intact).
    headers: HeaderMap,
    /// Resolved for every request so refreshed credentials reach a session
    /// that outlives the access token captured at connect time.
    header_provider: Option<Arc<dyn McpHeaderProvider>>,
    session_id: Mutex<Option<String>>,
    next_id: AtomicU64,
}

impl StreamableHttpTransport {
    /// Create a new Streamable HTTP transport
    pub async fn connect(
        server: &str,
        url: &str,
        headers: &HashMap<String, String>,
        header_provider: Option<Arc<dyn McpHeaderProvider>>,
    ) -> Result<Self, McpError> {
        let mut header_map = HeaderMap::new();
        for (k, v) in headers {
            let name = reqwest::header::HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| McpError::Transport(format!("Invalid header name '{}': {}", k, e)))?;
            let value = HeaderValue::from_str(v)
                .map_err(|e| McpError::Transport(format!("Invalid header value '{}': {}", v, e)))?;
            header_map.insert(name, value);
        }

        Ok(Self {
            client: reqwest::Client::new(),
            server: server.to_string(),
            url: url.to_string(),
            headers: header_map,
            header_provider,
            session_id: Mutex::new(None),
            next_id: AtomicU64::new(1),
        })
    }

    pub fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Headers for one request: statically configured entries first, then
    /// whatever the provider resolves now (static entries win on conflict).
    async fn request_headers(&self, force_refresh: bool) -> HeaderMap {
        let mut headers = self.headers.clone();
        if let Some(provider) = &self.header_provider {
            let dynamic = provider.headers(&self.server, &self.url, force_refresh).await;
            apply_dynamic_headers(&mut headers, dynamic);
        }
        headers
    }

    /// Build request with session ID header if available
    async fn build_request(&self, body: &str, force_refresh: bool) -> reqwest::RequestBuilder {
        let mut req = self
            .client
            .post(&self.url)
            .headers(self.request_headers(force_refresh).await)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json, text/event-stream");

        if let Some(sid) = self.session_id.lock().await.as_ref() {
            req = req.header("Mcp-Session-Id", sid.as_str());
        }

        req.body(body.to_string())
    }

    async fn send(&self, body: &str, force_refresh: bool) -> Result<reqwest::Response, McpError> {
        self.build_request(body, force_refresh)
            .await
            .send()
            .await
            .map_err(|e| McpError::Transport(format!("HTTP request failed: {}", e)))
    }

    /// Send `body`, retrying once with force-refreshed headers on HTTP 401.
    ///
    /// The retry only makes sense when a provider can hand out a new
    /// credential; without one the 401 is returned to the caller untouched.
    async fn send_with_auth_retry(&self, body: &str) -> Result<reqwest::Response, McpError> {
        let response = self.send(body, false).await?;
        if response.status() == StatusCode::UNAUTHORIZED && self.header_provider.is_some() {
            tracing::info!(server = %self.server, "MCP POST rejected with HTTP 401; refreshing credentials and retrying once");
            return self.send(body, true).await;
        }
        Ok(response)
    }

    /// Parse response based on content type
    async fn parse_response(&self, response: reqwest::Response) -> Result<JsonRpcResponse, McpError> {
        // Capture session ID from response headers
        if let Some(sid) = response.headers().get("mcp-session-id")
            && let Ok(sid_str) = sid.to_str()
        {
            *self.session_id.lock().await = Some(sid_str.to_string());
        }

        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        if content_type.contains("text/event-stream") {
            // SSE response: parse events to find the JSON-RPC response
            self.parse_sse_response(response).await
        } else {
            // Direct JSON response
            let text = response
                .text()
                .await
                .map_err(|e| McpError::Transport(format!("Read response body failed: {}", e)))?;
            serde_json::from_str(&text)
                .map_err(|e| McpError::Transport(format!("Parse JSON response failed: {} — raw: {}", e, text)))
        }
    }

    /// Parse an SSE stream response to extract JSON-RPC response
    async fn parse_sse_response(&self, response: reqwest::Response) -> Result<JsonRpcResponse, McpError> {
        use futures::StreamExt;

        let mut stream = response.bytes_stream();
        let mut buffer = String::new();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| McpError::Transport(format!("SSE read error: {}", e)))?;
            buffer.push_str(&String::from_utf8_lossy(&chunk));

            if let Some(rpc_response) = extract_jsonrpc_from_sse_buffer(&buffer) {
                return Ok(rpc_response);
            }
        }

        Err(McpError::Transport("SSE stream ended without JSON-RPC response".into()))
    }
}

/// Extract the first complete JSON-RPC response from an accumulated SSE buffer.
///
/// Tolerant of both LF (`\n`) and CRLF (`\r\n`) line endings: Node
/// `@modelcontextprotocol/sdk` servers emit LF, while Python `fastmcp` / MCP
/// SDK servers (via `sse-starlette`, whose default separator is CRLF) emit
/// `\r\n\r\n` event terminators. Returns `None` until the buffer holds a
/// complete event carrying a parseable JSON-RPC response.
fn extract_jsonrpc_from_sse_buffer(buffer: &str) -> Option<JsonRpcResponse> {
    // Normalize CRLF to LF so blank-line event boundaries are detectable.
    let normalized = buffer.replace('\r', "");

    for event_block in normalized.split("\n\n") {
        let mut data_lines = Vec::new();
        for line in event_block.lines() {
            if let Some(value) = line.strip_prefix("data:") {
                data_lines.push(value.trim());
            }
        }

        let data = data_lines.join("\n");
        if !data.is_empty()
            && let Ok(rpc_response) = serde_json::from_str::<JsonRpcResponse>(&data)
        {
            return Some(rpc_response);
        }
    }

    None
}

#[async_trait]
impl McpTransport for StreamableHttpTransport {
    async fn request(&self, req: &JsonRpcRequest) -> Result<JsonRpcResponse, McpError> {
        let body =
            serde_json::to_string(req).map_err(|e| McpError::Transport(format!("JSON serialize error: {}", e)))?;

        let response = self.send_with_auth_retry(&body).await?;

        if !response.status().is_success() {
            return Err(McpError::Transport(format!(
                "HTTP request returned status: {}",
                response.status()
            )));
        }

        let rpc_response = self.parse_response(response).await?;

        if let Some(err) = &rpc_response.error {
            return Err(McpError::JsonRpc {
                code: err.code,
                message: err.message.clone(),
            });
        }

        Ok(rpc_response)
    }

    async fn notify(&self, req: &JsonRpcRequest) -> Result<(), McpError> {
        let body =
            serde_json::to_string(req).map_err(|e| McpError::Transport(format!("JSON serialize error: {}", e)))?;

        let response = self.send_with_auth_retry(&body).await?;
        if !response.status().is_success() {
            return Err(McpError::Transport(format!(
                "Notification request returned status: {}",
                response.status()
            )));
        }

        Ok(())
    }

    async fn close(&self) -> Result<(), McpError> {
        // No persistent connection to close for HTTP
        Ok(())
    }
}

#[cfg(test)]
#[path = "streamable_http_test.rs"]
mod streamable_http_test;
