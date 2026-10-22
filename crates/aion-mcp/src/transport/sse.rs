use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use reqwest::StatusCode;
use reqwest::header::{HeaderMap, HeaderValue};
use tokio::sync::{Mutex, oneshot};

use super::headers::{McpHeaderProvider, apply_dynamic_headers};
use super::{McpError, McpTransport};
use crate::protocol::{JsonRpcRequest, JsonRpcResponse};

/// SSE transport: connects to an SSE endpoint for server→client events,
/// sends requests via POST to the endpoint URL received from the SSE stream
pub struct SseTransport {
    client: reqwest::Client,
    /// MCP server name, passed to the header provider.
    server: String,
    /// Configured SSE endpoint, used to resolve request headers. The POST
    /// endpoint may differ (it arrives in the `endpoint` event).
    url: String,
    /// The POST endpoint URL (received from the SSE stream's "endpoint" event)
    post_url: String,
    /// Statically configured headers. Entries here always win over
    /// provider-supplied ones (a user-supplied `Authorization` stays intact).
    headers: HeaderMap,
    /// Resolved for every POST so refreshed credentials reach a session that
    /// outlives the access token captured at connect time.
    ///
    /// The long-lived GET event stream is authenticated once at connect; a
    /// server that closes it on token expiry requires a reconnect, which this
    /// transport does not perform (pre-existing behavior).
    header_provider: Option<Arc<dyn McpHeaderProvider>>,
    /// Pending request-response channels, keyed by JSON-RPC id
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcResponse>>>>,
    next_id: AtomicU64,
    /// Handle to the background SSE listener task
    _listener: tokio::task::JoinHandle<()>,
}

impl SseTransport {
    /// Connect to an SSE MCP server
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

        // Keep only user-configured headers on the transport. A token resolved
        // for the initial GET must not shadow later provider updates.
        let mut connection_headers = header_map.clone();
        if let Some(provider) = &header_provider {
            let dynamic = provider.headers(server, url, false).await;
            apply_dynamic_headers(&mut connection_headers, dynamic);
        }

        let client = reqwest::Client::new();

        // GET the SSE endpoint to establish the event stream
        let mut response = client
            .get(url)
            .headers(connection_headers)
            .header("Accept", "text/event-stream")
            .send()
            .await
            .map_err(|e| McpError::Transport(format!("SSE connection failed: {}", e)))?;

        if response.status() == StatusCode::UNAUTHORIZED
            && let Some(provider) = &header_provider
        {
            tracing::info!(
                server,
                "MCP SSE connection rejected with HTTP 401; refreshing credentials and retrying once"
            );
            let mut refreshed_headers = header_map.clone();
            apply_dynamic_headers(&mut refreshed_headers, provider.headers(server, url, true).await);
            response = client
                .get(url)
                .headers(refreshed_headers)
                .header("Accept", "text/event-stream")
                .send()
                .await
                .map_err(|e| McpError::Transport(format!("SSE connection failed: {}", e)))?;
        }

        if !response.status().is_success() {
            return Err(McpError::Transport(format!(
                "SSE connection returned status: {}",
                response.status()
            )));
        }

        let pending: Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcResponse>>>> = Arc::new(Mutex::new(HashMap::new()));

        // Parse the SSE stream to find the endpoint URL
        // The server sends an "endpoint" event with the POST URL
        let base_url = extract_base_url(url);
        let mut bytes_stream = response.bytes_stream();
        let mut buffer = String::new();
        let mut post_url: Option<String> = None;

        use futures::StreamExt;
        // Read initial events to get the endpoint URL
        while let Some(chunk) = bytes_stream.next().await {
            let chunk = chunk.map_err(|e| McpError::Transport(format!("SSE read error: {}", e)))?;
            // Normalize CRLF to LF: Python `fastmcp` / MCP SDK servers emit
            // `\r\n` separators (via `sse-starlette`), so event boundaries are
            // `\r\n\r\n` and would not match a `\n\n` search otherwise.
            buffer.push_str(&String::from_utf8_lossy(&chunk).replace('\r', ""));

            // Parse SSE events from buffer
            while let Some(event_end) = buffer.find("\n\n") {
                let event_block = buffer[..event_end].to_string();
                buffer = buffer[event_end + 2..].to_string();

                let (event_type, event_data) = parse_sse_event(&event_block);

                if event_type == "endpoint" {
                    // The endpoint might be relative or absolute
                    let endpoint = if event_data.starts_with("http") {
                        event_data.clone()
                    } else {
                        format!("{}{}", base_url, event_data)
                    };
                    post_url = Some(endpoint);
                    break;
                }
            }

            if post_url.is_some() {
                break;
            }
        }

        let post_url = post_url.ok_or_else(|| McpError::Transport("No endpoint event received from SSE".into()))?;

        // Spawn background task to listen for SSE responses
        let pending_clone = pending.clone();
        let listener = tokio::spawn(async move {
            let mut buf = buffer; // carry over remaining buffer
            while let Some(chunk) = bytes_stream.next().await {
                let Ok(chunk) = chunk else { break };
                buf.push_str(&String::from_utf8_lossy(&chunk).replace('\r', ""));

                while let Some(event_end) = buf.find("\n\n") {
                    let event_block = buf[..event_end].to_string();
                    buf = buf[event_end + 2..].to_string();

                    let (event_type, event_data) = parse_sse_event(&event_block);

                    if (event_type == "message" || event_type.is_empty())
                        && let Ok(response) = serde_json::from_str::<JsonRpcResponse>(&event_data)
                        && let Some(id) = response.id
                    {
                        let mut map: tokio::sync::MutexGuard<'_, HashMap<u64, oneshot::Sender<JsonRpcResponse>>> =
                            pending_clone.lock().await;
                        if let Some(sender) = map.remove(&id) {
                            let _ = sender.send(response);
                        }
                    }
                }
            }
        });

        Ok(Self {
            client,
            server: server.to_string(),
            url: url.to_string(),
            post_url,
            headers: header_map,
            header_provider,
            pending,
            next_id: AtomicU64::new(1),
            _listener: listener,
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

    async fn send(&self, body: &str, force_refresh: bool) -> Result<reqwest::Response, McpError> {
        self.client
            .post(&self.post_url)
            .headers(self.request_headers(force_refresh).await)
            .header("Content-Type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .map_err(|e| McpError::Transport(format!("POST request failed: {}", e)))
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
}

#[async_trait]
impl McpTransport for SseTransport {
    async fn request(&self, req: &JsonRpcRequest) -> Result<JsonRpcResponse, McpError> {
        let req_id = req
            .id
            .ok_or_else(|| McpError::Transport("Request must have an id".into()))?;

        // Set up response channel before sending
        let (tx, rx) = oneshot::channel::<JsonRpcResponse>();
        {
            let mut map: tokio::sync::MutexGuard<'_, HashMap<u64, oneshot::Sender<JsonRpcResponse>>> =
                self.pending.lock().await;
            map.insert(req_id, tx);
        }

        // POST the request
        let body =
            serde_json::to_string(req).map_err(|e| McpError::Transport(format!("JSON serialize error: {}", e)))?;

        let response = match self.send_with_auth_retry(&body).await {
            Ok(response) => response,
            Err(err) => {
                self.pending.lock().await.remove(&req_id);
                return Err(err);
            }
        };

        if !response.status().is_success() {
            // Clean up pending
            self.pending.lock().await.remove(&req_id);
            return Err(McpError::Transport(format!(
                "POST returned status: {}",
                response.status()
            )));
        }

        // Wait for response from SSE stream
        let rpc_response = rx
            .await
            .map_err(|_| McpError::Transport("Response channel closed unexpectedly".into()))?;

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
                "Notification POST returned status: {}",
                response.status()
            )));
        }

        Ok(())
    }

    async fn close(&self) -> Result<(), McpError> {
        self._listener.abort();
        Ok(())
    }
}

/// Parse a single SSE event block into (event_type, data)
fn parse_sse_event(block: &str) -> (String, String) {
    let mut event_type = String::new();
    let mut data_lines = Vec::new();

    for line in block.lines() {
        if let Some(value) = line.strip_prefix("event:") {
            event_type = value.trim().to_string();
        } else if let Some(value) = line.strip_prefix("data:") {
            data_lines.push(value.trim().to_string());
        }
    }

    (event_type, data_lines.join("\n"))
}

/// Extract base URL (scheme + host + port) from a full URL
fn extract_base_url(url: &str) -> String {
    // Find the position after "://"
    if let Some(scheme_end) = url.find("://") {
        let rest = &url[scheme_end + 3..];
        if let Some(path_start) = rest.find('/') {
            return url[..scheme_end + 3 + path_start].to_string();
        }
    }
    url.to_string()
}

#[cfg(test)]
#[path = "sse_test.rs"]
mod sse_test;
