//! Request-time header resolution for MCP HTTP transports.

use std::collections::HashMap;

use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use tracing::warn;

/// Supplies headers for an MCP HTTP request.
///
/// The provider is consulted for every request instead of once at connect
/// time, so short-lived credentials (for example an OAuth access token that is
/// refreshed near expiry) can be re-resolved while a long-running session stays
/// alive.
#[async_trait]
pub trait McpHeaderProvider: Send + Sync + std::fmt::Debug {
    /// Headers to attach to the next request for `server` served at `url`.
    ///
    /// `force_refresh` is `true` when the previous attempt was rejected with
    /// HTTP 401: the implementation should bypass its usual "still valid" check
    /// and obtain a new credential.
    ///
    /// Returned entries never override headers configured statically for the
    /// server; a user-supplied `Authorization` always wins.
    async fn headers(&self, server: &str, url: &str, force_refresh: bool) -> Option<HashMap<String, String>>;
}

/// Merge provider-supplied headers into `headers`, keeping statically
/// configured entries on conflict.
///
/// Invalid names or values are dropped with a warning: they come from runtime
/// credential state, so a malformed token must leave the request
/// unauthenticated rather than abort the whole MCP transport.
pub(crate) fn apply_dynamic_headers(headers: &mut HeaderMap, dynamic: Option<HashMap<String, String>>) {
    let Some(dynamic) = dynamic else {
        return;
    };

    for (name, value) in dynamic {
        let Ok(header_name) = HeaderName::from_bytes(name.as_bytes()) else {
            warn!(header = %name, "mcp header provider returned an invalid header name; ignoring");
            continue;
        };
        if headers.contains_key(&header_name) {
            continue;
        }
        let Ok(header_value) = HeaderValue::from_str(&value) else {
            warn!(header = %name, "mcp header provider returned an invalid header value; ignoring");
            continue;
        };
        headers.insert(header_name, header_value);
    }
}

#[cfg(test)]
#[path = "headers_test.rs"]
mod headers_test;
