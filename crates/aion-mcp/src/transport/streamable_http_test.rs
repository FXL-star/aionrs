use super::*;

#[cfg(test)]
mod tests {
    use super::*;

    /// Servers built on the MCP Python SDK / `fastmcp` use `sse-starlette`,
    /// whose default SSE line separator is CRLF (`\r\n`). The event terminator
    /// is therefore `\r\n\r\n`, which does not contain `\n\n`. The parser must
    /// still recover the JSON-RPC response, otherwise such servers connect but
    /// expose no tools to the model.
    #[test]
    fn extracts_jsonrpc_from_crlf_delimited_sse() {
        let body = "event: message\r\ndata: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"tools\":[]}}\r\n\r\n";
        let resp = extract_jsonrpc_from_sse_buffer(body).expect("should parse CRLF SSE");
        assert_eq!(resp.id, Some(2));
        assert!(resp.result.is_some());
    }

    /// Node `@modelcontextprotocol/sdk` servers emit LF-delimited SSE.
    #[test]
    fn extracts_jsonrpc_from_lf_delimited_sse() {
        let body = "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}\n\n";
        let resp = extract_jsonrpc_from_sse_buffer(body).expect("should parse LF SSE");
        assert_eq!(resp.id, Some(1));
    }

    /// Data split across multiple `data:` lines must be reassembled.
    #[test]
    fn reassembles_multiline_data() {
        let body = "data: {\"jsonrpc\":\"2.0\",\r\ndata: \"id\":7,\"result\":{}}\r\n\r\n";
        let resp = extract_jsonrpc_from_sse_buffer(body).expect("should join data lines");
        assert_eq!(resp.id, Some(7));
    }

    /// Notifications (no `id`) and comment/ping lines must be skipped.
    #[test]
    fn returns_none_without_complete_response() {
        let body = ": keep-alive\r\n\r\nevent: message\r\ndata: not-json\r\n\r\n";
        assert!(extract_jsonrpc_from_sse_buffer(body).is_none());
    }

    // -----------------------------------------------------------------------
    // Request-time header resolution
    // -----------------------------------------------------------------------

    /// Records what the transport asked for and what the server received.
    #[derive(Debug)]
    struct TestProvider {
        /// `force_refresh` flag of every invocation, in order.
        calls: std::sync::Mutex<Vec<bool>>,
        /// Token handed out for each invocation, advancing per call.
        counter: std::sync::atomic::AtomicU64,
        /// Token returned when `force_refresh` is set, if any.
        refreshed: Option<&'static str>,
    }

    impl TestProvider {
        fn new(refreshed: Option<&'static str>) -> Self {
            Self {
                calls: std::sync::Mutex::new(Vec::new()),
                counter: std::sync::atomic::AtomicU64::new(1),
                refreshed,
            }
        }

        fn calls(&self) -> Vec<bool> {
            self.calls.lock().expect("calls lock").clone()
        }
    }

    #[async_trait]
    impl McpHeaderProvider for TestProvider {
        async fn headers(&self, _server: &str, _url: &str, force_refresh: bool) -> Option<HashMap<String, String>> {
            self.calls.lock().expect("calls lock").push(force_refresh);

            let token = match (force_refresh, self.refreshed) {
                (true, Some(token)) => token.to_string(),
                _ => {
                    let n = self.counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    format!("token-{n}")
                }
            };

            Some(HashMap::from([(
                "Authorization".to_string(),
                format!("Bearer {token}"),
            )]))
        }
    }

    /// One request as seen by the test server.
    struct RecordedRequest {
        authorization: Option<String>,
    }

    /// Minimal HTTP/1.1 server answering every request with `respond`.
    ///
    /// Returns the base URL plus the recorded requests. Each connection is
    /// closed after its response so the reader never has to handle keep-alive.
    async fn spawn_http_server<F>(respond: F) -> (String, Arc<std::sync::Mutex<Vec<RecordedRequest>>>)
    where
        F: Fn(&RecordedRequest) -> (u16, String) + Send + Sync + 'static,
    {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let addr = listener.local_addr().expect("local addr");
        let recorded = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded_writer = Arc::clone(&recorded);
        let respond = Arc::new(respond);

        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let recorded = Arc::clone(&recorded_writer);
                let respond = Arc::clone(&respond);

                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 1024];

                    // Read until the header block is complete.
                    let header_end = loop {
                        let Ok(n) = socket.read(&mut chunk).await else {
                            return;
                        };
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                        if let Some(pos) = find_header_end(&buf) {
                            break pos;
                        }
                    };

                    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
                    let content_length = head
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())?
                        })
                        .unwrap_or(0);

                    while buf.len() < header_end + 4 + content_length {
                        let Ok(n) = socket.read(&mut chunk).await else {
                            return;
                        };
                        if n == 0 {
                            break;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                    }

                    let authorization = head.lines().find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("authorization")
                            .then(|| value.trim().to_string())
                    });

                    let request = RecordedRequest { authorization };
                    let (status, body) = respond(&request);
                    recorded.lock().expect("recorded lock").push(request);

                    let response = format!(
                        "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.shutdown().await;
                });
            }
        });

        (format!("http://{addr}/mcp"), recorded)
    }

    fn find_header_end(buf: &[u8]) -> Option<usize> {
        buf.windows(4).position(|window| window == b"\r\n\r\n")
    }

    fn ok_response() -> (u16, String) {
        (
            200,
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"ok\":true}}".to_string(),
        )
    }

    /// A token resolved at connect time would go stale; every request must ask
    /// the provider again so a refreshed credential is picked up.
    #[tokio::test]
    async fn resolves_headers_for_every_request() {
        let (url, recorded) = spawn_http_server(|_| ok_response()).await;
        let provider = Arc::new(TestProvider::new(None));

        let transport = StreamableHttpTransport::connect(
            "test",
            &url,
            &HashMap::new(),
            Some(Arc::clone(&provider) as Arc<dyn McpHeaderProvider>),
        )
        .await
        .expect("connect");

        for id in [1, 2] {
            transport
                .request(&JsonRpcRequest::new(id, "ping", None))
                .await
                .expect("request succeeds");
        }

        let seen: Vec<_> = recorded
            .lock()
            .expect("recorded lock")
            .iter()
            .map(|request| request.authorization.clone())
            .collect();
        assert_eq!(
            seen,
            vec![Some("Bearer token-1".to_string()), Some("Bearer token-2".to_string())]
        );
        assert_eq!(provider.calls(), vec![false, false]);
    }

    /// A credential that expires mid-session surfaces as HTTP 401; the
    /// transport must ask for a forced refresh and replay the request once.
    #[tokio::test]
    async fn retries_once_with_forced_refresh_on_unauthorized() {
        let (url, recorded) = spawn_http_server(|request| {
            if request.authorization.as_deref() == Some("Bearer fresh") {
                ok_response()
            } else {
                (401, String::new())
            }
        })
        .await;
        let provider = Arc::new(TestProvider::new(Some("fresh")));

        let transport = StreamableHttpTransport::connect(
            "test",
            &url,
            &HashMap::new(),
            Some(Arc::clone(&provider) as Arc<dyn McpHeaderProvider>),
        )
        .await
        .expect("connect");
        // Connect itself does not resolve headers for this transport, but it
        // also must not send anything; the first request below is the retry.
        let response = transport
            .request(&JsonRpcRequest::new(1, "ping", None))
            .await
            .expect("retry should succeed");

        assert_eq!(response.id, Some(1));
        assert_eq!(provider.calls(), vec![false, true]);
        assert_eq!(recorded.lock().expect("recorded lock").len(), 2);
    }

    /// Without a provider there is nothing to refresh, so a 401 must not be
    /// sent twice.
    #[tokio::test]
    async fn does_not_retry_unauthorized_without_provider() {
        let (url, recorded) = spawn_http_server(|_| (401, String::new())).await;

        let transport = StreamableHttpTransport::connect("test", &url, &HashMap::new(), None)
            .await
            .expect("connect");

        let err = transport
            .request(&JsonRpcRequest::new(1, "ping", None))
            .await
            .expect_err("401 surfaces as an error");

        assert!(err.to_string().contains("401"), "unexpected error: {err}");
        assert_eq!(recorded.lock().expect("recorded lock").len(), 1);
    }
}
