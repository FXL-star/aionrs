use std::sync::Mutex as StdMutex;
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::*;

#[derive(Debug)]
struct TokenProvider {
    token: StdMutex<Option<&'static str>>,
    calls: StdMutex<Vec<bool>>,
}

impl TokenProvider {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            token: StdMutex::new(Some("old")),
            calls: StdMutex::new(Vec::new()),
        })
    }
}

#[async_trait]
impl McpHeaderProvider for TokenProvider {
    async fn headers(&self, _: &str, _: &str, force_refresh: bool) -> Option<HashMap<String, String>> {
        self.calls.lock().unwrap().push(force_refresh);
        let mut token = self.token.lock().unwrap();
        if force_refresh {
            *token = Some("fresh");
        }
        token.map(|token| HashMap::from([("Authorization".to_owned(), format!("Bearer {token}"))]))
    }
}

struct TestServer {
    url: String,
    requests: Arc<StdMutex<Vec<(String, Option<String>)>>>,
    handle: JoinHandle<()>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl TestServer {
    async fn start(reject_get: bool, reject_post: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/sse", listener.local_addr().unwrap());
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let recorded = requests.clone();
        let (events, mut receiver) = mpsc::unbounded_channel::<String>();
        let handle = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buffer = Vec::new();
                let mut chunk = [0; 1024];
                let header_end = loop {
                    let n = socket.read(&mut chunk).await.unwrap();
                    assert_ne!(n, 0);
                    buffer.extend_from_slice(&chunk[..n]);
                    if let Some(end) = buffer.windows(4).position(|s| s == b"\r\n\r\n") {
                        break end;
                    }
                };
                let head = String::from_utf8_lossy(&buffer[..header_end]).to_string();
                let header = |name: &str| {
                    head.lines().find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case(name).then(|| value.trim().to_owned())
                    })
                };
                let authorization = header("authorization");
                let length = header("content-length")
                    .and_then(|v| v.parse::<usize>().ok())
                    .unwrap_or(0);
                while buffer.len() < header_end + 4 + length {
                    let n = socket.read(&mut chunk).await.unwrap();
                    assert_ne!(n, 0);
                    buffer.extend_from_slice(&chunk[..n]);
                }
                let is_get = head.starts_with("GET ");
                recorded
                    .lock()
                    .unwrap()
                    .push((if is_get { "GET" } else { "POST" }.to_owned(), authorization.clone()));
                let rejected = if is_get {
                    reject_get && authorization.as_deref() != Some("Bearer fresh")
                } else {
                    reject_post
                };
                if rejected {
                    socket
                        .write_all(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                        .await
                        .unwrap();
                    continue;
                }
                if is_get {
                    // A persistent SSE response: transfer the receiver to its
                    // writer while the accept loop continues handling POSTs.
                    socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\nevent: endpoint\ndata: /messages\n\n").await.unwrap();
                    let (_, replacement) = mpsc::unbounded_channel();
                    let mut stream_events = std::mem::replace(&mut receiver, replacement);
                    tokio::spawn(async move {
                        while let Some(event) = stream_events.recv().await {
                            if socket.write_all(event.as_bytes()).await.is_err() {
                                break;
                            }
                        }
                    });
                } else {
                    let rpc: Value = serde_json::from_slice(&buffer[header_end + 4..header_end + 4 + length]).unwrap();
                    if let Some(id) = rpc.get("id").and_then(Value::as_u64) {
                        events.send(format!("event: message\ndata: {{\"jsonrpc\":\"2.0\",\"id\":{id},\"result\":{{\"ok\":true}}}}\n\n")).unwrap();
                    }
                    socket
                        .write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                        .await
                        .unwrap();
                }
            }
        });
        Self { url, requests, handle }
    }

    async fn connect(
        &self,
        provider: Option<Arc<dyn McpHeaderProvider>>,
        headers: HashMap<String, String>,
    ) -> SseTransport {
        tokio::time::timeout(
            Duration::from_secs(3),
            SseTransport::connect("test", &self.url, &headers, provider),
        )
        .await
        .unwrap()
        .unwrap()
    }
}

#[tokio::test]
async fn rotated_tokens_replace_the_initial_sse_token() {
    let server = TestServer::start(false, false).await;
    let provider = TokenProvider::new();
    let transport = server.connect(Some(provider.clone()), HashMap::new()).await;
    *provider.token.lock().unwrap() = Some("rotated");
    tokio::time::timeout(
        Duration::from_secs(3),
        transport.request(&JsonRpcRequest::new(1, "tools/call", None)),
    )
    .await
    .unwrap()
    .unwrap();
    // Logout must also remove the token rather than reuse the initial header.
    *provider.token.lock().unwrap() = None;
    transport
        .notify(&JsonRpcRequest::notification("notifications/initialized", None))
        .await
        .unwrap();
    assert_eq!(
        *server.requests.lock().unwrap(),
        vec![
            ("GET".to_owned(), Some("Bearer old".to_owned())),
            ("POST".to_owned(), Some("Bearer rotated".to_owned())),
            ("POST".to_owned(), None),
        ]
    );
    transport.close().await.unwrap();
}

#[tokio::test]
async fn initial_sse_401_forces_refresh_once() {
    let server = TestServer::start(true, false).await;
    let provider = TokenProvider::new();
    let transport = server.connect(Some(provider.clone()), HashMap::new()).await;
    assert_eq!(*provider.calls.lock().unwrap(), vec![false, true]);
    assert_eq!(server.requests.lock().unwrap()[1].1.as_deref(), Some("Bearer fresh"));
    transport.close().await.unwrap();
}

#[tokio::test]
async fn rejected_notification_returns_401_after_one_retry() {
    let server = TestServer::start(false, true).await;
    let provider = TokenProvider::new();
    let transport = server.connect(Some(provider.clone()), HashMap::new()).await;
    let error = transport
        .notify(&JsonRpcRequest::notification("notifications/initialized", None))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("401"), "{error}");
    assert_eq!(*provider.calls.lock().unwrap(), vec![false, false, true]);
    assert_eq!(server.requests.lock().unwrap()[2].1.as_deref(), Some("Bearer fresh"));
    transport.close().await.unwrap();
}

#[tokio::test]
async fn explicit_authorization_keeps_priority_over_oauth() {
    let server = TestServer::start(false, false).await;
    let provider = TokenProvider::new();
    let transport = server
        .connect(
            Some(provider.clone()),
            HashMap::from([("aUtHoRiZaTiOn".to_owned(), "Token manual".to_owned())]),
        )
        .await;
    *provider.token.lock().unwrap() = Some("rotated");
    transport
        .notify(&JsonRpcRequest::notification("notifications/initialized", None))
        .await
        .unwrap();
    assert!(
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|(_, auth)| auth.as_deref() == Some("Token manual"))
    );
    transport.close().await.unwrap();
}
