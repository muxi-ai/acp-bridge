//! End-to-end test: spawn the built `muxi-acp` binary, speak ACP JSON-RPC
//! over its stdin/stdout, and point it at a fake MUXI formation server that
//! replays recorded-style SSE fixtures.
//!
//! Also enforces byte-level stdout discipline: nothing non-JSON-RPC may
//! appear on stdout.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::{delete, get, post};
use axum::Router;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout};
use tokio::time::timeout;

const STEP_TIMEOUT: Duration = Duration::from_secs(15);

// ---------------------------------------------------------------------------
// Fake MUXI formation server
// ---------------------------------------------------------------------------

#[derive(Default)]
struct ServerState {
    cancelled_request_ids: Mutex<Vec<String>>,
    seen_client_keys: Mutex<Vec<String>>,
    seen_user_ids: Mutex<Vec<String>>,
    chat_payloads: Mutex<Vec<Value>>,
}

fn sse_response(frames: Vec<String>) -> Response {
    sse_response_with_hang(frames, Vec::new())
}

/// Send `frames`, then (if `after_hang` is non-empty) wait 30s and send those.
/// The hang gives a cancellation test time to interrupt the stream.
fn sse_response_with_hang(frames: Vec<String>, after_hang: Vec<String>) -> Response {
    let stream = futures::stream::unfold(
        (VecDeque::from(frames), VecDeque::from(after_hang), false),
        |(mut head, mut tail, mut slept)| async move {
            if let Some(frame) = head.pop_front() {
                return Some((
                    Ok::<_, std::convert::Infallible>(frame),
                    (head, tail, slept),
                ));
            }
            if !tail.is_empty() && !slept {
                tokio::time::sleep(Duration::from_secs(30)).await;
                slept = true;
            }
            tail.pop_front()
                .map(|frame| (Ok(frame), (head, tail, slept)))
        },
    );
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .body(Body::from_stream(stream))
        .unwrap()
}

fn data_frame(value: Value) -> String {
    format!("data: {value}\n\n")
}

async fn chat_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    body: String,
) -> Response {
    if let Some(key) = headers
        .get("x-muxi-client-key")
        .and_then(|value| value.to_str().ok())
    {
        state.seen_client_keys.lock().unwrap().push(key.to_string());
    }
    if let Some(user_id) = headers
        .get("x-muxi-user-id")
        .and_then(|value| value.to_str().ok())
    {
        state
            .seen_user_ids
            .lock()
            .unwrap()
            .push(user_id.to_string());
    }

    let payload: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    state.chat_payloads.lock().unwrap().push(payload.clone());

    let message = payload["message"].as_str().unwrap_or("");
    let session_id = payload["session_id"].as_str().unwrap_or("").to_string();
    let request_id = payload["request_id"].as_str().unwrap_or("").to_string();
    let stamp = |mut value: Value| {
        value["session_id"] = json!(session_id);
        value["request_id"] = json!(request_id);
        value
    };

    match message {
        // content deltas -> thinking (should be gated) -> tool_call pair ->
        // progress (dropped) -> completed -> ui -> done
        "happy" => sse_response(vec![
            ": keepalive\n\n".to_string(),
            data_frame(stamp(json!({"type": "content", "content": "Hello"}))),
            data_frame(stamp(json!({"type": "thinking", "content": "pondering"}))),
            data_frame(stamp(json!({
                "type": "tool_call", "tool_call_id": "tc_1",
                "name": "search", "status": "running"
            }))),
            data_frame(stamp(json!({
                "type": "tool_call", "tool_call_id": "tc_1",
                "status": "completed", "content": "found it"
            }))),
            data_frame(stamp(json!({"type": "progress", "content": "80%"}))),
            data_frame(stamp(json!({"type": "content", "content": " world"}))),
            data_frame(stamp(json!({"type": "completed"}))),
            format!(
                "event: ui\ndata: {}\n\n",
                json!({"ui": [{"type": "options", "id": "w1"}]})
            ),
            format!("event: done\ndata: {}\n\n", json!({"finished": true})),
        ]),
        // formation reports an error mid-turn
        "error" => sse_response(vec![
            data_frame(stamp(json!({"type": "content", "content": "partial"}))),
            data_frame(stamp(json!({"type": "error", "error": "kaboom"}))),
        ]),
        // stream dies mid-turn: no terminal event, connection just ends
        "truncate" => sse_response(vec![data_frame(stamp(
            json!({"type": "content", "content": "partial"}),
        ))]),
        // first delta arrives, then the stream hangs (cancel test window)
        "hang" => sse_response_with_hang(
            vec![data_frame(stamp(
                json!({"type": "content", "content": "started..."}),
            ))],
            vec![format!(
                "event: done\ndata: {}\n\n",
                json!({"finished": true})
            )],
        ),
        // a frame every 100ms, indefinitely-ish: total wall time far exceeds
        // any small turn_timeout while each gap stays under idle_timeout
        "trickle" => {
            let frames: Vec<String> = (0..200)
                .map(|i| {
                    data_frame(stamp(
                        json!({"type": "content", "content": format!("t{i} ")}),
                    ))
                })
                .collect();
            let stream = futures::stream::unfold(VecDeque::from(frames), |mut frames| async move {
                let frame = frames.pop_front()?;
                tokio::time::sleep(Duration::from_millis(100)).await;
                Some((Ok::<_, std::convert::Infallible>(frame), frames))
            });
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/event-stream")
                .body(Body::from_stream(stream))
                .unwrap()
        }
        // ~750 KiB of content frames as fast as the socket allows, then hang:
        // with a tiny max_buffered_bytes and a host that stops reading, the
        // bridge's northbound queue must overflow, not grow without bound
        "firehose" => {
            let blob = "x".repeat(200);
            let frames: Vec<String> = (0..3000)
                .map(|_| data_frame(stamp(json!({"type": "content", "content": blob}))))
                .collect();
            sse_response_with_hang(
                frames,
                vec![format!(
                    "event: done\ndata: {}\n\n",
                    json!({"finished": true})
                )],
            )
        }
        // A Buzz-shaped prompt (identity extraction test): reply and finish.
        buzz if buzz.contains("[Buzz event") => sse_response(vec![
            data_frame(stamp(json!({"type": "content", "content": "ack"}))),
            data_frame(stamp(json!({"type": "completed"}))),
            format!("event: done\ndata: {}\n\n", json!({"finished": true})),
        ]),
        other => panic!("fake server: unknown scenario '{other}'"),
    }
}

async fn cancel_handler(
    State(state): State<Arc<ServerState>>,
    Path(request_id): Path<String>,
) -> Response {
    // Post-#314/#315 runtime behavior: an unknown request id gets a 404.
    // The `doctor` cancellation probe relies on exactly this.
    if request_id.starts_with("doctor-probe") {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                json!({"error": "request not found", "code": "REQUEST_NOT_FOUND"}).to_string(),
            ))
            .unwrap();
    }
    state.cancelled_request_ids.lock().unwrap().push(request_id);
    // Mirror the known runtime defect: cancel returns 400 on success (§6).
    // The bridge must treat this as diagnostic-only.
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({"error": "already finished", "code": "REQUEST_NOT_ACTIVE"}).to_string(),
        ))
        .unwrap()
}

/// GET /v1/sessions: the client-key-authenticated read used by `doctor`'s
/// auth check. Valid key → 200, anything else → 401.
async fn sessions_handler(headers: HeaderMap) -> Response {
    let key = headers
        .get("x-muxi-client-key")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if key == "test-key-123" {
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(json!({"sessions": []}).to_string()))
            .unwrap()
    } else {
        Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                json!({"error": "invalid client key", "code": "UNAUTHORIZED"}).to_string(),
            ))
            .unwrap()
    }
}

async fn start_fake_server() -> (SocketAddr, Arc<ServerState>) {
    let state = Arc::new(ServerState::default());
    let app = Router::new()
        .route("/v1/chat", post(chat_handler))
        .route("/v1/requests/{request_id}", delete(cancel_handler))
        .route("/v1/sessions", get(sessions_handler))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, state)
}

// ---------------------------------------------------------------------------
// ACP client harness over the bridge's stdio
// ---------------------------------------------------------------------------

/// Write a config file pointing at the fake server; `extra_config` is
/// appended inside `[profiles.test]`.
fn write_config(
    addr: SocketAddr,
    config_dir: &std::path::Path,
    extra_config: &str,
) -> std::path::PathBuf {
    let config_path = config_dir.join("config.toml");
    std::fs::write(
        &config_path,
        format!(
            r#"
default_profile = "test"

[profiles.test]
base_url = "http://{addr}/v1"
client_key = "env:MUXI_ACP_TEST_KEY"
# The fake server is plaintext loopback; TLS enforcement requires the opt-in.
allow_insecure_localhost = true
{extra_config}
"#
        ),
    )
    .unwrap();
    config_path
}

struct AcpChild {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: tokio::io::Lines<BufReader<ChildStdout>>,
    /// Every raw stdout line, for byte-level discipline assertions.
    raw_stdout: Vec<String>,
    next_id: i64,
}

impl AcpChild {
    async fn spawn(addr: SocketAddr, config_dir: &std::path::Path) -> Self {
        Self::spawn_with_config(addr, config_dir, "").await
    }

    /// `extra_config` is appended inside `[profiles.test]` (put nested tables
    /// like `[profiles.test.limits]` at the end of the string).
    async fn spawn_with_config(
        addr: SocketAddr,
        config_dir: &std::path::Path,
        extra_config: &str,
    ) -> Self {
        let config_path = write_config(addr, config_dir, extra_config);

        let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_muxi-acp"))
            .arg("--config")
            .arg(&config_path)
            .env("MUXI_ACP_TEST_KEY", "test-key-123")
            .env("RUST_LOG", "info")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn muxi-acp");

        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap()).lines();
        Self {
            child,
            stdin: Some(stdin),
            stdout,
            raw_stdout: Vec::new(),
            next_id: 0,
        }
    }

    async fn send(&mut self, message: Value) {
        let stdin = self.stdin.as_mut().expect("stdin already closed");
        let mut line = message.to_string();
        line.push('\n');
        stdin.write_all(line.as_bytes()).await.unwrap();
        stdin.flush().await.unwrap();
    }

    /// Close the bridge's stdin (host disconnect). Graceful-shutdown trigger.
    fn close_stdin(&mut self) {
        self.stdin.take();
    }

    async fn read_message(&mut self) -> Value {
        let line = timeout(STEP_TIMEOUT, self.stdout.next_line())
            .await
            .expect("timed out waiting for a message from the bridge")
            .expect("stdout read failed")
            .expect("bridge closed stdout unexpectedly");
        self.raw_stdout.push(line.clone());
        serde_json::from_str(&line).unwrap_or_else(|err| {
            panic!("stdout discipline violated: non-JSON line {line:?}: {err}")
        })
    }

    /// Send a request; collect `session/update` notifications until the
    /// response for this request id arrives. Returns (notifications, response).
    async fn request(&mut self, method: &str, params: Value) -> (Vec<Value>, Value) {
        self.next_id += 1;
        let id = self.next_id;
        self.send(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
            .await;
        self.read_until_response(id).await
    }

    async fn read_until_response(&mut self, id: i64) -> (Vec<Value>, Value) {
        let mut notifications = Vec::new();
        loop {
            let message = self.read_message().await;
            if message.get("id") == Some(&json!(id)) {
                return (notifications, message);
            }
            assert_eq!(
                message["method"], "session/update",
                "unexpected interleaved message: {message}"
            );
            notifications.push(message);
        }
    }

    async fn notify(&mut self, method: &str, params: Value) {
        self.send(json!({"jsonrpc": "2.0", "method": method, "params": params}))
            .await;
    }
}

/// initialize + session/new, returning the minted session id.
async fn establish_session(acp: &mut AcpChild) -> String {
    let (_, response) = acp
        .request(
            "initialize",
            json!({"protocolVersion": 1, "clientInfo": {"name": "e2e", "version": "0"}}),
        )
        .await;
    assert!(response["result"].is_object(), "{response}");
    let (_, response) = acp
        .request("session/new", json!({"cwd": "/tmp", "mcpServers": []}))
        .await;
    response["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string()
}

/// Wait until the fake server has seen at least one DELETE /v1/requests/{id}.
async fn expect_upstream_cancel(server: &Arc<ServerState>) -> Vec<String> {
    let deadline = tokio::time::Instant::now() + STEP_TIMEOUT;
    loop {
        let cancelled = server.cancelled_request_ids.lock().unwrap().clone();
        if !cancelled.is_empty() {
            assert!(cancelled[0].starts_with("req_"), "{cancelled:?}");
            return cancelled;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "bridge never called cancel_request upstream"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn update_kind(notification: &Value) -> &str {
    notification["params"]["update"]["sessionUpdate"]
        .as_str()
        .unwrap_or("?")
}

fn chunk_text(notification: &Value) -> &str {
    notification["params"]["update"]["content"]["text"]
        .as_str()
        .unwrap_or("?")
}

// ---------------------------------------------------------------------------
// The test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bridge_end_to_end() {
    let (addr, server) = start_fake_server().await;
    let config_dir = tempfile::tempdir().unwrap();
    let mut acp = AcpChild::spawn(addr, config_dir.path()).await;

    // -- initialize: honest capability set (§4) --------------------------------
    let (_, response) = acp
        .request(
            "initialize",
            json!({"protocolVersion": 1, "clientInfo": {"name": "e2e", "version": "0"}}),
        )
        .await;
    let caps = &response["result"]["agentCapabilities"];
    assert_eq!(caps["loadSession"], json!(false));
    assert_eq!(caps["promptCapabilities"]["image"], json!(false));
    assert_eq!(caps["promptCapabilities"]["audio"], json!(false));
    assert_eq!(caps["promptCapabilities"]["embeddedContext"], json!(false));
    for capability in ["resume", "list", "close", "delete"] {
        assert!(
            caps["sessionCapabilities"][capability].is_object(),
            "sessionCapabilities.{capability} should be advertised: {caps}"
        );
    }
    assert_eq!(response["result"]["authMethods"], json!([]));

    // -- session/new: locally minted id ---------------------------------------
    let (_, response) = acp
        .request("session/new", json!({"cwd": "/tmp", "mcpServers": []}))
        .await;
    let session_id = response["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(session_id.starts_with("sess_"), "{session_id}");

    // -- happy path: deltas stream, thinking gated, tools mapped, EndTurn -----
    let (updates, response) = acp
        .request(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": "happy"}]
            }),
        )
        .await;
    assert_eq!(response["result"]["stopReason"], "end_turn");
    let kinds: Vec<&str> = updates.iter().map(update_kind).collect();
    assert_eq!(
        kinds,
        vec![
            "agent_message_chunk",
            "tool_call",
            "tool_call_update",
            "agent_message_chunk",
        ],
        "unexpected update sequence: {updates:#?}"
    );
    assert_eq!(chunk_text(&updates[0]), "Hello");
    assert_eq!(chunk_text(&updates[3]), " world");
    assert_eq!(updates[1]["params"]["update"]["toolCallId"], "tc_1");
    assert_eq!(updates[1]["params"]["update"]["status"], "in_progress");
    assert_eq!(updates[2]["params"]["update"]["status"], "completed");
    for update in &updates {
        assert_eq!(
            update["params"]["sessionId"].as_str().unwrap(),
            session_id,
            "update under wrong session id"
        );
    }

    // -- upstream error event: JSON-RPC error, no stop reason ------------------
    let (updates, response) = acp
        .request(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": "error"}]
            }),
        )
        .await;
    assert_eq!(chunk_text(&updates[0]), "partial");
    let error = &response["error"];
    assert!(error.is_object(), "expected error response: {response}");
    assert_eq!(error["message"], "kaboom");
    assert_eq!(error["data"]["code"], "BRIDGE_UPSTREAM_ERROR");

    // -- stream dies mid-turn: UnexpectedFailure-equivalent error --------------
    let (_, response) = acp
        .request(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": "truncate"}]
            }),
        )
        .await;
    assert_eq!(
        response["error"]["data"]["code"], "BRIDGE_STREAM_TRUNCATED",
        "response: {response}"
    );

    // -- non-text content is rejected (text-only v1) ---------------------------
    let (_, response) = acp
        .request(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [
                    {"type": "text", "text": "hi"},
                    {"type": "image", "data": "aGk=", "mimeType": "image/png"}
                ]
            }),
        )
        .await;
    assert_eq!(
        response["error"]["data"]["code"],
        "BRIDGE_UNSUPPORTED_CONTENT"
    );

    // -- unknown session is rejected -------------------------------------------
    let (_, response) = acp
        .request(
            "session/prompt",
            json!({
                "sessionId": "sess_doesnotexist",
                "prompt": [{"type": "text", "text": "happy"}]
            }),
        )
        .await;
    assert_eq!(response["error"]["data"]["code"], "BRIDGE_UNKNOWN_SESSION");

    // -- cancellation: fire cancel_request, drop stream, resolve Cancelled -----
    acp.next_id += 1;
    let prompt_id = acp.next_id;
    acp.send(json!({
        "jsonrpc": "2.0", "id": prompt_id, "method": "session/prompt",
        "params": {
            "sessionId": session_id,
            "prompt": [{"type": "text", "text": "hang"}]
        }
    }))
    .await;
    // Wait for the first delta so the turn is provably in flight.
    let first = acp.read_message().await;
    assert_eq!(update_kind(&first), "agent_message_chunk");
    assert_eq!(chunk_text(&first), "started...");

    acp.notify("session/cancel", json!({"sessionId": session_id}))
        .await;
    let (_, response) = acp.read_until_response(prompt_id).await;
    assert_eq!(
        response["result"]["stopReason"], "cancelled",
        "response: {response}"
    );

    // The bridge must have fired DELETE /v1/requests/{request_id} upstream.
    expect_upstream_cancel(&server).await;

    // -- session surface: list / resume / close --------------------------------
    let (_, response) = acp.request("session/list", json!({})).await;
    let sessions = response["result"]["sessions"].as_array().unwrap();
    assert!(sessions
        .iter()
        .any(|s| s["sessionId"].as_str() == Some(session_id.as_str())));

    let (_, response) = acp
        .request(
            "session/resume",
            json!({"sessionId": "sess_fromapastlife", "cwd": "/tmp", "mcpServers": []}),
        )
        .await;
    assert!(response["result"].is_object(), "{response}");

    let (_, response) = acp
        .request("session/close", json!({"sessionId": session_id}))
        .await;
    assert!(response["result"].is_object(), "{response}");
    let (_, response) = acp.request("session/list", json!({})).await;
    let sessions = response["result"]["sessions"].as_array().unwrap();
    assert!(!sessions
        .iter()
        .any(|s| s["sessionId"].as_str() == Some(session_id.as_str())));

    // -- southbound assertions --------------------------------------------------
    {
        let keys = server.seen_client_keys.lock().unwrap();
        assert!(!keys.is_empty());
        assert!(keys.iter().all(|key| key == "test-key-123"));

        let payloads = server.chat_payloads.lock().unwrap();
        for payload in payloads.iter() {
            assert_eq!(payload["stream"], json!(true));
            assert!(payload["request_id"].as_str().unwrap().starts_with("req_"));
            assert!(payload["session_id"].as_str().unwrap().starts_with("sess_"));
        }
    }

    // -- byte-level stdout discipline -------------------------------------------
    for line in &acp.raw_stdout {
        let value: Value = serde_json::from_str(line).expect("every stdout line must be JSON-RPC");
        assert_eq!(
            value["jsonrpc"], "2.0",
            "non-JSON-RPC frame on stdout: {line}"
        );
    }
}

// ---------------------------------------------------------------------------
// Reliability posture: timeouts, backpressure, graceful shutdown
// ---------------------------------------------------------------------------

/// A stream that goes silent mid-turn must fail with BRIDGE_IDLE_TIMEOUT and
/// cancel the MUXI request — not sit on the connection forever.
#[tokio::test]
async fn idle_timeout_fires_and_cancels_upstream() {
    let (addr, server) = start_fake_server().await;
    let config_dir = tempfile::tempdir().unwrap();
    let mut acp = AcpChild::spawn_with_config(
        addr,
        config_dir.path(),
        r#"
idle_timeout = "300ms"
turn_timeout = "30s"
"#,
    )
    .await;
    let session_id = establish_session(&mut acp).await;

    // "hang": one delta, then 30s of silence — far beyond idle_timeout.
    let (updates, response) = acp
        .request(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": "hang"}]
            }),
        )
        .await;
    assert_eq!(chunk_text(&updates[0]), "started...");
    assert_eq!(
        response["error"]["data"]["code"], "BRIDGE_IDLE_TIMEOUT",
        "response: {response}"
    );

    expect_upstream_cancel(&server).await;

    // The turn slot is free again: a new prompt is accepted (and times out
    // the same way, proving the state machine reset rather than wedged).
    let (_, response) = acp
        .request(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": "hang"}]
            }),
        )
        .await;
    assert_eq!(response["error"]["data"]["code"], "BRIDGE_IDLE_TIMEOUT");
}

/// A stream that keeps trickling frames (never idle) must still be bounded by
/// the overall turn_timeout.
#[tokio::test]
async fn turn_timeout_fires_and_cancels_upstream() {
    let (addr, server) = start_fake_server().await;
    let config_dir = tempfile::tempdir().unwrap();
    let mut acp = AcpChild::spawn_with_config(
        addr,
        config_dir.path(),
        r#"
turn_timeout = "500ms"
idle_timeout = "30s"
"#,
    )
    .await;
    let session_id = establish_session(&mut acp).await;

    // "trickle": a frame every 100ms for 20s — each gap is well under
    // idle_timeout, so only the turn deadline can end this.
    let (updates, response) = acp
        .request(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": "trickle"}]
            }),
        )
        .await;
    assert!(
        !updates.is_empty(),
        "expected some deltas before the deadline"
    );
    assert_eq!(
        response["error"]["data"]["code"], "BRIDGE_TURN_TIMEOUT",
        "response: {response}"
    );

    expect_upstream_cancel(&server).await;
}

/// A host that stops draining stdout must not make the bridge buffer without
/// bound: past max_buffered_bytes the turn fails with BRIDGE_BUFFER_OVERFLOW
/// and the MUXI request is cancelled. No update is silently dropped from a
/// turn that reports success (PRD §15.3).
#[tokio::test]
async fn buffer_overflow_fails_the_turn_and_cancels_upstream() {
    let (addr, server) = start_fake_server().await;
    let config_dir = tempfile::tempdir().unwrap();
    let mut acp = AcpChild::spawn_with_config(
        addr,
        config_dir.path(),
        r#"
[profiles.test.limits]
max_buffered_bytes = 4096
"#,
    )
    .await;
    let session_id = establish_session(&mut acp).await;

    // Fire the prompt but do NOT read stdout: the OS pipe fills, the writer
    // blocks, and the bridge's northbound queue starts growing.
    acp.next_id += 1;
    let prompt_id = acp.next_id;
    acp.send(json!({
        "jsonrpc": "2.0", "id": prompt_id, "method": "session/prompt",
        "params": {
            "sessionId": session_id,
            "prompt": [{"type": "text", "text": "firehose"}]
        }
    }))
    .await;

    // The overflow must be detected while the host is not reading.
    expect_upstream_cancel(&server).await;

    // Now drain: whatever was queued within the cap arrives, then the error.
    let (updates, response) = acp.read_until_response(prompt_id).await;
    assert!(
        !updates.is_empty(),
        "expected buffered deltas before the failure"
    );
    assert_eq!(
        response["error"]["data"]["code"], "BRIDGE_BUFFER_OVERFLOW",
        "response: {response}"
    );
}

/// Buzz identity extraction (spec §5): with `identity.host = "buzz"` in
/// channel mode, a Buzz-shaped prompt must reach MUXI with
/// `X-Muxi-User-ID: buzz:channel:<uuid>` — and a non-Buzz prompt on the same
/// profile must fall through to the per-session synthetic id, never a
/// fabricated one.
#[tokio::test]
async fn buzz_channel_identity_reaches_upstream() {
    let (addr, server) = start_fake_server().await;
    let config_dir = tempfile::tempdir().unwrap();
    let mut acp = AcpChild::spawn_with_config(
        addr,
        config_dir.path(),
        r#"
[profiles.test.identity]
host = "buzz"
host_unit = "channel"
"#,
    )
    .await;
    let session_id = establish_session(&mut acp).await;

    let uuid = "0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9";
    let alice = "a".repeat(64);
    let bob = "b".repeat(64);
    let prompt = format!(
        "[Buzz events — 2 events]\n\n\
         --- Event 1 (mention) ---\n\
         Event ID: deadbeef\n\
         Channel: eng-oncall (#{uuid})\n\
         Kind: 9\n\
         From: alice (npub: npub1alice, hex: {alice})\n\
         Content: the deploy is stuck\n\n\
         --- Event 2 (message) ---\n\
         Event ID: cafebabe\n\
         Channel: eng-oncall (#{uuid})\n\
         Kind: 9\n\
         From: bob (npub: npub1bob, hex: {bob})\n\
         Content: same here\n"
    );
    let (_, response) = acp
        .request(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": prompt}]
            }),
        )
        .await;
    assert_eq!(response["result"]["stopReason"], "end_turn", "{response}");

    {
        let user_ids = server.seen_user_ids.lock().unwrap();
        assert_eq!(
            user_ids.as_slice(),
            [format!("buzz:channel:{uuid}")],
            "extracted identity did not reach the fake server"
        );
    }

    // Parse-miss fallback: a prompt without a Buzz block falls through to the
    // per-session synthetic id (no default_user_id configured).
    let (_, response) = acp
        .request(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": "happy"}]
            }),
        )
        .await;
    assert_eq!(response["result"]["stopReason"], "end_turn", "{response}");
    {
        let user_ids = server.seen_user_ids.lock().unwrap();
        assert_eq!(user_ids.len(), 2);
        assert_eq!(user_ids[1], format!("acp:{session_id}"));
    }
}

/// Closing the bridge's stdin mid-turn (host went away) must cancel the MUXI
/// request upstream, flush, and exit 0 within the bounded shutdown window —
/// never leave a formation running a turn for a dead host (PRD §21).
#[tokio::test]
async fn stdin_eof_cancels_active_turn_and_exits_cleanly() {
    let (addr, server) = start_fake_server().await;
    let config_dir = tempfile::tempdir().unwrap();
    let mut acp = AcpChild::spawn(addr, config_dir.path()).await;
    let session_id = establish_session(&mut acp).await;

    // Start a turn against the hanging fixture and wait until it is provably
    // in flight (first delta observed).
    acp.next_id += 1;
    let prompt_id = acp.next_id;
    acp.send(json!({
        "jsonrpc": "2.0", "id": prompt_id, "method": "session/prompt",
        "params": {
            "sessionId": session_id,
            "prompt": [{"type": "text", "text": "hang"}]
        }
    }))
    .await;
    let first = acp.read_message().await;
    assert_eq!(update_kind(&first), "agent_message_chunk");
    assert_eq!(chunk_text(&first), "started...");

    // Host disconnect.
    acp.close_stdin();

    // The bridge must fire DELETE /v1/requests/{id} upstream...
    expect_upstream_cancel(&server).await;

    // ...and exit 0 within the 5s cancel window (plus slack).
    let status = timeout(Duration::from_secs(10), acp.child.wait())
        .await
        .expect("bridge did not exit after stdin EOF")
        .expect("wait failed");
    assert!(status.success(), "expected exit 0, got {status:?}");
}

// ---------------------------------------------------------------------------
// doctor: production dependency probe (no billable turn)
// ---------------------------------------------------------------------------

/// Run `muxi-acp doctor` against the fake server and return (exit ok, stdout).
async fn run_doctor(addr: SocketAddr, key: &str, json: bool) -> (bool, String) {
    let config_dir = tempfile::tempdir().unwrap();
    let config_path = write_config(addr, config_dir.path(), "");
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_muxi-acp"));
    command
        .arg("--config")
        .arg(&config_path)
        .arg("doctor")
        .env("MUXI_ACP_TEST_KEY", key)
        .env("RUST_LOG", "info");
    if json {
        command.arg("--json");
    }
    let output = timeout(STEP_TIMEOUT, command.output())
        .await
        .expect("doctor timed out")
        .expect("spawn muxi-acp doctor");
    (
        output.status.success(),
        String::from_utf8(output.stdout).expect("doctor stdout must be UTF-8"),
    )
}

/// Every check passes against the fake server (which answers GET /v1/sessions
/// with 200 for the right key and 404 for the doctor's cancel probe) and the
/// process exits 0. `--json` yields a machine-readable array; doctor must not
/// have started a chat turn (no billable model turn).
#[tokio::test]
async fn doctor_all_checks_pass_and_exit_zero() {
    let (addr, server) = start_fake_server().await;

    let (ok, stdout) = run_doctor(addr, "test-key-123", true).await;
    assert!(ok, "doctor should exit 0; stdout:\n{stdout}");
    let checks: Value = serde_json::from_str(&stdout).expect("doctor --json must emit JSON");
    let checks = checks.as_array().expect("doctor --json must emit an array");
    let expected = [
        "config",
        "tls-policy",
        "dns",
        "tcp+tls",
        "auth",
        "streaming",
        "cancellation",
        "identity",
    ];
    assert_eq!(checks.len(), expected.len(), "{checks:#?}");
    for name in expected {
        let check = checks
            .iter()
            .find(|c| c["check"] == name)
            .unwrap_or_else(|| panic!("missing check '{name}': {checks:#?}"));
        assert_eq!(check["status"], "pass", "check '{name}': {check:#?}");
        assert!(check["detail"].is_string(), "{check:#?}");
    }
    // The cancellation probe is honest about what 404 proves.
    let cancel = checks
        .iter()
        .find(|c| c["check"] == "cancellation")
        .unwrap();
    assert!(
        cancel["detail"].as_str().unwrap().contains("404"),
        "{cancel:#?}"
    );
    // Secrets are never echoed.
    assert!(!stdout.contains("test-key-123"), "key leaked:\n{stdout}");

    // No billable turn: the fake server saw no POST /v1/chat.
    assert!(
        server.chat_payloads.lock().unwrap().is_empty(),
        "doctor must never start a chat turn"
    );

    // Human report also passes and carries the summary line.
    let (ok, stdout) = run_doctor(addr, "test-key-123", false).await;
    assert!(ok, "{stdout}");
    assert!(stdout.contains("PASS"), "{stdout}");
    assert!(stdout.contains("doctor: ok (8 pass)"), "{stdout}");
}

/// A wrong client key must fail the auth check (401 → bad credentials) and
/// exit 1, while unrelated checks still report independently.
#[tokio::test]
async fn doctor_bad_key_fails_auth_and_exits_one() {
    let (addr, _server) = start_fake_server().await;

    let (ok, stdout) = run_doctor(addr, "wrong-key", true).await;
    assert!(
        !ok,
        "doctor should exit 1 on auth failure; stdout:\n{stdout}"
    );
    let checks: Value = serde_json::from_str(&stdout).unwrap();
    let checks = checks.as_array().unwrap();
    let status_of = |name: &str| {
        checks
            .iter()
            .find(|c| c["check"] == name)
            .unwrap_or_else(|| panic!("missing check '{name}'"))
            .clone()
    };
    let auth = status_of("auth");
    assert_eq!(auth["status"], "fail", "{auth:#?}");
    assert!(
        auth["detail"].as_str().unwrap().contains("401"),
        "{auth:#?}"
    );
    // The run continued past the failure: independent checks still pass.
    for name in ["config", "tls-policy", "dns", "tcp+tls", "identity"] {
        assert_eq!(status_of(name)["status"], "pass", "check '{name}'");
    }
    assert!(!stdout.contains("wrong-key"), "key leaked:\n{stdout}");
}
