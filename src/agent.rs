//! ACP agent surface (northbound): builds the connection, registers handlers
//! per binding spec §3/§4, and runs prompt turns against MUXI.

use std::sync::Arc;

use agent_client_protocol::schema::v1::{
    AgentCapabilities, CancelNotification, CloseSessionRequest, CloseSessionResponse, ContentBlock,
    DeleteSessionRequest, DeleteSessionResponse, Error, Implementation, InitializeRequest,
    InitializeResponse, ListSessionsRequest, ListSessionsResponse, NewSessionRequest,
    NewSessionResponse, PromptCapabilities, PromptRequest, PromptResponse, ResumeSessionRequest,
    ResumeSessionResponse, SessionCapabilities, SessionCloseCapabilities,
    SessionDeleteCapabilities, SessionInfo, SessionListCapabilities, SessionNotification,
    SessionResumeCapabilities, StopReason,
};
use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::{
    on_receive_notification, on_receive_request, Agent, Client, ConnectionTo, LineDirection,
    Responder, Stdio,
};
use futures::StreamExt;
use muxi_rust::FormationClient;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use crate::buzz::HostIdentity;
use crate::config::resolve_user_id;
use crate::mux::chat_payload;
use crate::session::{new_id, SessionRegistry, TurnError};
use crate::translate::{
    stream_end_outcome, Translator, TurnEvent, CODE_BUFFER_OVERFLOW, CODE_IDLE_TIMEOUT,
    CODE_SESSION_LIMIT, CODE_TRANSPORT_ERROR, CODE_TURN_LIMIT, CODE_TURN_TIMEOUT,
};

/// Shared bridge state, cloned into every handler.
pub struct BridgeState {
    pub sessions: SessionRegistry,
    pub mux: FormationClient,
    pub agent_id: Option<String>,
    pub cli_user_id: Option<String>,
    pub default_user_id: Option<String>,
    /// Tier-2 host identity extraction (spec §5.2), e.g. Buzz prompt parsing.
    /// `None` when `identity.host = "none"`.
    pub host_extractor: Option<Box<dyn HostIdentity + Send + Sync>>,
    pub forward_thoughts: bool,
    /// Wall-clock cap for a whole prompt turn.
    pub turn_timeout: std::time::Duration,
    /// Cap on time between SSE frames (keepalive comments never surface from
    /// the SDK parser, so this is simply time since the last event).
    pub idle_timeout: std::time::Duration,
}

impl BridgeState {
    /// Identity without prompt text (tiers 1/3/4 only): used by cancel and
    /// shutdown paths, where no prompt is at hand. That is fine — the MUXI
    /// cancel endpoint is keyed by `request_id`; the user id header on a
    /// cancel is best-effort context, not a lookup key.
    pub(crate) fn user_id_for(&self, acp_session_id: &str) -> String {
        resolve_user_id(
            self.cli_user_id.as_deref(),
            None,
            self.default_user_id.as_deref(),
            acp_session_id,
        )
    }

    /// Full identity resolution for one prompt turn (spec §5.2): `--user-id`
    /// flag > host extraction from *this turn's* prompt text >
    /// `default_user_id` > per-session synthetic.
    ///
    /// Resolved per-turn, not per-session, because the identity signal lives
    /// in each turn's prompt: in `sender` mode a Buzz channel session carries
    /// turns from different people, so the same session's `user_id` may
    /// legitimately vary between turns — attribution follows whoever sent the
    /// message being answered, not whoever happened to open the session. In
    /// `channel` mode the extracted id is stable for the session's lifetime.
    pub(crate) fn user_id_for_turn(&self, acp_session_id: &str, prompt_text: &str) -> String {
        // Tier 1 short-circuits extraction entirely: no point parsing (or
        // warning about) prompt text the flag will override anyway.
        let flag_set = self.cli_user_id.as_deref().is_some_and(|id| !id.is_empty());
        let extracted = if flag_set {
            None
        } else {
            self.host_extractor.as_ref().and_then(|extractor| {
                extractor.extract(prompt_text).map(|identity| {
                    for diagnostic in &identity.diagnostics {
                        tracing::warn!(session_id = acp_session_id, "{diagnostic}");
                    }
                    identity.user_id
                })
            })
        };
        resolve_user_id(
            self.cli_user_id.as_deref(),
            extracted.as_deref(),
            self.default_user_id.as_deref(),
            acp_session_id,
        )
    }
}

/// Capability set per binding spec §4: text-only prompts, no session/load,
/// no MCP, no auth methods; resume/list/close/delete served locally.
fn capabilities() -> AgentCapabilities {
    AgentCapabilities::new()
        .load_session(false)
        .prompt_capabilities(PromptCapabilities::new())
        .session_capabilities(
            SessionCapabilities::new()
                .list(SessionListCapabilities::new())
                .resume(SessionResumeCapabilities::new())
                .close(SessionCloseCapabilities::new())
                .delete(SessionDeleteCapabilities::new()),
        )
}

fn bridge_error(code: &str, message: &str) -> Error {
    Error::new(-32603, message).data(json!({ "code": code }))
}

/// Fire the MUXI-side cancel for a turn without blocking the event loop.
/// Known runtime defect: the cancel endpoint returns 400 on a cancellation
/// that succeeded (spec §6) — errors are logged, never surfaced.
fn spawn_mux_cancel(
    cx: &ConnectionTo<Client>,
    state: &Arc<BridgeState>,
    session_id: &str,
    request_id: &str,
) {
    let mux = state.mux.clone();
    let user_id = state.user_id_for(session_id);
    let session_id = session_id.to_string();
    let request_id = request_id.to_string();
    let result = cx.spawn(async move {
        if let Err(err) = mux.cancel_request(&request_id, &user_id).await {
            tracing::debug!(
                session_id,
                request_id,
                error = %err,
                "MUXI cancel_request reported an error (expected: cancel returns 400 on success)"
            );
        }
        Ok(())
    });
    if let Err(err) = result {
        tracing::warn!(error = ?err, "failed to spawn MUXI cancel task");
    }
}

/// stdout-writer hook (`Stdio::with_debug`): invoked for each line as the
/// writer dequeues it. `session/update` lines complete one buffer reservation
/// for their session — the send path reserved the bytes, so queued-but-unwritten
/// is exactly (reserved - written). See `SessionRegistry::buffer_reserve`.
fn note_line_written(sessions: &SessionRegistry, line: &str) {
    // Cheap pre-filter: skip request/response frames without parsing.
    if !line.contains("\"session/update\"") {
        return;
    }
    let Ok(value) = serde_json::from_str::<Value>(line) else {
        return;
    };
    if value.get("method").and_then(Value::as_str) != Some("session/update") {
        return;
    }
    if let Some(session_id) = value.pointer("/params/sessionId").and_then(Value::as_str) {
        sessions.buffer_written(session_id);
    }
}

/// Run the ACP agent over stdio until the client disconnects.
pub async fn run(state: Arc<BridgeState>) -> Result<(), Error> {
    let st_new = state.clone();
    let st_prompt = state.clone();
    let st_cancel = state.clone();
    let st_resume = state.clone();
    let st_list = state.clone();
    let st_close = state.clone();
    let st_delete = state.clone();

    Agent
        .builder()
        .name("muxi-acp")
        .on_receive_request(
            async move |request: InitializeRequest,
                        responder: Responder<InitializeResponse>,
                        _cx| {
                tracing::info!(
                    client = ?request.client_info.as_ref().map(|info| info.name.clone()),
                    "initialize"
                );
                responder.respond(
                    InitializeResponse::new(ProtocolVersion::V1)
                        .agent_capabilities(capabilities())
                        .agent_info(Implementation::new("muxi-acp", env!("CARGO_PKG_VERSION"))),
                )
            },
            on_receive_request!(),
        )
        .on_receive_request(
            async move |request: NewSessionRequest,
                        responder: Responder<NewSessionResponse>,
                        _cx| {
                match st_new.sessions.create(request.cwd.clone()) {
                    Ok(session_id) => {
                        tracing::info!(session_id, cwd = %request.cwd.display(), "session/new");
                        responder.respond(NewSessionResponse::new(session_id))
                    }
                    Err(_) => {
                        tracing::warn!("session/new rejected: max_sessions reached");
                        responder.respond_with_error(Error::invalid_params().data(json!({
                            "code": CODE_SESSION_LIMIT,
                            "message": "session limit reached (limits.max_sessions); close a session first",
                        })))
                    }
                }
            },
            on_receive_request!(),
        )
        .on_receive_request(
            async move |request: PromptRequest, responder: Responder<PromptResponse>, cx| {
                handle_prompt(&st_prompt, request, responder, cx)
            },
            on_receive_request!(),
        )
        .on_receive_notification(
            async move |notification: CancelNotification, cx| {
                let session_id = notification.session_id.0.to_string();
                tracing::info!(session_id, "session/cancel");
                if let Some(turn) = st_cancel.sessions.cancel_active_turn(&session_id) {
                    spawn_mux_cancel(&cx, &st_cancel, &session_id, &turn.request_id);
                }
                Ok(())
            },
            on_receive_notification!(),
        )
        .on_receive_request(
            async move |request: ResumeSessionRequest,
                        responder: Responder<ResumeSessionResponse>,
                        _cx| {
                let session_id = request.session_id.0.to_string();
                match st_resume.sessions.resume(&session_id, request.cwd.clone()) {
                    Ok(()) => {
                        tracing::info!(session_id, "session/resume (local rebind)");
                        responder.respond(ResumeSessionResponse::new())
                    }
                    Err(_) => {
                        tracing::warn!(session_id, "session/resume rejected: max_sessions reached");
                        responder.respond_with_error(Error::invalid_params().data(json!({
                            "code": CODE_SESSION_LIMIT,
                            "message": "session limit reached (limits.max_sessions); close a session first",
                        })))
                    }
                }
            },
            on_receive_request!(),
        )
        .on_receive_request(
            async move |_request: ListSessionsRequest,
                        responder: Responder<ListSessionsResponse>,
                        _cx| {
                let sessions = st_list
                    .sessions
                    .list()
                    .into_iter()
                    .map(|(id, cwd)| SessionInfo::new(id, cwd))
                    .collect();
                responder.respond(ListSessionsResponse::new(sessions))
            },
            on_receive_request!(),
        )
        .on_receive_request(
            async move |request: CloseSessionRequest,
                        responder: Responder<CloseSessionResponse>,
                        cx| {
                let session_id = request.session_id.0.to_string();
                tracing::info!(session_id, "session/close");
                if let Some(turn) = st_close.sessions.remove(&session_id) {
                    spawn_mux_cancel(&cx, &st_close, &session_id, &turn.request_id);
                }
                responder.respond(CloseSessionResponse::new())
            },
            on_receive_request!(),
        )
        .on_receive_request(
            async move |request: DeleteSessionRequest,
                        responder: Responder<DeleteSessionResponse>,
                        cx| {
                let session_id = request.session_id.0.to_string();
                tracing::info!(session_id, "session/delete (local only)");
                if let Some(turn) = st_delete.sessions.remove(&session_id) {
                    spawn_mux_cancel(&cx, &st_delete, &session_id, &turn.request_id);
                }
                responder.respond(DeleteSessionResponse::new())
            },
            on_receive_request!(),
        )
        .connect_to(Stdio::new().with_debug({
            // Not for debugging: this is the write-completion signal for the
            // per-turn buffer cap. The callback fires as each line reaches the
            // stdout writer, i.e. when it leaves the outgoing queue.
            let state = state.clone();
            move |line: &str, direction: LineDirection| {
                if direction == LineDirection::Stdout {
                    note_line_written(&state.sessions, line);
                }
            }
        }))
        .await
}

/// Validate the prompt, claim the session's turn slot, and hand the actual
/// streaming work to a spawned task so the event loop stays responsive
/// (session/cancel must be processable mid-turn).
fn handle_prompt(
    state: &Arc<BridgeState>,
    request: PromptRequest,
    responder: Responder<PromptResponse>,
    cx: ConnectionTo<Client>,
) -> Result<(), Error> {
    let session_id = request.session_id.0.to_string();

    if !state.sessions.contains(&session_id) {
        return responder.respond_with_error(Error::invalid_params().data(json!({
            "code": "BRIDGE_UNKNOWN_SESSION",
            "sessionId": session_id,
        })));
    }

    // Text-only in v1: concatenate text blocks in order, reject the rest.
    let mut text = String::new();
    for block in &request.prompt {
        match block {
            ContentBlock::Text(content) => text.push_str(&content.text),
            other => {
                return responder.respond_with_error(Error::invalid_params().data(json!({
                    "code": "BRIDGE_UNSUPPORTED_CONTENT",
                    "message": "muxi-acp v1 accepts text content blocks only",
                    "kind": content_block_kind(other),
                })));
            }
        }
    }

    let request_id = new_id("req");
    let cancel = match state.sessions.begin_turn(&session_id, &request_id) {
        Ok(token) => token,
        Err(TurnError::TurnInProgress) => {
            return responder.respond_with_error(Error::invalid_params().data(json!({
                "code": "BRIDGE_TURN_IN_PROGRESS",
                "message": "a prompt is already running for this session",
            })));
        }
        Err(TurnError::TurnLimit) => {
            // Reject, never queue: the ACP host owns queuing and pacing (Buzz
            // already keeps a per-channel prompt queue and will re-submit).
            // A bridge-side queue would double-buffer prompts and hide the
            // real saturation point from the host (spec §2 limits).
            return responder.respond_with_error(Error::invalid_params().data(json!({
                "code": CODE_TURN_LIMIT,
                "message": "too many turns in flight (limits.max_concurrent_turns); retry after one finishes",
            })));
        }
        Err(TurnError::UnknownSession) => {
            return responder.respond_with_error(Error::invalid_params().data(json!({
                "code": "BRIDGE_UNKNOWN_SESSION",
                "sessionId": session_id,
            })));
        }
    };

    tracing::info!(session_id, request_id, bytes = text.len(), "session/prompt");

    // Identity is resolved here, per-turn, from this turn's prompt text —
    // see `user_id_for_turn` for why it cannot be a session-level constant.
    let user_id = state.user_id_for_turn(&session_id, &text);

    let turn = run_turn(
        state.clone(),
        cx.clone(),
        responder,
        session_id,
        request_id,
        user_id,
        cancel,
        text,
    );
    cx.spawn(turn)?;
    Ok(())
}

fn content_block_kind(block: &ContentBlock) -> &'static str {
    match block {
        ContentBlock::Text(_) => "text",
        ContentBlock::Image(_) => "image",
        ContentBlock::Audio(_) => "audio",
        ContentBlock::ResourceLink(_) => "resource_link",
        ContentBlock::Resource(_) => "resource",
        _ => "unknown",
    }
}

/// One prompt turn: chat_stream -> translate -> session/update notifications,
/// resolved with exactly one terminal result. Never retries (spec §6).
///
/// Reliability posture (spec §6, PRD §15.3/§26):
/// - `turn_timeout` bounds the whole turn; `idle_timeout` bounds the gap
///   between SSE frames. Either expiry cancels upstream and fails the turn.
/// - `limits.max_buffered_bytes` bounds updates queued northbound but not yet
///   written; overflow cancels upstream and fails the turn — an update is
///   never dropped while the turn reports success.
/// - A host disconnect (stdin EOF) abandons the turn without responding; the
///   shutdown path in `main` fires the upstream cancels.
#[allow(clippy::too_many_arguments)]
async fn run_turn(
    state: Arc<BridgeState>,
    cx: ConnectionTo<Client>,
    responder: Responder<PromptResponse>,
    session_id: String,
    request_id: String,
    user_id: String,
    cancel: CancellationToken,
    text: String,
) -> Result<(), Error> {
    let payload = chat_payload(&text, &session_id, &request_id, state.agent_id.as_deref());
    let mux = state.mux.clone();
    let mut translator = Translator::new(state.forward_thoughts);

    let stream = mux.chat_stream(payload, Some(&user_id));
    futures::pin_mut!(stream);

    let mut responder = Some(responder);
    let mut saw_terminal = false;
    // When the host is gone there is no one to respond to: leave the turn in
    // the registry so the shutdown path can cancel it upstream.
    let mut leave_turn_registered = false;

    let turn_deadline = tokio::time::Instant::now() + state.turn_timeout;
    let mut idle_deadline = tokio::time::Instant::now() + state.idle_timeout;

    'turn: loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                // Cancellation won the race: MUST resolve Cancelled. The
                // MUXI-side cancel_request was already fired by the cancel
                // handler; dropping the stream tears the HTTP request down.
                tracing::info!(session_id, request_id, "turn cancelled");
                if let Some(responder) = responder.take() {
                    finish(responder.respond(PromptResponse::new(StopReason::Cancelled)));
                }
                break 'turn;
            }
            () = cx.incoming_closed() => {
                // Host disconnected mid-turn (stdin EOF). Stop streaming so
                // the connection can wind down; `main`'s shutdown path fires
                // the MUXI-side cancel for every turn still registered.
                tracing::info!(session_id, request_id, "host disconnected mid-turn");
                leave_turn_registered = true;
                break 'turn;
            }
            () = tokio::time::sleep_until(turn_deadline) => {
                tracing::warn!(session_id, request_id, "turn timeout expired");
                spawn_mux_cancel(&cx, &state, &session_id, &request_id);
                if let Some(responder) = responder.take() {
                    finish(responder.respond_with_error(bridge_error(
                        CODE_TURN_TIMEOUT,
                        &format!(
                            "turn exceeded turn_timeout ({})",
                            humantime::format_duration(state.turn_timeout)
                        ),
                    )));
                }
                break 'turn;
            }
            () = tokio::time::sleep_until(idle_deadline) => {
                tracing::warn!(session_id, request_id, "idle timeout expired");
                spawn_mux_cancel(&cx, &state, &session_id, &request_id);
                if let Some(responder) = responder.take() {
                    finish(responder.respond_with_error(bridge_error(
                        CODE_IDLE_TIMEOUT,
                        &format!(
                            "no upstream event within idle_timeout ({})",
                            humantime::format_duration(state.idle_timeout)
                        ),
                    )));
                }
                break 'turn;
            }
            item = stream.next() => match item {
                None => {
                    if let Some(responder) = responder.take() {
                        match stream_end_outcome(saw_terminal) {
                            Ok(reason) => finish(responder.respond(PromptResponse::new(reason))),
                            Err((code, message)) => {
                                tracing::warn!(session_id, request_id, code, "stream ended without terminal");
                                finish(responder.respond_with_error(bridge_error(&code, &message)));
                            }
                        }
                    }
                    break 'turn;
                }
                Some(Err(err)) => {
                    tracing::warn!(session_id, request_id, error = %err, "transport failure mid-turn");
                    if let Some(responder) = responder.take() {
                        finish(responder.respond_with_error(
                            bridge_error(CODE_TRANSPORT_ERROR, &err.to_string()),
                        ));
                    }
                    break 'turn;
                }
                Some(Ok(event)) => {
                    idle_deadline = tokio::time::Instant::now() + state.idle_timeout;
                    for turn_event in translator.translate(&event) {
                        match turn_event {
                            TurnEvent::Update(update) => {
                                let notification =
                                    SessionNotification::new(session_id.clone(), update);
                                // Reserve the (approximate: params-only) wire
                                // bytes against the per-turn cap before
                                // queueing; the stdout-writer hook releases
                                // the reservation once the line is written.
                                let bytes = serde_json::to_string(&notification)
                                    .map(|s| s.len())
                                    .unwrap_or(0);
                                if let Err(overflow) =
                                    state.sessions.buffer_reserve(&session_id, bytes)
                                {
                                    tracing::warn!(
                                        session_id,
                                        request_id,
                                        buffered = overflow.buffered_bytes,
                                        limit = overflow.limit,
                                        "per-turn buffer cap exceeded; failing the turn"
                                    );
                                    spawn_mux_cancel(&cx, &state, &session_id, &request_id);
                                    if let Some(responder) = responder.take() {
                                        finish(responder.respond_with_error(bridge_error(
                                            CODE_BUFFER_OVERFLOW,
                                            &format!(
                                                "host is not draining session/update fast enough \
                                                 ({} bytes queued > limits.max_buffered_bytes = {})",
                                                overflow.buffered_bytes, overflow.limit
                                            ),
                                        )));
                                    }
                                    break 'turn;
                                }
                                if let Err(err) = cx.send_notification(notification) {
                                    // Connection is going away; nothing more to do.
                                    tracing::warn!(error = ?err, "failed to send session/update");
                                    break 'turn;
                                }
                            }
                            TurnEvent::Completed => saw_terminal = true,
                            TurnEvent::Done => {
                                if let Some(responder) = responder.take() {
                                    finish(responder.respond(PromptResponse::new(StopReason::EndTurn)));
                                }
                                break 'turn;
                            }
                            TurnEvent::Error { code, message } => {
                                tracing::warn!(session_id, request_id, code, message, "upstream error");
                                if let Some(responder) = responder.take() {
                                    finish(responder.respond_with_error(bridge_error(&code, &message)));
                                }
                                break 'turn;
                            }
                        }
                    }
                }
            }
        }
    }

    if !leave_turn_registered {
        state.sessions.end_turn(&session_id, &request_id);
    }
    // Never propagate errors out of a spawned task: that would tear down the
    // whole connection over a single failed turn.
    Ok(())
}

fn finish(result: Result<(), Error>) {
    if let Err(err) = result {
        tracing::warn!(error = ?err, "failed to deliver terminal prompt result");
    }
}
