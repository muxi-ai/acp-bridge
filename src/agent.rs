//! ACP agent surface (northbound): builds the connection, registers handlers
//! per binding spec §3/§4, and runs prompt turns against MUXI.

use std::sync::Arc;

use agent_client_protocol::schema::v1::{
    AgentCapabilities, CancelNotification, CloseSessionRequest, CloseSessionResponse,
    ContentBlock, DeleteSessionRequest, DeleteSessionResponse, Error, InitializeRequest,
    InitializeResponse, Implementation, ListSessionsRequest, ListSessionsResponse,
    NewSessionRequest, NewSessionResponse, PromptCapabilities, PromptRequest, PromptResponse,
    ResumeSessionRequest, ResumeSessionResponse, SessionCapabilities, SessionCloseCapabilities,
    SessionDeleteCapabilities, SessionInfo, SessionListCapabilities, SessionNotification,
    SessionResumeCapabilities, StopReason,
};
use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::{
    on_receive_notification, on_receive_request, Agent, Client, ConnectionTo, Responder, Stdio,
};
use futures::StreamExt;
use muxi_rust::FormationClient;
use serde_json::json;
use tokio_util::sync::CancellationToken;

use crate::config::resolve_user_id;
use crate::mux::chat_payload;
use crate::session::{new_id, ActiveTurn, SessionRegistry, TurnError};
use crate::translate::{
    stream_end_outcome, Translator, TurnEvent, CODE_TRANSPORT_ERROR,
};

/// Shared bridge state, cloned into every handler.
pub struct BridgeState {
    pub sessions: SessionRegistry,
    pub mux: FormationClient,
    pub agent_id: Option<String>,
    pub cli_user_id: Option<String>,
    pub default_user_id: Option<String>,
    pub forward_thoughts: bool,
}

impl BridgeState {
    fn user_id_for(&self, acp_session_id: &str) -> String {
        resolve_user_id(
            self.cli_user_id.as_deref(),
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
    turn: ActiveTurn,
) {
    let mux = state.mux.clone();
    let user_id = state.user_id_for(session_id);
    let session_id = session_id.to_string();
    let result = cx.spawn(async move {
        if let Err(err) = mux.cancel_request(&turn.request_id, &user_id).await {
            tracing::debug!(
                session_id,
                request_id = turn.request_id,
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
            async move |request: InitializeRequest, responder: Responder<InitializeResponse>, _cx| {
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
            async move |request: NewSessionRequest, responder: Responder<NewSessionResponse>, _cx| {
                let session_id = st_new.sessions.create(request.cwd.clone());
                tracing::info!(session_id, cwd = %request.cwd.display(), "session/new");
                responder.respond(NewSessionResponse::new(session_id))
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
                    spawn_mux_cancel(&cx, &st_cancel, &session_id, turn);
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
                st_resume.sessions.resume(&session_id, request.cwd.clone());
                tracing::info!(session_id, "session/resume (local rebind)");
                responder.respond(ResumeSessionResponse::new())
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
                    spawn_mux_cancel(&cx, &st_close, &session_id, turn);
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
                    spawn_mux_cancel(&cx, &st_delete, &session_id, turn);
                }
                responder.respond(DeleteSessionResponse::new())
            },
            on_receive_request!(),
        )
        .connect_to(Stdio::new())
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
        return responder.respond_with_error(
            Error::invalid_params().data(json!({
                "code": "BRIDGE_UNKNOWN_SESSION",
                "sessionId": session_id,
            })),
        );
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
        Err(TurnError::UnknownSession) => {
            return responder.respond_with_error(Error::invalid_params().data(json!({
                "code": "BRIDGE_UNKNOWN_SESSION",
                "sessionId": session_id,
            })));
        }
    };

    tracing::info!(session_id, request_id, bytes = text.len(), "session/prompt");

    let turn = run_turn(
        state.clone(),
        cx.clone(),
        responder,
        session_id,
        request_id,
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
async fn run_turn(
    state: Arc<BridgeState>,
    cx: ConnectionTo<Client>,
    responder: Responder<PromptResponse>,
    session_id: String,
    request_id: String,
    cancel: CancellationToken,
    text: String,
) -> Result<(), Error> {
    let user_id = state.user_id_for(&session_id);
    let payload = chat_payload(&text, &session_id, &request_id, state.agent_id.as_deref());
    let mux = state.mux.clone();
    let mut translator = Translator::new(state.forward_thoughts);

    let stream = mux.chat_stream(payload, Some(&user_id));
    futures::pin_mut!(stream);

    let mut responder = Some(responder);
    let mut saw_terminal = false;

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
                    for turn_event in translator.translate(&event) {
                        match turn_event {
                            TurnEvent::Update(update) => {
                                let notification =
                                    SessionNotification::new(session_id.clone(), update);
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

    state.sessions.end_turn(&session_id, &request_id);
    // Never propagate errors out of a spawned task: that would tear down the
    // whole connection over a single failed turn.
    Ok(())
}

fn finish(result: Result<(), Error>) {
    if let Err(err) = result {
        tracing::warn!(error = ?err, "failed to deliver terminal prompt result");
    }
}
