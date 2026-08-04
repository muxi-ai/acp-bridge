//! MUXI SSE event -> ACP `session/update` translation.
//!
//! Pure translation layer per binding spec §7/§8: no I/O, no protocol
//! plumbing, so every mapping row is unit-testable.

use std::collections::HashSet;

use agent_client_protocol::schema::v1::{
    ContentChunk, Plan, PlanEntry, PlanEntryPriority, PlanEntryStatus, SessionUpdate, StopReason,
    ToolCall, ToolCallStatus, ToolCallUpdate, ToolCallUpdateFields,
};
use muxi_rust::SseEvent;
use serde_json::Value;

/// Stable diagnostic code: the upstream stream ended without a terminal event.
pub const CODE_STREAM_TRUNCATED: &str = "BRIDGE_STREAM_TRUNCATED";
/// Stable diagnostic code: the formation reported an error event mid-turn.
pub const CODE_UPSTREAM_ERROR: &str = "BRIDGE_UPSTREAM_ERROR";
/// Stable diagnostic code: the HTTPS/SSE transport failed mid-turn.
pub const CODE_TRANSPORT_ERROR: &str = "BRIDGE_TRANSPORT_ERROR";

/// What a single MUXI SSE frame means for the current ACP turn.
#[derive(Debug, Clone, PartialEq)]
pub enum TurnEvent {
    /// Forward a `session/update` notification.
    Update(SessionUpdate),
    /// The formation reported the turn as completed (terminal).
    Completed,
    /// `event: done` — end of stream.
    Done,
    /// The formation reported an error; the turn must fail.
    Error { code: String, message: String },
}

/// Stateful per-turn translator.
///
/// State is only used for tool-call first-sighting: the first frame carrying a
/// given tool id becomes `tool_call`, subsequent frames become
/// `tool_call_update`.
pub struct Translator {
    forward_thoughts: bool,
    seen_tools: HashSet<String>,
}

impl Translator {
    pub fn new(forward_thoughts: bool) -> Self {
        Self {
            forward_thoughts,
            seen_tools: HashSet::new(),
        }
    }

    /// Translate one SSE frame into zero or more turn events.
    pub fn translate(&mut self, event: &SseEvent) -> Vec<TurnEvent> {
        match event.event.as_str() {
            "done" => vec![TurnEvent::Done],
            // v1: UI widgets are not mapped; the text stream always carries
            // the fallback, so widgets degrade gracefully (spec §7).
            "ui" => Vec::new(),
            // The SDK converts named `event: error` frames into `Err` items,
            // but handle one defensively if it ever reaches us.
            "error" => vec![upstream_error(&parse_json(&event.data).unwrap_or(Value::Null))],
            _ => self.translate_data_frame(&event.data),
        }
    }

    fn translate_data_frame(&mut self, data: &str) -> Vec<TurnEvent> {
        let Some(json) = parse_json(data) else {
            return Vec::new();
        };

        // The /chat route wraps each streamed item as {"token": <item>} where
        // the item is either a raw text delta or an event object. Unwrap it;
        // frames that already carry a top-level `type` pass through unchanged.
        let obj = match json.get("token") {
            Some(Value::String(text)) => return message_chunk(text),
            Some(inner @ Value::Object(_)) => inner.clone(),
            Some(_) => return Vec::new(),
            None => json,
        };

        let event_type = obj.get("type").and_then(Value::as_str).unwrap_or("");
        match event_type {
            "content" => message_chunk(&event_text(&obj)),
            "thinking" => {
                // Gated off by default (spec §7): only forward when the
                // operator opted in via `forward_thoughts`.
                if self.forward_thoughts {
                    vec![TurnEvent::Update(SessionUpdate::AgentThoughtChunk(
                        ContentChunk::new(event_text(&obj).into()),
                    ))]
                } else {
                    Vec::new()
                }
            }
            "planning" | "replanning" => {
                let text = event_text(&obj);
                vec![TurnEvent::Update(SessionUpdate::Plan(Plan::new(vec![
                    PlanEntry::new(
                        text,
                        PlanEntryPriority::Medium,
                        PlanEntryStatus::InProgress,
                    ),
                ])))]
            }
            "tool_call" => self.translate_tool_call(&obj),
            // No general ACP progress channel; never fabricate content (§7).
            "progress" => Vec::new(),
            "completed" => vec![TurnEvent::Completed],
            "error" => vec![upstream_error(&obj)],
            _ => Vec::new(),
        }
    }

    fn translate_tool_call(&mut self, obj: &Value) -> Vec<TurnEvent> {
        let id = ["tool_call_id", "tool_id", "id", "tool", "name"]
            .iter()
            .find_map(|k| obj.get(k).and_then(Value::as_str))
            .unwrap_or("tool")
            .to_string();
        let title = ["title", "name", "tool", "tool_name"]
            .iter()
            .find_map(|k| obj.get(k).and_then(Value::as_str))
            .unwrap_or(&id)
            .to_string();
        let status = obj
            .get("status")
            .and_then(Value::as_str)
            .and_then(tool_status);
        let content_text = obj.get("content").and_then(Value::as_str);

        if self.seen_tools.insert(id.clone()) {
            let mut call =
                ToolCall::new(id, title).status(status.unwrap_or(ToolCallStatus::InProgress));
            if let Some(text) = content_text {
                call = call.content(vec![text.into()]);
            }
            vec![TurnEvent::Update(SessionUpdate::ToolCall(call))]
        } else {
            let mut fields = ToolCallUpdateFields::new().status(status);
            if let Some(text) = content_text {
                fields = fields.content(vec![text.into()]);
            }
            vec![TurnEvent::Update(SessionUpdate::ToolCallUpdate(
                ToolCallUpdate::new(id, fields),
            ))]
        }
    }
}

/// Resolve the turn when the SSE stream ends (spec §8).
///
/// A terminal event (`completed` or `done`) means the turn finished cleanly.
/// A stream that ends without one is an unexpected failure — never guessed
/// into `MaxTokens`/`MaxTurnRequests`/`Refusal`, which the bridge never emits.
pub fn stream_end_outcome(saw_terminal: bool) -> Result<StopReason, (String, String)> {
    if saw_terminal {
        Ok(StopReason::EndTurn)
    } else {
        Err((
            CODE_STREAM_TRUNCATED.to_string(),
            "stream ended without a terminal event".to_string(),
        ))
    }
}

fn parse_json(data: &str) -> Option<Value> {
    serde_json::from_str(data).ok()
}

fn message_chunk(text: &str) -> Vec<TurnEvent> {
    if text.is_empty() {
        return Vec::new();
    }
    vec![TurnEvent::Update(SessionUpdate::AgentMessageChunk(
        ContentChunk::new(text.into()),
    ))]
}

fn event_text(obj: &Value) -> String {
    ["content", "text", "message", "plan"]
        .iter()
        .find_map(|k| obj.get(k).and_then(Value::as_str))
        .unwrap_or("")
        .to_string()
}

fn tool_status(status: &str) -> Option<ToolCallStatus> {
    match status {
        "pending" => Some(ToolCallStatus::Pending),
        "in_progress" | "running" | "started" => Some(ToolCallStatus::InProgress),
        "completed" | "success" => Some(ToolCallStatus::Completed),
        "failed" | "error" => Some(ToolCallStatus::Failed),
        _ => None,
    }
}

fn upstream_error(obj: &Value) -> TurnEvent {
    let code = obj
        .get("type")
        .or_else(|| obj.get("code"))
        .and_then(Value::as_str)
        .filter(|c| *c != "error")
        .unwrap_or(CODE_UPSTREAM_ERROR)
        .to_string();
    let message = obj
        .get("error")
        .or_else(|| obj.get("message"))
        .or_else(|| obj.get("content"))
        .and_then(Value::as_str)
        .unwrap_or("upstream error")
        .to_string();
    TurnEvent::Error { code, message }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(data: &str) -> SseEvent {
        SseEvent {
            event: "message".to_string(),
            data: data.to_string(),
        }
    }

    fn named(event: &str, data: &str) -> SseEvent {
        SseEvent {
            event: event.to_string(),
            data: data.to_string(),
        }
    }

    fn text_of(update: &SessionUpdate) -> String {
        match update {
            SessionUpdate::AgentMessageChunk(chunk) | SessionUpdate::AgentThoughtChunk(chunk) => {
                match &chunk.content {
                    agent_client_protocol::schema::v1::ContentBlock::Text(text) => {
                        text.text.clone()
                    }
                    other => panic!("expected text content, got {other:?}"),
                }
            }
            other => panic!("expected content chunk, got {other:?}"),
        }
    }

    #[test]
    fn content_maps_to_agent_message_chunk() {
        let mut t = Translator::new(false);
        let out = t.translate(&frame(r#"{"type":"content","content":"Hello"}"#));
        assert_eq!(out.len(), 1);
        match &out[0] {
            TurnEvent::Update(update @ SessionUpdate::AgentMessageChunk(_)) => {
                assert_eq!(text_of(update), "Hello");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn token_string_wrapper_maps_to_agent_message_chunk() {
        let mut t = Translator::new(false);
        let out = t.translate(&frame(r#"{"token":"Hel"}"#));
        assert_eq!(out.len(), 1);
        match &out[0] {
            TurnEvent::Update(update @ SessionUpdate::AgentMessageChunk(_)) => {
                assert_eq!(text_of(update), "Hel");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn token_object_wrapper_is_unwrapped() {
        let mut t = Translator::new(false);
        let out = t.translate(&frame(r#"{"token":{"type":"content","content":"Hi"}}"#));
        assert_eq!(out.len(), 1);
        assert!(matches!(
            out[0],
            TurnEvent::Update(SessionUpdate::AgentMessageChunk(_))
        ));
    }

    #[test]
    fn thinking_is_gated_off_by_default() {
        let mut t = Translator::new(false);
        let out = t.translate(&frame(r#"{"type":"thinking","content":"hmm"}"#));
        assert!(out.is_empty());
    }

    #[test]
    fn thinking_forwards_as_thought_chunk_when_opted_in() {
        let mut t = Translator::new(true);
        let out = t.translate(&frame(r#"{"type":"thinking","content":"hmm"}"#));
        assert_eq!(out.len(), 1);
        match &out[0] {
            TurnEvent::Update(update @ SessionUpdate::AgentThoughtChunk(_)) => {
                assert_eq!(text_of(update), "hmm");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn planning_and_replanning_map_to_plan() {
        let mut t = Translator::new(false);
        for event_type in ["planning", "replanning"] {
            let data = format!(r#"{{"type":"{event_type}","content":"step 1"}}"#);
            let out = t.translate(&frame(&data));
            assert_eq!(out.len(), 1, "{event_type}");
            match &out[0] {
                TurnEvent::Update(SessionUpdate::Plan(plan)) => {
                    assert_eq!(plan.entries.len(), 1);
                    assert_eq!(plan.entries[0].content, "step 1");
                    assert_eq!(plan.entries[0].status, PlanEntryStatus::InProgress);
                }
                other => panic!("unexpected for {event_type}: {other:?}"),
            }
        }
    }

    #[test]
    fn tool_call_first_sighting_creates_then_updates() {
        let mut t = Translator::new(false);

        let first = t.translate(&frame(
            r#"{"type":"tool_call","tool_call_id":"tc_1","name":"search","status":"running"}"#,
        ));
        assert_eq!(first.len(), 1);
        match &first[0] {
            TurnEvent::Update(SessionUpdate::ToolCall(call)) => {
                assert_eq!(call.tool_call_id.0.as_ref(), "tc_1");
                assert_eq!(call.title, "search");
                assert_eq!(call.status, ToolCallStatus::InProgress);
            }
            other => panic!("unexpected: {other:?}"),
        }

        let second = t.translate(&frame(
            r#"{"type":"tool_call","tool_call_id":"tc_1","status":"completed","content":"42"}"#,
        ));
        assert_eq!(second.len(), 1);
        match &second[0] {
            TurnEvent::Update(SessionUpdate::ToolCallUpdate(update)) => {
                assert_eq!(update.tool_call_id.0.as_ref(), "tc_1");
                assert_eq!(update.fields.status, Some(ToolCallStatus::Completed));
                assert!(update.fields.content.is_some());
            }
            other => panic!("unexpected: {other:?}"),
        }

        // A different tool id is a new first sighting.
        let third = t.translate(&frame(r#"{"type":"tool_call","tool_call_id":"tc_2"}"#));
        assert!(matches!(
            third[0],
            TurnEvent::Update(SessionUpdate::ToolCall(_))
        ));
    }

    #[test]
    fn progress_is_dropped() {
        let mut t = Translator::new(false);
        let out = t.translate(&frame(r#"{"type":"progress","content":"working..."}"#));
        assert!(out.is_empty());
    }

    #[test]
    fn completed_is_terminal() {
        let mut t = Translator::new(false);
        let out = t.translate(&frame(r#"{"type":"completed","content":"done"}"#));
        assert_eq!(out, vec![TurnEvent::Completed]);
    }

    #[test]
    fn error_data_frame_fails_the_turn() {
        let mut t = Translator::new(false);
        let out = t.translate(&frame(r#"{"type":"error","error":"boom"}"#));
        assert_eq!(out.len(), 1);
        match &out[0] {
            TurnEvent::Error { message, .. } => assert_eq!(message, "boom"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn named_error_frame_fails_the_turn() {
        let mut t = Translator::new(false);
        let out = t.translate(&named("error", r#"{"error":"boom","type":"RUNTIME_ERROR"}"#));
        assert_eq!(out.len(), 1);
        match &out[0] {
            TurnEvent::Error { code, message } => {
                assert_eq!(code, "RUNTIME_ERROR");
                assert_eq!(message, "boom");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn ui_frame_is_ignored_in_v1() {
        let mut t = Translator::new(false);
        let out = t.translate(&named("ui", r#"{"ui":[{"type":"options"}]}"#));
        assert!(out.is_empty());
    }

    #[test]
    fn done_ends_the_turn() {
        let mut t = Translator::new(false);
        let out = t.translate(&named("done", r#"{"finished":true}"#));
        assert_eq!(out, vec![TurnEvent::Done]);
    }

    #[test]
    fn unknown_and_malformed_frames_are_ignored() {
        let mut t = Translator::new(false);
        assert!(t.translate(&frame(r#"{"type":"mystery"}"#)).is_empty());
        assert!(t.translate(&frame("not json")).is_empty());
        assert!(t.translate(&frame(r#"{"token":42}"#)).is_empty());
    }

    #[test]
    fn stream_end_with_terminal_is_end_turn() {
        assert_eq!(stream_end_outcome(true), Ok(StopReason::EndTurn));
    }

    #[test]
    fn stream_end_without_terminal_is_unexpected_failure() {
        let err = stream_end_outcome(false).unwrap_err();
        assert_eq!(err.0, CODE_STREAM_TRUNCATED);
    }

    #[test]
    fn max_tokens_and_friends_are_never_emitted() {
        // The only success outcome the bridge produces is EndTurn; the only
        // other stop reason is Cancelled (produced by the cancel path in
        // agent.rs). MaxTokens / MaxTurnRequests / Refusal are unreachable.
        for saw_terminal in [true, false] {
            if let Ok(reason) = stream_end_outcome(saw_terminal) {
                assert_eq!(reason, StopReason::EndTurn);
            }
        }
    }
}
