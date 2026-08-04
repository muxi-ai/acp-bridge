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
/// Stable diagnostic code: the turn exceeded the profile's `turn_timeout`.
pub const CODE_TURN_TIMEOUT: &str = "BRIDGE_TURN_TIMEOUT";
/// Stable diagnostic code: no SSE frame arrived within `idle_timeout`.
pub const CODE_IDLE_TIMEOUT: &str = "BRIDGE_IDLE_TIMEOUT";
/// Stable diagnostic code: the turn's queued-but-unwritten `session/update`
/// bytes exceeded `limits.max_buffered_bytes` (PRD §15.3).
pub const CODE_BUFFER_OVERFLOW: &str = "BRIDGE_BUFFER_OVERFLOW";
/// Stable diagnostic code: `session/new` rejected — `limits.max_sessions`.
pub const CODE_SESSION_LIMIT: &str = "BRIDGE_SESSION_LIMIT";
/// Stable diagnostic code: `session/prompt` rejected — `limits.max_concurrent_turns`.
pub const CODE_TURN_LIMIT: &str = "BRIDGE_TURN_LIMIT";

/// What a single MUXI SSE frame means for the current ACP turn.
// `Update` dominates the enum size, but values are translated and consumed
// one frame at a time — never stored in bulk — so boxing buys nothing here.
#[allow(clippy::large_enum_variant)]
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
    /// Whether any assistant text was streamed as `content` deltas this turn.
    /// Decides if a terminal `completed` frame's text is new or a duplicate.
    streamed_content: bool,
}

impl Translator {
    pub fn new(forward_thoughts: bool) -> Self {
        Self {
            forward_thoughts,
            seen_tools: HashSet::new(),
            streamed_content: false,
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
            "error" => vec![upstream_error(
                &parse_json(&event.data).unwrap_or(Value::Null),
            )],
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
            Some(Value::String(text)) => {
                self.streamed_content = true;
                return message_chunk(text);
            }
            Some(inner @ Value::Object(_)) => inner.clone(),
            Some(_) => return Vec::new(),
            None => json,
        };

        let event_type = obj.get("type").and_then(Value::as_str).unwrap_or("");
        match event_type {
            "content" => {
                self.streamed_content = true;
                message_chunk(&event_text(&obj))
            }
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
                    PlanEntry::new(text, PlanEntryPriority::Medium, PlanEntryStatus::InProgress),
                ])))]
            }
            "tool_call" => self.translate_tool_call(&obj),
            // No general ACP progress channel; never fabricate content (§7).
            "progress" => Vec::new(),
            "completed" => {
                // The runtime does not stream `content` deltas for a plain
                // turn: the full assistant text arrives HERE, in the terminal
                // event's `content` field (verified against a live formation).
                // Emit it as the final message chunk — unless deltas were
                // already streamed, in which case it duplicates what the host
                // has seen (PRD §15.2 dedup rule).
                let text = event_text(&obj);
                if !self.streamed_content && !text.is_empty() {
                    let mut out = message_chunk(&text);
                    out.push(TurnEvent::Completed);
                    out
                } else {
                    vec![TurnEvent::Completed]
                }
            }
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
    fn completed_after_streamed_content_is_terminal_only() {
        // Deltas were streamed, so the terminal text is a duplicate (§15.2).
        let mut t = Translator::new(false);
        t.translate(&frame(r#"{"type":"content","content":"Hello."}"#));
        let out = t.translate(&frame(r#"{"type":"completed","content":"Hello."}"#));
        assert_eq!(out, vec![TurnEvent::Completed]);
    }

    #[test]
    fn completed_without_streamed_content_carries_the_reply() {
        // A plain live turn: the runtime streams no content deltas — the full
        // assistant text arrives only in the terminal `completed` frame.
        let mut t = Translator::new(false);
        t.translate(&frame(r#"{"type":"progress","content":"working"}"#));
        t.translate(&frame(r#"{"type":"planning","content":"plan"}"#));
        let out = t.translate(&frame(r#"{"type":"completed","content":"Hello."}"#));
        assert_eq!(out.len(), 2);
        match &out[0] {
            TurnEvent::Update(update) => assert_eq!(text_of(update), "Hello."),
            other => panic!("expected message chunk, got {other:?}"),
        }
        assert_eq!(out[1], TurnEvent::Completed);
    }

    #[test]
    fn completed_with_empty_content_is_terminal_only() {
        let mut t = Translator::new(false);
        let out = t.translate(&frame(r#"{"type":"completed"}"#));
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
        let out = t.translate(&named(
            "error",
            r#"{"error":"boom","type":"RUNTIME_ERROR"}"#,
        ));
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

/// Property tests: the translator is the trust boundary for upstream bytes,
/// so it must hold its invariants for *arbitrary* frame data, not just the
/// fixtures above. Case counts are kept CI-friendly (256–512 per property).
#[cfg(test)]
mod proptests {
    use proptest::prelude::*;
    use serde_json::{json, Value};

    use super::*;

    fn frame(event: &str, data: &str) -> SseEvent {
        SseEvent {
            event: event.to_string(),
            data: data.to_string(),
        }
    }

    /// `TurnEvent::Error` can only ever be produced from a frame that itself
    /// carries "error" (as the SSE event name or a `type` field). If the
    /// input contains no "error" substring at all, no Error may come out —
    /// a sound over-approximation that needs no re-parsing in the test.
    fn assert_no_fabricated_error(events: &[TurnEvent], event: &str, data: &str) {
        if event != "error" && !data.contains("error") {
            assert!(
                events.iter().all(|e| !matches!(e, TurnEvent::Error { .. })),
                "fabricated Error from event={event:?} data={data:?}: {events:?}"
            );
        }
    }

    /// Arbitrary JSON values, deep enough to hit the token-unwrap and every
    /// typed branch.
    fn arb_json() -> impl Strategy<Value = Value> {
        let leaf = prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(|n| json!(n)),
            ".*".prop_map(Value::String),
            // Weight the field names the translator actually looks at, so the
            // generator reaches the interesting branches often.
            prop_oneof![
                Just("content"),
                Just("thinking"),
                Just("tool_call"),
                Just("completed"),
                Just("planning"),
                Just("done"),
                Just("token"),
                Just("error")
            ]
            .prop_map(|s| Value::String(s.to_string())),
        ];
        leaf.prop_recursive(3, 24, 6, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
                prop::collection::vec(
                    (
                        prop_oneof![
                            Just("type".to_string()),
                            Just("token".to_string()),
                            Just("content".to_string()),
                            Just("status".to_string()),
                            Just("tool_call_id".to_string()),
                            Just("error".to_string()),
                            ".*".prop_map(String::from),
                        ],
                        inner
                    ),
                    0..6
                )
                .prop_map(|pairs| Value::Object(pairs.into_iter().collect())),
            ]
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        /// Wholly arbitrary event names and frame data: never panic, always
        /// yield a Vec (possibly empty), never fabricate an Error.
        #[test]
        fn arbitrary_text_frames_never_panic_or_fabricate_errors(
            event in prop_oneof![
                Just("message".to_string()),
                Just("done".to_string()),
                Just("ui".to_string()),
                Just("error".to_string()),
                ".*",
            ],
            data in ".*",
            forward_thoughts in any::<bool>(),
        ) {
            let mut translator = Translator::new(forward_thoughts);
            let events = translator.translate(&frame(&event, &data));
            assert_no_fabricated_error(&events, &event, &data);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(384))]

        /// Structured-but-arbitrary JSON exercises every typed branch without
        /// panicking, including sequences of frames against one translator
        /// (the only cross-frame state is tool first-sighting and the
        /// streamed-content flag).
        #[test]
        fn arbitrary_json_frame_sequences_never_panic(
            values in prop::collection::vec(arb_json(), 1..5),
            forward_thoughts in any::<bool>(),
        ) {
            let mut translator = Translator::new(forward_thoughts);
            for value in &values {
                let data = value.to_string();
                let events = translator.translate(&frame("message", &data));
                assert_no_fabricated_error(&events, "message", &data);
            }
        }

        /// Truncating a valid frame at any char boundary (mid-token, mid-
        /// string, mid-escape) must never panic and never fabricate an Error
        /// — a torn frame is ignored, exactly like any other non-JSON data.
        /// (SSE data is always a valid-UTF-8 `String` by the SDK's contract,
        /// so char boundaries are the finest slicing the type system allows.)
        #[test]
        fn truncated_json_never_panics_and_never_errors(
            value in arb_json(),
            cut in any::<prop::sample::Index>(),
        ) {
            let data = value.to_string();
            let mut end = cut.index(data.len() + 1);
            while end < data.len() && !data.is_char_boundary(end) {
                end += 1;
            }
            let truncated = &data[..end];
            let mut translator = Translator::new(true);
            let events = translator.translate(&frame("message", truncated));
            assert_no_fabricated_error(&events, "message", truncated);
        }
    }
}
