//! In-memory ACP session registry.
//!
//! MUXI has no conversation resource: the client owns `session_id` (binding
//! spec §3), so the bridge mints ids locally and uses the same id on both
//! sides. The registry only tracks which sessions exist, their `cwd`, and
//! per-session one-active-turn state. Nothing survives the process.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;

use rand::Rng;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct SessionEntry {
    pub cwd: PathBuf,
    pub active_turn: Option<ActiveTurn>,
}

#[derive(Debug, Clone)]
pub struct ActiveTurn {
    /// Bridge-generated MUXI `request_id` — the only cancellation handle.
    pub request_id: String,
    pub cancel: CancellationToken,
}

#[derive(Debug, PartialEq)]
pub enum TurnError {
    UnknownSession,
    TurnInProgress,
}

#[derive(Default)]
pub struct SessionRegistry {
    inner: Mutex<HashMap<String, SessionEntry>>,
}

impl SessionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Mint a new session id and register it.
    pub fn create(&self, cwd: PathBuf) -> String {
        let id = new_id("sess");
        self.inner.lock().unwrap().insert(
            id.clone(),
            SessionEntry {
                cwd,
                active_turn: None,
            },
        );
        id
    }

    /// Rebind a session id (session/resume). Cheap and honest: since the
    /// bridge owns the id space, an unknown id (e.g. after a restart) is
    /// simply re-registered — MUXI-side continuity depends on the formation's
    /// own buffer state.
    pub fn resume(&self, session_id: &str, cwd: PathBuf) {
        let mut inner = self.inner.lock().unwrap();
        inner
            .entry(session_id.to_string())
            .and_modify(|entry| entry.cwd = cwd.clone())
            .or_insert(SessionEntry {
                cwd,
                active_turn: None,
            });
    }

    pub fn contains(&self, session_id: &str) -> bool {
        self.inner.lock().unwrap().contains_key(session_id)
    }

    pub fn list(&self) -> Vec<(String, PathBuf)> {
        let mut sessions: Vec<(String, PathBuf)> = self
            .inner
            .lock()
            .unwrap()
            .iter()
            .map(|(id, entry)| (id.clone(), entry.cwd.clone()))
            .collect();
        sessions.sort_by(|a, b| a.0.cmp(&b.0));
        sessions
    }

    /// Close/delete: cancel any active turn and drop the entry. Returns the
    /// active turn (if any) so the caller can fire the MUXI-side cancel.
    pub fn remove(&self, session_id: &str) -> Option<ActiveTurn> {
        let entry = self.inner.lock().unwrap().remove(session_id)?;
        if let Some(turn) = &entry.active_turn {
            turn.cancel.cancel();
        }
        entry.active_turn
    }

    /// Begin a turn, enforcing one active turn per session.
    pub fn begin_turn(
        &self,
        session_id: &str,
        request_id: &str,
    ) -> Result<CancellationToken, TurnError> {
        let mut inner = self.inner.lock().unwrap();
        let entry = inner.get_mut(session_id).ok_or(TurnError::UnknownSession)?;
        if entry.active_turn.is_some() {
            return Err(TurnError::TurnInProgress);
        }
        let token = CancellationToken::new();
        entry.active_turn = Some(ActiveTurn {
            request_id: request_id.to_string(),
            cancel: token.clone(),
        });
        Ok(token)
    }

    /// End a turn if `request_id` still owns it.
    pub fn end_turn(&self, session_id: &str, request_id: &str) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(entry) = inner.get_mut(session_id) {
            if entry
                .active_turn
                .as_ref()
                .is_some_and(|turn| turn.request_id == request_id)
            {
                entry.active_turn = None;
            }
        }
    }

    /// Fire the cancellation token for the session's active turn (if any) and
    /// return it so the caller can also cancel the MUXI request.
    pub fn cancel_active_turn(&self, session_id: &str) -> Option<ActiveTurn> {
        let inner = self.inner.lock().unwrap();
        let turn = inner.get(session_id)?.active_turn.clone()?;
        turn.cancel.cancel();
        Some(turn)
    }
}

/// Mint an id like `sess_h1x9k2p4q7z3` / `req_...`.
pub fn new_id(prefix: &str) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();
    let suffix: String = (0..12)
        .map(|_| ALPHABET[rng.gen_range(0..ALPHABET.len())] as char)
        .collect();
    format!("{prefix}_{suffix}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_list_sessions() {
        let registry = SessionRegistry::new();
        let id = registry.create(PathBuf::from("/tmp"));
        assert!(id.starts_with("sess_"));
        assert!(registry.contains(&id));
        assert_eq!(registry.list().len(), 1);
    }

    #[test]
    fn one_active_turn_per_session() {
        let registry = SessionRegistry::new();
        let id = registry.create(PathBuf::from("/tmp"));

        registry.begin_turn(&id, "req_1").unwrap();
        assert_eq!(
            registry.begin_turn(&id, "req_2").unwrap_err(),
            TurnError::TurnInProgress
        );

        registry.end_turn(&id, "req_1");
        registry.begin_turn(&id, "req_2").unwrap();
    }

    #[test]
    fn begin_turn_on_unknown_session_fails() {
        let registry = SessionRegistry::new();
        assert_eq!(
            registry.begin_turn("sess_missing", "req_1").unwrap_err(),
            TurnError::UnknownSession
        );
    }

    #[test]
    fn cancel_fires_token_and_reports_request_id() {
        let registry = SessionRegistry::new();
        let id = registry.create(PathBuf::from("/tmp"));
        let token = registry.begin_turn(&id, "req_1").unwrap();

        let turn = registry.cancel_active_turn(&id).unwrap();
        assert_eq!(turn.request_id, "req_1");
        assert!(token.is_cancelled());

        assert!(registry.cancel_active_turn("sess_missing").is_none());
    }

    #[test]
    fn resume_rebinds_unknown_sessions() {
        let registry = SessionRegistry::new();
        registry.resume("sess_fromlastrun", PathBuf::from("/tmp"));
        assert!(registry.contains("sess_fromlastrun"));
    }

    #[test]
    fn remove_cancels_active_turn() {
        let registry = SessionRegistry::new();
        let id = registry.create(PathBuf::from("/tmp"));
        let token = registry.begin_turn(&id, "req_1").unwrap();

        let turn = registry.remove(&id).unwrap();
        assert!(token.is_cancelled());
        assert_eq!(turn.request_id, "req_1");
        assert!(!registry.contains(&id));
    }
}
