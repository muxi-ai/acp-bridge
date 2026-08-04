//! In-memory ACP session registry.
//!
//! MUXI has no conversation resource: the client owns `session_id` (binding
//! spec §3), so the bridge mints ids locally and uses the same id on both
//! sides. The registry tracks which sessions exist, their `cwd`, per-session
//! one-active-turn state, the concurrency caps (spec §2 limits), and per-turn
//! northbound buffer accounting (PRD §15.3). Nothing survives the process.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Mutex;

use rand::Rng;
use tokio_util::sync::CancellationToken;

use crate::config::Limits;

#[derive(Debug)]
pub struct SessionEntry {
    pub cwd: PathBuf,
    active_turn: Option<TurnState>,
}

/// Cancellation handle for a turn, as handed to callers.
#[derive(Debug, Clone)]
pub struct ActiveTurn {
    /// Bridge-generated MUXI `request_id` — the only cancellation handle.
    pub request_id: String,
    pub cancel: CancellationToken,
}

/// Full per-turn state as stored in the registry: the cancellation handle
/// plus northbound buffer accounting.
#[derive(Debug)]
struct TurnState {
    handle: ActiveTurn,
    /// FIFO of per-update byte sizes queued northbound but not yet written.
    /// The stdout writer preserves send order, so completions pop the front.
    pending_writes: VecDeque<usize>,
    buffered_bytes: usize,
}

#[derive(Debug, PartialEq)]
pub enum TurnError {
    UnknownSession,
    TurnInProgress,
    /// `max_concurrent_turns` reached across all sessions.
    TurnLimit,
}

/// `max_sessions` reached; `session/new` must be rejected.
#[derive(Debug, PartialEq)]
pub struct SessionLimitExceeded;

/// The turn's queued-but-unwritten updates would exceed `max_buffered_bytes`.
#[derive(Debug, PartialEq)]
pub struct BufferOverflow {
    pub buffered_bytes: usize,
    pub limit: usize,
}

pub struct SessionRegistry {
    inner: Mutex<HashMap<String, SessionEntry>>,
    limits: Limits,
}

impl SessionRegistry {
    pub fn new(limits: Limits) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            limits,
        }
    }

    /// Mint a new session id and register it, enforcing `max_sessions`.
    pub fn create(&self, cwd: PathBuf) -> Result<String, SessionLimitExceeded> {
        let mut inner = self.inner.lock().unwrap();
        if inner.len() >= self.limits.max_sessions {
            return Err(SessionLimitExceeded);
        }
        let id = new_id("sess");
        inner.insert(
            id.clone(),
            SessionEntry {
                cwd,
                active_turn: None,
            },
        );
        Ok(id)
    }

    /// Rebind a session id (session/resume). Cheap and honest: since the
    /// bridge owns the id space, an unknown id (e.g. after a restart) is
    /// simply re-registered — MUXI-side continuity depends on the formation's
    /// own buffer state. Rebinding counts against `max_sessions` too.
    pub fn resume(&self, session_id: &str, cwd: PathBuf) -> Result<(), SessionLimitExceeded> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(entry) = inner.get_mut(session_id) {
            entry.cwd = cwd;
            return Ok(());
        }
        if inner.len() >= self.limits.max_sessions {
            return Err(SessionLimitExceeded);
        }
        inner.insert(
            session_id.to_string(),
            SessionEntry {
                cwd,
                active_turn: None,
            },
        );
        Ok(())
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
        let turn = entry.active_turn?;
        turn.handle.cancel.cancel();
        Some(turn.handle)
    }

    /// Begin a turn, enforcing one active turn per session and the global
    /// `max_concurrent_turns` cap.
    pub fn begin_turn(
        &self,
        session_id: &str,
        request_id: &str,
    ) -> Result<CancellationToken, TurnError> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.contains_key(session_id) {
            return Err(TurnError::UnknownSession);
        }
        if inner
            .get(session_id)
            .is_some_and(|entry| entry.active_turn.is_some())
        {
            return Err(TurnError::TurnInProgress);
        }
        let in_flight = inner
            .values()
            .filter(|entry| entry.active_turn.is_some())
            .count();
        if in_flight >= self.limits.max_concurrent_turns {
            return Err(TurnError::TurnLimit);
        }
        let token = CancellationToken::new();
        let entry = inner.get_mut(session_id).expect("checked above");
        entry.active_turn = Some(TurnState {
            handle: ActiveTurn {
                request_id: request_id.to_string(),
                cancel: token.clone(),
            },
            pending_writes: VecDeque::new(),
            buffered_bytes: 0,
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
                .is_some_and(|turn| turn.handle.request_id == request_id)
            {
                entry.active_turn = None;
            }
        }
    }

    /// Fire the cancellation token for the session's active turn (if any) and
    /// return it so the caller can also cancel the MUXI request.
    pub fn cancel_active_turn(&self, session_id: &str) -> Option<ActiveTurn> {
        let inner = self.inner.lock().unwrap();
        let handle = inner.get(session_id)?.active_turn.as_ref()?.handle.clone();
        handle.cancel.cancel();
        Some(handle)
    }

    /// Account for one `session/update` about to be queued northbound.
    ///
    /// Enforces `max_buffered_bytes` per turn: on overflow nothing is
    /// recorded and the caller must fail the turn — never silently drop an
    /// update while reporting success (PRD §15.3). A missing session/turn is
    /// not an error: the turn is already ending and the send is moot.
    pub fn buffer_reserve(&self, session_id: &str, bytes: usize) -> Result<(), BufferOverflow> {
        let mut inner = self.inner.lock().unwrap();
        let Some(turn) = inner
            .get_mut(session_id)
            .and_then(|entry| entry.active_turn.as_mut())
        else {
            return Ok(());
        };
        let would_be = turn.buffered_bytes.saturating_add(bytes);
        if would_be > self.limits.max_buffered_bytes {
            return Err(BufferOverflow {
                buffered_bytes: would_be,
                limit: self.limits.max_buffered_bytes,
            });
        }
        turn.pending_writes.push_back(bytes);
        turn.buffered_bytes = would_be;
        Ok(())
    }

    /// One previously reserved update for this session reached the stdout
    /// writer. Pops the oldest reservation (writes preserve send order).
    pub fn buffer_written(&self, session_id: &str) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(turn) = inner
            .get_mut(session_id)
            .and_then(|entry| entry.active_turn.as_mut())
        {
            if let Some(bytes) = turn.pending_writes.pop_front() {
                turn.buffered_bytes = turn.buffered_bytes.saturating_sub(bytes);
            }
        }
    }

    /// Bytes currently reserved for a session's active turn (test-only probe).
    #[cfg(test)]
    pub fn buffered_bytes(&self, session_id: &str) -> usize {
        self.inner
            .lock()
            .unwrap()
            .get(session_id)
            .and_then(|entry| entry.active_turn.as_ref())
            .map_or(0, |turn| turn.buffered_bytes)
    }

    /// Graceful shutdown: cancel every active turn and return the handles so
    /// the caller can fire the MUXI-side cancels (spec §6 / PRD §21 — never
    /// leave a formation running a turn for a dead host).
    pub fn drain_active_turns(&self) -> Vec<(String, ActiveTurn)> {
        let mut inner = self.inner.lock().unwrap();
        let mut turns = Vec::new();
        for (session_id, entry) in inner.iter_mut() {
            if let Some(turn) = entry.active_turn.take() {
                turn.handle.cancel.cancel();
                turns.push((session_id.clone(), turn.handle));
            }
        }
        turns
    }
}

/// Mint an id like `sess_h1x9k2p4q7z3` / `req_...`.
pub fn new_id(prefix: &str) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    let suffix: String = (0..12)
        .map(|_| ALPHABET[rng.random_range(0..ALPHABET.len())] as char)
        .collect();
    format!("{prefix}_{suffix}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> SessionRegistry {
        SessionRegistry::new(Limits::default())
    }

    fn registry_with(limits: Limits) -> SessionRegistry {
        SessionRegistry::new(limits)
    }

    #[test]
    fn create_and_list_sessions() {
        let registry = registry();
        let id = registry.create(PathBuf::from("/tmp")).unwrap();
        assert!(id.starts_with("sess_"));
        assert!(registry.contains(&id));
        assert_eq!(registry.list().len(), 1);
    }

    #[test]
    fn one_active_turn_per_session() {
        let registry = registry();
        let id = registry.create(PathBuf::from("/tmp")).unwrap();

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
        let registry = registry();
        assert_eq!(
            registry.begin_turn("sess_missing", "req_1").unwrap_err(),
            TurnError::UnknownSession
        );
    }

    #[test]
    fn max_sessions_is_enforced() {
        let registry = registry_with(Limits {
            max_sessions: 2,
            ..Limits::default()
        });
        registry.create(PathBuf::from("/tmp")).unwrap();
        registry.create(PathBuf::from("/tmp")).unwrap();
        assert_eq!(
            registry.create(PathBuf::from("/tmp")).unwrap_err(),
            SessionLimitExceeded
        );

        // Resuming a *known* session never counts as a new one...
        let known = registry.list()[0].0.clone();
        registry
            .resume(&known, PathBuf::from("/elsewhere"))
            .unwrap();
        // ...but resurrecting an unknown id is a create and hits the cap.
        assert_eq!(
            registry
                .resume("sess_fromlastrun", PathBuf::from("/tmp"))
                .unwrap_err(),
            SessionLimitExceeded
        );
    }

    #[test]
    fn max_concurrent_turns_is_enforced_across_sessions() {
        let registry = registry_with(Limits {
            max_concurrent_turns: 2,
            ..Limits::default()
        });
        let a = registry.create(PathBuf::from("/tmp")).unwrap();
        let b = registry.create(PathBuf::from("/tmp")).unwrap();
        let c = registry.create(PathBuf::from("/tmp")).unwrap();

        registry.begin_turn(&a, "req_a").unwrap();
        registry.begin_turn(&b, "req_b").unwrap();
        assert_eq!(
            registry.begin_turn(&c, "req_c").unwrap_err(),
            TurnError::TurnLimit
        );

        // Ending one turn frees a slot.
        registry.end_turn(&a, "req_a");
        registry.begin_turn(&c, "req_c").unwrap();
    }

    #[test]
    fn cancel_fires_token_and_reports_request_id() {
        let registry = registry();
        let id = registry.create(PathBuf::from("/tmp")).unwrap();
        let token = registry.begin_turn(&id, "req_1").unwrap();

        let turn = registry.cancel_active_turn(&id).unwrap();
        assert_eq!(turn.request_id, "req_1");
        assert!(token.is_cancelled());

        assert!(registry.cancel_active_turn("sess_missing").is_none());
    }

    #[test]
    fn resume_rebinds_unknown_sessions() {
        let registry = registry();
        registry
            .resume("sess_fromlastrun", PathBuf::from("/tmp"))
            .unwrap();
        assert!(registry.contains("sess_fromlastrun"));
    }

    #[test]
    fn remove_cancels_active_turn() {
        let registry = registry();
        let id = registry.create(PathBuf::from("/tmp")).unwrap();
        let token = registry.begin_turn(&id, "req_1").unwrap();

        let turn = registry.remove(&id).unwrap();
        assert!(token.is_cancelled());
        assert_eq!(turn.request_id, "req_1");
        assert!(!registry.contains(&id));
    }

    #[test]
    fn buffer_accounting_reserves_writes_and_overflows() {
        let registry = registry_with(Limits {
            max_buffered_bytes: 100,
            ..Limits::default()
        });
        let id = registry.create(PathBuf::from("/tmp")).unwrap();
        registry.begin_turn(&id, "req_1").unwrap();

        registry.buffer_reserve(&id, 40).unwrap();
        registry.buffer_reserve(&id, 40).unwrap();
        assert_eq!(registry.buffered_bytes(&id), 80);

        // Exceeding the cap records nothing and reports the totals.
        let overflow = registry.buffer_reserve(&id, 40).unwrap_err();
        assert_eq!(
            overflow,
            BufferOverflow {
                buffered_bytes: 120,
                limit: 100,
            }
        );
        assert_eq!(registry.buffered_bytes(&id), 80);

        // A completed write frees the oldest reservation (FIFO).
        registry.buffer_written(&id);
        assert_eq!(registry.buffered_bytes(&id), 40);
        registry.buffer_reserve(&id, 40).unwrap();

        // Ending the turn clears all accounting.
        registry.end_turn(&id, "req_1");
        assert_eq!(registry.buffered_bytes(&id), 0);

        // Reserve/written on a session without an active turn are no-ops.
        registry.buffer_reserve(&id, 10_000).unwrap();
        registry.buffer_written(&id);
    }

    #[test]
    fn drain_active_turns_cancels_everything() {
        let registry = registry();
        let a = registry.create(PathBuf::from("/tmp")).unwrap();
        let b = registry.create(PathBuf::from("/tmp")).unwrap();
        let token_a = registry.begin_turn(&a, "req_a").unwrap();
        registry.create(PathBuf::from("/tmp")).unwrap(); // idle session

        let mut drained = registry.drain_active_turns();
        drained.sort_by(|x, y| x.1.request_id.cmp(&y.1.request_id));
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].0, a);
        assert_eq!(drained[0].1.request_id, "req_a");
        assert!(token_a.is_cancelled());

        // Turns are gone: a new one can start immediately.
        registry.begin_turn(&b, "req_b2").unwrap();
        assert!(registry.drain_active_turns().len() == 1);
    }
}
