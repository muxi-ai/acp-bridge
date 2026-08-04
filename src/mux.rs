//! Thin wrapper over the MUXI Rust SDK (southbound).

use muxi_rust::{FormationClient, FormationConfig};
use serde_json::{json, Value};

use crate::config::{ConfigError, Profile};

/// Build a `FormationClient` from a resolved profile + client key.
pub fn client_from_profile(profile: &Profile, client_key: &str) -> Result<FormationClient, ConfigError> {
    let config = if let Some(base_url) = &profile.base_url {
        FormationConfig::with_base_url(base_url, client_key, "")
    } else {
        let (Some(server_url), Some(formation)) = (&profile.server_url, &profile.formation) else {
            return Err(ConfigError::InvalidEndpoint(
                "set either base_url, or both server_url and formation".to_string(),
            ));
        };
        FormationConfig::new(server_url, formation, client_key, "")
    };
    FormationClient::new(config)
        .map_err(|err| ConfigError::InvalidEndpoint(err.to_string()))
}

/// Chat payload per binding spec §3: `request_id` is bridge-generated and
/// mandatory — it is the only handle for cancellation. `stream: true` is
/// force-set by the SDK, but we set it anyway for clarity.
pub fn chat_payload(
    message: &str,
    session_id: &str,
    request_id: &str,
    agent_id: Option<&str>,
) -> Value {
    let mut payload = json!({
        "message": message,
        "session_id": session_id,
        "request_id": request_id,
        "stream": true,
    });
    if let Some(agent_id) = agent_id.filter(|id| !id.is_empty()) {
        payload["agent_id"] = json!(agent_id);
    }
    payload
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_shape() {
        let payload = chat_payload("hi", "sess_1", "req_1", None);
        assert_eq!(payload["message"], "hi");
        assert_eq!(payload["session_id"], "sess_1");
        assert_eq!(payload["request_id"], "req_1");
        assert_eq!(payload["stream"], true);
        assert!(payload.get("agent_id").is_none());

        let pinned = chat_payload("hi", "sess_1", "req_1", Some("scout"));
        assert_eq!(pinned["agent_id"], "scout");

        let empty = chat_payload("hi", "sess_1", "req_1", Some(""));
        assert!(empty.get("agent_id").is_none());
    }
}
