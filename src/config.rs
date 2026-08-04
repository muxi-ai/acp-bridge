//! TOML profile configuration (binding spec §2).
//!
//! Server information lives in the file; secrets live as *references*
//! (`env:` / `file:` / `keychain:`), never as literal values.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ConfigFile {
    pub default_profile: Option<String>,
    #[serde(default)]
    pub profiles: HashMap<String, Profile>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    /// MUXI Server URL (proxied via `/api/{formation_id}/*`); requires `formation`.
    pub server_url: Option<String>,
    /// Formation id when going through a MUXI Server.
    pub formation: Option<String>,
    /// Direct formation-runtime base URL (alternative to server_url+formation).
    pub base_url: Option<String>,
    /// Secret *reference* for the client key: `env:NAME`, `file:/path`, or `keychain:...`.
    pub client_key: Option<String>,
    /// Optional: pin a specific agent_id; empty/absent lets the overlord route.
    #[serde(default)]
    pub agent: Option<String>,
    /// Forward `thinking` events as `agent_thought_chunk`. Off by default (spec §7).
    #[serde(default)]
    pub forward_thoughts: bool,
    #[serde(default)]
    pub identity: Identity,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Identity {
    /// Used when no `--user-id` flag is given. Empty means unset.
    #[serde(default)]
    pub default_user_id: Option<String>,
}

#[derive(Debug)]
pub enum ConfigError {
    Io(PathBuf, std::io::Error),
    Parse(PathBuf, toml::de::Error),
    UnknownProfile(String),
    NoProfiles,
    MissingClientKey,
    InvalidEndpoint(String),
    Secret(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(path, err) => write!(f, "cannot read config {}: {err}", path.display()),
            Self::Parse(path, err) => write!(f, "cannot parse config {}: {err}", path.display()),
            Self::UnknownProfile(name) => write!(f, "profile '{name}' not found in config"),
            Self::NoProfiles => write!(f, "config defines no profiles"),
            Self::MissingClientKey => write!(f, "profile is missing 'client_key'"),
            Self::InvalidEndpoint(msg) => write!(f, "invalid endpoint config: {msg}"),
            Self::Secret(msg) => write!(f, "cannot resolve secret reference: {msg}"),
        }
    }
}

impl std::error::Error for ConfigError {}

/// Default config location: `$XDG_CONFIG_HOME/muxi-acp/config.toml`
/// (macOS: `~/Library/Application Support/muxi-acp/config.toml`).
pub fn default_config_path() -> PathBuf {
    let home = std::env::var_os("HOME").map(PathBuf::from).unwrap_or_default();
    let base = if cfg!(target_os = "macos") {
        home.join("Library/Application Support")
    } else if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME") {
        PathBuf::from(xdg)
    } else {
        home.join(".config")
    };
    base.join("muxi-acp/config.toml")
}

pub fn load(path: &Path) -> Result<ConfigFile, ConfigError> {
    let raw = std::fs::read_to_string(path)
        .map_err(|err| ConfigError::Io(path.to_path_buf(), err))?;
    toml::from_str(&raw).map_err(|err| ConfigError::Parse(path.to_path_buf(), err))
}

/// Select a profile: explicit name > `default_profile` > sole profile.
pub fn select_profile(config: &ConfigFile, name: Option<&str>) -> Result<Profile, ConfigError> {
    let name = match name {
        Some(name) => name.to_string(),
        None => match &config.default_profile {
            Some(default) => default.clone(),
            None if config.profiles.len() == 1 => config.profiles.keys().next().unwrap().clone(),
            None => return Err(ConfigError::NoProfiles),
        },
    };
    config
        .profiles
        .get(&name)
        .cloned()
        .ok_or(ConfigError::UnknownProfile(name))
}

/// Resolve a secret reference. Schemes: `env:` | `file:` | `keychain:` (stub).
/// Literal values are rejected — secrets never live in the config file.
pub fn resolve_secret(reference: &str) -> Result<String, ConfigError> {
    if let Some(var) = reference.strip_prefix("env:") {
        std::env::var(var).map_err(|_| {
            ConfigError::Secret(format!("environment variable '{var}' is not set"))
        })
    } else if let Some(path) = reference.strip_prefix("file:") {
        std::fs::read_to_string(path)
            .map(|contents| contents.trim().to_string())
            .map_err(|err| ConfigError::Secret(format!("cannot read '{path}': {err}")))
    } else if reference.starts_with("keychain:") {
        // TODO: OS keychain integration is a later build-order step (spec §9.6).
        Err(ConfigError::Secret(
            "keychain: references are not yet implemented".to_string(),
        ))
    } else {
        Err(ConfigError::Secret(
            "client_key must be a secret reference (env:NAME, file:/path, or keychain:...), never a literal".to_string(),
        ))
    }
}

impl Profile {
    /// Validate the endpoint shape: either base_url, or server_url+formation.
    pub fn validate_endpoint(&self) -> Result<(), ConfigError> {
        match (&self.base_url, &self.server_url, &self.formation) {
            (Some(_), None, None) => Ok(()),
            (None, Some(_), Some(_)) => Ok(()),
            (Some(_), _, _) => Err(ConfigError::InvalidEndpoint(
                "base_url is mutually exclusive with server_url/formation".to_string(),
            )),
            _ => Err(ConfigError::InvalidEndpoint(
                "set either base_url, or both server_url and formation".to_string(),
            )),
        }
    }

    pub fn client_key_reference(&self) -> Result<&str, ConfigError> {
        self.client_key
            .as_deref()
            .filter(|reference| !reference.is_empty())
            .ok_or(ConfigError::MissingClientKey)
    }
}

/// Identity resolution (binding spec §5.2, PoC subset):
/// `--user-id` flag > `identity.default_user_id` > per-session `acp:<session_id>`.
/// Host (Buzz) extraction is tier 2 and not implemented yet — see `buzz.rs`.
pub fn resolve_user_id(
    cli_user_id: Option<&str>,
    default_user_id: Option<&str>,
    acp_session_id: &str,
) -> String {
    if let Some(user_id) = cli_user_id.filter(|id| !id.is_empty()) {
        return user_id.to_string();
    }
    if let Some(user_id) = default_user_id.filter(|id| !id.is_empty()) {
        return user_id.to_string();
    }
    format!("acp:{acp_session_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ConfigFile {
        toml::from_str(
            r#"
            default_profile = "prod"

            [profiles.prod]
            server_url = "https://hero.example.com"
            formation  = "operations-hero"
            client_key = "env:MUXI_KEY"

            [profiles.direct]
            base_url = "http://127.0.0.1:5050/v1"
            client_key = "file:/tmp/key"
            forward_thoughts = true

            [profiles.direct.identity]
            default_user_id = "shared-brain"
            "#,
        )
        .unwrap()
    }

    #[test]
    fn selects_default_and_named_profiles() {
        let config = sample();
        assert!(select_profile(&config, None).unwrap().server_url.is_some());
        let direct = select_profile(&config, Some("direct")).unwrap();
        assert!(direct.forward_thoughts);
        assert_eq!(direct.identity.default_user_id.as_deref(), Some("shared-brain"));
        assert!(matches!(
            select_profile(&config, Some("nope")),
            Err(ConfigError::UnknownProfile(_))
        ));
    }

    #[test]
    fn endpoint_validation() {
        let config = sample();
        select_profile(&config, Some("prod")).unwrap().validate_endpoint().unwrap();
        select_profile(&config, Some("direct")).unwrap().validate_endpoint().unwrap();

        let bad = Profile {
            server_url: Some("https://x".into()),
            ..Profile::default()
        };
        assert!(bad.validate_endpoint().is_err());
    }

    #[test]
    fn env_secret_resolution() {
        std::env::set_var("MUXI_ACP_TEST_KEY", "sekrit");
        assert_eq!(resolve_secret("env:MUXI_ACP_TEST_KEY").unwrap(), "sekrit");
        assert!(resolve_secret("env:MUXI_ACP_TEST_MISSING").is_err());
    }

    #[test]
    fn file_secret_resolution_trims() {
        let dir = std::env::temp_dir().join("muxi-acp-test-secret");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("key");
        std::fs::write(&path, "sekrit\n").unwrap();
        let reference = format!("file:{}", path.display());
        assert_eq!(resolve_secret(&reference).unwrap(), "sekrit");
    }

    #[test]
    fn keychain_is_a_stub_and_literals_are_rejected() {
        assert!(resolve_secret("keychain:muxi-acp/prod").is_err());
        assert!(resolve_secret("literal-key-value").is_err());
    }

    #[test]
    fn user_id_precedence() {
        assert_eq!(resolve_user_id(Some("ran"), Some("dflt"), "sess_1"), "ran");
        assert_eq!(resolve_user_id(None, Some("dflt"), "sess_1"), "dflt");
        assert_eq!(resolve_user_id(None, None, "sess_1"), "acp:sess_1");
        assert_eq!(resolve_user_id(Some(""), Some(""), "sess_1"), "acp:sess_1");
    }
}
