//! TOML profile configuration (binding spec §2).
//!
//! Server information lives in the file; secrets live as *references*
//! (`env:` / `file:` / `keychain:`), never as literal values.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Deserializer};

#[derive(Debug, Deserialize)]
pub struct ConfigFile {
    pub default_profile: Option<String>,
    #[serde(default)]
    pub profiles: HashMap<String, Profile>,
}

#[derive(Debug, Clone, Deserialize)]
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
    /// Wall-clock cap for a whole prompt turn (spec §2). Humantime string.
    #[serde(default = "default_turn_timeout", deserialize_with = "de_duration")]
    pub turn_timeout: Duration,
    /// Cap on time between SSE frames (PRD §15.3). The SDK's SSE parser eats
    /// keepalive comment lines before they reach the bridge, so "idle" is
    /// simply time since the last surfaced event. Humantime string.
    #[serde(default = "default_idle_timeout", deserialize_with = "de_duration")]
    pub idle_timeout: Duration,
    /// Permit `http://`/`ws://` endpoints when the host is loopback. Off by
    /// default: plaintext to anything non-loopback is always rejected.
    #[serde(default)]
    pub allow_insecure_localhost: bool,
    #[serde(default)]
    pub identity: Identity,
    #[serde(default)]
    pub limits: Limits,
}

impl Default for Profile {
    /// Mirrors the serde defaults so hand-built profiles (tests) behave like
    /// an empty TOML table.
    fn default() -> Self {
        Self {
            server_url: None,
            formation: None,
            base_url: None,
            client_key: None,
            agent: None,
            forward_thoughts: false,
            turn_timeout: default_turn_timeout(),
            idle_timeout: default_idle_timeout(),
            allow_insecure_localhost: false,
            identity: Identity::default(),
            limits: Limits::default(),
        }
    }
}

/// Concurrency and buffering caps (spec §2 `[profiles.X.limits]`).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Limits {
    /// Maximum live ACP sessions.
    pub max_sessions: usize,
    /// Maximum prompt turns in flight across all sessions.
    pub max_concurrent_turns: usize,
    /// Per-turn cap on `session/update` bytes queued northbound but not yet
    /// written to stdout (PRD §15.3 backpressure).
    pub max_buffered_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_sessions: 8,
            max_concurrent_turns: 4,
            max_buffered_bytes: 1024 * 1024,
        }
    }
}

fn default_turn_timeout() -> Duration {
    Duration::from_secs(30 * 60)
}

fn default_idle_timeout() -> Duration {
    Duration::from_secs(10 * 60)
}

fn de_duration<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    let raw = String::deserialize(deserializer)?;
    humantime::parse_duration(&raw).map_err(serde::de::Error::custom)
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
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default();
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
    let raw =
        std::fs::read_to_string(path).map_err(|err| ConfigError::Io(path.to_path_buf(), err))?;
    toml::from_str(&raw).map_err(|err| ConfigError::Parse(path.to_path_buf(), err))
}

/// Select a profile: explicit name > `default_profile` > sole profile.
/// Returns the resolved name too, so diagnostics can cite the config key.
pub fn select_profile(
    config: &ConfigFile,
    name: Option<&str>,
) -> Result<(String, Profile), ConfigError> {
    let name = match name {
        Some(name) => name.to_string(),
        None => match &config.default_profile {
            Some(default) => default.clone(),
            None if config.profiles.len() == 1 => config.profiles.keys().next().unwrap().clone(),
            None => return Err(ConfigError::NoProfiles),
        },
    };
    match config.profiles.get(&name) {
        Some(profile) => Ok((name, profile.clone())),
        None => Err(ConfigError::UnknownProfile(name)),
    }
}

/// Resolve a secret reference. Schemes: `env:` | `file:` | `keychain:` (stub).
/// Literal values are rejected — secrets never live in the config file.
pub fn resolve_secret(reference: &str) -> Result<String, ConfigError> {
    if let Some(var) = reference.strip_prefix("env:") {
        std::env::var(var)
            .map_err(|_| ConfigError::Secret(format!("environment variable '{var}' is not set")))
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

    /// TLS enforcement at config load: plaintext schemes (`http://`, `ws://`)
    /// are rejected unless the host is loopback AND the profile opted in via
    /// `allow_insecure_localhost = true`. The error names the offending
    /// profile key so the fix is obvious from the startup log.
    pub fn validate_transport_security(&self, profile_name: &str) -> Result<(), ConfigError> {
        for (key, url) in [
            ("base_url", &self.base_url),
            ("server_url", &self.server_url),
        ] {
            let Some(url) = url else { continue };
            let scheme_is_plaintext = ["http://", "ws://"]
                .iter()
                .any(|scheme| url.starts_with(scheme));
            if !scheme_is_plaintext {
                continue;
            }
            if !is_loopback_host(url) {
                return Err(ConfigError::InvalidEndpoint(format!(
                    "profiles.{profile_name}.{key} = \"{url}\" uses a plaintext scheme to a \
                     non-loopback host; use https:// (TLS is required off-box)"
                )));
            }
            if !self.allow_insecure_localhost {
                return Err(ConfigError::InvalidEndpoint(format!(
                    "profiles.{profile_name}.{key} = \"{url}\" is plaintext; loopback is only \
                     allowed with profiles.{profile_name}.allow_insecure_localhost = true"
                )));
            }
        }
        Ok(())
    }

    pub fn client_key_reference(&self) -> Result<&str, ConfigError> {
        self.client_key
            .as_deref()
            .filter(|reference| !reference.is_empty())
            .ok_or(ConfigError::MissingClientKey)
    }
}

/// True when the URL's authority is a loopback host: `127.0.0.1`, `localhost`,
/// or `::1` (bracketed or bare), with or without a port.
fn is_loopback_host(url: &str) -> bool {
    let Some(rest) = url.split_once("://").map(|(_, rest)| rest) else {
        return false;
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host = if let Some(bracketed) = authority.strip_prefix('[') {
        // IPv6 literal: `[::1]` or `[::1]:8080`.
        bracketed.split(']').next().unwrap_or("")
    } else {
        authority
            .rsplit_once(':')
            .map_or(authority, |(host, port)| {
                // Only treat the suffix as a port if it is numeric; a bare IPv6
                // host without brackets keeps its full form (and won't match).
                if port.chars().all(|c| c.is_ascii_digit()) {
                    host
                } else {
                    authority
                }
            })
    };
    matches!(host, "127.0.0.1" | "localhost" | "::1")
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
        let (name, profile) = select_profile(&config, None).unwrap();
        assert_eq!(name, "prod");
        assert!(profile.server_url.is_some());
        let (_, direct) = select_profile(&config, Some("direct")).unwrap();
        assert!(direct.forward_thoughts);
        assert_eq!(
            direct.identity.default_user_id.as_deref(),
            Some("shared-brain")
        );
        assert!(matches!(
            select_profile(&config, Some("nope")),
            Err(ConfigError::UnknownProfile(_))
        ));
    }

    #[test]
    fn endpoint_validation() {
        let config = sample();
        select_profile(&config, Some("prod"))
            .unwrap()
            .1
            .validate_endpoint()
            .unwrap();
        select_profile(&config, Some("direct"))
            .unwrap()
            .1
            .validate_endpoint()
            .unwrap();

        let bad = Profile {
            server_url: Some("https://x".into()),
            ..Profile::default()
        };
        assert!(bad.validate_endpoint().is_err());
    }

    #[test]
    fn timeouts_default_and_parse_humantime() {
        let config = sample();
        let (_, prod) = select_profile(&config, Some("prod")).unwrap();
        assert_eq!(prod.turn_timeout, Duration::from_secs(30 * 60));
        assert_eq!(prod.idle_timeout, Duration::from_secs(10 * 60));

        let parsed: ConfigFile = toml::from_str(
            r#"
            [profiles.p]
            base_url = "https://x/v1"
            client_key = "env:K"
            turn_timeout = "90s"
            idle_timeout = "250ms"
            "#,
        )
        .unwrap();
        let (_, profile) = select_profile(&parsed, Some("p")).unwrap();
        assert_eq!(profile.turn_timeout, Duration::from_secs(90));
        assert_eq!(profile.idle_timeout, Duration::from_millis(250));
    }

    #[test]
    fn invalid_timeout_string_is_a_parse_error() {
        let result = toml::from_str::<ConfigFile>(
            r#"
            [profiles.p]
            base_url = "https://x/v1"
            client_key = "env:K"
            turn_timeout = "not-a-duration"
            "#,
        );
        assert!(result.is_err());
    }

    #[test]
    fn limits_default_and_override() {
        let config = sample();
        let (_, prod) = select_profile(&config, Some("prod")).unwrap();
        assert_eq!(prod.limits.max_sessions, 8);
        assert_eq!(prod.limits.max_concurrent_turns, 4);
        assert_eq!(prod.limits.max_buffered_bytes, 1024 * 1024);

        let parsed: ConfigFile = toml::from_str(
            r#"
            [profiles.p]
            base_url = "https://x/v1"
            client_key = "env:K"

            [profiles.p.limits]
            max_sessions = 2
            max_concurrent_turns = 1
            max_buffered_bytes = 4096
            "#,
        )
        .unwrap();
        let (_, profile) = select_profile(&parsed, Some("p")).unwrap();
        assert_eq!(profile.limits.max_sessions, 2);
        assert_eq!(profile.limits.max_concurrent_turns, 1);
        assert_eq!(profile.limits.max_buffered_bytes, 4096);
    }

    #[test]
    fn plaintext_to_non_loopback_is_rejected() {
        let profile = Profile {
            base_url: Some("http://hero.example.com/v1".into()),
            allow_insecure_localhost: true, // flag must NOT rescue off-box plaintext
            ..Profile::default()
        };
        let err = profile.validate_transport_security("prod").unwrap_err();
        let message = err.to_string();
        assert!(message.contains("profiles.prod.base_url"), "{message}");
        assert!(message.contains("https://"), "{message}");
    }

    #[test]
    fn plaintext_loopback_requires_the_flag() {
        for url in [
            "http://127.0.0.1:5050/v1",
            "http://localhost/v1",
            "http://[::1]:5050/v1",
            "ws://127.0.0.1:5050",
        ] {
            let bare = Profile {
                base_url: Some(url.into()),
                ..Profile::default()
            };
            let err = bare.validate_transport_security("dev").unwrap_err();
            assert!(
                err.to_string().contains("allow_insecure_localhost"),
                "{url}: {err}"
            );

            let flagged = Profile {
                base_url: Some(url.into()),
                allow_insecure_localhost: true,
                ..Profile::default()
            };
            flagged.validate_transport_security("dev").unwrap();
        }
    }

    #[test]
    fn https_needs_no_flag_and_server_url_is_checked_too() {
        let profile = Profile {
            server_url: Some("https://hero.example.com".into()),
            formation: Some("ops".into()),
            ..Profile::default()
        };
        profile.validate_transport_security("prod").unwrap();

        let plaintext_server = Profile {
            server_url: Some("http://hero.example.com".into()),
            formation: Some("ops".into()),
            ..Profile::default()
        };
        let err = plaintext_server
            .validate_transport_security("prod")
            .unwrap_err();
        assert!(err.to_string().contains("profiles.prod.server_url"));
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
