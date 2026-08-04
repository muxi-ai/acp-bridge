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

/// Which ACP host's identity signal to extract (spec §5.2 tier 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IdentityHost {
    /// Parse Buzz's `[Buzz events …]` prompt block (see `buzz.rs`).
    Buzz,
    /// No host extraction: tiers 1/3/4 only.
    #[default]
    None,
}

/// What a Buzz-extracted id identifies (spec §5.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HostUnit {
    /// `<prefix>:channel:<uuid>` — memory follows the conversation. The
    /// recommended default: the only unit well-defined when a batch mixes
    /// senders, and in a DM the channel *is* the person.
    #[default]
    Channel,
    /// `<prefix>:pubkey:<hex>` — memory follows the person across channels.
    /// Under batching the turn is attributed to the LAST event's sender.
    Sender,
}

/// `[profiles.X.identity]` (spec §2/§5).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Identity {
    /// Enables tier-2 host extraction. `"none"` (default) disables it.
    pub host: IdentityHost,
    /// Buzz only: what the extracted id identifies.
    pub host_unit: HostUnit,
    /// Used when no `--user-id` flag is given and host extraction is
    /// unavailable or fails. Empty means unset.
    pub default_user_id: Option<String>,
    /// Namespaces extracted ids so hosts can never collide.
    pub id_prefix: String,
}

impl Default for Identity {
    fn default() -> Self {
        Self {
            host: IdentityHost::default(),
            host_unit: HostUnit::default(),
            default_user_id: None,
            id_prefix: "buzz".to_string(),
        }
    }
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

/// Resolve a secret reference. Schemes: `env:` | `file:` | `keychain:`.
/// Literal values are rejected — secrets never live in the config file.
pub fn resolve_secret(reference: &str) -> Result<String, ConfigError> {
    if let Some(var) = reference.strip_prefix("env:") {
        std::env::var(var)
            .map_err(|_| ConfigError::Secret(format!("environment variable '{var}' is not set")))
    } else if let Some(path) = reference.strip_prefix("file:") {
        std::fs::read_to_string(path)
            .map(|contents| contents.trim().to_string())
            .map_err(|err| ConfigError::Secret(format!("cannot read '{path}': {err}")))
    } else if let Some(rest) = reference.strip_prefix("keychain:") {
        let (service, account) = parse_keychain_reference(rest)?;
        keychain_lookup(service, account)
    } else {
        Err(ConfigError::Secret(
            "client_key must be a secret reference (env:NAME, file:/path, or \
             keychain:<service>/<account>), never a literal"
                .to_string(),
        ))
    }
}

/// Parse `keychain:<service>/<account>`: split on the FIRST slash, so the
/// account may itself contain slashes (`keychain:muxi-acp/prod/client-key`
/// is service `muxi-acp`, account `prod/client-key`).
fn parse_keychain_reference(rest: &str) -> Result<(&str, &str), ConfigError> {
    let Some((service, account)) = rest.split_once('/') else {
        return Err(ConfigError::Secret(format!(
            "keychain reference 'keychain:{rest}' is missing an account; the form is \
             'keychain:<service>/<account>'"
        )));
    };
    if service.is_empty() {
        return Err(ConfigError::Secret(
            "keychain reference has an empty service; the form is \
             'keychain:<service>/<account>'"
                .to_string(),
        ));
    }
    if account.is_empty() {
        return Err(ConfigError::Secret(
            "keychain reference has an empty account; the form is \
             'keychain:<service>/<account>'"
                .to_string(),
        ));
    }
    Ok((service, account))
}

/// Read one secret from the OS keychain (macOS Keychain, Windows Credential
/// Manager, Linux Secret Service via the pure-Rust zbus D-Bus client).
///
/// The lookup runs on a dedicated OS thread: the Linux store drives its own
/// D-Bus executor with blocking waits, and parking a fresh thread can never
/// collide with the bridge's tokio runtime. Startup-only cost, one join.
fn keychain_lookup(service: &str, account: &str) -> Result<String, ConfigError> {
    let service = service.to_string();
    let account = account.to_string();
    std::thread::spawn(move || {
        let entry = keyring::Entry::new(&service, &account)
            .map_err(|err| map_keyring_error(&service, &account, err))?;
        entry
            .get_password()
            .map_err(|err| map_keyring_error(&service, &account, err))
    })
    .join()
    .map_err(|_| ConfigError::Secret("keychain lookup thread panicked".to_string()))?
}

/// Map a `keyring` error onto an actionable message. Distinguishes "entry
/// not found" (fix: create it) from "keychain denied/unavailable" (fix:
/// unlock/grant/start the service). NEVER includes any retrieved value —
/// note that `BadEncoding` carries the raw secret bytes, so its payload (and
/// its `Display`, which is safe today but not contractually) must not be
/// echoed.
fn map_keyring_error(service: &str, account: &str, err: keyring::Error) -> ConfigError {
    use keyring::Error as KeyringError;
    ConfigError::Secret(match err {
        KeyringError::NoEntry => format!(
            "keychain entry not found for service '{service}', account '{account}'; create it \
             first (macOS: security add-generic-password -s '{service}' -a '{account}' -w)"
        ),
        KeyringError::Ambiguous(matches) => format!(
            "keychain holds {} entries matching service '{service}', account '{account}'; \
             delete the duplicates so exactly one remains",
            matches.len()
        ),
        KeyringError::NoStorageAccess(err) => format!(
            "keychain access denied for service '{service}', account '{account}': {err}; \
             unlock the keychain or grant this binary access, then retry"
        ),
        KeyringError::PlatformFailure(err) => format!(
            "keychain unavailable ({err}); on Linux a Secret Service provider (GNOME Keyring \
             or KWallet) must be running on the session D-Bus"
        ),
        KeyringError::NoDefaultStore => format!(
            "no OS keychain is available on this platform for service '{service}', account \
             '{account}'; use an env: or file: reference instead"
        ),
        KeyringError::BadEncoding(_) => format!(
            "keychain entry for service '{service}', account '{account}' is not valid UTF-8; \
             re-create it as a plain-text password"
        ),
        other => {
            format!("keychain lookup failed for service '{service}', account '{account}': {other}")
        }
    })
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

/// Identity resolution (binding spec §5.2):
/// `--user-id` flag > host extraction (tier 2, see `buzz.rs`) >
/// `identity.default_user_id` > per-session `acp:<session_id>`.
pub fn resolve_user_id(
    cli_user_id: Option<&str>,
    host_extracted: Option<&str>,
    default_user_id: Option<&str>,
    acp_session_id: &str,
) -> String {
    if let Some(user_id) = cli_user_id.filter(|id| !id.is_empty()) {
        return user_id.to_string();
    }
    if let Some(user_id) = host_extracted.filter(|id| !id.is_empty()) {
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
    fn literals_are_rejected() {
        let err = resolve_secret("literal-key-value").unwrap_err();
        assert!(err.to_string().contains("never a literal"), "{err}");
    }

    #[test]
    fn keychain_reference_parses_on_first_slash() {
        // Account may contain slashes: split on the FIRST one only.
        assert_eq!(
            parse_keychain_reference("muxi-acp/prod").unwrap(),
            ("muxi-acp", "prod")
        );
        assert_eq!(
            parse_keychain_reference("muxi-acp/prod/client-key").unwrap(),
            ("muxi-acp", "prod/client-key")
        );
    }

    #[test]
    fn keychain_reference_shape_errors_are_actionable() {
        for (reference, expected) in [
            ("no-slash-here", "missing an account"),
            ("/account-only", "empty service"),
            ("service-only/", "empty account"),
        ] {
            let err = parse_keychain_reference(reference).unwrap_err();
            let message = err.to_string();
            assert!(message.contains(expected), "{reference}: {message}");
            assert!(
                message.contains("keychain:<service>/<account>"),
                "{reference}: {message}"
            );
        }
    }

    #[test]
    fn keychain_error_mapping_distinguishes_not_found_from_access_denied() {
        // Not found: actionable "create it" guidance naming service+account.
        let not_found = map_keyring_error("muxi-acp", "prod", keyring::Error::NoEntry).to_string();
        assert!(not_found.contains("not found"), "{not_found}");
        assert!(not_found.contains("muxi-acp"), "{not_found}");
        assert!(not_found.contains("prod"), "{not_found}");
        assert!(not_found.contains("create it"), "{not_found}");

        // Access denied: "unlock/grant" guidance, distinct from not-found.
        let denied = map_keyring_error(
            "muxi-acp",
            "prod",
            keyring::Error::NoStorageAccess("keychain is locked".into()),
        )
        .to_string();
        assert!(denied.contains("access denied"), "{denied}");
        assert!(denied.contains("unlock"), "{denied}");
        assert!(!denied.contains("not found"), "{denied}");

        // Unavailable platform store: points at the Linux Secret Service.
        let unavailable = map_keyring_error(
            "muxi-acp",
            "prod",
            keyring::Error::PlatformFailure("dbus not running".into()),
        )
        .to_string();
        assert!(unavailable.contains("unavailable"), "{unavailable}");

        // A non-UTF-8 entry must never echo the raw bytes.
        let secret_bytes = b"\xffsuper-secret\xfe".to_vec();
        let bad = map_keyring_error(
            "muxi-acp",
            "prod",
            keyring::Error::BadEncoding(secret_bytes),
        )
        .to_string();
        assert!(bad.contains("not valid UTF-8"), "{bad}");
        assert!(!bad.contains("super-secret"), "value echoed: {bad}");
    }

    /// Live keychain roundtrip — touches the real OS keychain, so it is gated
    /// behind MUXI_ACP_KEYCHAIN_TESTS=1 and skipped in headless CI. Run it
    /// locally with:
    ///
    /// ```sh
    /// MUXI_ACP_KEYCHAIN_TESTS=1 cargo test keychain_live
    /// ```
    #[test]
    fn keychain_live_roundtrip() {
        if std::env::var("MUXI_ACP_KEYCHAIN_TESTS").as_deref() != Ok("1") {
            eprintln!("skipping keychain_live_roundtrip (set MUXI_ACP_KEYCHAIN_TESTS=1 to run)");
            return;
        }
        let service = "muxi-acp-test";
        let account = "live/roundtrip"; // slash in the account, on purpose
        let entry = keyring::Entry::new(service, account).unwrap();
        entry.set_password("live-test-value").unwrap();
        let resolved = resolve_secret(&format!("keychain:{service}/{account}"));
        entry.delete_credential().unwrap();
        assert_eq!(resolved.unwrap(), "live-test-value");

        // And a missing entry maps to the not-found error, not a panic.
        let missing = resolve_secret("keychain:muxi-acp-test/definitely-not-there").unwrap_err();
        assert!(missing.to_string().contains("not found"), "{missing}");
    }

    #[test]
    fn user_id_precedence() {
        // flag > host extraction > default_user_id > per-session synthetic
        assert_eq!(
            resolve_user_id(Some("ran"), Some("buzz:channel:x"), Some("dflt"), "sess_1"),
            "ran"
        );
        assert_eq!(
            resolve_user_id(None, Some("buzz:channel:x"), Some("dflt"), "sess_1"),
            "buzz:channel:x"
        );
        assert_eq!(resolve_user_id(None, None, Some("dflt"), "sess_1"), "dflt");
        assert_eq!(resolve_user_id(None, None, None, "sess_1"), "acp:sess_1");
        assert_eq!(
            resolve_user_id(Some(""), Some(""), Some(""), "sess_1"),
            "acp:sess_1"
        );
    }

    #[test]
    fn identity_section_defaults_and_parse() {
        let (_, prod) = select_profile(&sample(), Some("prod")).unwrap();
        assert_eq!(prod.identity.host, IdentityHost::None);
        assert_eq!(prod.identity.host_unit, HostUnit::Channel);
        assert_eq!(prod.identity.id_prefix, "buzz");

        let parsed: ConfigFile = toml::from_str(
            r#"
            [profiles.p]
            base_url = "https://x/v1"
            client_key = "env:K"

            [profiles.p.identity]
            host = "buzz"
            host_unit = "sender"
            id_prefix = "nostr"
            default_user_id = "shared"
            "#,
        )
        .unwrap();
        let (_, profile) = select_profile(&parsed, Some("p")).unwrap();
        assert_eq!(profile.identity.host, IdentityHost::Buzz);
        assert_eq!(profile.identity.host_unit, HostUnit::Sender);
        assert_eq!(profile.identity.id_prefix, "nostr");
        assert_eq!(profile.identity.default_user_id.as_deref(), Some("shared"));

        // Typos in the identity table are rejected, not silently ignored.
        assert!(toml::from_str::<ConfigFile>(
            r#"
            [profiles.p]
            base_url = "https://x/v1"
            client_key = "env:K"

            [profiles.p.identity]
            host_units = "sender"
            "#,
        )
        .is_err());
        // And so are unknown enum values.
        assert!(toml::from_str::<ConfigFile>(
            r#"
            [profiles.p]
            base_url = "https://x/v1"
            client_key = "env:K"

            [profiles.p.identity]
            host = "slack"
            "#,
        )
        .is_err());
    }
}
