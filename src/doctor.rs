//! `muxi-acp doctor` — production dependency probe (PRD §12.3, §34.1).
//!
//! Each check reports independently (PASS/WARN/FAIL/SKIP plus a one-line
//! detail) and the run continues past failures. No check creates a billable
//! model turn:
//! - auth probes `GET /v1/sessions`, a cheap authenticated read (2xx = key
//!   accepted, 401 = bad credentials);
//! - cancellation probes `DELETE /v1/requests/<nonexistent id>` — a 404
//!   proves the route exists and responds, a 400/405 means an older runtime
//!   (pre cancel-route fix) and is reported as a degraded WARN;
//! - streaming is verified at the *transport* level only: the SSE endpoint
//!   shares the origin exercised by the auth check, and actually opening a
//!   stream (`POST /v1/chat` with `stream: true`) IS a billable turn, so the
//!   output says honestly that stream mechanics are only exercised by a real
//!   turn.
//!
//! stdout note: the bridge's hard rule ("stdout carries ACP JSON-RPC frames
//! only", see `main.rs`) applies to ACP/connect mode. `doctor` never speaks
//! ACP, so both the human report and `--json` write to stdout; incidental
//! logging still goes to stderr.
//!
//! Secrets are never echoed: the config check prints the reference *scheme*
//! (`env:` / `file:` / `keychain:`) only, and HTTP failures are reported as
//! bare status codes, never server response bodies (PRD §18.3).

use std::path::Path;
use std::process::ExitCode;
use std::time::Duration;

use muxi_rust::MuxiError;
use serde_json::{json, Value};

use crate::config::{self, Profile};
use crate::mux;

/// Bound on the raw TCP connect probe (the SDK's HTTP client applies its own
/// timeout to the HTTP round-trips).
const TCP_PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// Request id used for the cancellation probe. Never issued by a real turn
/// (bridge request ids are `req_<uuid>`), so a 404 is the expected answer.
const PROBE_REQUEST_ID: &str = "doctor-probe-nonexistent";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Pass,
    Warn,
    Fail,
    Skip,
}

impl Status {
    fn label(self) -> &'static str {
        match self {
            Self::Pass => "PASS",
            Self::Warn => "WARN",
            Self::Fail => "FAIL",
            Self::Skip => "SKIP",
        }
    }

    fn json(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Warn => "warn",
            Self::Fail => "fail",
            Self::Skip => "skip",
        }
    }
}

#[derive(Debug)]
pub struct CheckResult {
    pub check: &'static str,
    pub status: Status,
    pub detail: String,
}

impl CheckResult {
    fn new(check: &'static str, status: Status, detail: impl Into<String>) -> Self {
        Self {
            check,
            status,
            detail: detail.into(),
        }
    }
}

/// Exit code policy (§34.1): 0 when nothing failed (warnings allowed, the
/// summary line notes them), 1 when any check failed.
pub fn exit_code(results: &[CheckResult]) -> u8 {
    if results.iter().any(|r| r.status == Status::Fail) {
        1
    } else {
        0
    }
}

/// Human-readable report: one aligned line per check plus a summary.
pub fn format_human(results: &[CheckResult]) -> String {
    let width = results.iter().map(|r| r.check.len()).max().unwrap_or(0);
    let mut out = String::new();
    for result in results {
        out.push_str(&format!(
            "  {}  {:width$}  {}\n",
            result.status.label(),
            result.check,
            result.detail,
        ));
    }
    let count = |status: Status| results.iter().filter(|r| r.status == status).count();
    let (passed, warned, failed, skipped) = (
        count(Status::Pass),
        count(Status::Warn),
        count(Status::Fail),
        count(Status::Skip),
    );
    let verdict = if failed > 0 {
        "failed"
    } else if warned > 0 {
        "ok with warnings"
    } else {
        "ok"
    };
    let mut tally = format!("{passed} pass");
    for (n, label) in [(warned, "warn"), (failed, "fail"), (skipped, "skip")] {
        if n > 0 {
            tally.push_str(&format!(", {n} {label}"));
        }
    }
    out.push_str(&format!("doctor: {verdict} ({tally})\n"));
    out
}

/// Machine-readable report: a JSON array of `{check, status, detail}`.
pub fn format_json(results: &[CheckResult]) -> String {
    let array: Vec<Value> = results
        .iter()
        .map(|r| {
            json!({
                "check": r.check,
                "status": r.status.json(),
                "detail": r.detail,
            })
        })
        .collect();
    serde_json::to_string_pretty(&array).expect("static shape always serializes")
}

/// Endpoint origin for the network probes: (scheme, host, port).
/// Uses `base_url` when set, otherwise `server_url`.
fn endpoint_origin(profile: &Profile) -> Result<(String, String, u16), String> {
    let url = profile
        .base_url
        .as_deref()
        .or(profile.server_url.as_deref())
        .ok_or("profile has no base_url or server_url")?;
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| format!("endpoint '{url}' has no scheme"))?;
    let default_port = match scheme {
        "https" | "wss" => 443,
        "http" | "ws" => 80,
        other => return Err(format!("unsupported scheme '{other}'")),
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    if authority.is_empty() {
        return Err(format!("endpoint '{url}' has no host"));
    }
    let (host, port) = if let Some(bracketed) = authority.strip_prefix('[') {
        // IPv6 literal: `[::1]` or `[::1]:8080`.
        let (host, after) = bracketed
            .split_once(']')
            .ok_or_else(|| format!("endpoint '{url}' has an unterminated IPv6 literal"))?;
        let port = match after.strip_prefix(':') {
            Some(port) => port
                .parse()
                .map_err(|_| format!("endpoint '{url}' has an invalid port"))?,
            None => default_port,
        };
        (host.to_string(), port)
    } else {
        match authority.rsplit_once(':') {
            Some((host, port)) if port.chars().all(|c| c.is_ascii_digit()) && !port.is_empty() => (
                host.to_string(),
                port.parse()
                    .map_err(|_| format!("endpoint '{url}' has an invalid port"))?,
            ),
            _ => (authority.to_string(), default_port),
        }
    };
    Ok((scheme.to_string(), host, port))
}

/// HTTP status carried by a typed SDK error; `None` for transport-level
/// failures (connection refused, TLS handshake, timeouts, JSON).
fn http_status(err: &MuxiError) -> Option<u16> {
    match err {
        MuxiError::Authentication { status, .. }
        | MuxiError::Authorization { status, .. }
        | MuxiError::NotFound { status, .. }
        | MuxiError::Conflict { status, .. }
        | MuxiError::Validation { status, .. }
        | MuxiError::RateLimit { status, .. }
        | MuxiError::Server { status, .. }
        | MuxiError::Unknown { status, .. } => Some(*status),
        MuxiError::Connection(_) | MuxiError::Request(_) | MuxiError::Json(_) => None,
    }
}

/// Run every check in order, continuing past failures, then print the report
/// (human or `--json`) to stdout and return the exit code.
pub async fn run(
    config_path: &Path,
    profile_flag: Option<&str>,
    cli_user_id: Option<&str>,
    json_output: bool,
) -> ExitCode {
    let results = collect(config_path, profile_flag, cli_user_id).await;
    // doctor is not ACP mode: stdout is the report channel here (see the
    // module comment; the JSON-RPC-only rule applies to `connect`).
    if json_output {
        println!("{}", format_json(&results));
    } else {
        println!("{}", format_human(&results));
    }
    ExitCode::from(exit_code(&results))
}

async fn collect(
    config_path: &Path,
    profile_flag: Option<&str>,
    cli_user_id: Option<&str>,
) -> Vec<CheckResult> {
    let mut results = Vec::new();

    // -- 1. config: profile loads, endpoint shape ok, secret reference
    //    resolves (scheme only is ever printed) ------------------------------
    let loaded = config::load(config_path)
        .map_err(|err| err.to_string())
        .and_then(|file| config::select_profile(&file, profile_flag).map_err(|err| err.to_string()))
        .and_then(|(name, profile)| {
            profile
                .validate_endpoint()
                .map_err(|err| err.to_string())
                .map(|()| (name, profile))
        });
    let (profile_name, profile) = match loaded {
        Ok(loaded) => loaded,
        Err(detail) => {
            results.push(CheckResult::new("config", Status::Fail, detail));
            let reason = "config did not load";
            for check in [
                "tls-policy",
                "dns",
                "tcp+tls",
                "auth",
                "streaming",
                "cancellation",
                "identity",
            ] {
                results.push(CheckResult::new(check, Status::Skip, reason));
            }
            return results;
        }
    };

    let client_key = match profile.client_key_reference() {
        Ok(reference) => {
            let scheme = reference.split(':').next().unwrap_or("?").to_string();
            match config::resolve_secret(reference) {
                Ok(key) => {
                    results.push(CheckResult::new(
                        "config",
                        Status::Pass,
                        format!(
                            "profile '{profile_name}' loaded; client key resolves via \
                             '{scheme}:' reference"
                        ),
                    ));
                    Some(key)
                }
                Err(err) => {
                    // The resolver never includes the secret value in errors.
                    results.push(CheckResult::new(
                        "config",
                        Status::Fail,
                        format!(
                            "profile '{profile_name}' loaded but the '{scheme}:' client key \
                             reference does not resolve: {err}"
                        ),
                    ));
                    None
                }
            }
        }
        Err(err) => {
            results.push(CheckResult::new(
                "config",
                Status::Fail,
                format!("profile '{profile_name}' loaded but {err}"),
            ));
            None
        }
    };

    // -- 2. tls-policy: same enforcement as connect --------------------------
    match profile.validate_transport_security(&profile_name) {
        Ok(()) => {
            let plaintext = [profile.base_url.as_deref(), profile.server_url.as_deref()]
                .into_iter()
                .flatten()
                .any(|url| url.starts_with("http://") || url.starts_with("ws://"));
            let detail = if plaintext {
                "plaintext loopback endpoint permitted by allow_insecure_localhost (dev mode)"
            } else {
                "endpoint scheme passes connect-time TLS policy (https enforced off-box)"
            };
            results.push(CheckResult::new("tls-policy", Status::Pass, detail));
        }
        Err(err) => {
            results.push(CheckResult::new(
                "tls-policy",
                Status::Fail,
                err.to_string(),
            ));
        }
    }

    // -- 3. dns --------------------------------------------------------------
    let origin = endpoint_origin(&profile);
    let mut first_addr = None;
    match &origin {
        Ok((_, host, port)) => match tokio::net::lookup_host((host.as_str(), *port)).await {
            Ok(addrs) => {
                let addrs: Vec<_> = addrs.collect();
                if let Some(first) = addrs.first() {
                    first_addr = Some(*first);
                    results.push(CheckResult::new(
                        "dns",
                        Status::Pass,
                        format!(
                            "{host} resolved to {} address(es), first {first}",
                            addrs.len()
                        ),
                    ));
                } else {
                    results.push(CheckResult::new(
                        "dns",
                        Status::Fail,
                        format!("{host} resolved to no addresses"),
                    ));
                }
            }
            Err(err) => {
                results.push(CheckResult::new(
                    "dns",
                    Status::Fail,
                    format!("cannot resolve {host}: {err}"),
                ));
            }
        },
        Err(detail) => {
            results.push(CheckResult::new("dns", Status::Fail, detail.clone()));
        }
    }

    // The network/API probes share one SDK client. An unresolved client key
    // only degrades the auth/cancellation checks; transport probes proceed.
    let client = mux::client_from_profile(&profile, client_key.as_deref().unwrap_or(""))
        .map_err(|err| err.to_string());

    // -- 4. tcp+tls: raw TCP connect, then an HTTP(S) round-trip via the SDK.
    //    Any HTTP status (even an error status) proves TCP+TLS+HTTP; only a
    //    transport-level failure fails this check. reqwest exposes no TLS
    //    introspection, so a completed https round-trip is the evidence.
    let mut origin_http_ok = false;
    match (&origin, first_addr) {
        (Ok((scheme, _, _)), Some(addr)) => {
            let tcp = tokio::time::timeout(TCP_PROBE_TIMEOUT, tokio::net::TcpStream::connect(addr))
                .await
                .map_err(|_| "timed out".to_string())
                .and_then(|r| r.map_err(|err| err.to_string()));
            match tcp {
                Ok(_) => {
                    let round_trip = match &client {
                        Ok(client) => match client.health().await {
                            Ok(_) => Ok("HTTP 2xx".to_string()),
                            Err(err) => match http_status(&err) {
                                Some(status) => Ok(format!("HTTP {status}")),
                                None => Err(err.to_string()),
                            },
                        },
                        Err(err) => Err(format!("cannot build client: {err}")),
                    };
                    match round_trip {
                        Ok(status) => {
                            origin_http_ok = true;
                            let detail = if scheme == "https" || scheme == "wss" {
                                format!(
                                    "TCP connect to {addr} ok; TLS negotiated \
                                     (https round-trip returned {status})"
                                )
                            } else {
                                format!(
                                    "TCP connect to {addr} ok; plaintext HTTP round-trip \
                                     returned {status} (dev mode, no TLS)"
                                )
                            };
                            results.push(CheckResult::new("tcp+tls", Status::Pass, detail));
                        }
                        Err(err) => {
                            results.push(CheckResult::new(
                                "tcp+tls",
                                Status::Fail,
                                format!(
                                    "TCP connect to {addr} ok but HTTP(S) request failed: {err}"
                                ),
                            ));
                        }
                    }
                }
                Err(err) => {
                    results.push(CheckResult::new(
                        "tcp+tls",
                        Status::Fail,
                        format!("TCP connect to {addr} failed: {err}"),
                    ));
                }
            }
        }
        _ => {
            results.push(CheckResult::new(
                "tcp+tls",
                Status::Skip,
                "endpoint did not resolve; see dns",
            ));
        }
    }

    // Identity used for the authenticated probes mirrors connect-time
    // resolution with a synthetic per-session tail.
    let user_id = config::resolve_user_id(
        cli_user_id,
        profile.identity.default_user_id.as_deref(),
        "doctor",
    );

    // -- 5. auth: GET /v1/sessions — cheap authenticated read, no model turn.
    //    Failures report the bare status, never the response body (§18.3).
    match (&client, &client_key) {
        (Ok(client), Some(_)) => match client.get_sessions(&user_id, Some(1)).await {
            Ok(_) => {
                results.push(CheckResult::new(
                    "auth",
                    Status::Pass,
                    "GET /v1/sessions returned 2xx; client key accepted",
                ));
            }
            Err(err) => match http_status(&err) {
                Some(401) => {
                    results.push(CheckResult::new(
                        "auth",
                        Status::Fail,
                        "bad credentials: GET /v1/sessions returned 401",
                    ));
                }
                Some(status) => {
                    results.push(CheckResult::new(
                        "auth",
                        Status::Fail,
                        format!("GET /v1/sessions returned HTTP {status}"),
                    ));
                }
                None => {
                    results.push(CheckResult::new(
                        "auth",
                        Status::Fail,
                        format!("cannot reach endpoint: {err}"),
                    ));
                }
            },
        },
        (Err(err), _) => {
            results.push(CheckResult::new(
                "auth",
                Status::Fail,
                format!("cannot build client: {err}"),
            ));
        }
        (_, None) => {
            results.push(CheckResult::new(
                "auth",
                Status::Skip,
                "client key did not resolve; see config",
            ));
        }
    }

    // -- 6. streaming: transport only. Opening a real SSE stream (POST
    //    /v1/chat, stream: true) is a billable model turn, which doctor must
    //    never create — so this attests reachability of the shared origin and
    //    says so honestly.
    if origin_http_ok {
        results.push(CheckResult::new(
            "streaming",
            Status::Pass,
            "SSE endpoint shares the verified origin; stream mechanics are only \
             exercised by a real (billable) turn, which doctor never starts",
        ));
    } else {
        results.push(CheckResult::new(
            "streaming",
            Status::Skip,
            "origin unreachable; see tcp+tls",
        ));
    }

    // -- 7. cancellation: DELETE /v1/requests/<nonexistent>. 404 proves the
    //    route exists and responds; 400/405 means a runtime that predates the
    //    cancel-route fix (runtime PRs #314/#315) — degraded, not fatal.
    match (&client, &client_key) {
        (Ok(client), Some(_)) => match client.cancel_request(PROBE_REQUEST_ID, &user_id).await {
            Err(err) => match http_status(&err) {
                Some(404) => {
                    results.push(CheckResult::new(
                        "cancellation",
                        Status::Pass,
                        "cancellation endpoint present (404 for unknown id, as expected)",
                    ));
                }
                Some(status @ (400 | 405)) => {
                    results.push(CheckResult::new(
                        "cancellation",
                        Status::Warn,
                        format!(
                            "cancellation endpoint returned {status} for an unknown id — \
                             runtime predates the cancel-route fix (PR #314/#315); \
                             cancels still fire but degraded, upgrade recommended"
                        ),
                    ));
                }
                Some(status) => {
                    results.push(CheckResult::new(
                        "cancellation",
                        Status::Fail,
                        format!("DELETE /v1/requests/<unknown> returned HTTP {status}"),
                    ));
                }
                None => {
                    results.push(CheckResult::new(
                        "cancellation",
                        Status::Fail,
                        format!("cannot reach endpoint: {err}"),
                    ));
                }
            },
            Ok(_) => {
                results.push(CheckResult::new(
                    "cancellation",
                    Status::Fail,
                    "unexpected 2xx cancelling a nonexistent request id",
                ));
            }
        },
        (Err(err), _) => {
            results.push(CheckResult::new(
                "cancellation",
                Status::Fail,
                format!("cannot build client: {err}"),
            ));
        }
        (_, None) => {
            results.push(CheckResult::new(
                "cancellation",
                Status::Skip,
                "client key did not resolve; see config",
            ));
        }
    }

    // -- 8. identity: which tier is active (informational) -------------------
    let detail = if cli_user_id.is_some_and(|id| !id.is_empty()) {
        "tier: flag — --user-id pins every session's memory partition"
    } else if profile
        .identity
        .default_user_id
        .as_deref()
        .is_some_and(|id| !id.is_empty())
    {
        "tier: default — identity.default_user_id from the profile pins the partition"
    } else {
        "tier: per-session — synthetic 'acp:<session_id>' partition per session"
    };
    results.push(CheckResult::new("identity", Status::Pass, detail));

    results
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result(check: &'static str, status: Status) -> CheckResult {
        CheckResult::new(check, status, format!("{check} detail"))
    }

    #[test]
    fn exit_code_is_zero_unless_something_failed() {
        assert_eq!(exit_code(&[result("a", Status::Pass)]), 0);
        assert_eq!(
            exit_code(&[result("a", Status::Pass), result("b", Status::Warn)]),
            0,
            "warnings alone must not fail the run"
        );
        assert_eq!(
            exit_code(&[result("a", Status::Pass), result("b", Status::Skip)]),
            0,
            "skips alone must not fail the run"
        );
        assert_eq!(
            exit_code(&[result("a", Status::Pass), result("b", Status::Fail)]),
            1
        );
        assert_eq!(exit_code(&[]), 0);
    }

    #[test]
    fn human_format_has_one_line_per_check_and_a_summary() {
        let results = vec![
            result("config", Status::Pass),
            result("auth", Status::Fail),
            result("cancellation", Status::Warn),
        ];
        let report = format_human(&results);
        assert!(report.contains("PASS  config"), "{report}");
        assert!(report.contains("FAIL  auth"), "{report}");
        assert!(report.contains("WARN  cancellation"), "{report}");
        assert!(
            report.contains("doctor: failed (1 pass, 1 warn, 1 fail)"),
            "{report}"
        );
    }

    #[test]
    fn human_summary_notes_warnings_without_failing() {
        let report = format_human(&[result("a", Status::Pass), result("b", Status::Warn)]);
        assert!(
            report.contains("doctor: ok with warnings (1 pass, 1 warn)"),
            "{report}"
        );
        let clean = format_human(&[result("a", Status::Pass)]);
        assert!(clean.contains("doctor: ok (1 pass)"), "{clean}");
    }

    #[test]
    fn json_format_is_a_machine_readable_array() {
        let results = vec![result("config", Status::Pass), result("auth", Status::Fail)];
        let parsed: Value = serde_json::from_str(&format_json(&results)).unwrap();
        let array = parsed.as_array().unwrap();
        assert_eq!(array.len(), 2);
        assert_eq!(array[0]["check"], "config");
        assert_eq!(array[0]["status"], "pass");
        assert_eq!(array[0]["detail"], "config detail");
        assert_eq!(array[1]["status"], "fail");
    }

    #[test]
    fn endpoint_origin_parses_schemes_hosts_and_ports() {
        let profile = |url: &str| Profile {
            base_url: Some(url.to_string()),
            ..Profile::default()
        };
        assert_eq!(
            endpoint_origin(&profile("https://hero.example.com/v1")).unwrap(),
            ("https".into(), "hero.example.com".into(), 443)
        );
        assert_eq!(
            endpoint_origin(&profile("http://127.0.0.1:5050/v1")).unwrap(),
            ("http".into(), "127.0.0.1".into(), 5050)
        );
        assert_eq!(
            endpoint_origin(&profile("http://[::1]:5050/v1")).unwrap(),
            ("http".into(), "::1".into(), 5050)
        );
        assert_eq!(
            endpoint_origin(&profile("http://localhost/v1")).unwrap(),
            ("http".into(), "localhost".into(), 80)
        );
        assert!(endpoint_origin(&profile("hero.example.com")).is_err());
        assert!(endpoint_origin(&Profile::default()).is_err());
    }

    #[test]
    fn endpoint_origin_falls_back_to_server_url() {
        let profile = Profile {
            server_url: Some("https://hero.example.com".to_string()),
            formation: Some("ops".to_string()),
            ..Profile::default()
        };
        assert_eq!(
            endpoint_origin(&profile).unwrap(),
            ("https".into(), "hero.example.com".into(), 443)
        );
    }

    #[test]
    fn http_status_extraction_covers_typed_and_transport_errors() {
        let auth = MuxiError::Authentication {
            code: "UNAUTHORIZED".into(),
            message: "no".into(),
            status: 401,
        };
        assert_eq!(http_status(&auth), Some(401));
        let unknown = MuxiError::Unknown {
            code: "ERROR".into(),
            message: "odd".into(),
            status: 405,
        };
        assert_eq!(http_status(&unknown), Some(405));
        assert_eq!(http_status(&MuxiError::Connection("refused".into())), None);
    }
}
