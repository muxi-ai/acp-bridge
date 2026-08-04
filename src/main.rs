//! muxi-acp: presents a remote MUXI formation as an ACP agent over stdio.
//!
//! Hard rule: stdout carries ACP JSON-RPC frames only. All logging goes to
//! stderr via `tracing`. (This rule applies to ACP/connect mode — the default
//! when no subcommand is given. Plain CLI subcommands such as `doctor` never
//! speak ACP and print their reports to stdout; see `doctor.rs`.)

mod agent;
mod buzz;
mod config;
mod doctor;
mod mux;
mod session;
mod translate;

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "muxi-acp",
    version,
    about = "ACP <-> MUXI bridge: speak Agent Client Protocol on stdio, chat with a remote MUXI formation"
)]
struct Cli {
    /// Path to the TOML config file (default: platform config dir).
    #[arg(long, env = "MUXI_ACP_CONFIG", global = true)]
    config: Option<PathBuf>,

    /// Named profile to use (default: `default_profile` from the config).
    #[arg(long, env = "MUXI_ACP_PROFILE", global = true)]
    profile: Option<String>,

    /// Pin every session to this MUXI user id (memory partition).
    /// Overrides the profile's `identity.default_user_id`.
    #[arg(long, global = true)]
    user_id: Option<String>,

    /// Forward `thinking` events as agent_thought_chunk (overrides config).
    #[arg(long)]
    forward_thoughts: bool,

    /// No subcommand = ACP/connect mode on stdio.
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Validate production dependencies (config, TLS policy, DNS, TCP+TLS,
    /// auth, streaming transport, cancellation, identity) without creating a
    /// billable model turn. Exit 0 when nothing failed (warnings allowed).
    Doctor {
        /// Emit a machine-readable JSON array instead of the human report.
        #[arg(long)]
        json: bool,
    },
}

/// Bound on the MUXI-side cancel sweep during shutdown (best-effort).
const SHUTDOWN_CANCEL_WINDOW: std::time::Duration = std::time::Duration::from_secs(5);

#[tokio::main]
async fn main() -> ExitCode {
    // stderr only — stdout is reserved for ACP protocol frames.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    let config_path = cli
        .config
        .clone()
        .unwrap_or_else(config::default_config_path);

    if let Some(Command::Doctor { json }) = &cli.command {
        return doctor::run(
            &config_path,
            cli.profile.as_deref(),
            cli.user_id.as_deref(),
            *json,
        )
        .await;
    }

    let state = match build_state(&cli, &config_path) {
        Ok(state) => state,
        Err(message) => {
            tracing::error!("{message}");
            return ExitCode::FAILURE;
        }
    };

    tracing::info!(config = %config_path.display(), "muxi-acp ready; speaking ACP on stdio");

    // Run the connection until stdin EOF (clean host disconnect) or a
    // termination signal. Either way: stop accepting requests, cancel every
    // active turn upstream, flush stdout, exit 0 (PRD §21 — never leave a
    // formation running a turn for a dead host).
    let exit = tokio::select! {
        result = agent::run(state.clone()) => match result {
            Ok(()) => {
                tracing::info!("host disconnected; shutting down");
                ExitCode::SUCCESS
            }
            Err(err) => {
                tracing::error!(error = ?err, "connection terminated with error");
                ExitCode::FAILURE
            }
        },
        () = termination_signal() => {
            tracing::info!("termination signal received; shutting down");
            ExitCode::SUCCESS
        }
    };

    shutdown(&state).await;
    exit
}

/// Resolves on SIGTERM or SIGINT.
async fn termination_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(term) => term,
            Err(err) => {
                tracing::error!(error = %err, "cannot install SIGTERM handler");
                std::future::pending::<()>().await;
                unreachable!()
            }
        };
        let mut int = match signal(SignalKind::interrupt()) {
            Ok(int) => int,
            Err(err) => {
                tracing::error!(error = %err, "cannot install SIGINT handler");
                std::future::pending::<()>().await;
                unreachable!()
            }
        };
        tokio::select! {
            _ = term.recv() => {}
            _ = int.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        if tokio::signal::ctrl_c().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

/// Cancel every still-active turn on the MUXI side (best-effort, bounded by
/// `SHUTDOWN_CANCEL_WINDOW`), then flush stdout.
async fn shutdown(state: &Arc<agent::BridgeState>) {
    let turns = state.sessions.drain_active_turns();
    if !turns.is_empty() {
        tracing::info!(
            turns = turns.len(),
            "cancelling active MUXI turns before exit"
        );
        let cancels = turns.into_iter().map(|(session_id, turn)| {
            let mux = state.mux.clone();
            let user_id = state.user_id_for(&session_id);
            async move {
                // Cancel returning an error is expected (the runtime's cancel
                // endpoint 400s on success, spec §6) — log and move on.
                if let Err(err) = mux.cancel_request(&turn.request_id, &user_id).await {
                    tracing::debug!(
                        session_id,
                        request_id = turn.request_id,
                        error = %err,
                        "shutdown cancel_request reported an error"
                    );
                }
            }
        });
        if tokio::time::timeout(SHUTDOWN_CANCEL_WINDOW, futures::future::join_all(cancels))
            .await
            .is_err()
        {
            tracing::warn!("shutdown cancel window elapsed; exiting anyway");
        }
    }

    // The ACP writer flushes per line; this covers the process-wide handle.
    use std::io::Write as _;
    let _ = std::io::stdout().flush();
}

fn build_state(cli: &Cli, config_path: &Path) -> Result<Arc<agent::BridgeState>, String> {
    let file = config::load(config_path).map_err(|err| err.to_string())?;
    let (profile_name, profile) =
        config::select_profile(&file, cli.profile.as_deref()).map_err(|err| err.to_string())?;
    profile.validate_endpoint().map_err(|err| err.to_string())?;
    profile
        .validate_transport_security(&profile_name)
        .map_err(|err| err.to_string())?;

    let key_reference = profile
        .client_key_reference()
        .map_err(|err| err.to_string())?;
    let client_key = config::resolve_secret(key_reference).map_err(|err| err.to_string())?;

    let mux = mux::client_from_profile(&profile, &client_key).map_err(|err| err.to_string())?;

    Ok(Arc::new(agent::BridgeState {
        sessions: session::SessionRegistry::new(profile.limits.clone()),
        mux,
        agent_id: profile.agent.clone().filter(|id| !id.is_empty()),
        cli_user_id: cli.user_id.clone(),
        default_user_id: profile.identity.default_user_id.clone(),
        host_extractor: buzz::host_extractor_from(&profile.identity),
        forward_thoughts: cli.forward_thoughts || profile.forward_thoughts,
        turn_timeout: profile.turn_timeout,
        idle_timeout: profile.idle_timeout,
    }))
}
