//! muxi-acp: presents a remote MUXI formation as an ACP agent over stdio.
//!
//! Hard rule: stdout carries ACP JSON-RPC frames only. All logging goes to
//! stderr via `tracing`.

mod agent;
mod buzz;
mod config;
mod mux;
mod session;
mod translate;

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(
    name = "muxi-acp",
    version,
    about = "ACP <-> MUXI bridge: speak Agent Client Protocol on stdio, chat with a remote MUXI formation"
)]
struct Cli {
    /// Path to the TOML config file (default: platform config dir).
    #[arg(long, env = "MUXI_ACP_CONFIG")]
    config: Option<PathBuf>,

    /// Named profile to use (default: `default_profile` from the config).
    #[arg(long, env = "MUXI_ACP_PROFILE")]
    profile: Option<String>,

    /// Pin every session to this MUXI user id (memory partition).
    /// Overrides the profile's `identity.default_user_id`.
    #[arg(long)]
    user_id: Option<String>,

    /// Forward `thinking` events as agent_thought_chunk (overrides config).
    #[arg(long)]
    forward_thoughts: bool,
}

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

    let config_path = cli.config.clone().unwrap_or_else(config::default_config_path);
    let state = match build_state(&cli, &config_path) {
        Ok(state) => state,
        Err(message) => {
            tracing::error!("{message}");
            return ExitCode::FAILURE;
        }
    };

    tracing::info!(config = %config_path.display(), "muxi-acp ready; speaking ACP on stdio");

    match agent::run(state).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!(error = ?err, "connection terminated with error");
            ExitCode::FAILURE
        }
    }
}

fn build_state(cli: &Cli, config_path: &Path) -> Result<Arc<agent::BridgeState>, String> {
    let file = config::load(config_path).map_err(|err| err.to_string())?;
    let profile = config::select_profile(&file, cli.profile.as_deref())
        .map_err(|err| err.to_string())?;
    profile.validate_endpoint().map_err(|err| err.to_string())?;

    let key_reference = profile.client_key_reference().map_err(|err| err.to_string())?;
    let client_key = config::resolve_secret(key_reference).map_err(|err| err.to_string())?;

    let mux = mux::client_from_profile(&profile, &client_key).map_err(|err| err.to_string())?;

    Ok(Arc::new(agent::BridgeState {
        sessions: session::SessionRegistry::new(),
        mux,
        agent_id: profile.agent.clone().filter(|id| !id.is_empty()),
        cli_user_id: cli.user_id.clone(),
        default_user_id: profile.identity.default_user_id.clone(),
        forward_thoughts: cli.forward_thoughts || profile.forward_thoughts,
    }))
}
