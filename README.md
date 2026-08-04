# muxi-acp

[![CI](https://github.com/muxi-ai/acp-bridge/actions/workflows/ci.yml/badge.svg)](https://github.com/muxi-ai/acp-bridge/actions/workflows/ci.yml)

> **Status: WIP** — proof of concept. Nothing here is stable, supported, or
> safe to depend on yet.

A small standalone bridge that presents a remote [MUXI](https://muxi.ai)
formation as an [Agent Client Protocol](https://agentclientprotocol.com)
agent. It speaks ACP over stdio to a local host (an IDE, [Buzz](https://github.com/block/buzz),
or any ACP client), and talks to MUXI using the MUXI Rust SDK — nothing
runs locally except the translation.

```text
ACP host ── ACP/JSON-RPC over stdio ──> muxi-acp ── HTTPS + SSE ──> MUXI formation
```

Design notes live in the MUXI engineering docs (binding spec: SDK choice,
config format, ACP↔MUXI method/event mappings, identity resolution).

## The one hard rule

**stdout is protocol-only.** Every byte on stdout is an ACP JSON-RPC frame;
all logging goes to stderr (`tracing`, controlled by `RUST_LOG`). The
integration test enforces this at the byte level.

## Quickstart

### 1. Config

Create `~/Library/Application Support/muxi-acp/config.toml` (macOS) or
`$XDG_CONFIG_HOME/muxi-acp/config.toml` (Linux), or pass `--config <path>`:

```toml
default_profile = "production"

[profiles.production]
# Either server_url + formation (proxied via the MUXI Server), ...
server_url = "https://hero.example.com"
formation  = "operations-hero"
# ... or a direct formation runtime instead:
# base_url = "http://127.0.0.1:5050/v1"

# Secret *reference*, never a literal. Schemes: env: | file: | keychain: (stub)
client_key = "env:MUXI_CLIENT_KEY"

# Optional: pin a specific agent; empty/absent lets the overlord route.
agent = ""

# Forward `thinking` events as agent_thought_chunk (off by default).
forward_thoughts = false

[profiles.production.identity]
# Used when no --user-id flag is given; if both are unset, each session
# gets a synthetic partition id `acp:<session_id>`.
default_user_id = ""
```

### 2. Run against a real formation

```sh
export MUXI_CLIENT_KEY=...   # whatever your client_key reference points at
muxi-acp                     # speaks ACP on stdin/stdout
muxi-acp --profile staging --user-id ran   # pin the memory partition
```

Point any ACP host at the binary (e.g. a Zed `agent_servers` entry, or the
ACP conductor). Manual smoke test from a shell:

```sh
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}' \
  '{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp","mcpServers":[]}}' \
  | muxi-acp
```

Then send `session/prompt` with the returned `sessionId` on the same stdin.

### 3. Fake-server demo (no formation needed)

The integration test spins up a fake MUXI SSE server, spawns the binary,
and drives a full conversation — including cancellation — over ACP stdio:

```sh
cargo test --test e2e -- --nocapture
```

Fixtures cover: content deltas → ui → done, an upstream error event, a
stream that dies mid-turn, and a hanging stream interrupted by
`session/cancel`.

## CLI

| Flag | Meaning |
|---|---|
| `--config <path>` | Config file (also `MUXI_ACP_CONFIG`) |
| `--profile <name>` | Profile to use (also `MUXI_ACP_PROFILE`); defaults to `default_profile` |
| `--user-id <id>` | Pin every session to this MUXI memory partition |
| `--forward-thoughts` | Forward `thinking` events as `agent_thought_chunk` |

Identity precedence: `--user-id` → `identity.default_user_id` →
per-session synthetic `acp:<session_id>`.

## Status

| Area | State |
|---|---|
| `initialize` capability set | ✅ honest per spec: `loadSession: false`, text-only prompts, no auth methods, no MCP |
| `session/new` / `session/prompt` / streaming | ✅ SSE → `session/update`, exactly one terminal per turn |
| `session/cancel` | ✅ fires MUXI `cancel_request` + drops the stream; resolves `cancelled` unless a terminal won the race |
| `session/resume` / `list` / `close` / `delete` | ✅ local registry only (resume = cheap rebind, by design — MUXI has no conversation resource) |
| Event mapping | ✅ content / planning / replanning / tool_call (first-sighting) / completed / done; `thinking` gated behind `forward_thoughts`; `progress` and `ui` dropped in v1 |
| Stop reasons | ✅ `end_turn` / `cancelled` only; failures are JSON-RPC errors with stable `data.code` diagnostics — `MaxTokens`/`MaxTurnRequests`/`Refusal` are never emitted |
| Retries | ✅ never for prompts (deliberate: a retried prompt re-runs the whole turn server-side) |
| Session persistence | ❌ in-memory only; dies with the process. `session/resume` still works across restarts because the bridge owns the id space, but MUXI-side context depends on the formation's buffer. |
| Buzz identity extraction (`_meta` / prompt-text parsing) | ❌ stub only (`src/buzz.rs`); tiers 1/3/4 of identity resolution work today |
| `keychain:` secret references | ❌ stub — returns "not yet implemented"; use `env:` or `file:` |
| `session/load` (history replay) | ❌ deliberately not advertised (MUXI's history endpoint can't honor it honestly) |
| UI widgets → `elicitation` | ❌ v2 idea; the text stream always carries the fallback |
| Static builds / rustls | ❌ `muxi-rust`'s reqwest default features pull native-tls (OpenSSL/Security.framework), which blocks a fully static binary; the planned fix is a small SDK PR switching to `rustls-tls` |

## Development

```sh
cargo fmt && cargo clippy --all-targets -- -D warnings && cargo test
```

## License

MIT
