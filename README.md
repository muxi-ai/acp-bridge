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

## Logging & redaction

At the default log levels (`info`/`warn`/`error`), stderr never carries
conversation content or credentials — only session/request ids, stable
diagnostic codes, and byte lengths:

- Prompt and response text are never logged (prompts log `bytes = <len>`).
- Upstream error messages are content-bearing (formations echo model/tool
  text into them): WARN logs the code and message length; the full text goes
  to the host in the JSON-RPC error, and to stderr only at `debug`.
- Transport failures log the fact at WARN and the error detail at `debug`
  (SDK errors can quote raw SSE frame data).
- Buzz parse-miss diagnostics describe the failure structurally and never
  quote prompt lines.
- Resolved user ids are sent upstream as headers, not logged; client keys are
  never logged (`doctor` prints only the reference *scheme*).
- The ACP SDK (`agent_client_protocol`) logs full JSON-RPC error objects at
  WARN, so the bridge caps that crate at `error` unless you name it in
  `RUST_LOG` yourself (e.g. `RUST_LOG=info,agent_client_protocol=debug`).

The e2e suite enforces this: a run at `RUST_LOG=info` plants canary strings
in the prompt, the response, and an upstream error message, then asserts none
of them (nor the client key) appear on stderr.

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

# Secret *reference*, never a literal. Schemes:
#   env:NAME                        environment variable
#   file:/path                      file contents (trimmed)
#   keychain:<service>/<account>    OS keychain (macOS Keychain, Windows
#                                   Credential Manager, Linux Secret Service);
#                                   split on the FIRST slash — the account may
#                                   itself contain slashes
client_key = "env:MUXI_CLIENT_KEY"

# Optional: pin a specific agent; empty/absent lets the overlord route.
agent = ""

# Forward `thinking` events as agent_thought_chunk (off by default).
forward_thoughts = false

# Reliability posture (humantime strings; defaults shown).
turn_timeout = "30m"   # wall-clock cap per prompt turn
idle_timeout = "10m"   # cap on silence between SSE frames

# Plaintext endpoints (http:// or ws://) are rejected at startup unless the
# host is loopback AND this is set. Off-box traffic is always TLS.
allow_insecure_localhost = false

[profiles.production.limits]
max_sessions         = 8        # session/new beyond this -> BRIDGE_SESSION_LIMIT
max_concurrent_turns = 4        # prompts beyond this -> BRIDGE_TURN_LIMIT (never queued)
max_buffered_bytes   = 1048576  # per-turn cap on updates queued but unwritten

[profiles.production.identity]
# Tier-2 host identity extraction: "buzz" parses the Buzz events block out
# of each turn's prompt text; "none" (default) disables extraction.
host = "none"
# Buzz only: what the extracted id identifies.
#   "channel" (default) -> buzz:channel:<uuid>  (memory follows the conversation)
#   "sender"            -> buzz:pubkey:<hex>    (memory follows the person;
#                          batches with mixed senders attribute to the LAST
#                          event's sender and log a stderr diagnostic)
host_unit = "channel"
# Namespaces extracted ids so hosts can never collide.
id_prefix = "buzz"
# Fallback when no --user-id flag is given and extraction is unavailable or
# fails; if everything is unset, each session gets a synthetic partition id
# `acp:<session_id>`.
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
stream that dies mid-turn, a hanging stream interrupted by
`session/cancel`, idle/turn timeouts, a firehose that overflows the
northbound buffer cap, stdin EOF cancelling an in-flight turn, a
Buzz-shaped prompt whose extracted channel identity must reach the server
as `X-Muxi-User-ID`, and a redaction sweep asserting that canary strings in
the prompt/response/error text never reach stderr at `RUST_LOG=info`.

## CLI

Running with no subcommand starts ACP/connect mode on stdio.

| Flag | Meaning |
|---|---|
| `--config <path>` | Config file (also `MUXI_ACP_CONFIG`) |
| `--profile <name>` | Profile to use (also `MUXI_ACP_PROFILE`); defaults to `default_profile` |
| `--user-id <id>` | Pin every session to this MUXI memory partition |
| `--forward-thoughts` | Forward `thinking` events as `agent_thought_chunk` |

### `doctor`

```sh
muxi-acp doctor [--profile NAME] [--json]
```

Validates production dependencies without creating a billable model turn.
Each check reports PASS/WARN/FAIL/SKIP with a one-line detail, and the run
continues past failures:

| Check | What it proves |
|---|---|
| `config` | Profile loads; client key *reference* resolves (only the `env:`/`file:`/`keychain:` scheme is ever printed) |
| `tls-policy` | Endpoint scheme passes the same TLS enforcement as connect |
| `dns` | Endpoint host resolves |
| `tcp+tls` | TCP connect + an HTTP(S) round-trip; for `https://` a completed round-trip is the TLS evidence |
| `auth` | `GET /v1/sessions` with the client key — 2xx = accepted, 401 = bad credentials |
| `streaming` | Transport only: the SSE endpoint shares the verified origin. Stream *mechanics* are only exercised by a real (billable) turn, which doctor never starts |
| `cancellation` | `DELETE /v1/requests/<nonexistent>` — 404 = route present (expected); 400/405 = older runtime (WARN, degraded) |
| `identity` | Which identity tier is active: flag / profile default / per-session (informational) |

Exit 0 when nothing failed (warnings allowed — the summary notes them),
1 when any check failed. `--json` emits a machine-readable array of
`{check, status, detail}` on stdout. Doctor is not ACP mode, so its report
goes to stdout; the JSON-RPC-only rule applies to connect mode.

Identity precedence: `--user-id` → host extraction (`identity.host`, per
turn from the prompt text) → `identity.default_user_id` → per-session
synthetic `acp:<session_id>`.

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
| Turn / idle timeouts | ✅ `turn_timeout` (30m) bounds the whole turn, `idle_timeout` (10m) bounds silence between SSE frames; expiry cancels upstream and fails the turn (`BRIDGE_TURN_TIMEOUT` / `BRIDGE_IDLE_TIMEOUT`) |
| Bounded northbound buffering | ✅ per-turn `limits.max_buffered_bytes` (1 MiB) on updates queued but not yet written to stdout; overflow cancels upstream and fails the turn (`BRIDGE_BUFFER_OVERFLOW`) — updates are never silently dropped |
| Concurrency caps | ✅ `limits.max_sessions` (8) and `limits.max_concurrent_turns` (4); over-cap requests are rejected (`BRIDGE_SESSION_LIMIT` / `BRIDGE_TURN_LIMIT`), never queued — the ACP host owns queuing |
| TLS enforcement | ✅ plaintext (`http://` / `ws://`) endpoints rejected at startup unless loopback + `allow_insecure_localhost = true`; error names the offending profile key |
| Graceful shutdown | ✅ stdin EOF or SIGTERM/SIGINT: stop accepting requests, cancel all active MUXI turns (bounded 5s window), flush stdout, exit 0 |
| Session persistence | ❌ in-memory only; dies with the process. `session/resume` still works across restarts because the bridge owns the id space, but MUXI-side context depends on the formation's buffer. |
| Buzz identity extraction | ✅ prompt-text parser (`src/buzz.rs`): `channel` (default) / `sender` units, strict validation, soft-fail to `default_user_id` / per-session id, multi-sender + header-count diagnostics. `_meta`-based extraction stays pending the upstream `buzz-acp` proposal |
| `keychain:` secret references | ✅ `keychain:<service>/<account>` via the `keyring` crate: macOS Keychain, Windows Credential Manager, Linux Secret Service (pure-Rust zbus D-Bus client — no OpenSSL, needs GNOME Keyring / KWallet running). Errors distinguish "entry not found" from "access denied / store unavailable" and never echo the value |
| `session/load` (history replay) | ❌ deliberately not advertised (MUXI's history endpoint can't honor it honestly) |
| UI widgets → `elicitation` | ❌ v2 idea; the text stream always carries the fallback |
| Static builds / rustls | ❌ `muxi-rust`'s reqwest default features pull native-tls (OpenSSL/Security.framework), which blocks a fully static binary; the planned fix is a small SDK PR switching to `rustls-tls` |

## Development

```sh
cargo fmt && cargo clippy --all-targets -- -D warnings && cargo test
```

Unit tests include property tests (`proptest`, 256–512 cases per property)
over the SSE translator, the Buzz extractor, and the stdout-writer's JSON-RPC
line parser: arbitrary/truncated input never panics, never fabricates an
error event, and never yields a malformed identity.

The keychain tests that touch the real OS keychain are gated off in CI
(headless runners have no keychain/Secret Service). Run them locally with:

```sh
MUXI_ACP_KEYCHAIN_TESTS=1 cargo test keychain_live
```

## License

Apache 2.0 — matching the MUXI SDKs and tooling (the runtime and server are ELv2).
