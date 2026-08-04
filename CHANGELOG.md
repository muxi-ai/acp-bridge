# Changelog

## [unreleased]

### Added

- **`doctor` subcommand** — validates production dependencies without a
  billable model turn: config, TLS policy, DNS, TCP+TLS, auth, streaming
  transport, cancellation endpoint, and active identity tier, each reported
  PASS/WARN/FAIL/SKIP with `--json` output and exit-code semantics.
- **Buzz identity extraction** — tier-2 host identity (`identity.host = "buzz"`):
  parses the channel UUID and sender pubkeys out of Buzz's rendered event
  blocks; `channel` (default) or `sender` units, strict validation, soft-fail
  to the config default, multi-sender and header-count diagnostics. Designed
  for replacement by `_meta.buzz` (upstream proposal: block/buzz#4745,
  working PR block/buzz#4751).
- **`keychain:` secret references** — `keychain:<service>/<account>` via the
  OS keychain on macOS/Windows/Linux (pure-Rust D-Bus client on Linux; no
  OpenSSL). Errors distinguish not-found from access-denied and never echo
  values.
- **Stream-discontinuity handling** — when the runtime flags
  `stream_discontinuity` on a terminal `completed` event (provider failed
  mid-stream and the fallback regenerated; runtime PR #317), the terminal
  text is treated as authoritative and emitted even though deltas were
  streamed, rendered as a fresh block.
- **Property tests** — proptest coverage over the SSE translator, the Buzz
  extractor, and JSON-RPC line handling: arbitrary/truncated input never
  panics, never fabricates errors, never yields a malformed identity.

### Changed

- **Log redaction hardened** — default levels (`info`/`warn`/`error`) never
  carry conversation content or credentials: prompt lines no longer quoted in
  parse diagnostics, upstream error text and raw SSE frame detail moved to
  `debug`, and the ACP SDK's own WARN logging (which echoes full JSON-RPC
  error objects) capped at `error` unless explicitly enabled via `RUST_LOG`.
  Enforced by a canary e2e test.
- Dependency bumps (Dependabot): `toml` 0.8 → 1.1, `rand` 0.8 → 0.10 (API
  migration included), `actions/checkout` v7, `upload-artifact` v7,
  `download-artifact` v8, `softprops/action-gh-release` v3. The release-only
  action bumps are first exercised by the next tagged release.

## [0.1.0-alpha] — 2026-08-04

Initial release. A standalone bridge presenting a remote MUXI formation as an
Agent Client Protocol agent — ACP over stdio northbound, the MUXI Rust SDK
(HTTPS + SSE) southbound.

- **ACP surface**: `initialize` (honest capability set — `loadSession: false`,
  text-only prompts, no auth methods), `session/new` (locally minted ids —
  MUXI has no conversation resource), `session/prompt` with streamed
  `session/update`s, `session/cancel` with race-safe exactly-one-terminal
  resolution, and local-registry `resume`/`list`/`close`/`delete`.
- **Event translation** verified against a live formation: `content` →
  `agent_message_chunk` (including the terminal-`completed`-carries-the-text
  case with dedup), `planning`/`replanning` → `plan`, `tool_call` →
  `tool_call`/`tool_call_update` by first sighting, `thinking` gated behind
  `forward_thoughts`, stable `BRIDGE_*` diagnostic codes for all failure
  modes.
- **Reliability posture**: prompts are never retried (a retry re-runs the
  whole turn server-side); turn (`30m`) and idle (`10m`) timeouts cancel
  upstream on expiry; per-turn northbound buffering is bounded (1 MiB) with
  cancel-on-overflow — never silent drops; session (8) and concurrent-turn
  (4) caps reject rather than queue; graceful shutdown cancels all active
  MUXI turns in a bounded window.
- **Security**: TLS required off-loopback (plaintext needs
  `allow_insecure_localhost`), stdout reserved for protocol frames at the
  byte level, secret references (`env:`/`file:`) instead of literals.
- **Identity**: 4-tier memory-partition resolution — `--user-id` flag →
  host extraction → `identity.default_user_id` → per-session synthetic id.
- **Config**: TOML profiles; CI on Ubuntu + macOS; release builds for
  macOS (arm64/x86_64), Linux (x86_64/arm64), and Windows with SHA256SUMS
  and an SPDX SBOM.

[0.1.0-alpha]: https://github.com/muxi-ai/acp-bridge/releases/tag/v0.1.0-alpha
