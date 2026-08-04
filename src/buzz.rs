//! Buzz identity extraction (binding spec §5.1/§5.3/§5.4) — NOT YET IMPLEMENTED.
//!
//! Tier-2 identity resolution parses the `[Buzz events — N events]` prompt
//! block for the channel uuid (`#<uuid>`) and sender pubkeys (`hex: <64-hex>`),
//! partitioning memory by `buzz:channel:<uuid>` (default) or
//! `buzz:pubkey:<hex>` (last-event rule under batching).
//!
//! TODO(spec §5.4): implement the parser — and, in parallel, propose the
//! upstream `_meta` change to `buzz-acp` so this module can be deleted before
//! it calcifies. Extraction must fail *soft*: on a parse miss, fall through to
//! tier 3 (`default_user_id`) and emit a stderr diagnostic; never guess.

/// Placeholder so the identity tiers are visible in code. Always `None` today,
/// which makes resolution fall through to `default_user_id` / per-session ids.
#[allow(dead_code)] // wired in when tier-2 host extraction lands
pub fn extract_user_id(_prompt_text: &str) -> Option<String> {
    None
}
