//! Buzz identity extraction (binding spec §5.1/§5.3/§5.4).
//!
//! Tier-2 identity resolution parses the events block that Buzz's
//! `format_event_block` renders into the prompt text, and derives the MUXI
//! memory-partition id from it:
//!
//! - `host_unit = "channel"` (default): `<prefix>:channel:<uuid>` from the
//!   FIRST `Channel:` line. Memory follows the conversation — the only unit
//!   that stays well-defined when a batch mixes senders.
//! - `host_unit = "sender"`: `<prefix>:pubkey:<hex>` from the LAST event
//!   section's `From:` line. Buzz itself derives thread scope and the reply
//!   anchor from `batch.events.last()`, so the last-event rule follows Buzz's
//!   own convention. When a batch contains multiple distinct senders the turn
//!   is still attributed to the last, and the ambiguity is surfaced as a
//!   stderr diagnostic (spec §5.3).
//!
//! Extraction fails *soft* (spec §5.4): any parse miss returns `None` with a
//! single WARN diagnostic, and the caller falls through to
//! `identity.default_user_id`, then the per-session synthetic id. An id is
//! never fabricated and never built from a partially-matched line.
//!
//! ## Honest limitations
//!
//! This is a screen-scraper over prompt text — text that is model-visible and
//! **spoofable by quoted content**. The parser defends against inline tricks
//! (a `From:` label containing literal `hex: …` text cannot displace the real
//! capture, because captures are anchored to the *end* of structurally-valid
//! lines), and against injected lines inside message bodies by positional
//! rules: the real `Channel:` line precedes any `Content:` in its section, so
//! the FIRST `Channel:` line wins; the real `From:` line precedes content in
//! the last section, so the FIRST `From:` line of that section wins. But a
//! message whose content injects a full section separator plus forged header
//! lines is structurally indistinguishable from the real thing. The real fix
//! is upstream: `buzz-acp` populating ACP `_meta` on `session/prompt` with
//! the channel id and sender pubkeys — at which point a `BuzzMetaExtractor`
//! implementing [`HostIdentity`] replaces this parser without touching any
//! caller, and this module's parsing code is deleted.

use crate::config::{HostUnit, Identity, IdentityHost};

/// A host-extracted identity for one prompt turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractedIdentity {
    /// Memory-partition id, e.g. `buzz:channel:<uuid>` or `buzz:pubkey:<hex>`.
    pub user_id: String,
    /// Non-fatal stderr diagnostics for the caller to log (multi-sender
    /// ambiguity, header/section count mismatch, …).
    pub diagnostics: Vec<String>,
}

/// Tier-2 host identity extraction (spec §5.2). Implementations derive the
/// turn's `user_id` from the material a host provides — today the prompt text
/// ([`BuzzPromptExtractor`]), eventually ACP `_meta.buzz` (a future
/// `BuzzMetaExtractor`) — behind one interface so callers never change.
pub trait HostIdentity {
    /// `None` means extraction failed (or the material carries no identity):
    /// the caller falls through to `default_user_id`, then the per-session
    /// synthetic id. Implementations log their own WARN on a parse miss.
    fn extract(&self, prompt_text: &str) -> Option<ExtractedIdentity>;
}

/// Build the configured extractor, if any. Kept here so wiring stays a
/// one-liner in `main` and swapping in a `_meta`-based extractor later is a
/// change to this function alone.
pub fn host_extractor_from(identity: &Identity) -> Option<Box<dyn HostIdentity + Send + Sync>> {
    match identity.host {
        IdentityHost::None => None,
        IdentityHost::Buzz => Some(Box::new(BuzzPromptExtractor::new(
            identity.host_unit,
            identity.id_prefix.clone(),
        ))),
    }
}

/// Parses Buzz's `[Buzz events — N events]` / `[Buzz event: <tag>]` prompt
/// block (spec §5.1). See the module docs for rules and limitations.
pub struct BuzzPromptExtractor {
    host_unit: HostUnit,
    id_prefix: String,
}

impl BuzzPromptExtractor {
    pub fn new(host_unit: HostUnit, id_prefix: String) -> Self {
        Self {
            host_unit,
            id_prefix,
        }
    }

    fn try_extract(&self, prompt_text: &str) -> Result<ExtractedIdentity, String> {
        let lines: Vec<&str> = prompt_text.lines().collect();
        let marker = lines
            .iter()
            .position(|line| parse_multi_header(line).is_some() || is_single_header(line))
            .ok_or("no [Buzz events — N events] / [Buzz event: <tag>] block in prompt")?;

        let mut diagnostics = Vec::new();
        let sections: Vec<&[&str]> = if let Some(declared) = parse_multi_header(lines[marker]) {
            let body = &lines[marker + 1..];
            // Sections are delimited by `--- Event <n> (<tag>) ---` lines;
            // anything before the first separator is preamble and ignored.
            let mut bounds: Vec<usize> = body
                .iter()
                .enumerate()
                .filter_map(|(index, line)| is_section_separator(line).then_some(index))
                .collect();
            bounds.push(body.len());
            let sections: Vec<&[&str]> = bounds
                .windows(2)
                .map(|pair| &body[pair[0] + 1..pair[1]])
                .collect();
            if sections.len() != declared {
                // Sanity check, not a failure: the count is advisory and the
                // sections themselves are self-delimiting.
                diagnostics.push(format!(
                    "Buzz header declares {declared} events but {} sections were parsed; \
                     parsing anyway",
                    sections.len()
                ));
            }
            sections
        } else {
            // Single-event form: one section, no separator.
            vec![&lines[marker + 1..]]
        };

        if sections.is_empty() {
            return Err("Buzz events block contains no event sections".to_string());
        }

        let user_id = match self.host_unit {
            HostUnit::Channel => {
                // FIRST `Channel:` line in the block. Within a section the
                // real Channel line precedes any Content line, so an injected
                // lookalike inside a message body can never come first.
                let line = sections
                    .iter()
                    .flat_map(|section| section.iter())
                    .find(|line| line.starts_with("Channel: "))
                    .ok_or("no `Channel:` line in Buzz events block")?;
                let uuid = channel_uuid(line).ok_or_else(|| {
                    format!("first `Channel:` line has no valid `(#<uuid>)`: {line:?}")
                })?;
                format!("{}:channel:{uuid}", self.id_prefix)
            }
            HostUnit::Sender => {
                // One sender per section: the FIRST `From:` line (the real one
                // precedes any Content line). Attribution goes to the LAST
                // section's sender, mirroring Buzz's `batch.events.last()`.
                let senders: Vec<Option<&str>> = sections
                    .iter()
                    .map(|section| {
                        section
                            .iter()
                            .find(|line| line.starts_with("From: "))
                            .and_then(|line| from_hex(line))
                    })
                    .collect();
                let last = senders
                    .last()
                    .copied()
                    .flatten()
                    .ok_or("last event section has no valid `From: … hex: <64-hex>` line")?;
                let mut distinct: Vec<&str> = senders.iter().copied().flatten().collect();
                distinct.sort_unstable();
                distinct.dedup();
                if distinct.len() > 1 {
                    diagnostics.push(format!(
                        "Buzz batch contains {} distinct senders; attributing the turn to the \
                         last event's sender (spec §5.3)",
                        distinct.len()
                    ));
                }
                format!("{}:pubkey:{last}", self.id_prefix)
            }
        };

        Ok(ExtractedIdentity {
            user_id,
            diagnostics,
        })
    }
}

impl HostIdentity for BuzzPromptExtractor {
    fn extract(&self, prompt_text: &str) -> Option<ExtractedIdentity> {
        match self.try_extract(prompt_text) {
            Ok(identity) => Some(identity),
            Err(reason) => {
                tracing::warn!(
                    reason,
                    "Buzz identity extraction failed; falling through to default_user_id / \
                     per-session id"
                );
                None
            }
        }
    }
}

/// `[Buzz events — N events]` → `Some(N)`. The em dash and wording are what
/// Buzz's `format_prompt` emits; anything else is not a multi-event header.
fn parse_multi_header(line: &str) -> Option<usize> {
    let rest = line.trim().strip_prefix("[Buzz events — ")?;
    let rest = rest.strip_suffix(']')?;
    let count = rest
        .strip_suffix(" events")
        .or_else(|| rest.strip_suffix(" event"))?;
    count.parse().ok()
}

/// `[Buzz event: <tag>]` — the single-event form.
fn is_single_header(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed.starts_with("[Buzz event: ") && trimmed.ends_with(']')
}

/// `--- Event <n> (<tag>) ---`
fn is_section_separator(line: &str) -> bool {
    let Some(rest) = line.trim().strip_prefix("--- Event ") else {
        return false;
    };
    let Some(rest) = rest.strip_suffix(") ---") else {
        return false;
    };
    let Some((number, _tag)) = rest.split_once(" (") else {
        return false;
    };
    !number.is_empty() && number.bytes().all(|b| b.is_ascii_digit())
}

/// `Channel: <name> (#<uuid>)` → the uuid. Anchored on line structure: the
/// uuid is whatever follows the LAST `(#` and the line must close with `)`,
/// so a channel *name* containing parens (or a literal `(#…`) cannot displace
/// the real capture.
fn channel_uuid(line: &str) -> Option<&str> {
    let rest = line.strip_prefix("Channel: ")?;
    let rest = rest.strip_suffix(')')?;
    let (_, candidate) = rest.rsplit_once("(#")?;
    is_uuid(candidate).then_some(candidate)
}

/// `From: <label> (npub: <npub>, hex: <64-hex>)` — or, unlabeled,
/// `From: <npub> (hex: <64-hex>)` — → the hex pubkey. Anchored on line
/// structure: the pubkey is whatever follows the LAST `hex: ` and the line
/// must close with `)`, so a label containing literal `hex: …` text cannot
/// displace the real capture.
fn from_hex(line: &str) -> Option<&str> {
    let rest = line.strip_prefix("From: ")?;
    let rest = rest.strip_suffix(')')?;
    let (_, candidate) = rest.rsplit_once("hex: ")?;
    is_hex64(candidate).then_some(candidate)
}

/// Strict uuid shape: 36 chars, dashes at 8/13/18/23, lowercase hex elsewhere
/// (Buzz renders uuids lowercase; never partially match).
fn is_uuid(candidate: &str) -> bool {
    let bytes = candidate.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    bytes.iter().enumerate().all(|(index, byte)| match index {
        8 | 13 | 18 | 23 => *byte == b'-',
        _ => matches!(byte, b'0'..=b'9' | b'a'..=b'f'),
    })
}

/// Exactly 64 lowercase hex chars — a Nostr pubkey as Buzz renders it.
fn is_hex64(candidate: &str) -> bool {
    candidate.len() == 64
        && candidate
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

#[cfg(test)]
mod tests {
    use super::*;

    const UUID: &str = "0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9";
    const ALICE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const BOB: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn channel_extractor() -> BuzzPromptExtractor {
        BuzzPromptExtractor::new(HostUnit::Channel, "buzz".to_string())
    }

    fn sender_extractor() -> BuzzPromptExtractor {
        BuzzPromptExtractor::new(HostUnit::Sender, "buzz".to_string())
    }

    fn multi_event_prompt() -> String {
        format!(
            "Context: you are on-call support.\n\
             \n\
             [Buzz events — 2 events]\n\
             \n\
             --- Event 1 (mention) ---\n\
             Event ID: deadbeef\n\
             Channel: eng-oncall (#{UUID})\n\
             Kind: 9\n\
             From: alice (npub: npub1alice, hex: {ALICE})\n\
             Time: 2026-08-04T10:00:00Z\n\
             Content: the deploy is stuck\n\
             Tags: []\n\
             \n\
             --- Event 2 (message) ---\n\
             Event ID: cafebabe\n\
             Channel: eng-oncall (#{UUID})\n\
             Kind: 9\n\
             From: bob (npub: npub1bob, hex: {BOB})\n\
             Time: 2026-08-04T10:00:05Z\n\
             Content: same here\n\
             Tags: []\n"
        )
    }

    #[test]
    fn multi_event_channel_mode_takes_first_channel() {
        let identity = channel_extractor().extract(&multi_event_prompt()).unwrap();
        assert_eq!(identity.user_id, format!("buzz:channel:{UUID}"));
        assert!(
            identity.diagnostics.is_empty(),
            "{:?}",
            identity.diagnostics
        );
    }

    #[test]
    fn multi_event_sender_mode_takes_last_sender_and_flags_ambiguity() {
        let identity = sender_extractor().extract(&multi_event_prompt()).unwrap();
        assert_eq!(identity.user_id, format!("buzz:pubkey:{BOB}"));
        assert_eq!(identity.diagnostics.len(), 1);
        assert!(
            identity.diagnostics[0].contains("2 distinct senders"),
            "{:?}",
            identity.diagnostics
        );
    }

    #[test]
    fn single_distinct_sender_has_no_ambiguity_diagnostic() {
        let prompt = multi_event_prompt().replace(BOB, ALICE);
        let identity = sender_extractor().extract(&prompt).unwrap();
        assert_eq!(identity.user_id, format!("buzz:pubkey:{ALICE}"));
        assert!(
            identity.diagnostics.is_empty(),
            "{:?}",
            identity.diagnostics
        );
    }

    #[test]
    fn single_event_form() {
        let prompt = format!(
            "[Buzz event: mention]\n\
             Event ID: deadbeef\n\
             Channel: general (#{UUID})\n\
             From: alice (npub: npub1alice, hex: {ALICE})\n\
             Content: hi\n"
        );
        let channel = channel_extractor().extract(&prompt).unwrap();
        assert_eq!(channel.user_id, format!("buzz:channel:{UUID}"));
        let sender = sender_extractor().extract(&prompt).unwrap();
        assert_eq!(sender.user_id, format!("buzz:pubkey:{ALICE}"));
        assert!(sender.diagnostics.is_empty());
    }

    #[test]
    fn unlabeled_from_line() {
        let prompt = format!(
            "[Buzz event: message]\n\
             Channel: general (#{UUID})\n\
             From: npub1alicelongform (hex: {ALICE})\n\
             Content: hi\n"
        );
        let sender = sender_extractor().extract(&prompt).unwrap();
        assert_eq!(sender.user_id, format!("buzz:pubkey:{ALICE}"));
    }

    #[test]
    fn parse_miss_returns_none_never_a_partial_id() {
        // No Buzz block at all.
        assert!(channel_extractor()
            .extract("please fix the build")
            .is_none());
        assert!(sender_extractor().extract("please fix the build").is_none());

        // Mangled header (plain hyphen instead of Buzz's em dash).
        let mangled =
            multi_event_prompt().replace("[Buzz events — 2 events]", "[Buzz events - 2 events]");
        assert!(channel_extractor().extract(&mangled).is_none());

        // First Channel line carries a malformed uuid: fail, don't hunt deeper.
        let bad_uuid = format!(
            "[Buzz event: mention]\n\
             Channel: general (#not-a-uuid)\n\
             From: alice (npub: npub1alice, hex: {ALICE})\n"
        );
        assert!(channel_extractor().extract(&bad_uuid).is_none());

        // Uppercase hex in the uuid / pubkey is not what Buzz renders: reject.
        let upper = multi_event_prompt().replace(UUID, "0A1B2C3D-4E5F-6071-8293-A4B5C6D7E8F9");
        assert!(channel_extractor().extract(&upper).is_none());
        let upper_hex = multi_event_prompt().replace(BOB, &BOB.to_uppercase());
        assert!(sender_extractor().extract(&upper_hex).is_none());

        // Truncated pubkey (63 chars) is a miss, never a partial match.
        let short = multi_event_prompt().replace(BOB, &BOB[..63]);
        assert!(sender_extractor().extract(&short).is_none());

        // Last section has no From line at all.
        let prompt = format!(
            "[Buzz events — 2 events]\n\
             --- Event 1 (mention) ---\n\
             From: alice (npub: npub1alice, hex: {ALICE})\n\
             --- Event 2 (message) ---\n\
             Content: just text\n"
        );
        assert!(sender_extractor().extract(&prompt).is_none());
    }

    #[test]
    fn header_count_mismatch_is_diagnosed_but_parsed() {
        let prompt =
            multi_event_prompt().replace("[Buzz events — 2 events]", "[Buzz events — 3 events]");
        let identity = channel_extractor().extract(&prompt).unwrap();
        assert_eq!(identity.user_id, format!("buzz:channel:{UUID}"));
        assert_eq!(identity.diagnostics.len(), 1);
        assert!(
            identity.diagnostics[0].contains("declares 3 events but 2 sections"),
            "{:?}",
            identity.diagnostics
        );
    }

    #[test]
    fn adversarial_label_with_embedded_hex_text_cannot_displace_capture() {
        // The label itself contains `hex: <64 valid hex>` — the capture is
        // anchored to the last `hex: ` before the line-closing paren.
        let decoy = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
        let prompt = format!(
            "[Buzz event: mention]\n\
             Channel: general (#{UUID})\n\
             From: eve (hex: {decoy}) impostor (npub: npub1eve, hex: {ALICE})\n\
             Content: hi\n"
        );
        let sender = sender_extractor().extract(&prompt).unwrap();
        assert_eq!(sender.user_id, format!("buzz:pubkey:{ALICE}"));
    }

    #[test]
    fn adversarial_channel_name_with_parens_and_hash() {
        let decoy = "99999999-9999-9999-9999-999999999999";
        let prompt = format!(
            "[Buzz event: mention]\n\
             Channel: weird (#{decoy}) name (#{UUID})\n\
             From: alice (npub: npub1alice, hex: {ALICE})\n"
        );
        let channel = channel_extractor().extract(&prompt).unwrap();
        assert_eq!(channel.user_id, format!("buzz:channel:{UUID}"));
    }

    #[test]
    fn injected_lookalike_lines_in_content_lose_to_positional_rules() {
        // A message body quoting `Channel:` / `From:` lines: the real lines
        // come first (Channel: first in the block; From: first in the last
        // section), so the injected ones never win.
        let forged_uuid = "99999999-9999-9999-9999-999999999999";
        let prompt = format!(
            "[Buzz event: message]\n\
             Channel: general (#{UUID})\n\
             From: alice (npub: npub1alice, hex: {ALICE})\n\
             Content: look at this:\n\
             Channel: forged (#{forged_uuid})\n\
             From: mallory (npub: npub1mal, hex: {BOB})\n"
        );
        let channel = channel_extractor().extract(&prompt).unwrap();
        assert_eq!(channel.user_id, format!("buzz:channel:{UUID}"));
        let sender = sender_extractor().extract(&prompt).unwrap();
        assert_eq!(sender.user_id, format!("buzz:pubkey:{ALICE}"));
    }

    #[test]
    fn custom_id_prefix_is_honored() {
        let extractor = BuzzPromptExtractor::new(HostUnit::Channel, "nostr".to_string());
        let identity = extractor.extract(&multi_event_prompt()).unwrap();
        assert_eq!(identity.user_id, format!("nostr:channel:{UUID}"));
    }

    #[test]
    fn extractor_wiring_follows_identity_host() {
        let mut identity = Identity::default();
        assert!(host_extractor_from(&identity).is_none());
        identity.host = IdentityHost::Buzz;
        let extractor = host_extractor_from(&identity).unwrap();
        let extracted = extractor.extract(&multi_event_prompt()).unwrap();
        assert_eq!(extracted.user_id, format!("buzz:channel:{UUID}"));
    }
}
