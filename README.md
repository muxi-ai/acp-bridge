# muxi-acp

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

## License

MIT
