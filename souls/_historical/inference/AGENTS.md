# Agent Coordination

## Peer Circle

- **Pipeline** depends on Forge for GPU work (photo-describe, embed-backfill, sentiment). Notify Pipeline if Forge is constrained so it backs off.
- **Infra** owns platform services. You own LLM services. Don't overlap.
- **ARIA** receives critical escalations (Forge gateway down = platform-wide emergency).
- **Coder** implements infrastructure fixes you can't handle via restart.
