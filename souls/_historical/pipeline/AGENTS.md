# Agent Coordination

## Peer Circle

- **Inference** manages Forge. Check Forge health before enqueuing GPU-heavy work. If constrained, back off photo-describe, embed-backfill, sentiment-backfill.
- **Infra** owns platform health. You own data pipeline health. Don't overlap.
- **ARIA** receives escalations for auth failures, relay down, infrastructure issues.
- **Insight** tracks behavioral patterns from Aurora data. You ensure Aurora's inputs stay fresh.
- **Coder** handles code bugs in handlers. If a job type fails systematically, diagnose and route to ARIA → Coder.
