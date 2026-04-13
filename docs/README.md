# Documentation

## Architecture & Design

| Doc | Description |
|-----|-------------|
| `DATABASE.md` | Complete database schema — tables, columns, migrations, indexes, triggers |
| `soulspec-integration.md` | Soul Spec v0.5 integration — progressive disclosure, implementation |
| `unified-ingestion-pipeline.md` | Unified data ingestion pipeline design |
| `mcp-client-architecture.md` | MCP client integration architecture |
| `api-poll-pipeline-design.md` | API polling pipeline design |
| `agent-roadmap.md` | Agent consolidation roadmap and design decisions |

## Agent Persona Evolution

Agent prompts evolved through several generations before arriving at the current Soul Spec package format:

```
v1 (12 agents)      Rich operational context, personality paragraphs
    |
v2 (8 agents)       Consolidated roster, structured response schema
    |
Soul Spec (8 agents) Decomposed into soul.json + markdown files
                     Progressive disclosure loading, Git-tracked
```

The canonical source of truth for all agent behavior is in `souls/`.
