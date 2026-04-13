# Soul Spec Integration

**Status:** Implemented (core loading + 7 soul packages). External import planned.
**Spec Version:** ClawSouls Soul Spec v0.5
**Goal:** Package every Nexus agent as a soul spec-compliant persona bundle. Enable importing external souls from the ClawSouls registry as Nexus agents.

---

## Why

1. **Portability** — Agent personas become self-contained packages that can be versioned, backed up, shared, and diffed. Prompt changes are git-tracked as file changes, not buried in SQL migrations.
2. **Standardization** — The soul spec gives us a structured way to define agent identity, behavior, tools, and constraints instead of ad-hoc system prompts.
3. **Discovery** — Progressive disclosure (3 levels) lets agents efficiently describe themselves to each other. ARIA doesn't need to load every agent's full prompt — just their `disclosure.summary`.
4. **Ecosystem** — Soul spec compatibility means Nexus agents are interoperable with OpenClaw, Cursor, ClawdBot, and any framework that reads SOUL.md. External souls from the 13K+ ClawSouls registry could be installed as Nexus agents.
5. **Separation of concerns** — Agent identity (SOUL.md) is separate from platform runtime (agent_registry). The soul package defines WHO the agent is. The registry defines HOW it runs.

---

## Architecture

### Directory Structure

```
nexus/
├── souls/                           # All agent soul packages
│   ├── monitor/
│   │   ├── soul.json                # Manifest
│   │   ├── SOUL.md                  # System prompt (mission, scope, cycle, classify)
│   │   ├── IDENTITY.md              # Voice, personality, communication style
│   │   ├── AGENTS.md                # Coordination rules, peer relationships
│   │   └── examples/
│   │       ├── good-outputs.md      # Calibration: what good decisions look like
│   │       └── bad-outputs.md       # Calibration: what to avoid
│   ├── model-ops/
│   │   ├── soul.json
│   │   ├── SOUL.md
│   │   ├── IDENTITY.md
│   │   ├── AGENTS.md
│   │   └── REFERENCE.md             # Infrastructure reference data (ports, VRAM, services)
│   ├── collector/
│   │   ├── soul.json
│   │   ├── SOUL.md
│   │   ├── IDENTITY.md
│   │   ├── AGENTS.md
│   │   └── REFERENCE.md             # Data sources, backfill payloads, thresholds
│   ├── analyst/
│   │   ├── soul.json
│   │   ├── SOUL.md
│   │   ├── IDENTITY.md
│   │   └── AGENTS.md
│   ├── fixer/
│   │   ├── soul.json
│   │   ├── SOUL.md
│   │   ├── IDENTITY.md
│   │   └── AGENTS.md
│   ├── relationships/
│   │   ├── soul.json
│   │   ├── SOUL.md
│   │   ├── IDENTITY.md
│   │   └── AGENTS.md
│   └── aria/
│       ├── soul.json
│       ├── SOUL.md
│       ├── IDENTITY.md
│       ├── AGENTS.md
│       └── HEARTBEAT.md             # Check-in behavior, briefing cadence
```

### File Mapping to v2 Prompts

The current monolithic system prompt for each agent gets split across soul spec files:

| v2 Prompt Section | Soul Spec File | Progressive Disclosure Level |
|---|---|---|
| Agent name + one-line mission | `soul.json` → `disclosure.summary` | Level 1 (discovery) |
| Mission + Scope + Cycle Loop + Classify + Constraints | `SOUL.md` | Level 2 (active use) |
| Voice / personality line | `IDENTITY.md` | Level 2 (active use) |
| Coordination section | `AGENTS.md` | Level 3 (deep dive) |
| Infrastructure reference data (ports, SQL, thresholds) | `REFERENCE.md` (extension) | Level 3 (deep dive) |
| Good/bad output calibration | `examples/` | Level 3 (deep dive) |

### Loading Strategy

The Worker loads soul packages at cycle start using progressive disclosure:

```typescript
// Level 2 — loaded every cycle (SOUL.md + IDENTITY.md)
const soul = await loadSoulPackage(agentId, 'active');
// Assembles: SOUL.md content + IDENTITY.md content

// Level 3 — loaded when needed (AGENTS.md, REFERENCE.md, examples)
// Loaded when: agent has inbox messages from peers, or complex coordination needed
const deepContext = await loadSoulPackage(agentId, 'deep');
```

**Token budget management:**
- Level 2 (SOUL.md + IDENTITY.md): target < 1500 tokens per agent
- Level 3 adds AGENTS.md + REFERENCE.md: up to 3000 tokens total
- Platform preamble: ~400 tokens (shared across all agents)
- State injection (inbox, tasks, memories): variable, up to 2000 tokens

### soul.json Schema (Nexus Extension)

Nexus extends the ClawSouls soul.json with platform-specific fields:

```json
{
  "specVersion": "0.5",
  "name": "monitor",
  "displayName": "Platform Monitor",
  "version": "2.0.0",
  "description": "Unified platform health monitor — infrastructure, jobs, performance, costs, security.",
  "author": {
    "name": "Anonymous",
    "github": "anonymous"
  },
  "license": "Apache-2.0",
  "tags": ["infrastructure", "monitoring", "health", "security"],
  "category": "operations/monitoring",
  "compatibility": {
    "frameworks": ["nexus"],
    "models": ["qwen3.5-35b-a3b"],
    "minTokenContext": 8192
  },
  "allowedTools": [
    "query_db", "check_system", "inbox_send", "remember",
    "search_tools", "request_tool", "escalate", "pushover_notify"
  ],
  "files": {
    "soul": "SOUL.md",
    "identity": "IDENTITY.md",
    "agents": "AGENTS.md"
  },
  "examples": {
    "good": "examples/good-outputs.md",
    "bad": "examples/bad-outputs.md"
  },
  "disclosure": {
    "summary": "Monitors platform infrastructure, jobs, agents, performance, costs, and security posture."
  },
  "deprecated": false,

  "nexus": {
    "schedule_interval_sec": 900,
    "autonomy": "moderate",
    "executor": "nexus",
    "llm_config": {
      "model": "qwen3.5-35b-a3b",
      "url": "http://localhost:8088/v1",
      "temperature": 0.1,
      "max_tokens": 2000
    },
    "absorbs": ["keeper", "performance-analyst", "cost-accountant", "security-auditor"],
    "response_schema": "nexus-v2"
  }
}
```

The `nexus` extension block contains platform-specific runtime config that isn't part of the soul spec standard but is needed for Nexus agent registration.

---

## Database Changes

### agent_registry Extensions

```sql
ALTER TABLE agent_registry ADD COLUMN IF NOT EXISTS soul_spec_version TEXT;
ALTER TABLE agent_registry ADD COLUMN IF NOT EXISTS soul_package_path TEXT;
ALTER TABLE agent_registry ADD COLUMN IF NOT EXISTS disclosure_summary TEXT;
```

- `soul_spec_version`: tracks which spec version this agent uses (e.g., "0.5")
- `soul_package_path`: path to the soul package directory (e.g., "souls/monitor")
- `disclosure_summary`: one-line summary for Level 1 discovery (max 200 chars)

### Soul Loader Service

New service in `@nexus/core`:

```typescript
// packages/core/src/services/soul-loader.ts

interface SoulPackage {
  manifest: SoulJson;
  soul: string;         // SOUL.md content
  identity: string;     // IDENTITY.md content
  agents?: string;      // AGENTS.md content (Level 3)
  reference?: string;   // REFERENCE.md content (Level 3)
  heartbeat?: string;   // HEARTBEAT.md content (Level 3)
}

async function loadSoulPackage(agentId: string, level: 'scan' | 'active' | 'deep'): Promise<SoulPackage>;
async function installSoul(packagePath: string): Promise<void>;  // Register soul as Nexus agent
async function validateSoul(packagePath: string): Promise<ValidationResult>;
```

### Prompt Assembly (Worker Change)

The agent-runner currently loads `system_prompt` from `agent_registry`. It would instead:

1. Read `soul_package_path` from agent_registry
2. Load SOUL.md + IDENTITY.md (Level 2)
3. If agent has unread inbox → also load AGENTS.md (Level 3)
4. If agent has REFERENCE.md → load it (Level 3, for agents with operational data)
5. Prepend platform preamble
6. Append gathered state (inbox, tasks, memories, tools)

```typescript
async function buildSystemPrompt(agent: AgentConfig): Promise<string> {
  const preamble = getPlatformPreamble();
  const soul = await loadSoulPackage(agent.agent_id, hasInbox ? 'deep' : 'active');
  
  let prompt = preamble + '\n\n';
  prompt += soul.soul + '\n\n';          // SOUL.md (mission, scope, cycle, classify)
  prompt += soul.identity + '\n\n';      // IDENTITY.md (voice, personality)
  
  if (soul.agents && hasInbox) {
    prompt += soul.agents + '\n\n';      // AGENTS.md (coordination, only when needed)
  }
  if (soul.reference) {
    prompt += soul.reference + '\n\n';   // REFERENCE.md (operational data)
  }
  
  prompt += buildStateMessage(agent);    // inbox, tasks, memories, tools
  return prompt;
}
```

---

## External Soul Import

### Installing a Soul from ClawSouls Registry

```bash
# CLI command (future)
nexus soul install senior-devops-engineer

# What it does:
# 1. Fetch soul package from ClawSouls registry
# 2. Validate soul.json against spec
# 3. Check compatibility (frameworks includes "nexus" or is omitted)
# 4. Check allowedTools against platform_tools
# 5. Create souls/<name>/ directory with files
# 6. Register in agent_registry with nexus extension defaults
# 7. Grant allowed tools via agent_tool_grants
# 8. Generate API key
```

### Compatibility Requirements for External Souls

External souls need these to work on Nexus:
- `specVersion: "0.5"` (or compatible)
- `files.soul` must point to a valid SOUL.md
- `allowedTools` must map to existing `platform_tools`
- `compatibility.models` should include qwen patterns or be omitted
- `compatibility.minTokenContext` should be ≤ 65536 (Forge's max)

### Tool Mapping for External Souls

External souls use generic tool names. Nexus maps them:

| Soul Spec Tool | Nexus Tool |
|---|---|
| `browser` | `web_fetch` (if implemented) |
| `exec` | `check_system` (read-only subset) |
| `web_search` | `web_search` (if implemented) |
| `github` | `github_*` tools |
| `file_read` | `read_file` |
| `file_write` | `write_file` (Fixer only) |

Unmapped tools → agent can use `request_tool` to discover and request access.

---

## Nexus Extensions to Soul Spec

Fields we add beyond the standard:

| Field | Location | Purpose |
|---|---|---|
| `nexus.schedule_interval_sec` | soul.json | How often the agent cycles |
| `nexus.autonomy` | soul.json | moderate / approval_gated / high |
| `nexus.executor` | soul.json | "nexus" (always, for Nexus agents) |
| `nexus.llm_config` | soul.json | Model, URL, temperature, max_tokens |
| `nexus.absorbs` | soul.json | List of legacy agents this one replaces |
| `nexus.response_schema` | soul.json | Response format version ("nexus-v2") |
| `REFERENCE.md` | files | Operational reference data (non-standard file) |

These live under a `nexus` namespace in soul.json to avoid polluting the standard spec.

---

## Implementation Status

### Step 1: Create Soul Packages for All 7 Agents — DONE

All 7 agents have soul packages in `souls/`:
- `monitor/` — SOUL.md, IDENTITY.md, AGENTS.md
- `model-ops/` — SOUL.md, IDENTITY.md, AGENTS.md, REFERENCE.md (ports, VRAM, thresholds)
- `collector/` — SOUL.md, IDENTITY.md, AGENTS.md, REFERENCE.md (sources, payloads, thresholds)
- `analyst/` — SOUL.md, IDENTITY.md, AGENTS.md
- `fixer/` — SOUL.md, IDENTITY.md, AGENTS.md
- `relationships/` — SOUL.md, IDENTITY.md, AGENTS.md
- `aria/` — SOUL.md, IDENTITY.md, AGENTS.md, HEARTBEAT.md (briefing behavior)

### Step 2: Soul Loader Service — DONE

`packages/core/src/services/soul-loader.ts` implemented with:
- `loadSoulPackage(agentId, level)` — progressive disclosure loading
- `assembleSoulPrompt(soul, level)` — file combination into prompt string
- `listSoulPackages()` — directory scan for available packages
- `validateSoulPackage(agentId)` — spec compliance validation
- `clearCache(agentId?)` — manifest and file content cache management
- Exported from `@nexus/core` as `soulLoader`

### Step 3: Worker Integration — DONE

`packages/worker/src/agent-runner.ts` modified:
- `buildSystemPrompt()` calls `loadSoulPrompt()` first, falls back to `agent.system_prompt` from DB
- `loadSoulPrompt()` determines disclosure level based on inbox state (Level 2 normally, Level 3 when inbox has messages)
- Fully backward compatible — agents without soul packages load from DB column

### Step 4: Database Migration — DONE

Migration `016-soul-spec.sql`:
- Added `soul_spec_version`, `soul_package_path`, `disclosure_summary` columns to `agent_registry`
- Seeded all 7 agents with paths and summaries
- `Agent` TypeScript type updated with new fields

### Step 5: Calibration Examples — TODO

`examples/good-outputs.md` and `examples/bad-outputs.md` for each agent. Concrete examples of correct/incorrect cycle behavior that calibrate LLM output quality.

### Step 6: External Soul Import — TODO (Future)

Build `nexus soul install` CLI and ClawSouls registry integration. Lower priority — internal agents come first.

---

## What Changes vs What Stays

| Aspect | Before (v2) | After (Soul Spec) |
|---|---|---|
| Prompt storage | Single `system_prompt` column in DB | soul package directory on disk, git-tracked |
| Prompt editing | SQL migration per change | Edit SOUL.md, restart worker |
| Agent identity | Implicit in prompt text | Explicit in soul.json manifest |
| Progressive disclosure | N/A — full prompt every cycle | 3 levels: scan / active / deep |
| Tool declarations | `agent_tool_grants` table only | Also declared in soul.json `allowedTools` |
| Personality | Embedded in prompt | Separate IDENTITY.md |
| Coordination rules | Embedded in prompt | Separate AGENTS.md (loaded only when needed) |
| Operational data | Embedded in prompt | Separate REFERENCE.md (loaded only when needed) |
| Calibration | None | examples/good-outputs.md + bad-outputs.md |
| Portability | None — tied to Nexus DB | soul.json is framework-agnostic |
| Versioning | Migration sequence number | semver on soul package |

---

## Open Questions

1. **Hot reload:** Should the Worker watch soul package files for changes and reload without restart? Or require `systemctl restart nexus-worker`?

2. **Prompt hash verification:** Should the Worker hash SOUL.md on load and compare to a stored hash in agent_registry to detect unauthorized changes?

3. **Level 3 loading trigger:** Currently proposed: load AGENTS.md when inbox has messages. Should REFERENCE.md always load, or only when specific tool calls are likely? Token cost vs completeness tradeoff.

4. **External soul sandboxing:** When installing a soul from the registry, should there be a review/approval step before it can run cycles? Or auto-run with read-only tools only?

5. **Soul spec version pinning:** Should Nexus pin to spec v0.5, or track upstream changes? The spec is still pre-1.0 and may change.
