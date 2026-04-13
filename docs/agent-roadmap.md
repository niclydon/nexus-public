# Nexus Agent Roadmap
## From Monitoring Platform to Personal AI Family Office

*Created 2026-04-03. Living document — update as phases complete.*

---

## Current State (v3)

8 agents on Llama 3.3 70B (ARIA on Qwen 3.5 35B-A3B), staggered schedules (5 min -- 6 hours), 50+ checklist items, family office model implemented. Agents actively completing checklists and doing proactive work. MCP tool grants configured in all soul.json files. 300K knowledge facts (74% embedded), all 982 eligible entities summarized, 163K photos fully described/geocoded/embedded. ARIA isolation enforced at 4 levels.

---

## Phase 1: Foundation Lock-In (This Week)

**Goal:** Every agent completes its daily checklist items reliably. Zero false escalations to the owner.

### Milestones
- [x] All 8 agents call `get_checklist` every cycle and `complete_checklist_item` for finished items (verified in all soul packages)
- [x] ARIA isolation enforced at 4 levels (tool grants, query blocking, inbox filtering, state message)
- [~] Pipeline completes all 4 daily items consistently (embedding coverage, source freshness, enrichment throughput, enrichment gap analysis) -- mostly done, occasional gaps
- [ ] ARIA completes morning briefing, proactive insight delivery, and email triage
- [x] Insight enqueues knowledge-summarize jobs every cycle -- all 982 eligible entities summarized as of 2026-04-07
- [ ] delegate_task async fallback working (zero null failures)
- [x] MCP tool grants configured in all agent soul.json files

### Actions
- Monitor checklist completion rates via: `SELECT agent_id, COUNT(*) FILTER (WHERE last_completed_at > CURRENT_DATE) FROM agent_checklists WHERE is_active GROUP BY agent_id`
- Tune SOUL.md/CHECKLIST.md based on observed failures (wrong params, scope violations)
- Track KG summarization progress: `SELECT COUNT(NULLIF(summary, '')) FROM knowledge_entities`

---

## Phase 2: MCP Tool Activation (Week 2)

**Goal:** Agents actively use MCP tools for discovery and data enrichment. First cross-source insights.

> **Note (2026-04-12):** MCP tool grants are configured in all agent soul.json files (gmail, github, cloudflare, brave-search, apple-photos, etc.) but active usage is still ramping up. ARIA uses `gmail__search_emails` regularly; other agents have access but aren't yet using MCP tools consistently in cycles.

### ARIA
- [ ] Uses `gmail__search_emails` every cycle for unanswered email triage
- [ ] Uses calendar MCP tools for meeting prep with relationship context
- [ ] Uses `gmail__modify_email` to archive promotions/bulk mail
- [ ] Drafts replies for the owner's review via `gmail__draft_email`

### Pipeline
- [ ] Explores Google Drive via MCP — indexes documents into knowledge graph
- [ ] Explores Notion workspace — maps databases, identifies ingestible data
- [ ] Begins ingesting Notion databases as new data source
- [ ] Reports MCP coverage gaps weekly (which servers have tools we don't use)

### Insight
- [ ] Uses `apple-photos__search_photos` for photo intelligence analysis
- [ ] Uses `brave-search__brave_web_search` to research unfamiliar topics from conversations
- [ ] Cross-references photo locations with calendar events to detect undocumented trips
- [ ] Produces first weekly behavioral narrative with health/location/sentiment data

### Circle
- [ ] Uses `gmail__search_emails` to find unanswered emails from inner circle
- [ ] Uses `apple-photos__search_photos` to find photos with contacts for birthday prep
- [ ] Produces first meeting prep relationship dossier
- [ ] Cross-references iMessage + Gmail + Photos for social graph enrichment

### Coder
- [ ] Uses `github__list_issues` and `github__list_pull_requests` for repo awareness
- [ ] Uses `brave-search__brave_web_search` to check for dependency vulnerabilities
- [ ] Creates first proactive PR from code health scan findings

### Infra
- [ ] Uses `cloudflare__list_zones` and DNS tools for infrastructure audit
- [ ] Uses `github__get_file_contents` to cross-reference deployed code with repo
- [ ] Produces first database growth forecast with week-over-week comparison

### Inference
- [ ] Explores Cloudflare Workers AI capabilities via MCP tools
- [ ] Produces first model performance analysis with cost comparison

---

## Phase 3: Proactive Intelligence (Week 3-4)

**Goal:** Agents generate insights the owner never asked for. The platform anticipates needs.

### Email Intelligence
- [ ] ARIA identifies emails requiring response and drafts suggestions
- [ ] ARIA tracks subscription renewals and alerts 30 days before expiration
- [ ] Circle detects relationship drift from email response patterns (not just iMessage)

### Calendar Intelligence
- [ ] ARIA prepares briefings for every meeting with attendees, pushed via Pushover 30 min before
- [ ] Circle prepares relationship dossiers for non-trivial attendees
- [ ] Insight correlates meeting density with health/energy metrics

### Knowledge Graph Maturity
- [x] All 982 eligible entities (fact_count >= 5) summarized as of 2026-04-07. Remaining ~42K entities below threshold.
- [ ] 300K facts >80% embedded (currently 74%)
- [ ] Entity dedup complete — consolidated from 40K to estimated ~15K unique entities
- [ ] Cross-entity relationship mapping (who knows whom, what connects to what)

### Personal Health Intelligence
- [ ] Insight tracks step count, sleep, heart rate trends weekly
- [ ] Insight correlates health metrics with communication patterns and calendar density
- [ ] Insight detects anomalies (sudden drop in steps, unusual heart rate) and alerts ARIA
- [ ] ARIA surfaces health insights to the owner without being asked

### Photo Intelligence
- [x] All 162K photos embedded (100% as of 2026-04-05)
- [ ] Insight identifies most-photographed people, locations, and seasonal patterns
- [ ] Insight detects undocumented trips (photo GPS clusters with no calendar/knowledge match)
- [ ] Circle uses photo frequency to enrich relationship health scores

---

## Phase 4: Autonomous Operations (Month 2)

**Goal:** Agents handle routine tasks end-to-end without human intervention.

### ARIA as Executive Assistant
- [ ] Manages Gmail inbox: archives bulk, flags important, drafts replies
- [ ] Prepares daily briefings (morning and evening) automatically
- [ ] Manages Notion as the external knowledge base — keeps reference pages current
- [ ] Handles routine requests from iOS chat without needing to escalate

### Pipeline as Data Ops
- [ ] Monitors all 14 data sources autonomously — detects and reports stale sources
- [ ] Manages enrichment pipeline end-to-end — balances embed/sentiment/knowledge loads
- [ ] Begins ingesting new data sources (Google Drive docs, Notion databases)
- [ ] Produces weekly data health report

### Circle as Relationship Concierge
- [ ] Birthday preparation: gift suggestions, message drafts, photo compilations — 7 days before
- [ ] Unanswered message detection: flags within 48 hours, suggests responses
- [ ] Meeting prep: full dossier for every external meeting, pushed to ARIA
- [ ] Relationship quarterly review: who have you lost touch with? Who is emerging?

### Insight as Pattern Oracle
- [ ] Weekly behavioral narrative delivered via Pushover every Sunday
- [ ] Life chapter detection: "You're in a [work sprint / social phase / travel period]"
- [ ] Cross-source anomaly detection: flags when 2+ metrics shift together
- [ ] Knowledge graph insights: "You've mentioned X 15 times this month but never followed up"

### Coder as Code Guardian
- [ ] Weekly automated dependency audit with CVE checking
- [ ] Proactive PR creation for dead code removal, TODO resolution
- [ ] Architecture drift report: PLAN.md vs reality, quarterly

---

## Phase 5: Multi-Agent Collaboration (Month 3)

**Goal:** Agents work together on complex multi-step tasks.

### Task Chains
- [ ] ARIA receives "prepare for meeting with John" → creates tasks for Circle (dossier) + Insight (knowledge search) + Pipeline (recent emails) → assembles briefing from all three
- [ ] Circle detects birthday → delegates to Insight (interest research) + ARIA (gift ordering) → end-to-end birthday handling
- [ ] Infra detects DB growth issue → delegates to Coder (create migration for archival) → Pipeline (verify data integrity) → automated cleanup

### Shared Context
- [ ] Working memory visible across agents (not just per-agent)
- [ ] Shared investigation boards: multi-agent tracking of complex issues
- [ ] Agent-to-agent delegation with result tracking (not just fire-and-forget inbox)

### Quality Loop
- [ ] Insight scores every agent decision automatically (eval scoring active)
- [ ] Weekly quality report: which agents are improving, which are regressing
- [ ] Automated Soul Spec tuning suggestions based on eval patterns

---

## Phase 6: External Integration (Month 4+)

**Goal:** Platform interacts with the outside world beyond data ingestion.

### Outbound Actions
- [ ] ARIA sends emails on the owner's behalf (with approval workflow)
- [ ] ARIA manages calendar (create events, accept/decline invitations)
- [ ] Circle sends birthday messages via iMessage (with approval)
- [ ] Coder creates GitHub issues and PRs autonomously

### New Data Sources
- [ ] Financial data integration (bank transactions, investment accounts)
- [ ] Smart home integration (HomeKit via Apple ecosystem MCP)
- [ ] Music listening patterns (Apple Music / Spotify)
- [ ] Travel bookings and loyalty programs

### A2A Protocol
- [ ] Agent-to-agent protocol for external agent interoperability
- [ ] Allow trusted external agents to query the knowledge graph
- [ ] Federated agent collaboration across platforms

---

## Success Metrics

| Metric | Current | Phase 1 Target | Phase 4 Target |
|--------|---------|---------------|---------------|
| Daily checklist completion rate | ~10% | >80% | >95% |
| MCP tools used per day | 0 | 20+ | 100+ |
| False escalations to the owner per day | ~15 | <3 | 0 |
| Proactive insights delivered/week | ~9 | 25+ | 50+ |
| Knowledge entities summarized | 982/982 eligible (100%) | 5K | 40K |
| Knowledge facts embedded | 220K/300K (74%) | 60K | 300K |
| Agent-initiated email actions/day | 0 | 5+ | 20+ |
| Meeting briefings delivered | 0 | 1/meeting | every meeting |
| Weekly behavioral narrative | none | first one | automated |
| Cross-source insights/week | 0 | 3+ | 10+ |
