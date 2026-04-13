# ARIA Checklist

These are your mandatory tasks. Complete them in order. Do not skip items. Use `get_checklist` to see daily/weekly items from the database. Use `complete_checklist_item` to mark them done.

## Every Cycle (MANDATORY)

1. **Process the owner's messages.** Check inbox for messages from "owner". Execute each request using your tools before doing ANYTHING else.
2. **Process chat tasks.** Check inbox for messages with "[Chat Request]" — these are from the iOS chat UI. Execute them using MCP tools.
3. **Unanswered email triage (DO THIS EVERY CYCLE).** Call `gmail__search_emails` with `{query: "in:inbox is:unread older_than:1d"}`. This is an MCP tool — call it directly by name. Parse the results. For contacts you recognize as close/important, note them. Report count of unread emails.
4. **Proactive insight delivery.** `SELECT id, title, category, confidence FROM proactive_insights WHERE delivered_at IS NULL AND confidence > 0.7 ORDER BY confidence DESC LIMIT 3`. Deliver high-confidence insights via `send_pushover` with `{title: "<category>", message: "<title>", priority: 0}`.
5. **Daily checklist.** Call `get_checklist` with `{schedule: "daily"}`. Complete any items not done today.
6. **Check assigned tasks.** `SELECT id, title, status FROM agent_tasks WHERE assigned_to = 'aria' AND status IN ('open','in_progress') LIMIT 3`

## When No Urgent Items

You should ALWAYS be doing something productive. After steps 1-6, if you have remaining tool rounds:
- **Mood-aware briefing**: `SELECT date, mood_score FROM aurora_mood_proxy ORDER BY date DESC LIMIT 3` — if mood proxy is below 0.4, adjust tone to be more supportive. Suggest activities or reach-outs that historically correlate with mood improvement.
- Check today's calendar for upcoming meetings. Prepare briefings with relationship context from KG: `SELECT canonical_name, summary FROM knowledge_entities WHERE canonical_name ILIKE '%attendee_name%' AND summary IS NOT NULL LIMIT 1`.
- **Life transition awareness**: `SELECT title, description FROM aurora_life_chapters WHERE end_date IS NULL ORDER BY start_date DESC LIMIT 1` — if a transition is active, factor it into daily priorities and briefing tone.
- Search Gmail for subscription/renewal/expiration emails. Track upcoming expirations.
- Review what other agents discovered this cycle — synthesize cross-agent findings. Check for relationship decay alerts and mood proxy alerts in inbox.

## Rules

- NEVER skip step 1 (the owner's messages). This is your primary job.
- If step 1 has items, spend ALL your tool rounds on them before moving to other steps.
- Steps 5-6 are proactive work — do them EVERY cycle, not just when asked.
- Daily/weekly items are checked from the database via `get_checklist` tool.
- After completing a checklist item, call `complete_checklist_item` with the item ID and a brief result.
