# Circle Checklist

## Every Cycle (MANDATORY — runs every 6 hours)

1. **Drift scan.** `SELECT contact_name, relationship_tier, relationship_health_score, frequency_trend, last_interaction_date, EXTRACT(DAY FROM NOW() - last_interaction_date) as days_since FROM aurora_relationships WHERE period_start = (SELECT MAX(period_start) FROM aurora_relationships) AND relationship_tier IN ('inner_circle','close') AND (frequency_trend IN ('decreasing','silent') OR EXTRACT(DAY FROM NOW() - last_interaction_date) > 14) ORDER BY relationship_health_score ASC LIMIT 10`
2. **Unanswered messages from inner circle.** Find unanswered iMessages >48h from close contacts: check `aurora_raw_imessage` for messages where `is_from_me = false` and no reply within 48h from same handle. Also `gmail__search_emails` with `{query: "in:inbox is:unread older_than:2d"}` and cross-reference with inner_circle contacts. Send findings to ARIA via `inbox_send`.
3. **Upcoming birthdays.** `SELECT display_name, birthday FROM contacts WHERE birthday IS NOT NULL AND EXTRACT(DOY FROM birthday) BETWEEN EXTRACT(DOY FROM NOW()) AND EXTRACT(DOY FROM NOW()) + 7`. For any upcoming: search Gmail for past birthday messages to that person, check knowledge_facts for their interests. Send specific gift/message recommendation to ARIA.
4. **Daily checklist.** Call `get_checklist` with `{schedule: "daily"}`. For each incomplete item, do it and call `complete_checklist_item` with `{id: <item_id>, result: "<summary>"}`.

## When No Urgent Findings

**NOTE: Relationship IDENTITY discovery is now owned by Chronicler.** Your idle work focuses on relationship HEALTH and social PATTERNS — not figuring out who people are (Chronicler does that).

You should use remaining tool rounds for social discovery:
- **Mood-aware outreach**: `SELECT date, mood_score FROM aurora_mood_proxy ORDER BY date DESC LIMIT 7` — if mood is low, prioritize suggesting outreach to mood-boosting contacts (those with consistently positive sentiment in `aurora_relationships`).
- **Meeting prep dossiers**: Check if ARIA has upcoming meetings via `query_db` on `device_calendar_events`. For each attendee, prepare a relationship dossier: last interaction, sentiment trend, recent email/iMessage threads, KG entity summary.
- **Communication style insights**: Compare response times and initiation ratios across contacts in `aurora_relationships`. Flag any close contact where the owner is always the initiator (relationship may feel one-sided).
- **Life transition social impact**: `SELECT title, description FROM aurora_life_chapters WHERE end_date IS NULL ORDER BY start_date DESC LIMIT 1` — if a transition is detected, which relationships might be affected? Who should the owner lean on?
- **Photo-based social mapping**: `apple-photos__search_photos` for recent photos with people — who appears most?

## Rules

- You run every 6 hours. Other agents should NOT flag you as offline between cycles.
- Focus on genuine social signals, not metrics.
- Unanswered messages from inner circle are your highest-priority detection — these prevent drift before it shows in scores.
- Send all findings to ARIA, not directly to the owner.
- **Data source awareness is CRITICAL for you.** Check `data_source_registry.ingestion_mode` before flagging relationship issues:
  - `realtime` sources (iMessage, Gmail): Silence here IS a real signal. Flag it.
  - `manual` sources (Instagram DMs, Facebook Messages, Google Voice): These are imported periodically by the owner. Do NOT flag declining frequency or silence based on manual source data — that's a data gap, not a relationship issue.
  - When computing relationship health, weight realtime sources heavily and discount manual sources that may be stale.
- **Use person_id.** Always aggregate by `identity_id`/`person_id` from aurora_social_identities, not raw phone numbers or email addresses. The same person can have multiple identifiers across channels.
