# Chronicler Checklist

## Every Cycle (MANDATORY — runs every 3 hours)

1. **Relationship discovery.** Find the top unconnected person (most messages, no `person_connections` entry). Read their messages across the full time range, cross-reference with known family/friends, check calendar/photos/contacts. Record findings or send inference to ARIA for confirmation.

2. **Check for new data.** Look for recently ingested messages that mention known people in new contexts:
   ```sql
   SELECT identity_id, contact_name, LEFT(content_text, 200), timestamp::date
   FROM aurora_unified_communication
   WHERE timestamp > NOW() - INTERVAL '6 hours'
   AND content_text IS NOT NULL
   ORDER BY timestamp DESC LIMIT 20
   ```
   If anything reveals new relationship information, investigate and record.

3. **Daily checklist.** Call `get_checklist` with `{schedule: "daily"}`. Complete each item.

## Daily Items

- **Duplicate scan.** Check for potential person duplicates using name similarity. Merge obvious duplicates (same person, different contact entries). Flag uncertain ones for the owner via ARIA.
- **Birthday check.** Cross-reference `device_calendar_events` birthday events with `aurora_social_identities.birth_year`. Fill any gaps where calendar has data but the person record doesn't.
- **Coverage report.** How many persons have `person_connections` entries? How many don't? Track progress toward full coverage.

## Weekly Items

- **Life chapter extraction.** Read through one month of historical messages (starting from oldest unprocessed) and extract life facts: where the owner was living, who he was spending time with, what was happening. Store as knowledge_facts with domain='biography'.
- **Family tree audit.** Check `person_connections` for completeness. Are all known family members connected bidirectionally? Are inverse relationships correct? Fix any gaps.
- **Enrichment pass.** For people with confirmed identities but sparse data, search Apple Photos, Gmail, and calendar for additional context. Update person records with any new information found.

## Rules

- You run every 3 hours. Most cycles should produce 1-2 new relationship discoveries or life facts.
- Quality over quantity. One well-evidenced relationship discovery is worth more than five guesses.
- Always check `table_catalog` before querying unfamiliar tables.
- Send all unconfirmed inferences to ARIA — never insert relationship data you're not confident about.
- For high-confidence findings (exact match in Apple Contacts relations, explicit "my brother" in messages), you can insert directly.
