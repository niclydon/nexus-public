# Infra Playbooks

Investigation playbooks for incidents this agent has seen before. Add to this file whenever a new pattern emerges. Each playbook starts with the symptom and walks through the diagnostic steps that led to the root cause.

---

## Playbook: "A service died and I want to know who/what killed it"

### Symptom
A nexus service (or any systemd unit) is `inactive (dead)` with `code=killed, signal=TERM`. The journal entry just shows "Received SIGTERM, shutting down" — no caller identified.

### Investigation steps

**Step 1 — Confirm the service was killed externally (not a clean restart).**
```bash
ssh primary-server 'sudo systemctl status <service> --no-pager 2>&1 | head -10'
```
Look for `Main PID: NNN (code=killed, signal=TERM)`. If `code=exited, status=0/SUCCESS` it was a clean shutdown via systemctl stop. If `code=killed, signal=KILL` it was OOM or hard kill. `signal=TERM` means a graceful kill — someone deliberately sent SIGTERM.

**Step 2 — Find the exact moment of death.**
```bash
ssh primary-server 'sudo journalctl -u <service> --since "<approx_time>" --until "<+1 hour>" --no-pager | grep -iE "sigterm|stop|terminat|deactivat|exited"'
```
Get the precise UTC second the SIGTERM was received.

**Step 3 — Look at every other event in that exact second.**
```bash
ssh primary-server 'sudo journalctl --since "<second-1>" --until "<second+1>" --no-pager 2>&1 | grep -v "llama-embed\|llama-server\|llama-vlm\|llama-priority"' | head -80
```
Filter out the noisy llama-* services first — they spam dozens of lines per second. What's left is the diagnostic surface. Look for:
- Other services that died in the same second (cascade indicator)
- Any process that logged a "killing", "stopping", "exiting" message right before
- Cron / timer firings
- Deploy scripts (rsync, systemctl restart)

**Step 4 — Cross-reference PIDs.**
```bash
sudo journalctl -u <service> | grep "Main PID"
sudo journalctl -u <other-service> | grep "Main PID"
```
If multiple services died with consecutive PIDs (e.g. 308927, 308928, 308929), they were started by systemd in the same boot/restart cycle. If they all died at the exact same second, you're looking at a CASCADING kill — something killed a parent that took children with it, or hit a process group.

**Step 5 — Rule out OOM kill.**
```bash
ssh primary-server 'sudo journalctl -k --since "<window>" --until "<window>" 2>&1 | grep -iE "oom|out of memory|killed process"'
```
Empty result = not OOM.

**Step 6 — Rule out external triggers.**
```bash
ssh primary-server 'crontab -l 2>/dev/null; sudo crontab -l 2>/dev/null; sudo ls /etc/cron.d/; sudo systemctl list-timers --no-pager | grep <service>'
```
If a cron / timer fires within ±60 seconds of the death, it's a strong suspect.

**Step 7 — Look for explicit kill commands in your own code.**
```bash
grep -rn "process.kill\|child.kill\|spawn\|pkill\|killAll" packages/worker/src/ packages/api/src/
```
This is how I found the autoscaler kill bug — `killAllWorkers()` calls `child.kill('SIGTERM')` on bulk-runners that share the parent's process group (`detached: false`).

**Step 8 — Check process group / cgroup leak.**
A child spawned with `spawn(...)` and `detached: false` inherits the parent's process group. If something signals the parent's PGID (rare), all children get hit. Check:
```bash
ssh primary-server 'sudo ps -eo pid,pgid,sid,cmd | grep -E "node|nexus" | head'
```

### Real example: April 5, 2026 16:10:20 UTC

**Symptom**: nexus-api `inactive (dead)`, "Received SIGTERM" with no obvious cause. Down for 2 days before discovery.

**Findings**:
- nexus-api (PID 308927), nexus-mcp (PID 308928), and 4 bulk-runner workers all died at exactly `16:10:20 UTC`.
- nexus-worker (PID 308929) survived. Logged at the same instant: `[autoscaler] Scaling to zero: killing 4 workers`.
- All four were sibling node processes spawned with consecutive PIDs by systemd.
- No OOM, no cron, no deploy script.
- The autoscaler's `killAllWorkers()` calls `child.kill('SIGTERM')` on bulk-runner ChildProcess handles. With `detached: false`, those children share the parent's process group.

**Hypothesis**: When the autoscaler's kill loop fires, *something* in the kill path is hitting the wider process group (nexus-api + nexus-mcp share the systemd-supervised process tree). Root cause not yet definitively confirmed, but the temporal correlation is unambiguous.

**Mitigation**: Platform Health Watchdog (`nexus-health-watchdog.timer`, 60s interval) now detects any service going down within 60 seconds and sends a Pushover alert. Recovery is also alerted.

**Permanent fix (TODO)**: Audit `packages/worker/src/autoscaler.ts` and ensure spawned bulk-runners use `detached: true` + `setpgid(0,0)` so they're in their own process group, eliminating any cross-process-group signal possibility.

---

## Playbook: "Jobs are stuck pending and never run"

### Symptom
`tempo_jobs` has many rows in `status='pending'` with `created_at` from days ago. Worker logs show it's actively claiming and processing OTHER jobs.

### Investigation steps

**Step 1 — Check the executor column on stuck jobs.**
```sql
SELECT job_type, executor, count(*) FROM tempo_jobs WHERE status='pending' GROUP BY 1,2 ORDER BY count DESC;
```
The worker only claims jobs where `executor='nexus'`. If executor is `'tempo'` or NULL, those are LEGACY aria-tempo jobs that nothing will ever process.

**Step 2 — Check routing.**
```sql
SELECT job_type FROM nexus_job_routing WHERE job_type IN ('<stuck_job_types>');
```
If the job_type is missing from `nexus_job_routing`, the BEFORE INSERT trigger `route_nexus_jobs()` won't set `executor='nexus'` on new jobs.

**Step 3 — Fix.**
```sql
INSERT INTO nexus_job_routing (job_type) VALUES ('<job_type>') ON CONFLICT DO NOTHING;
UPDATE tempo_jobs SET executor='nexus' WHERE status='pending' AND job_type='<job_type>';
```
The trigger only runs on INSERT, not on existing rows — the `UPDATE` is needed to unstick already-pending rows.

### Real example: April 7, 2026

**Symptom**: 22 stuck jobs from up to 4 days ago (gmail-sync, topic-model, data-hygiene).

**Cause**: Those job types were missing from `nexus_job_routing`, so the trigger never set `executor='nexus'` on new inserts.

**Fix**: Added missing routings + UPDATE'd existing pending rows. All 22 cleared on the next worker tick.

---

## Playbook: "I created a tempo_job manually and it never runs"

### Symptom
You inserted into `tempo_jobs` from psql or a script, the row exists with `status='pending'`, but the worker never picks it up.

### Cause
Same as above — the `route_nexus_jobs()` trigger only sets `executor='nexus'` if the job_type was already in `nexus_job_routing` AT THE MOMENT OF INSERT. If you inserted before adding the routing, the row gets `executor='tempo'` (the default) and the worker filters it out.

### Fix
Either:
```sql
UPDATE tempo_jobs SET executor='nexus' WHERE id='<your_job_id>';
```
Or insert the routing first, THEN insert the job.

---

## Playbook: "Handler completes successfully but writes 0 rows"

### Symptom
A bulk handler (sentiment-backfill, knowledge-summarize, embed-backfill, etc.) is claiming jobs and the jobs are returning `status='completed'` — but the underlying data table is not gaining rows. Sometimes the result includes `processed: 0` or `entities_summarized: 0`. Sometimes the result looks normal but the row count doesn't move.

This is the most insidious failure mode in the platform — jobs *appear* healthy but accomplish nothing. Hours can pass before anyone notices the gap isn't closing.

### Investigation steps

**Step 1 — Verify the gap is real, not a join bug.**
Before chasing the handler, confirm your gap-counting query is correct. Many handlers use `guid` as `source_id` (not `id`):
```sql
-- WRONG (false-positive gap, never matches)
NOT EXISTS (SELECT 1 FROM aurora_sentiment s WHERE s.source='imessage' AND s.source_id = aurora_raw_imessage.id::text)
-- RIGHT
NOT EXISTS (SELECT 1 FROM aurora_sentiment s WHERE s.source='imessage' AND s.source_id = aurora_raw_imessage.guid)
```
Read the handler's `getChunk` query (or equivalent) to see exactly how `source_id` is populated. Use the same expression in your gap query.

**Step 2 — Check the actual job result, not just status.**
```sql
SELECT id, status, EXTRACT(EPOCH FROM (completed_at - claimed_at))::int AS dur,
       result->>'processed' AS processed,
       result->>'errors' AS errors,
       last_error
FROM tempo_jobs
WHERE job_type = '<handler>' AND status = 'completed'
  AND created_at > now() - interval '15 minutes'
ORDER BY completed_at DESC LIMIT 10;
```
If `processed = 0` consistently, the handler is reaching the LLM call but failing inside it. If `errors` > 0, the LLM call is throwing.

**Step 3 — Suspect #1: Anthropic batch API.**
If the handler uses `routeRequest({useBatch: true, ...})`, every Anthropic call goes to a 24-hour-pending batch queue. The job marks "complete" because the batch was *submitted*, not because the work *finished*. Check `grep -n useBatch packages/worker/src/handlers/<handler>.ts`. Switch to `useBatch: false` for real-time work.

**Step 4 — Suspect #2: hardcoded localhost in the LLM client.**
Many older handlers create their own OpenAI client pointing at a hardcoded `http://localhost:NNNN`. From the primary worker on Primary-Server, `localhost` resolves correctly. **From a Secondary-Server bulk worker, `localhost` resolves to Secondary-Server — and Secondary-Server doesn't run those backends.** The fetch call hangs, the consecutive-error counter hits its threshold, the chunk aborts with `processed: 0`. Audit:
```bash
grep -rn "localhost:\(8080\|8088\|8081\|8082\|8642\|7702\)" packages/worker/src/handlers/
```
Every match should also read an env var with a sensible default. Fix pattern:
```ts
// WRONG
const TOOL_SERVER = 'http://localhost:7702';
// RIGHT
const TOOL_SERVER = process.env.TOOL_SERVER_URL || 'http://localhost:7702';
```
Then ensure Secondary-Server's `.env` has the corresponding `*_URL` variables set to `http://primary-tb.example.io:NNNN/...` (resolves via direct Thunderbolt link).

**Step 5 — Suspect #3: missing API key on Secondary-Server.**
If a handler routes through `routeRequest` and falls back to a cloud provider, but Secondary-Server doesn't have the cloud provider's API key, the SDK *hangs* instead of erroring cleanly. Check:
```bash
ssh secondary-server 'sudo grep -E "ANTHROPIC_API_KEY|OPENAI_API_KEY|GOOGLE_GENAI_API_KEY|FORGE_API_KEY" /opt/nexus/Services/nexus/.env | sed "s/=.*/=***/"'
```
Secondary-Server needs ALL the same cloud keys Primary-Server has. Pull from secrets vault if any are missing.

**Step 6 — Suspect #4: LLM provider is rate-limited or jammed.**
If the handler IS reaching the LLM but the LLM is returning slowly:
- Anthropic Tier 1 is 50 RPM for Haiku — easy to exhaust under bulk load. Check if the same API key is being used by knowledge-summarize + sentiment-backfill simultaneously.
- The bulk slot (`localhost:8080`, the 70B model) gets saturated by agent cycles sending 5K-token prompts. Inference takes 60+ seconds per call when queued.
- **Move different handlers to different rate-limit pools.** Generation tier → Anthropic Haiku, classification tier → Google Gemini Flash Lite. They have independent quotas.

### Real example: April 7, 2026

**Symptom**: sentiment-backfill ran 6,077 jobs overnight, all "completed", `aurora_sentiment` row count gained only 148 rows (vs expected ~120K). Knowledge-summarize separately ran 9 jobs returning `entities_summarized: 0` each.

**Causes** (different bugs, same symptom):
1. `knowledge-summarize` had `useBatch: true` — submissions queued in Anthropic's 24-hour batch queue.
2. `sentiment-backfill` had a hardcoded `LLAMA_SERVER_URL` fallback to `localhost:8080`. From Secondary-Server, `localhost:8080` was nothing. The handler also targeted `model: 'qwen3.5-35b-a3b'` (the VIP slot).
3. Secondary-Server's `.env` was missing `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `GOOGLE_GENAI_API_KEY` — so any cloud fallback in the router hung silently.
4. The bulk slot `localhost:8080` was saturated with 5K-token agent cycles when handlers tried to use it.

**Fixes** (commits cb2aff5, 3ce1e9e, c86a38f, 4bc2cb0, d7aa774):
1. Switched `useBatch` to `false` in knowledge-summarize.
2. Refactored sentiment-backfill to use `routeRequest({taskTier:'classification'})` (router-driven).
3. Synced cloud API keys to Secondary-Server's `.env`.
4. Promoted Gemini Flash Lite to classification tier #0 in `llm_model_config` (different rate-limit pool from Haiku, 10× cheaper, faster for structured output).
5. Audited every handler for hardcoded-localhost — fixed `drive-ingest`, added `FORGE_VLM_URL` to Secondary-Server for `photo-describe-mcp`.

**Result**: sentiment-backfill went from 0 rows/min → ~150-275 rows/min sustained on a single big chain. iMessage backlog (81K records) cleared in ~15 min.

---

## Playbook: "Self-chaining handler chains keep dying"

### Symptom
A self-chaining handler (sentiment-backfill, embed-backfill, etc.) was running fine, processing chunks and self-chaining, but suddenly the chain dies. No new pending jobs, no errors, just nothing.

### Cause
The self-chain insert at the end of the handler is guarded by a `SELECT ... LIMIT 1` check that aborts the chain if any other job of the same type is already pending:
```ts
if (hasMore && !abort) {
  const { rows: existing } = await pool.query(
    `SELECT id FROM tempo_jobs WHERE job_type = '...' AND status = 'pending' LIMIT 1`,
  );
  if (existing.length === 0) {
    // INSERT next chunk
  } else {
    // SKIP — let the existing one continue
  }
}
```
When **multiple chains run in parallel** (e.g. you seeded 4 jobs at once), they all complete around the same time, all check for pending siblings, and at least one sees no pending → inserts → the others see that one and skip. Over 2-3 cycles, the chain count converges to 1, then often to 0 (race condition on the check vs insert).

### Fix
**Don't seed parallel chains.** Use ONE job with `chunk_size=1000` instead of 4 jobs with `chunk_size=50`. Single big chain runs continuously and the autoscaler keeps it busy. To resume after the chain dies, just enqueue ONE seed job:
```sql
INSERT INTO tempo_jobs (job_type, payload, status, priority, executor) VALUES
  ('sentiment-backfill', '{"channel":"imessage","chunk_size":1000,"triggered_by":"resume"}'::jsonb, 'pending', 5, 'nexus');
```

### Real example: April 7, 2026
Seeded `gemini-grind-imessage` + `gemini-grind-gmail` simultaneously to "saturate" 4 Secondary-Server workers. Each completed and checked for siblings. Over 2 cycles, both chains died. Single big chain at `chunk_size=1000` sustained 13 rows/sec for an hour straight without intervention.
