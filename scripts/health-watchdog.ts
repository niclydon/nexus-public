/**
 * health-watchdog.ts
 *
 * Platform Health Alerting watchdog. Runs once per invocation (driven by
 * nexus-health-watchdog.timer every 60s). For every row in
 * platform_health_targets WHERE enabled = true:
 *
 *   1. Run the appropriate check (http / systemd / tcp / psql)
 *   2. Record the result in platform_health_checks (raw history)
 *   3. UPSERT platform_health_state (current status + consecutive failures)
 *   4. On state transitions, fire alerts:
 *        - down → ok    : send "recovered" notification
 *        - ok → down    : after `alert_after_failures` consecutive failures, send alert
 *        - down → down  : re-alert at most every 30 minutes (resend window)
 *
 * Alerts go via Pushover (per CLAUDE.md user pref). All alerts logged to
 * platform_health_alerts for audit/dedup.
 *
 * Required env: DATABASE_URL, PUSHOVER_APP_TOKEN, PUSHOVER_USER_KEY
 * Usage: npx tsx scripts/health-watchdog.ts
 */

import 'dotenv/config';
import pg from 'pg';
import * as net from 'node:net';
import { execSync } from 'node:child_process';

if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL not set');

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 4 });
const RESEND_WINDOW_MS = 30 * 60 * 1000; // re-alert at most every 30 min while still down

const PUSHOVER_TOKEN = process.env.PUSHOVER_APP_TOKEN;
const PUSHOVER_USER  = process.env.PUSHOVER_USER_KEY;

interface Target {
  service: string;
  display_name: string;
  check_type: 'http' | 'systemd' | 'tcp' | 'psql' | 'command';
  endpoint: string;
  expected: string;
  timeout_ms: number;
  severity_on_down: 'critical' | 'warning';
  alert_after_failures: number;
  mute_until: Date | null;
}

interface CheckResult {
  status: 'ok' | 'down' | 'degraded' | 'unknown';
  latency_ms: number;
  detail: string;
}

// ─────────────────── checks ───────────────────

async function checkHttp(t: Target): Promise<CheckResult> {
  const start = Date.now();
  try {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), t.timeout_ms);
    const res = await fetch(t.endpoint, { signal: ctrl.signal });
    clearTimeout(timer);
    const elapsed = Date.now() - start;
    const expected = parseInt(t.expected || '200', 10);
    if (res.status === expected) {
      return { status: 'ok', latency_ms: elapsed, detail: `${res.status} ${res.statusText}` };
    }
    return { status: 'down', latency_ms: elapsed, detail: `HTTP ${res.status} (expected ${expected})` };
  } catch (e: any) {
    return { status: 'down', latency_ms: Date.now() - start, detail: e.message || 'fetch failed' };
  }
}

function checkSystemd(t: Target): CheckResult {
  const start = Date.now();
  // expected defaults to "active" (this service should be running). Set to
  // "inactive" for zombie units that should stay disabled — we want to alert
  // if they ever turn back on (e.g. legacy forge.service crash loop).
  const wantState = (t.expected || 'active').trim();
  let out: string;
  try {
    out = execSync(`systemctl is-active ${t.endpoint}`, { encoding: 'utf-8', timeout: t.timeout_ms }).trim();
  } catch (e: any) {
    out = (e.stdout?.toString().trim() || e.message || 'unknown').trim();
  }
  const elapsed = Date.now() - start;
  if (out === wantState) return { status: 'ok', latency_ms: elapsed, detail: out };
  return { status: 'down', latency_ms: elapsed, detail: `${out} (expected ${wantState})` };
}

async function checkTcp(t: Target): Promise<CheckResult> {
  const start = Date.now();
  const [host, portStr] = t.endpoint.split(':');
  const port = parseInt(portStr, 10);
  return new Promise((resolve) => {
    const sock = new net.Socket();
    let done = false;
    const finish = (status: CheckResult['status'], detail: string) => {
      if (done) return;
      done = true;
      sock.destroy();
      resolve({ status, latency_ms: Date.now() - start, detail });
    };
    sock.setTimeout(t.timeout_ms);
    sock.once('connect', () => finish('ok', 'open'));
    sock.once('timeout', () => finish('down', 'timeout'));
    sock.once('error', (e) => finish('down', e.message));
    sock.connect(port, host || 'localhost');
  });
}

async function checkPsql(t: Target): Promise<CheckResult> {
  const start = Date.now();
  try {
    const r = await pool.query('SELECT 1');
    if (r.rows.length === 1) return { status: 'ok', latency_ms: Date.now() - start, detail: 'connected' };
    return { status: 'down', latency_ms: Date.now() - start, detail: 'unexpected response' };
  } catch (e: any) {
    return { status: 'down', latency_ms: Date.now() - start, detail: e.message };
  }
}

/**
 * Shell command check. The command in `endpoint` is executed and its stdout
 * is compared against `expected`. Expected format:
 *
 *   max:N       — numeric, down if parsed output > N     (e.g. disk 70 vs max:85)
 *   min:N       — numeric, down if parsed output < N     (e.g. free RAM > threshold)
 *   contains:S  — down if output does not include S
 *   equals:S    — down unless output trim-equals S
 *   <empty>     — down if exit code != 0 (just run it)
 *
 * The first numeric token in stdout is used for numeric comparisons, so
 * outputs like "70" or "70%" both parse as 70.
 */
function checkCommand(t: Target): CheckResult {
  const start = Date.now();
  let stdout = '';
  let exitCode = 0;
  try {
    stdout = execSync(t.endpoint, {
      encoding: 'utf-8',
      timeout: t.timeout_ms,
      shell: '/bin/bash',
      stdio: ['ignore', 'pipe', 'pipe'],
    }).trim();
  } catch (e: any) {
    exitCode = e.status ?? 1;
    stdout = (e.stdout?.toString() ?? '').trim();
    if (!t.expected) {
      return {
        status: 'down',
        latency_ms: Date.now() - start,
        detail: `exit ${exitCode}: ${e.stderr?.toString().trim() || e.message || 'command failed'}`.slice(0, 300),
      };
    }
    // Continue to threshold comparison below using whatever stdout we got.
  }
  const elapsed = Date.now() - start;

  const expected = (t.expected || '').trim();
  if (!expected) {
    return exitCode === 0
      ? { status: 'ok', latency_ms: elapsed, detail: stdout.slice(0, 200) || 'exit 0' }
      : { status: 'down', latency_ms: elapsed, detail: `exit ${exitCode}` };
  }

  const [kind, ...rest] = expected.split(':');
  const rhs = rest.join(':');

  if (kind === 'max' || kind === 'min') {
    const threshold = parseFloat(rhs);
    const match = stdout.match(/-?\d+(?:\.\d+)?/);
    if (!match) {
      return { status: 'down', latency_ms: elapsed, detail: `no numeric output: "${stdout.slice(0, 100)}"` };
    }
    const value = parseFloat(match[0]);
    const ok = kind === 'max' ? value <= threshold : value >= threshold;
    return {
      status: ok ? 'ok' : 'down',
      latency_ms: elapsed,
      detail: `${value} ${kind === 'max' ? '<=' : '>='} ${threshold}? ${ok ? 'yes' : 'no'}`,
    };
  }

  if (kind === 'contains') {
    return stdout.includes(rhs)
      ? { status: 'ok', latency_ms: elapsed, detail: `contains "${rhs}"` }
      : { status: 'down', latency_ms: elapsed, detail: `missing "${rhs}": ${stdout.slice(0, 150)}` };
  }

  if (kind === 'equals') {
    return stdout === rhs
      ? { status: 'ok', latency_ms: elapsed, detail: stdout.slice(0, 200) }
      : { status: 'down', latency_ms: elapsed, detail: `got "${stdout.slice(0, 100)}", want "${rhs.slice(0, 100)}"` };
  }

  // Unknown kind — treat as exact string match on the whole expected value.
  return stdout === expected
    ? { status: 'ok', latency_ms: elapsed, detail: stdout.slice(0, 200) }
    : { status: 'down', latency_ms: elapsed, detail: `got "${stdout.slice(0, 100)}", want "${expected.slice(0, 100)}"` };
}

async function runCheck(t: Target): Promise<CheckResult> {
  switch (t.check_type) {
    case 'http':    return checkHttp(t);
    case 'systemd': return checkSystemd(t);
    case 'tcp':     return checkTcp(t);
    case 'psql':    return checkPsql(t);
    case 'command': return checkCommand(t);
    default:        return { status: 'unknown', latency_ms: 0, detail: `unknown check_type: ${t.check_type}` };
  }
}

// ─────────────────── alerting ───────────────────

async function sendPushover(title: string, message: string, priority: number): Promise<{ ok: boolean; error?: string }> {
  if (!PUSHOVER_TOKEN || !PUSHOVER_USER) {
    return { ok: false, error: 'PUSHOVER credentials not set' };
  }
  try {
    const res = await fetch('https://api.pushover.net/1/messages.json', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        token: PUSHOVER_TOKEN,
        user: PUSHOVER_USER,
        title,
        message,
        priority: String(priority),
      }),
    });
    if (!res.ok) return { ok: false, error: `pushover ${res.status}` };
    return { ok: true };
  } catch (e: any) {
    return { ok: false, error: e.message };
  }
}

async function fireAlert(
  service: string,
  severity: 'critical' | 'warning' | 'recovery',
  title: string,
  body: string,
  fingerprint: string,
): Promise<void> {
  // Pushover priority: critical=1 (sound), warning=0 (normal), recovery=-1 (silent)
  const priority = severity === 'critical' ? 1 : severity === 'warning' ? 0 : -1;
  const result = await sendPushover(title, body, priority);
  await pool.query(
    `INSERT INTO platform_health_alerts (service, severity, title, body, fingerprint, delivered_to, delivery_ok, delivery_error)
     VALUES ($1, $2, $3, $4, $5, ARRAY['pushover'], $6, $7)`,
    [service, severity, title, body, fingerprint, result.ok, result.error || null],
  );
}

// ─────────────────── main ───────────────────

async function processTarget(t: Target): Promise<{ service: string; status: string; transition?: string }> {
  // Skip muted services
  if (t.mute_until && new Date(t.mute_until) > new Date()) {
    return { service: t.service, status: 'muted' };
  }

  const r = await runCheck(t);

  // Record raw check
  await pool.query(
    `INSERT INTO platform_health_checks (service, check_type, status, latency_ms, detail) VALUES ($1, $2, $3, $4, $5)`,
    [t.service, t.check_type, r.status, r.latency_ms, r.detail.slice(0, 500)],
  );

  // Pull current state
  const stateRes = await pool.query(
    `SELECT current_status, status_since, consecutive_failures, last_alert_sent_at, last_alert_status FROM platform_health_state WHERE service = $1`,
    [t.service],
  );
  const prev = stateRes.rows[0];
  const isUp = r.status === 'ok';
  const wasUp = prev?.current_status === 'ok';

  // New state values
  const consecutiveFailures = isUp ? 0 : (prev?.consecutive_failures || 0) + 1;
  const newStatusSince = (prev && prev.current_status === r.status)
    ? prev.status_since
    : new Date();

  let transition: string | undefined;
  let alertFired = false;

  // First-time-seen-as-down: alert immediately so freshly-detected outages aren't silent
  if (!prev && !isUp && consecutiveFailures >= t.alert_after_failures) {
    transition = 'first-seen→down';
    const title = `[${t.severity_on_down.toUpperCase()}] ${t.display_name} is DOWN`;
    const body = `Service: ${t.service}\nCheck: ${t.check_type} ${t.endpoint}\nDetail: ${r.detail}\nFirst observation`;
    await fireAlert(t.service, t.severity_on_down, title, body, `${t.service}|down`);
    alertFired = true;
  }
  // Up -> Down: alert when consecutive_failures hits the threshold
  else if (wasUp && !isUp && consecutiveFailures >= t.alert_after_failures) {
    transition = 'up→down';
    const title = `[${t.severity_on_down.toUpperCase()}] ${t.display_name} is DOWN`;
    const body = `Service: ${t.service}\nCheck: ${t.check_type} ${t.endpoint}\nDetail: ${r.detail}\nConsecutive failures: ${consecutiveFailures}`;
    await fireAlert(t.service, t.severity_on_down, title, body, `${t.service}|down`);
    alertFired = true;
  }
  // Down -> Up: send recovery notification
  else if (!wasUp && isUp && prev) {
    transition = 'down→up';
    const downSince = prev.status_since ? new Date(prev.status_since) : null;
    const downFor = downSince ? Math.round((Date.now() - downSince.getTime()) / 1000) : 0;
    const title = `[OK] ${t.display_name} recovered`;
    const body = `Service: ${t.service} is back up.\nDown for: ${downFor < 60 ? downFor + 's' : Math.round(downFor / 60) + 'm'}\nLatency: ${r.latency_ms}ms`;
    await fireAlert(t.service, 'recovery', title, body, `${t.service}|recovery`);
    alertFired = true;
    // Resolve any unresolved down alerts for this service
    await pool.query(
      `UPDATE platform_health_alerts SET resolved_at = now() WHERE service = $1 AND severity != 'recovery' AND resolved_at IS NULL`,
      [t.service],
    );
  }
  // Still Down: re-alert after RESEND_WINDOW_MS
  else if (!isUp && consecutiveFailures >= t.alert_after_failures && prev?.last_alert_sent_at) {
    const sinceLast = Date.now() - new Date(prev.last_alert_sent_at).getTime();
    if (sinceLast > RESEND_WINDOW_MS && prev.last_alert_status !== 'recovery') {
      transition = 'down→down (resend)';
      const downFor = Math.round((Date.now() - new Date(prev.status_since).getTime()) / 60000);
      const title = `[STILL DOWN] ${t.display_name} (${downFor}m)`;
      const body = `Service: ${t.service}\nDown for: ${downFor} minutes\nLast detail: ${r.detail}`;
      await fireAlert(t.service, t.severity_on_down, title, body, `${t.service}|down|resend`);
      alertFired = true;
    }
  }

  // UPSERT state
  await pool.query(
    `INSERT INTO platform_health_state (service, current_status, status_since, last_checked_at, consecutive_failures, last_alert_sent_at, last_alert_status, detail)
     VALUES ($1, $2, $3, now(), $4, $5, $6, $7)
     ON CONFLICT (service) DO UPDATE SET
       current_status = EXCLUDED.current_status,
       status_since = EXCLUDED.status_since,
       last_checked_at = now(),
       consecutive_failures = EXCLUDED.consecutive_failures,
       last_alert_sent_at = COALESCE(EXCLUDED.last_alert_sent_at, platform_health_state.last_alert_sent_at),
       last_alert_status = COALESCE(EXCLUDED.last_alert_status, platform_health_state.last_alert_status),
       detail = EXCLUDED.detail,
       updated_at = now()`,
    [
      t.service,
      r.status,
      newStatusSince,
      consecutiveFailures,
      alertFired ? new Date() : (prev?.last_alert_sent_at || null),
      alertFired ? (transition?.includes('recovery') ? 'recovery' : t.severity_on_down) : (prev?.last_alert_status || null),
      r.detail.slice(0, 500),
    ],
  );

  return { service: t.service, status: r.status, transition };
}

async function main() {
  // --force bypasses the Creative Mode short-circuit. Used for manual
  // verification runs where we WANT the checks to execute even while
  // services are deliberately paused.
  const force = process.argv.includes('--force');

  // Creative Mode short-circuit. When the platform is intentionally in
  // creative mode (always-on inference services shut down to free GPU for
  // image/video gen), skip all checks so we don't spam Pushover with alerts
  // for services we just stopped on purpose. Per-target mute_until is still
  // honored as a secondary mechanism inside processTarget().
  if (!force) {
    try {
      const modeRes = await pool.query<{ mode: string; started_at: Date | null; started_by: string | null; reason: string | null }>(
        `SELECT mode, started_at, started_by, reason FROM platform_modes WHERE id = 1`
      );
      const m = modeRes.rows[0];
      if (m && m.mode === 'creative') {
        const since = m.started_at ? new Date(m.started_at).toISOString() : 'unknown';
        console.log(`[health-watchdog] creative mode active (since ${since}, by ${m.started_by || 'unknown'}, reason: ${m.reason || 'n/a'}) — skipping all checks`);
        await pool.end();
        return;
      }
    } catch (e: any) {
      // platform_modes table not present yet (pre-migration 046) — continue normally
      if (!/relation .*platform_modes.* does not exist/i.test(e.message || '')) {
        console.warn('[health-watchdog] platform_modes lookup failed:', e.message);
      }
    }
  } else {
    console.log('[health-watchdog] --force: bypassing creative mode check');
  }

  const targets = await pool.query<Target>(
    `SELECT service, display_name, check_type, endpoint, expected, timeout_ms,
            severity_on_down, alert_after_failures, mute_until
     FROM platform_health_targets WHERE enabled = true ORDER BY service`
  );

  const results = await Promise.all(targets.rows.map((t) => processTarget(t)));
  const summary: Record<string, number> = {};
  results.forEach((r) => { summary[r.status] = (summary[r.status] || 0) + 1; });

  const transitions = results.filter((r) => r.transition);
  const summaryStr = Object.entries(summary).map(([k, v]) => `${k}=${v}`).join(' ');
  if (transitions.length) {
    console.log(`[health-watchdog] ${summaryStr} | transitions: ${transitions.map((t) => `${t.service}:${t.transition}`).join(', ')}`);
  } else {
    console.log(`[health-watchdog] ${summaryStr}`);
  }

  await pool.end();
}

main().catch((e) => {
  console.error('[health-watchdog] failed:', e);
  process.exit(1);
});
