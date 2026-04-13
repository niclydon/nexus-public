/**
 * Pattern Maintenance — manages the anticipation_patterns lifecycle.
 *
 * Responsibilities:
 * 1. Decay: Reduce confidence of patterns not recently observed
 * 2. Promotion: Graduate high-confidence patterns to core_memory
 * 3. Cleanup: Deactivate patterns that have decayed below threshold
 */
import { getPool, createLogger } from '@nexus/core';
import { logEvent } from '../event-log.js';

const logger = createLogger('proactive-patterns');

const PROMOTION_THRESHOLD = 0.85;  // Promote to core_memory above this confidence
const DEACTIVATION_THRESHOLD = 0.1; // Deactivate below this confidence
const MIN_OBSERVATIONS_TO_PROMOTE = 5; // Need at least this many observations

interface PatternRow {
  id: string;
  pattern_type: string;
  description: string;
  confidence: number;
  observation_count: number;
  decay_rate: number;
  last_observed_at: Date;
  promoted_to_memory: boolean;
  memory_key: string | null;
}

/**
 * Apply confidence decay to all active patterns that haven't been
 * observed recently. Decay is proportional to time since last observation.
 */
async function decayPatterns(): Promise<number> {
  const pool = getPool();

  // Decay patterns not observed in the last 24 hours
  const result = await pool.query(
    `UPDATE anticipation_patterns
     SET confidence = GREATEST(0, confidence - decay_rate),
         updated_at = NOW()
     WHERE is_active = true
       AND last_observed_at < NOW() - INTERVAL '24 hours'
     RETURNING id`
  );

  return result.rowCount ?? 0;
}

/**
 * Promote high-confidence patterns to core_memory.
 * Uses the versioned memory system (insert new, supersede old).
 */
async function promotePatterns(): Promise<number> {
  const pool = getPool();
  const { rows } = await pool.query<PatternRow>(
    `SELECT * FROM anticipation_patterns
     WHERE is_active = true
       AND confidence >= $1
       AND observation_count >= $2
       AND promoted_to_memory = false`,
    [PROMOTION_THRESHOLD, MIN_OBSERVATIONS_TO_PROMOTE]
  );

  let promoted = 0;
  for (const pattern of rows) {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      const memoryKey = `pattern_${pattern.pattern_type}_${pattern.id.slice(0, 8)}`;
      const memoryValue = pattern.description;
      const category = 'recurring';

      // Check if key already exists
      const { rows: existing } = await client.query(
        `SELECT id, version FROM core_memory
         WHERE key = $1 AND superseded_by IS NULL`,
        [memoryKey]
      );

      const version = existing.length > 0 ? existing[0].version + 1 : 1;

      // Insert new memory
      const { rows: newRows } = await client.query(
        `INSERT INTO core_memory (category, key, value, version)
         VALUES ($1, $2, $3, $4)
         RETURNING id`,
        [category, memoryKey, memoryValue, version]
      );

      // Supersede old if exists
      if (existing.length > 0) {
        await client.query(
          `UPDATE core_memory SET superseded_by = $1 WHERE id = $2`,
          [newRows[0].id, existing[0].id]
        );
      }

      // Mark pattern as promoted
      await client.query(
        `UPDATE anticipation_patterns
         SET promoted_to_memory = true, memory_key = $1
         WHERE id = $2`,
        [memoryKey, pattern.id]
      );

      await client.query('COMMIT');
      promoted++;

      logger.log(`Promoted pattern to memory: ${memoryKey} = "${memoryValue}"`);
    } catch (err) {
      await client.query('ROLLBACK');
      logger.logMinimal(`Failed to promote pattern ${pattern.id}:`, err instanceof Error ? err.message : err);
    } finally {
      client.release();
    }
  }

  return promoted;
}

/**
 * Deactivate patterns that have decayed below the threshold.
 */
async function deactivatePatterns(): Promise<number> {
  const pool = getPool();
  const result = await pool.query(
    `UPDATE anticipation_patterns
     SET is_active = false, updated_at = NOW()
     WHERE is_active = true AND confidence < $1
     RETURNING id`,
    [DEACTIVATION_THRESHOLD]
  );
  return result.rowCount ?? 0;
}

/**
 * Aggregate recent feedback and adjust the global confidence threshold.
 * If users are dismissing >50% of insights, raise the threshold.
 * If users are accepting >80%, lower it slightly to surface more.
 */
async function adjustConfidenceThreshold(): Promise<{ adjusted: boolean; direction?: string }> {
  const pool = getPool();

  try {
    // Look at feedback from the last 7 days
    const { rows } = await pool.query<{ rating: number }>(
      `SELECT rating FROM anticipation_feedback
       WHERE created_at > NOW() - INTERVAL '7 days' AND rating IS NOT NULL`
    );

    if (rows.length < 5) return { adjusted: false }; // Need enough data

    const positive = rows.filter(r => r.rating === 1).length;
    const negative = rows.filter(r => r.rating === -1).length;
    const total = positive + negative;
    const acceptRate = positive / total;

    // Load current threshold
    const { rows: settings } = await pool.query(
      `SELECT value FROM proactive_settings WHERE key = 'confidence_threshold'`
    );
    const currentThreshold = (settings[0]?.value as number) ?? 0.7;

    let newThreshold = currentThreshold;
    let direction: string | undefined;

    if (acceptRate < 0.5 && currentThreshold < 0.95) {
      // Too many dismissals — raise threshold
      newThreshold = Math.min(0.95, currentThreshold + 0.05);
      direction = 'raised';
    } else if (acceptRate > 0.8 && currentThreshold > 0.4) {
      // High acceptance — lower threshold to surface more
      newThreshold = Math.max(0.4, currentThreshold - 0.03);
      direction = 'lowered';
    }

    if (newThreshold !== currentThreshold) {
      await pool.query(
        `UPDATE proactive_settings SET value = $1::jsonb WHERE key = 'confidence_threshold'`,
        [JSON.stringify(newThreshold)]
      );
      logger.log(`Confidence threshold ${direction}: ${currentThreshold} → ${newThreshold} (accept rate: ${Math.round(acceptRate * 100)}%)`);
      return { adjusted: true, direction };
    }

    return { adjusted: false };
  } catch {
    return { adjusted: false };
  }
}

/**
 * Accelerate decay for patterns that are linked to negatively-rated insights.
 * If a pattern has 2+ negative feedback in the last 30 days, increase its
 * decay_rate by 0.1 (capped at 1.0) and log to aria_self_improvement.
 */
async function accelerateNegativePatterns(): Promise<number> {
  const pool = getPool();

  const { rows } = await pool.query<{
    id: string;
    pattern_type: string;
    confidence: number;
    decay_rate: number;
    negative_count: string;
  }>(
    `SELECT ap.id, ap.pattern_type, ap.confidence, ap.decay_rate, COUNT(af.id) as negative_count
     FROM anticipation_patterns ap
     JOIN proactive_insights pi ON pi.category = ap.pattern_type
     JOIN anticipation_feedback af ON af.insight_id = pi.id AND af.rating = -1
     WHERE ap.is_active = true
       AND af.created_at >= NOW() - INTERVAL '30 days'
     GROUP BY ap.id, ap.pattern_type, ap.confidence, ap.decay_rate
     HAVING COUNT(af.id) >= 2`
  );

  for (const row of rows) {
    const negCount = parseInt(row.negative_count, 10);
    logger.logVerbose(`Pattern "${row.pattern_type}" (${row.id}) has ${negCount} negative ratings — accelerating decay`);

    await pool.query(
      `UPDATE anticipation_patterns SET decay_rate = LEAST(decay_rate + 0.1, 1.0) WHERE id = $1`,
      [row.id]
    );

    // Log to aria_self_improvement
    try {
      await pool.query(
        `INSERT INTO aria_self_improvement (category, status, title, description, source, confidence)
         VALUES ('proactive_tune', 'idea', $1, $2, 'pattern_maintenance', 0.8)`,
        [
          `Pattern receiving negative feedback: ${row.pattern_type}`,
          `Pattern has ${negCount} negative ratings in 30 days. Decay accelerated.`,
        ]
      );
    } catch (err) {
      logger.logMinimal('Failed to log to self_improvement:', err instanceof Error ? err.message : err);
    }
  }

  return rows.length;
}

/**
 * Decay confidence for AURORA correlations not validated in 24 hours.
 * Each correlation has its own decay_rate (default 0.01/day).
 */
async function decayAuroraCorrelations(): Promise<{ decayed: number; archived: number }> {
  const pool = getPool();

  const { rowCount: decayed } = await pool.query(
    `UPDATE aurora_correlations
     SET confidence = GREATEST(0, confidence - decay_rate)
     WHERE review_status NOT IN ('rejected', 'archived')
       AND last_validated < NOW() - INTERVAL '24 hours'
       AND confidence > 0`
  );

  const { rowCount: archived } = await pool.query(
    `UPDATE aurora_correlations
     SET review_status = 'archived'
     WHERE confidence < 0.1
       AND review_status NOT IN ('rejected', 'archived')`
  );

  return { decayed: decayed ?? 0, archived: archived ?? 0 };
}

/**
 * Decay confidence for AURORA behavioral DNA traits not updated in 24 hours.
 */
async function decayAuroraDna(): Promise<{ decayed: number; archived: number }> {
  const pool = getPool();

  const { rowCount: decayed } = await pool.query(
    `UPDATE aurora_behavioral_dna
     SET confidence = GREATEST(0, confidence - 0.005)
     WHERE active = true
       AND last_updated < NOW() - INTERVAL '24 hours'
       AND confidence > 0`
  );

  const { rowCount: archived } = await pool.query(
    `UPDATE aurora_behavioral_dna
     SET active = false, archived_at = NOW()
     WHERE confidence < 0.1 AND active = true`
  );

  return { decayed: decayed ?? 0, archived: archived ?? 0 };
}

/**
 * Run the full pattern maintenance cycle.
 */
export async function runPatternMaintenance(): Promise<Record<string, unknown>> {
  logger.logVerbose('runPatternMaintenance() entry');
  const stopTotal = logger.time('pattern-maintenance');

  const decayed = await decayPatterns();
  const promoted = await promotePatterns();
  const deactivated = await deactivatePatterns();
  const threshold = await adjustConfidenceThreshold();
  const accelerated = await accelerateNegativePatterns();

  // AURORA confidence decay
  let auroraCorr = { decayed: 0, archived: 0 };
  let auroraDna = { decayed: 0, archived: 0 };
  try {
    auroraCorr = await decayAuroraCorrelations();
    auroraDna = await decayAuroraDna();
    if (auroraCorr.decayed > 0 || auroraDna.decayed > 0) {
      logger.log(`AURORA decay: ${auroraCorr.decayed} correlations (${auroraCorr.archived} archived), ${auroraDna.decayed} DNA traits (${auroraDna.archived} archived)`);
    }
  } catch (err) {
    // AURORA tables may not exist yet — don't fail the whole job
    logger.logVerbose(`AURORA decay skipped: ${err instanceof Error ? err.message : err}`);
  }

  const summary = `Decayed ${decayed}, promoted ${promoted}, deactivated ${deactivated}, accelerated ${accelerated}${threshold.adjusted ? `, threshold ${threshold.direction}` : ''}`;
  logger.log(summary);
  stopTotal();

  logEvent({
    action: `Pattern maintenance: ${summary}`,
    component: 'proactive',
    category: 'self_maintenance',
    metadata: { decayed, promoted, deactivated, accelerated, threshold_adjusted: threshold.adjusted, threshold_direction: threshold.direction },
  });

  return {
    decayed, promoted, deactivated, accelerated, threshold_adjusted: threshold.adjusted,
    aurora_correlations_decayed: auroraCorr.decayed, aurora_correlations_archived: auroraCorr.archived,
    aurora_dna_decayed: auroraDna.decayed, aurora_dna_archived: auroraDna.archived,
  };
}
