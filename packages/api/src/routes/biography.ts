/**
 * Biography API Routes
 *
 * Endpoints for the biographical interview module: inference queue
 * review, decision handling with learning loop, life chapters, and stats.
 */
import { Router } from 'express';
import { query } from '@nexus/core';

const router = Router();

// GET /v1/biography/queue — pending inferences sorted by priority/confidence
router.get('/queue', async (req, res) => {
  try {
    const status = (req.query.status as string) ?? 'pending';
    const limit = Math.min(parseInt((req.query.limit as string) ?? '50'), 200);
    const inferenceType = req.query.type as string | undefined;

    const typeFilter = inferenceType ? 'AND biq.inference_type = $3' : '';
    const params: unknown[] = [status, limit];
    if (inferenceType) params.push(inferenceType);

    const { rows } = await query(
      `SELECT biq.*,
        sp.display_name AS subject_name,
        rp.display_name AS related_name,
        bir.rule_key, bir.description AS rule_description
       FROM bio_inference_queue biq
       LEFT JOIN aurora_social_identities sp ON sp.id = biq.subject_person_id
       LEFT JOIN aurora_social_identities rp ON rp.id = biq.related_person_id
       LEFT JOIN bio_inference_rules bir ON bir.id = biq.rule_id
       WHERE biq.status = $1 ${typeFilter}
       ORDER BY biq.priority DESC, biq.confidence DESC
       LIMIT $2`,
      params,
    );

    res.json({ inferences: rows, total: rows.length });
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// GET /v1/biography/queue/:id — single inference with evidence
router.get('/queue/:id', async (req, res) => {
  try {
    const { rows } = await query(
      `SELECT biq.*,
        sp.display_name AS subject_name,
        rp.display_name AS related_name,
        bir.rule_key, bir.description AS rule_description,
        bir.confidence_boost AS rule_boost
       FROM bio_inference_queue biq
       LEFT JOIN aurora_social_identities sp ON sp.id = biq.subject_person_id
       LEFT JOIN aurora_social_identities rp ON rp.id = biq.related_person_id
       LEFT JOIN bio_inference_rules bir ON bir.id = biq.rule_id
       WHERE biq.id = $1`,
      [req.params.id],
    );

    if (rows.length === 0) return res.status(404).json({ error: 'Not found' });
    res.json(rows[0]);
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// POST /v1/biography/queue/:id/decide — confirm, correct, reject, or skip
router.post('/queue/:id/decide', async (req, res) => {
  try {
    const { status, notes } = req.body as { status: string; notes?: string };

    if (!['confirmed', 'corrected', 'rejected', 'skipped'].includes(status)) {
      return res.status(400).json({ error: 'status must be confirmed, corrected, rejected, or skipped' });
    }

    // Fetch the inference
    const { rows: inferences } = await query(
      `SELECT * FROM bio_inference_queue WHERE id = $1`,
      [req.params.id],
    );
    if (inferences.length === 0) return res.status(404).json({ error: 'Not found' });

    const inference = inferences[0] as Record<string, unknown>;

    if (inference.status !== 'pending') {
      return res.status(400).json({ error: `Inference already decided: ${inference.status}` });
    }

    // Update the inference
    await query(
      `UPDATE bio_inference_queue
       SET status = $1, user_notes = $2, decided_at = NOW(), updated_at = NOW()
       WHERE id = $3`,
      [status, notes ?? null, req.params.id],
    );

    // Learning loop: adjust rule confidence_boost
    if (inference.rule_id && status !== 'skipped') {
      const column = status === 'confirmed' ? 'confirmed_count'
        : status === 'corrected' ? 'corrected_count'
        : 'rejected_count';

      const boost = status === 'confirmed' ? 0.02
        : status === 'corrected' ? -0.03
        : -0.05;

      await query(
        `UPDATE bio_inference_rules
         SET ${column} = ${column} + 1,
             confidence_boost = GREATEST(confidence_boost + $1, -0.5),
             updated_at = NOW()
         WHERE id = $2`,
        [boost, inference.rule_id],
      );

      // Auto-disable rules with <30% accuracy after 10+ applications
      await query(
        `UPDATE bio_inference_rules
         SET is_active = FALSE, updated_at = NOW()
         WHERE id = $1
           AND total_applied >= 10
           AND confirmed_count::real / GREATEST(total_applied, 1) < 0.3`,
        [inference.rule_id],
      );
    }

    // On confirm: write to person_connections or knowledge_facts
    if (status === 'confirmed') {
      const inferenceType = inference.inference_type as string;

      if (inferenceType === 'relationship' || inferenceType === 'connection') {
        const subjectId = inference.subject_person_id as number | null;
        const relatedId = inference.related_person_id as number | null;
        const hypothesis = inference.hypothesis as string;

        if (subjectId) {
          // Extract relationship type from hypothesis
          const relMatch = hypothesis.match(/likely (?:a )?(\w+)/i);
          const relationship = relMatch ? relMatch[1] : 'unknown';

          if (relatedId) {
            // Insert bidirectional person_connections
            await query(
              `INSERT INTO person_connections (person_id, related_person_id, relationship)
               VALUES ($1, $2, $3)
               ON CONFLICT DO NOTHING`,
              [subjectId, relatedId, relationship],
            );
          } else {
            // Store as knowledge fact
            await query(
              `INSERT INTO knowledge_facts (category, subject, predicate, object, confidence, source)
               VALUES ('biography', $1, 'has_relationship', $2, $3, 'biographical-interview')`,
              [
                (inference as Record<string, unknown>).subject_person_id?.toString() ?? 'unknown',
                hypothesis,
                inference.confidence,
              ],
            );
          }
        }
      } else if (inferenceType === 'life_event' || inferenceType === 'timeline') {
        // Store as knowledge fact
        await query(
          `INSERT INTO knowledge_facts (category, subject, predicate, object, confidence, source)
           VALUES ('biography', $1, $2, $3, $4, 'biographical-interview')`,
          [
            (inference.subject_person_id as number)?.toString() ?? 'unknown',
            inferenceType === 'life_event' ? 'life_event' : 'timeline_marker',
            inference.hypothesis,
            inference.confidence,
          ],
        );
      }
    }

    res.json({ status: 'ok', inference_id: req.params.id, decision: status });
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// GET /v1/biography/chapters — life chapters with enriched fields
router.get('/chapters', async (_req, res) => {
  try {
    const { rows } = await query(
      `SELECT lc.*,
        (SELECT array_agg(s.display_name)
         FROM aurora_social_identities s
         WHERE s.id = ANY(lc.key_people)) AS key_people_names
       FROM aurora_life_chapters lc
       ORDER BY lc.start_date DESC`,
    );

    res.json({ chapters: rows, total: rows.length });
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// GET /v1/biography/stats — counts, accuracy, rule performance
router.get('/stats', async (_req, res) => {
  try {
    // Queue status counts
    const { rows: statusCounts } = await query(
      `SELECT status, COUNT(*)::int AS count
       FROM bio_inference_queue
       GROUP BY status
       ORDER BY status`,
    );

    // By inference type
    const { rows: typeCounts } = await query(
      `SELECT inference_type, status, COUNT(*)::int AS count
       FROM bio_inference_queue
       GROUP BY inference_type, status
       ORDER BY inference_type, status`,
    );

    // Rule performance
    const { rows: rules } = await query(
      `SELECT rule_key, description, is_active, confidence_boost,
              total_applied, confirmed_count, corrected_count, rejected_count,
              CASE WHEN total_applied > 0
                THEN ROUND(confirmed_count::numeric / total_applied, 3)
                ELSE 0 END AS accuracy
       FROM bio_inference_rules
       ORDER BY total_applied DESC`,
    );

    // Recent activity
    const { rows: recentBatches } = await query(
      `SELECT batch_id, COUNT(*)::int AS count,
              MIN(created_at) AS started_at,
              AVG(confidence)::real AS avg_confidence
       FROM bio_inference_queue
       GROUP BY batch_id
       ORDER BY MIN(created_at) DESC
       LIMIT 10`,
    );

    res.json({
      queue: statusCounts,
      by_type: typeCounts,
      rules,
      recent_batches: recentBatches,
    });
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

export default router;
