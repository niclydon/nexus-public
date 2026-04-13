/**
 * Merge Review API Routes
 *
 * Endpoints for listing, merging, dismissing, and undoing entity merges.
 * Used by the Chancery merge review page.
 */
import { Router } from 'express';
import { query, entityMerge } from '@nexus/core';

const router = Router();

// GET /v1/merge-candidates — list candidates
router.get('/', async (req, res) => {
  try {
    const type = req.query.type as string | undefined;
    const status = (req.query.status as string) ?? 'pending';
    const minConfidence = parseFloat((req.query.min_confidence as string) ?? '0');
    const limit = Math.min(parseInt((req.query.limit as string) ?? '50'), 200);

    const typeFilter = type ? `AND mc.entity_type = '${type}'` : '';

    const { rows } = await query(
      `SELECT mc.*,
        CASE mc.entity_type
          WHEN 'person' THEN (SELECT display_name FROM aurora_social_identities WHERE id = mc.record_a_id::int)
          WHEN 'knowledge_entity' THEN (SELECT canonical_name FROM knowledge_entities WHERE id = mc.record_a_id::uuid)
          ELSE mc.record_a_id
        END as record_a_name,
        CASE mc.entity_type
          WHEN 'person' THEN (SELECT display_name FROM aurora_social_identities WHERE id = mc.record_b_id::int)
          WHEN 'knowledge_entity' THEN (SELECT canonical_name FROM knowledge_entities WHERE id = mc.record_b_id::uuid)
          ELSE mc.record_b_id
        END as record_b_name
       FROM merge_candidates mc
       WHERE mc.status = $1 AND mc.confidence >= $2 ${typeFilter}
       ORDER BY mc.confidence DESC
       LIMIT $3`,
      [status, minConfidence, limit],
    );

    res.json({ candidates: rows, total: rows.length });
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// GET /v1/merge-candidates/stats — counts by type and status
router.get('/stats', async (_req, res) => {
  try {
    const { rows } = await query(
      `SELECT entity_type, status, COUNT(*)::int as count
       FROM merge_candidates
       GROUP BY entity_type, status
       ORDER BY entity_type, status`,
    );
    res.json({ stats: rows });
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// GET /v1/merge-candidates/:id — single candidate with enriched data
router.get('/:id', async (req, res) => {
  try {
    const { rows } = await query(
      `SELECT * FROM merge_candidates WHERE id = $1`,
      [req.params.id],
    );
    if (rows.length === 0) return res.status(404).json({ error: 'Not found' });

    const candidate = rows[0] as Record<string, unknown>;

    // Enrich with record details based on entity type
    if (candidate.entity_type === 'person') {
      const [aResult, bResult] = await Promise.all([
        query(`SELECT si.*, (SELECT COUNT(*) FROM aurora_social_identity_links WHERE identity_id = si.id) as link_count,
               (SELECT COUNT(*) FROM aurora_relationships WHERE identity_id = si.id) as rel_count
               FROM aurora_social_identities si WHERE id = $1`, [candidate.record_a_id]),
        query(`SELECT si.*, (SELECT COUNT(*) FROM aurora_social_identity_links WHERE identity_id = si.id) as link_count,
               (SELECT COUNT(*) FROM aurora_relationships WHERE identity_id = si.id) as rel_count
               FROM aurora_social_identities si WHERE id = $1`, [candidate.record_b_id]),
      ]);
      candidate.record_a = aResult.rows[0];
      candidate.record_b = bResult.rows[0];
    } else if (candidate.entity_type === 'knowledge_entity') {
      const [aResult, bResult] = await Promise.all([
        query(`SELECT * FROM knowledge_entities WHERE id = $1`, [candidate.record_a_id]),
        query(`SELECT * FROM knowledge_entities WHERE id = $1`, [candidate.record_b_id]),
      ]);
      candidate.record_a = aResult.rows[0];
      candidate.record_b = bResult.rows[0];
    }

    res.json(candidate);
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// POST /v1/merge-candidates/:id/merge — execute merge
router.post('/:id/merge', async (req, res) => {
  try {
    const { rows } = await query(
      `SELECT * FROM merge_candidates WHERE id = $1 AND status = 'pending'`,
      [req.params.id],
    );
    if (rows.length === 0) return res.status(404).json({ error: 'Candidate not found or already processed' });

    const candidate = rows[0] as Record<string, unknown>;
    const { keeper_id, field_choices } = req.body;

    const keeperId = keeper_id ?? candidate.record_a_id;
    const mergedId = String(keeperId) === String(candidate.record_a_id) ? candidate.record_b_id : candidate.record_a_id;

    const result = await entityMerge.executeMerge({
      candidateId: req.params.id,
      keeperId: String(keeperId),
      mergedId: String(mergedId),
      entityType: String(candidate.entity_type),
      fieldChoices: field_choices,
      mergedBy: 'owner',
    });

    if (result.success) {
      res.json(result);
    } else {
      res.status(400).json(result);
    }
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// POST /v1/merge-candidates/:id/dismiss
router.post('/:id/dismiss', async (req, res) => {
  try {
    await entityMerge.dismissCandidate(req.params.id);
    res.json({ status: 'dismissed' });
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// POST /v1/merge-candidates/:id/defer
router.post('/:id/defer', async (req, res) => {
  try {
    await entityMerge.deferCandidate(req.params.id);
    res.json({ status: 'deferred' });
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// POST /v1/merge-candidates/batch-merge — merge all above threshold
router.post('/batch-merge', async (req, res) => {
  try {
    const minConfidence = parseFloat(req.body.min_confidence ?? '0.95');
    const entityType = req.body.entity_type as string | undefined;

    const typeFilter = entityType ? `AND entity_type = '${entityType}'` : '';
    const { rows } = await query(
      `SELECT id, entity_type, record_a_id, record_b_id FROM merge_candidates
       WHERE status = 'pending' AND confidence >= $1 ${typeFilter}
       ORDER BY confidence DESC LIMIT 100`,
      [minConfidence],
    );

    let merged = 0;
    let errors = 0;
    for (const row of rows as Array<Record<string, unknown>>) {
      const result = await entityMerge.executeMerge({
        candidateId: String(row.id),
        keeperId: String(row.record_a_id),
        mergedId: String(row.record_b_id),
        entityType: String(row.entity_type),
        mergedBy: 'owner',
      });
      if (result.success) merged++;
      else errors++;
    }

    res.json({ merged, errors, total: rows.length });
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

export default router;

// ── Merge Audit Router ──────────────────────────────────────

export const mergeAuditRouter = Router();

// GET /v1/merge-audit — recent merges
mergeAuditRouter.get('/', async (req, res) => {
  try {
    const limit = Math.min(parseInt((req.query.limit as string) ?? '20'), 100);
    const { rows } = await query(
      `SELECT * FROM merge_audit_log ORDER BY merged_at DESC LIMIT $1`,
      [limit],
    );
    res.json({ audits: rows });
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// POST /v1/merge-audit/:id/undo
mergeAuditRouter.post('/:id/undo', async (req, res) => {
  try {
    const result = await entityMerge.undoMerge(req.params.id);
    if (result.success) {
      res.json(result);
    } else {
      res.status(400).json(result);
    }
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});
