import { Router } from 'express';
import { semanticSearch, createLogger } from '@nexus/core';

const logger = createLogger('routes/search');
const router = Router();

// GET /v1/search?q=...&categories=...&limit=...&rerank=...
router.get('/', async (req, res) => {
  try {
    const q = req.query.q as string;
    if (!q || q.trim().length === 0) {
      res.status(400).json({ error: 'Missing required query parameter: q' });
      return;
    }

    const categories = req.query.categories
      ? (req.query.categories as string).split(',').map(c => c.trim()) as semanticSearch.SearchCategory[]
      : undefined;

    const limit = req.query.limit ? parseInt(req.query.limit as string, 10) : undefined;
    const rerank = req.query.rerank !== undefined ? req.query.rerank !== 'false' : undefined;
    const minSimilarity = req.query.min_similarity ? parseFloat(req.query.min_similarity as string) : undefined;

    const result = await semanticSearch.search({
      query: q,
      categories,
      limit,
      rerank,
      minSimilarity,
    });

    res.json(result);
  } catch (err) {
    logger.logMinimal('Search error:', (err as Error).message);
    res.status(500).json({ error: 'Search failed', detail: (err as Error).message });
  }
});

// POST /v1/search — same but with JSON body (for longer queries)
router.post('/', async (req, res) => {
  try {
    const { query: q, categories, limit, rerank, min_similarity } = req.body as {
      query?: string;
      categories?: semanticSearch.SearchCategory[];
      limit?: number;
      rerank?: boolean;
      min_similarity?: number;
    };

    if (!q || q.trim().length === 0) {
      res.status(400).json({ error: 'Missing required field: query' });
      return;
    }

    const result = await semanticSearch.search({
      query: q,
      categories,
      limit,
      rerank,
      minSimilarity: min_similarity,
    });

    res.json(result);
  } catch (err) {
    logger.logMinimal('Search error:', (err as Error).message);
    res.status(500).json({ error: 'Search failed', detail: (err as Error).message });
  }
});

export default router;
