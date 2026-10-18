import { Router } from 'express';
import { semanticSearch, createLogger } from '@nexus/core';

const logger = createLogger('routes/search');
const router = Router();

// GET /v1/search?q=...&categories=...&limit=...&rerank=...
router.get('/', async (req, res) => {
  try {
    // Express query values can be arrays or objects when a caller sends
    // q[]=x style parameters; validate real types instead of casting.
    const rawQ: unknown = req.query.q;
    if (typeof rawQ !== 'string' || rawQ.trim().length === 0) {
      res.status(400).json({ error: 'Missing required query parameter: q' });
      return;
    }
    const q: string = rawQ;

    const rawCategories: unknown = req.query.categories;
    const categories = typeof rawCategories === 'string'
      ? rawCategories.split(',').map(c => c.trim()) as semanticSearch.SearchCategory[]
      : undefined;

    const rawLimit: unknown = req.query.limit;
    const limit = typeof rawLimit === 'string' ? parseInt(rawLimit, 10) : undefined;
    const rerank = req.query.rerank !== undefined ? req.query.rerank !== 'false' : undefined;
    const rawMinSim: unknown = req.query.min_similarity;
    const minSimilarity = typeof rawMinSim === 'string' ? parseFloat(rawMinSim) : undefined;

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

    if (typeof q !== 'string' || q.trim().length === 0) {
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
