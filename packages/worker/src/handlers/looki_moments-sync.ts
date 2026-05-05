import { createLogger } from '@nexus/core';
import type { TempoJob } from '../job-worker.js';
import { ingest, registerNormalizer, type IngestRecord } from '../lib/ingestion-pipeline.js';

const logger = createLogger('looki_moments-sync');
const SOURCE_KEY = 'looki_moments';

registerNormalizer(SOURCE_KEY, normalizeLookiMoments);

export async function handleLookiMomentsSync(_job: TempoJob): Promise<Record<string, unknown>> {
  logger.log('Starting looki_moments sync');

  const rawItems = await fetchLookiMomentsData();
  const result = await ingest(SOURCE_KEY, rawItems);

  logger.log(`Completed looki_moments sync: ${result.ingested} ingested, ${result.skipped} skipped`);
  return result;
}

async function fetchLookiMomentsData(): Promise<unknown[]> {
  // Public example source: this handler intentionally ships as a no-op until a
  // real API client is added. The onboarding pattern is the important part.
  logger.log('No Looki fetch client configured in nexus-public; returning empty set');
  return [];
}

function normalizeLookiMoments(rawData: unknown): IngestRecord[] {
  const item = rawData as Record<string, unknown>;
  return [{
    source_key: SOURCE_KEY,
    source_id: String(item.id ?? 'TODO-source-id'),
    timestamp: new Date(String(item.captured_at ?? item.timestamp ?? Date.now())),
    category: 'photo',
    summary: String(item.caption ?? item.summary ?? 'Looki moment'),
    content: item.caption == null ? null : String(item.caption),
    entities: [],
    metadata: {
      latitude: item.latitude ?? null,
      longitude: item.longitude ?? null,
      place_label: item.place_label ?? null,
      media_url: item.media_url ?? null,
    },
    raw: item,
  }];
}
