import fs from 'node:fs/promises';
import path from 'node:path';
import { createLogger } from '../logger.js';

const logger = createLogger('soul-loader');

// Resolve souls directory relative to project root
const SOULS_DIR = process.env.SOULS_DIR
  ?? path.resolve(new URL('../../../../souls', import.meta.url).pathname);

export interface SoulManifest {
  specVersion: string;
  name: string;
  displayName: string;
  version: string;
  description: string;
  author: { name: string; github?: string };
  license: string;
  tags: string[];
  category: string;
  compatibility?: {
    frameworks?: string[];
    models?: string[];
    minTokenContext?: number;
  };
  allowedTools?: string[];
  recommendedSkills?: Array<{ name: string; version?: string; required?: boolean }>;
  files: {
    soul: string;
    identity?: string;
    checklist?: string;
    agents?: string;
    heartbeat?: string;
    style?: string;
    reference?: string;
  };
  examples?: {
    good?: string;
    bad?: string;
  };
  disclosure?: {
    summary?: string;
  };
  deprecated?: boolean;
  supersededBy?: string;
  nexus?: {
    schedule_interval_sec?: number;
    autonomy?: 'high' | 'moderate' | 'approval_gated';
    executor?: string;
    llm_config?: {
      model?: string;
      url?: string;
      temperature?: number;
      max_tokens?: number;
    };
    absorbs?: string[];
    event_driven?: boolean;
    response_schema?: string;
  };
}

export type DisclosureLevel = 'scan' | 'active' | 'deep';

export interface SoulPackage {
  manifest: SoulManifest;
  soul: string;
  identity: string | null;
  checklist: string | null;
  agents: string | null;
  reference: string | null;
  heartbeat: string | null;
  examples: { good: string | null; bad: string | null };
}

// Cache manifests and file content per agent (cleared on explicit reload)
const manifestCache = new Map<string, SoulManifest>();
const fileCache = new Map<string, string>();

/**
 * Load a soul package at the specified disclosure level.
 *
 * - scan:   manifest only (for discovery, disclosure.summary)
 * - active: SOUL.md + IDENTITY.md (every cycle)
 * - deep:   all files including AGENTS.md, REFERENCE.md, HEARTBEAT.md, examples
 */
export async function loadSoulPackage(
  agentId: string,
  level: DisclosureLevel = 'active',
): Promise<SoulPackage | null> {
  const soulDir = path.join(SOULS_DIR, agentId);

  // Load manifest (cached)
  const manifest = await loadManifest(agentId, soulDir);
  if (!manifest) return null;

  if (level === 'scan') {
    return {
      manifest,
      soul: '',
      identity: null,
      checklist: null,
      agents: null,
      reference: null,
      heartbeat: null,
      examples: { good: null, bad: null },
    };
  }

  // Level 2: active — SOUL.md + IDENTITY.md + CHECKLIST.md
  const soul = await loadFile(soulDir, manifest.files.soul);
  if (!soul) {
    logger.logMinimal(`SOUL.md missing for agent ${agentId} at ${soulDir}/${manifest.files.soul}`);
    return null;
  }

  const identity = manifest.files.identity
    ? await loadFile(soulDir, manifest.files.identity)
    : null;

  // CHECKLIST.md loaded at Level 2 — always visible, every cycle
  const checklist = manifest.files.checklist
    ? await loadFile(soulDir, manifest.files.checklist)
    : await loadFile(soulDir, 'CHECKLIST.md'); // auto-detect even without manifest entry

  if (level === 'active') {
    return {
      manifest,
      soul,
      identity,
      checklist,
      agents: null,
      reference: null,
      heartbeat: null,
      examples: { good: null, bad: null },
    };
  }

  // Level 3: deep — everything
  const agents = manifest.files.agents
    ? await loadFile(soulDir, manifest.files.agents)
    : null;

  const reference = manifest.files.reference
    ? await loadFile(soulDir, manifest.files.reference)
    : null;

  const heartbeat = manifest.files.heartbeat
    ? await loadFile(soulDir, manifest.files.heartbeat)
    : null;

  const goodExamples = manifest.examples?.good
    ? await loadFile(soulDir, manifest.examples.good)
    : null;

  const badExamples = manifest.examples?.bad
    ? await loadFile(soulDir, manifest.examples.bad)
    : null;

  return {
    manifest,
    soul,
    identity,
    checklist,
    agents,
    reference,
    heartbeat,
    examples: { good: goodExamples, bad: badExamples },
  };
}

/**
 * List all available soul packages.
 */
export async function listSoulPackages(): Promise<SoulManifest[]> {
  try {
    const entries = await fs.readdir(SOULS_DIR, { withFileTypes: true });
    const manifests: SoulManifest[] = [];

    for (const entry of entries) {
      if (!entry.isDirectory()) continue;
      const manifest = await loadManifest(entry.name, path.join(SOULS_DIR, entry.name));
      if (manifest) manifests.push(manifest);
    }

    return manifests;
  } catch {
    logger.logMinimal('Failed to list soul packages from:', SOULS_DIR);
    return [];
  }
}

/**
 * Validate a soul package for completeness and spec compliance.
 */
export async function validateSoulPackage(agentId: string): Promise<{
  valid: boolean;
  errors: string[];
  warnings: string[];
}> {
  const errors: string[] = [];
  const warnings: string[] = [];
  const soulDir = path.join(SOULS_DIR, agentId);

  // Check directory exists
  try {
    await fs.access(soulDir);
  } catch {
    return { valid: false, errors: [`Soul directory not found: ${soulDir}`], warnings };
  }

  // Check manifest
  const manifest = await loadManifest(agentId, soulDir);
  if (!manifest) {
    return { valid: false, errors: ['soul.json missing or invalid'], warnings };
  }

  // Required fields
  if (!manifest.specVersion) errors.push('specVersion is required');
  if (!manifest.name) errors.push('name is required');
  if (!manifest.displayName) errors.push('displayName is required');
  if (!manifest.version) errors.push('version is required');
  if (!manifest.description) errors.push('description is required');
  if (!manifest.author?.name) errors.push('author.name is required');
  if (!manifest.license) errors.push('license is required');
  if (!manifest.files?.soul) errors.push('files.soul is required');

  // Check SOUL.md exists
  const soul = await loadFile(soulDir, manifest.files.soul);
  if (!soul) errors.push(`SOUL.md not found at ${manifest.files.soul}`);

  // Check optional files
  if (manifest.files.identity) {
    const identity = await loadFile(soulDir, manifest.files.identity);
    if (!identity) warnings.push(`IDENTITY.md declared but not found at ${manifest.files.identity}`);
  }
  if (manifest.files.agents) {
    const agents = await loadFile(soulDir, manifest.files.agents);
    if (!agents) warnings.push(`AGENTS.md declared but not found at ${manifest.files.agents}`);
  }

  // Disclosure summary
  if (!manifest.disclosure?.summary) {
    warnings.push('disclosure.summary missing — agents cannot describe this soul at Level 1');
  }

  // Nexus extension
  if (!manifest.nexus) {
    warnings.push('nexus extension block missing — using defaults for schedule, autonomy, LLM config');
  }

  return { valid: errors.length === 0, errors, warnings };
}

/**
 * Build the system prompt from a soul package.
 * Combines files based on disclosure level.
 */
export function assembleSoulPrompt(soul: SoulPackage, level: DisclosureLevel): string {
  const parts: string[] = [];

  // SOUL.md is always included (Level 2+)
  if (soul.soul) {
    parts.push(soul.soul);
  }

  // IDENTITY.md is included at Level 2+
  if (soul.identity) {
    parts.push(soul.identity);
  }

  // CHECKLIST.md is included at Level 2+ (every cycle)
  if (soul.checklist) {
    parts.push(soul.checklist);
  }

  // Level 3 additions
  if (level === 'deep') {
    if (soul.agents) {
      parts.push(soul.agents);
    }
    if (soul.reference) {
      parts.push(soul.reference);
    }
    if (soul.heartbeat) {
      parts.push(soul.heartbeat);
    }
  }

  return parts.join('\n\n');
}

/**
 * Clear the cache for a specific agent or all agents.
 */
export function clearCache(agentId?: string): void {
  if (agentId) {
    manifestCache.delete(agentId);
    // Clear file cache entries for this agent
    for (const key of fileCache.keys()) {
      if (key.startsWith(`${agentId}/`)) fileCache.delete(key);
    }
  } else {
    manifestCache.clear();
    fileCache.clear();
  }
  logger.logVerbose('Cache cleared for:', agentId ?? 'all');
}

// --- Internal helpers ---

async function loadManifest(agentId: string, soulDir: string): Promise<SoulManifest | null> {
  if (manifestCache.has(agentId)) return manifestCache.get(agentId)!;

  try {
    const raw = await fs.readFile(path.join(soulDir, 'soul.json'), 'utf-8');
    const manifest = JSON.parse(raw) as SoulManifest;
    manifestCache.set(agentId, manifest);
    logger.logVerbose('Loaded manifest for:', agentId, 'v' + manifest.version);
    return manifest;
  } catch {
    logger.logVerbose('No soul.json found for:', agentId);
    return null;
  }
}

async function loadFile(soulDir: string, filePath: string): Promise<string | null> {
  const fullPath = path.join(soulDir, filePath);
  const cacheKey = fullPath;

  if (fileCache.has(cacheKey)) return fileCache.get(cacheKey)!;

  try {
    const content = await fs.readFile(fullPath, 'utf-8');
    fileCache.set(cacheKey, content);
    return content;
  } catch {
    return null;
  }
}
