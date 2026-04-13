-- Soul Spec integration: add soul package metadata to agent_registry

ALTER TABLE agent_registry ADD COLUMN IF NOT EXISTS soul_spec_version TEXT;
ALTER TABLE agent_registry ADD COLUMN IF NOT EXISTS soul_package_path TEXT;
ALTER TABLE agent_registry ADD COLUMN IF NOT EXISTS disclosure_summary TEXT;

-- Update existing agents with soul package paths and disclosure summaries
UPDATE agent_registry SET
  soul_spec_version = '0.5',
  soul_package_path = 'souls/' || agent_id,
  disclosure_summary = CASE agent_id
    WHEN 'monitor' THEN 'Monitors platform infrastructure, jobs, agents, performance, costs, and security posture.'
    WHEN 'model-ops' THEN 'Manages LLM inference services — Forge gateway, model health, VRAM, latency, restarts.'
    WHEN 'collector' THEN 'Monitors 14 data sources, orchestrates enrichment pipelines, coordinates Mac relay.'
    WHEN 'analyst' THEN 'Tracks decisions, surfaces Aurora behavioral patterns, maintains institutional memory.'
    WHEN 'fixer' THEN 'ONLY agent that can modify code. Implements fixes, schema changes, doc updates, git workflow.'
    WHEN 'relationships' THEN 'Monitors relationship health across all channels — drift alerts, emerging connections, meeting prep.'
    WHEN 'aria' THEN 'Personal assistant and team coordinator. Synthesizes briefings, routes escalations, manages tasks.'
    ELSE NULL
  END
WHERE agent_id IN ('monitor', 'model-ops', 'collector', 'analyst', 'fixer', 'relationships', 'aria');
