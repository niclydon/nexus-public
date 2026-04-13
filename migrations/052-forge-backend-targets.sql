-- Health watchdog v3 — close the Forge backend monitoring gap
--
-- Discovered during a Chancery UI browse pass on 2026-04-11: Forge
-- advertises 10 backends via /health/backends but the health watchdog
-- only monitors the 4 llama-* Primary-Server ports (text, priority, vlm,
-- embed). The other 6 (whisper, faces, rerank, ocr, tts, dev-server-vlm)
-- were completely invisible — if any of them crashed, the Chancery
-- /health dashboard would still show all green.
--
-- All 6 use the standard HTTP /health endpoint pattern. Severity is
-- `warning` rather than `critical` because these are auxiliary services
-- (speech, face detection, OCR, TTS, reranking, remote VLM) — a brief
-- outage on any of them doesn't cause user-visible failure the way a
-- llama-server crash would.
--
-- NOTE: Forge's own /health/backends reports dev-server-vlm at
-- http://10.0.0.1:8081 which is the wrong IP (Dev-Server's LAN address
-- differs from the gateway). Using the DNS hostname
-- dev-server.example.io:8081 instead — this resolves correctly from
-- Primary-Server and is more stable against IP changes. Forge's stale URL
-- is a separate bug to fix in the Forge repo.

INSERT INTO platform_health_targets (
  service, display_name, check_type, endpoint, expected,
  timeout_ms, severity_on_down, alert_after_failures
) VALUES
  (
    'llama-whisper',
    'Whisper STT (Primary-Server)',
    'http',
    'http://localhost:8083/health',
    '200',
    5000, 'warning', 3
  ),
  (
    'llama-faces',
    'InsightFace (Primary-Server)',
    'http',
    'http://localhost:8084/health',
    '200',
    5000, 'warning', 3
  ),
  (
    'llama-rerank',
    'Reranker (Primary-Server)',
    'http',
    'http://localhost:8085/health',
    '200',
    5000, 'warning', 3
  ),
  (
    'llama-ocr',
    'Florence-2 OCR (Primary-Server)',
    'http',
    'http://localhost:8086/health',
    '200',
    5000, 'warning', 3
  ),
  (
    'llama-tts',
    'Chatterbox TTS (Primary-Server)',
    'http',
    'http://localhost:8087/health',
    '200',
    5000, 'warning', 3
  ),
  (
    'dev-server-vlm',
    'Qwen3-VL (Dev-Server)',
    'http',
    'http://dev-server.example.io:8081/health',
    '200',
    8000, 'warning', 3
  )
ON CONFLICT (service) DO UPDATE SET
  display_name = EXCLUDED.display_name,
  check_type = EXCLUDED.check_type,
  endpoint = EXCLUDED.endpoint,
  expected = EXCLUDED.expected,
  timeout_ms = EXCLUDED.timeout_ms,
  severity_on_down = EXCLUDED.severity_on_down,
  alert_after_failures = EXCLUDED.alert_after_failures,
  updated_at = now();

INSERT INTO nexus_schema_version (version, description)
VALUES (52, 'Health watchdog — add 6 missing Forge backend targets (whisper/faces/rerank/ocr/tts/dev-server-vlm)')
ON CONFLICT DO NOTHING;
