# Infrastructure Reference

## Services

| Service | Port | Model | VRAM | systemd | Notes |
|---------|------|-------|------|---------|-------|
| llama-server | 8080 | qwen3.5-35b-a3b | 21 GB | user | 65K context, primary text |
| llama-priority | 8088 | qwen3.5-35b-a3b | 21 GB | user | 32K context, always-on for agents |
| llama-vlm | 8081 | qwen3-vl-32b | 20 GB | user | 8K context, vision |
| llama-embed | 8082 | nomic-embed | 0.3 GB | user | 8K context, embeddings |
| forge | 8642 | gateway | 0 GB | system | Routes to backends |

## Thresholds

| Metric | Normal | Warning | Critical |
|---|---|---|---|
| Error rate (15 min) | < 5% | 5-10% | > 10% |
| Text latency | < 10s | 10-30s | > 30s |
| Vision latency | < 30s | 30-120s | > 120s |
| VRAM headroom | > 10 GB | 5-10 GB | < 5 GB |

## Health Check Commands

- Forge API: `forge_health` check="health"
- Backend connectivity: `forge_health` check="backends"
- Service status: `check_system` "systemctl --user status llama-server llama-priority llama-vlm llama-embed"
- Forge system service: `check_system` "systemctl status forge"
- Process list: `check_system` "ps aux | grep llama-server | grep -v grep"

## Safe Restart Commands

Executed via `restart_service` tool (approval-gated):
- `llama-server`, `llama-priority`, `llama-embed`, `llama-vlm` (user services)
- `forge` (system service)
