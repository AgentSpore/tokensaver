# TokenSaver — Architecture (DEEP.md)

## Overview
LLM API cost optimizer providing prompt compression, semantic caching, batch deduplication, and cost tracking with model-aware pricing.

## Data Model

### Core Tables
- **cache** — SHA-256 keyed prompt/response store with hit tracking
- **stats** — singleton row for aggregate metrics
- **daily_log** — per-day per-model usage breakdown (UNIQUE day+model)
- **model_costs** — per-model pricing (input/output cost per 1M tokens)
- **profiles** — compression presets (3 built-in + custom)

### Relationships
```
profiles ──(by name)──> compress endpoint
model_costs ──(by name)──> daily_log cost calculation
cache ──(by model)──> model_costs (pricing lookup)
```

## Compression Pipeline
1. Extract and preserve code blocks (regex ```` ``` ```` matching)
2. Optionally strip examples (profile setting)
3. Optionally strip code comments (profile setting)
4. Normalize whitespace
5. Remove filler phrases (12 patterns)
6. Middle truncation if above target ratio (60% start / 40% end)
7. Restore code blocks

## Built-in Profiles
| Name | max_ratio | preserve_code | strip_examples | strip_comments |
|------|-----------|---------------|----------------|----------------|
| aggressive | 0.3 | yes | yes | yes |
| balanced | 0.5 | yes | no | no |
| minimal | 0.8 | yes | no | no |

## Cache Strategy
- Key: SHA-256 of `{model}:{prompt}`
- Exact match only (no fuzzy/similarity)
- LRU-style purge by `last_hit` timestamp
- Per-model filtering on purge

## Cost Calculation
- Each model has `input_cost_per_1m` and `output_cost_per_1m`
- Savings = tokens_saved * cost_per_1m / 1,000,000
- Fallback: $0.15/1M tokens if model not registered

## API Surface (v0.5.0)
| Method | Path | Description |
|--------|------|-------------|
| POST | /compress | Compress prompt (optional profile) |
| POST/GET/PATCH/DELETE | /profiles/* | Compression profiles CRUD |
| POST/GET/PATCH/DELETE | /models/* | Model cost profiles CRUD |
| GET/POST/DELETE | /cache/* | Cache operations |
| GET | /cache/analytics | Hit rate, top entries, model breakdown |
| POST | /cache/lookup | Check cache for prompt |
| POST | /cache/store | Store prompt/response |
| POST | /cache/purge | Purge old entries |
| POST | /batch | Batch process with dedup |
| GET | /stats | Aggregate usage stats |
| GET | /stats/daily | Daily breakdown (JSON) |
| GET | /stats/daily/csv | Daily breakdown (CSV download) |
| GET | /health | Health check |

## Key Decisions
- **SQLite + WAL**: single-file DB, good enough for moderate load
- **No fuzzy cache**: exact hash match avoids false positives; similarity search deferred to future version with embeddings
- **Built-in profiles immutable**: prevents accidental modification of defaults; users create custom profiles instead
- **CSV export via PlainTextResponse**: lightweight, no extra dependencies
