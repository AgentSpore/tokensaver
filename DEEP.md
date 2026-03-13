# TokenSaver — Architecture (DEEP.md)

## Overview
LLM API cost optimizer: prompt compression, semantic caching, batch dedup, model cost profiles, cost estimation, compression benchmarks, budget tracking.

## Stack
- FastAPI + aiosqlite + Pydantic v2
- SQLite with auto-migration on startup

## Modules
- **main.py** — FastAPI app, all route handlers (29 endpoints)
- **cache.py** — DB init, cache ops, stats, model costs, profiles, analytics, cost estimation, benchmarks, budgets
- **compressor.py** — Prompt compression logic (ratio-based, code preservation, example/comment stripping)
- **models.py** — Pydantic v2 request/response schemas

## Key Features (v0.6.0)
1. Prompt compression with configurable profiles
2. Semantic caching with hash-based lookup
3. Batch processing with deduplication
4. Model cost profiles (CRUD)
5. Compression profiles (CRUD, built-in defaults)
6. Cache analytics (top entries, model breakdown)
7. Daily stats with CSV export
8. **Cost estimation** across all registered models
9. **Compression benchmark** — test all profiles on a prompt
10. **Budget tracking** — daily/monthly token limits with alerts

## Database Tables
- cache, stats, model_costs, compression_profiles, daily_stats, budgets

## API Endpoints (29 total)
- Health: GET /health
- Compression: POST /compress
- Cost Estimation: POST /estimate
- Benchmark: POST /benchmark
- Budget: PUT /budget, GET /budget
- Profiles: CRUD /profiles
- Models: CRUD /models
- Cache: GET/POST/DELETE /cache, /cache/lookup, /cache/store, /cache/purge, /cache/analytics
- Batch: POST /batch
- Stats: GET /stats, GET /stats/daily, GET /stats/daily/csv
