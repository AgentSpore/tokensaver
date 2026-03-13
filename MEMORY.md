# TokenSaver — Development Log (MEMORY.md)

## v0.1.0 — Initial MVP
- Prompt compression with filler removal and code block preservation
- Token estimation (4 chars/token heuristic)
- Middle truncation for oversized prompts

## v0.2.0 — Semantic Cache
- SHA-256 based prompt cache (exact match)
- Cache CRUD (lookup, store, list, delete)
- Hit counter and last-access tracking
- Basic usage stats (total requests, tokens saved/used)

## v0.3.0 — Batch Dedup & Cache Purge
- Batch endpoint with deduplication
- Cache purge by age (days since last hit)
- Model-specific purge filtering
- Daily stats table with per-day breakdown

## v0.4.0 — Model Cost Profiles
- Per-model pricing (input/output cost per 1M tokens)
- Model CRUD (create, list, get, update, delete)
- Cost-aware savings calculation
- Daily stats with model-specific pricing
- Fallback to $0.15/1M default rate

## v0.5.0 — Compression Profiles, Cache Analytics, CSV Export
- **Compression profiles**: 3 built-in (aggressive/balanced/minimal) + custom profiles CRUD
- Profile settings: max_ratio, preserve_code, strip_examples, strip_comments
- Built-in profiles are immutable (403 on modify/delete)
- **Cache analytics**: total entries/hits, hit rate, avg hits/entry, top-N entries, model breakdown
- **CSV export**: GET /stats/daily/csv with days and model filter, Content-Disposition header
- Compressor extended with strip_examples and strip_comments options
- CompressRequest accepts optional profile name (overrides manual settings)
