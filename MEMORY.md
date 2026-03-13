# TokenSaver — Development Log (MEMORY.md)

## v0.6.0 (2026-03-13)
- Added cost estimation across all registered models (POST /estimate)
- Added compression benchmark — test all profiles on a prompt (POST /benchmark)
- Added budget tracking with daily/monthly token limits and alerts (PUT/GET /budget)
- New DB table: budgets (auto-migrated)
- 4 new endpoints, 29 total

## v0.5.0
- Added compression profiles (CRUD + 3 built-in: balanced, aggressive, minimal)
- Added cache analytics (top entries, model breakdown, hit rate)
- Added daily stats CSV export
- 25 endpoints

## v0.4.0
- Added model cost profiles (CRUD)
- Added daily stats tracking with model breakdown
- 20 endpoints

## v0.3.0
- Batch processing with deduplication
- Cache purge with model filter
- 15 endpoints

## v0.2.0
- Semantic caching (hash-based)
- Usage statistics
- 10 endpoints

## v0.1.0
- Initial MVP: prompt compression, health check
