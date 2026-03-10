from __future__ import annotations
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware

from models import (
    CompressRequest, CompressResponse,
    BatchRequest, BatchResponse, BatchResultItem,
    UsageStats, CachePurgeRequest, CachePurgeResponse,
    DailyStatsEntry,
)
from compressor import compress_prompt, estimate_tokens
from cache import (
    init_db, cache_get, cache_set, cache_list, cache_delete,
    get_cache_entry, purge_cache, get_stats, record_compression,
    get_daily_stats,
)

DB_PATH = os.getenv("DB_PATH", "tokensaver.db")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await init_db(DB_PATH)
    yield
    await app.state.db.close()


app = FastAPI(
    title="TokenSaver",
    description="LLM API cost optimizer: prompt compression, semantic caching, batch deduplication, daily cost analytics.",
    version="0.3.0",
    lifespan=lifespan,
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/health")
async def health():
    return {"status": "ok", "version": "0.3.0"}


@app.post("/compress", response_model=CompressResponse)
async def compress(req: CompressRequest):
    original_tokens = estimate_tokens(req.prompt)
    compressed = compress_prompt(req.prompt, req.max_ratio, req.preserve_code)
    compressed_tokens = estimate_tokens(compressed)
    savings_pct = round((1 - compressed_tokens / max(original_tokens, 1)) * 100, 1)
    await record_compression(app.state.db, original_tokens, compressed_tokens)
    return CompressResponse(
        original=req.prompt,
        compressed=compressed,
        original_tokens=original_tokens,
        compressed_tokens=compressed_tokens,
        savings_pct=savings_pct,
        compression_ratio=round(compressed_tokens / max(original_tokens, 1), 3),
    )


@app.get("/cache", response_model=list[dict])
async def list_cache(limit: int = Query(50, ge=1, le=500)):
    return await cache_list(app.state.db, limit)


@app.get("/cache/{prompt_hash}")
async def get_cache(prompt_hash: str):
    entry = await get_cache_entry(app.state.db, prompt_hash)
    if not entry:
        raise HTTPException(404, "Cache entry not found")
    return entry


@app.post("/cache/lookup")
async def lookup_cache(
    prompt: str = Body(..., embed=True),
    model: str = Body("", embed=True),
):
    entry = await cache_get(app.state.db, prompt, model)
    if not entry:
        return {"hit": False, "response": None}
    return {"hit": True, **entry}


@app.post("/cache/store")
async def store_cache(
    prompt: str = Body(..., embed=True),
    model: str = Body("", embed=True),
    response: str = Body(..., embed=True),
    tokens_used: int = Body(0, embed=True),
):
    h = await cache_set(app.state.db, prompt, model, response, tokens_used)
    return {"cached": True, "prompt_hash": h[:16]}


@app.post("/cache/purge", response_model=CachePurgeResponse)
async def purge_cache_endpoint(req: CachePurgeRequest):
    count = await purge_cache(app.state.db, req.older_than_days, req.model)
    model_note = f" for model '{req.model}'" if req.model else ""
    return CachePurgeResponse(
        purged=count,
        message=f"Removed {count} cache entr{'y' if count == 1 else 'ies'} not accessed in {req.older_than_days}+ days{model_note}.",
    )


@app.delete("/cache/{prompt_hash}")
async def delete_cache_entry(prompt_hash: str):
    await cache_delete(app.state.db, prompt_hash)
    return {"deleted": True}


@app.post("/batch", response_model=BatchResponse)
async def batch_process(req: BatchRequest):
    seen_prompts: dict[str, str] = {}
    results: list[BatchResultItem] = []
    total_used = 0
    total_saved = 0
    deduped = 0
    cached = 0

    for item in req.items:
        hit = await cache_get(app.state.db, item.prompt, item.model)
        if hit:
            results.append(BatchResultItem(
                id=item.id, status="cached",
                response=hit["response"],
                tokens_used=0, tokens_saved=hit["tokens_saved"],
            ))
            total_saved += hit["tokens_saved"]
            cached += 1
            continue

        if req.dedup and item.prompt in seen_prompts:
            results.append(BatchResultItem(
                id=item.id, status="deduped",
                response=f"[same as {seen_prompts[item.prompt]}]",
                tokens_saved=item.max_tokens,
            ))
            total_saved += item.max_tokens
            deduped += 1
            continue

        mock_response = f"[Mock LLM response for: {item.prompt[:60]}...]"
        tokens = estimate_tokens(item.prompt) + item.max_tokens // 2
        await cache_set(app.state.db, item.prompt, item.model, mock_response, tokens)
        seen_prompts[item.prompt] = item.id
        results.append(BatchResultItem(
            id=item.id, status="ok",
            response=mock_response, tokens_used=tokens,
        ))
        total_used += tokens

    return BatchResponse(
        results=results,
        total_tokens_used=total_used,
        total_tokens_saved=total_saved,
        deduped_count=deduped,
        cached_count=cached,
    )


# ── Stats ────────────────────────────────────────────────────────────────────

@app.get("/stats/daily", response_model=list[DailyStatsEntry])
async def daily_stats(
    days: int = Query(30, ge=1, le=365, description="Look-back window in days"),
    model: str | None = Query(None, description="Filter by model name"),
):
    """Daily breakdown of token usage, savings, cache performance. For cost dashboards."""
    return await get_daily_stats(app.state.db, days, model)


@app.get("/stats", response_model=UsageStats)
async def usage_stats():
    return await get_stats(app.state.db)
