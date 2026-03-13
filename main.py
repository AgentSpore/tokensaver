from __future__ import annotations
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse

from models import (
    CompressRequest, CompressResponse,
    BatchRequest, BatchResponse, BatchResultItem,
    UsageStats, CachePurgeRequest, CachePurgeResponse,
    DailyStatsEntry,
    ModelCostCreate, ModelCostUpdate, ModelCostResponse,
    ProfileCreate, ProfileUpdate, ProfileResponse,
    CacheAnalyticsResponse,
)
from compressor import compress_prompt, estimate_tokens
from cache import (
    init_db, cache_get, cache_set, cache_list, cache_delete,
    get_cache_entry, purge_cache, get_stats, record_compression,
    get_daily_stats,
    create_model_cost, list_model_costs, get_model_cost, update_model_cost, delete_model_cost,
    create_profile, list_profiles, get_profile, update_profile, delete_profile,
    get_cache_analytics, export_daily_csv,
)

DB_PATH = os.getenv("DB_PATH", "tokensaver.db")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await init_db(DB_PATH)
    yield
    await app.state.db.close()


app = FastAPI(
    title="TokenSaver",
    description="LLM API cost optimizer: prompt compression with profiles, semantic caching, batch deduplication, model cost profiles, cache analytics.",
    version="0.5.0",
    lifespan=lifespan,
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/health")
async def health():
    return {"status": "ok", "version": "0.5.0"}


@app.post("/compress", response_model=CompressResponse)
async def compress(req: CompressRequest):
    profile_used = None
    max_ratio = req.max_ratio
    preserve_code = req.preserve_code
    strip_examples = False
    strip_comments = False

    if req.profile:
        p = await get_profile(app.state.db, req.profile)
        if not p:
            raise HTTPException(404, f"Compression profile '{req.profile}' not found")
        max_ratio = p["max_ratio"]
        preserve_code = p["preserve_code"]
        strip_examples = p["strip_examples"]
        strip_comments = p["strip_comments"]
        profile_used = req.profile

    original_tokens = estimate_tokens(req.prompt)
    compressed = compress_prompt(
        req.prompt, max_ratio, preserve_code,
        strip_examples=strip_examples, strip_comments=strip_comments,
    )
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
        profile_used=profile_used,
    )


# ── Compression Profiles ────────────────────────────────────────────────────

@app.post("/profiles", response_model=ProfileResponse, status_code=201)
async def add_profile(body: ProfileCreate):
    try:
        return await create_profile(app.state.db, body.model_dump())
    except ValueError as e:
        raise HTTPException(409, str(e))


@app.get("/profiles", response_model=list[ProfileResponse])
async def get_profiles():
    return await list_profiles(app.state.db)


@app.get("/profiles/{name}", response_model=ProfileResponse)
async def get_profile_detail(name: str):
    p = await get_profile(app.state.db, name)
    if not p:
        raise HTTPException(404, "Profile not found")
    return p


@app.patch("/profiles/{name}", response_model=ProfileResponse)
async def patch_profile(name: str, body: ProfileUpdate):
    try:
        p = await update_profile(app.state.db, name, body.model_dump(exclude_unset=True))
    except ValueError as e:
        raise HTTPException(403, str(e))
    if not p:
        raise HTTPException(404, "Profile not found")
    return p


@app.delete("/profiles/{name}", status_code=204)
async def remove_profile(name: str):
    try:
        ok = await delete_profile(app.state.db, name)
    except ValueError as e:
        raise HTTPException(403, str(e))
    if not ok:
        raise HTTPException(404, "Profile not found")


# ── Model Cost Profiles ─────────────────────────────────────────────────────

@app.post("/models", response_model=ModelCostResponse, status_code=201)
async def add_model(body: ModelCostCreate):
    try:
        return await create_model_cost(app.state.db, body.model_dump())
    except ValueError as e:
        raise HTTPException(409, str(e))


@app.get("/models", response_model=list[ModelCostResponse])
async def get_models():
    return await list_model_costs(app.state.db)


@app.get("/models/{name}", response_model=ModelCostResponse)
async def get_model_detail(name: str):
    m = await get_model_cost(app.state.db, name)
    if not m:
        raise HTTPException(404, "Model not found")
    return m


@app.patch("/models/{name}", response_model=ModelCostResponse)
async def patch_model(name: str, body: ModelCostUpdate):
    m = await update_model_cost(app.state.db, name, body.model_dump(exclude_unset=True))
    if not m:
        raise HTTPException(404, "Model not found")
    return m


@app.delete("/models/{name}", status_code=204)
async def remove_model(name: str):
    ok = await delete_model_cost(app.state.db, name)
    if not ok:
        raise HTTPException(404, "Model not found")


# ── Cache ────────────────────────────────────────────────────────────────────

@app.get("/cache", response_model=list[dict])
async def list_cache(limit: int = Query(50, ge=1, le=500)):
    return await cache_list(app.state.db, limit)


@app.get("/cache/analytics", response_model=CacheAnalyticsResponse)
async def cache_analytics(top: int = Query(10, ge=1, le=100)):
    return await get_cache_analytics(app.state.db, top)


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
    return await get_daily_stats(app.state.db, days, model)


@app.get("/stats/daily/csv")
async def daily_stats_csv(
    days: int = Query(90, ge=1, le=365),
    model: str | None = Query(None),
):
    csv_content = await export_daily_csv(app.state.db, days, model)
    return PlainTextResponse(csv_content, media_type="text/csv", headers={
        "Content-Disposition": "attachment; filename=tokensaver_daily_stats.csv",
    })


@app.get("/stats", response_model=UsageStats)
async def usage_stats():
    return await get_stats(app.state.db)
