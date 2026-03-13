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
    CostEstimateRequest, CostEstimateResponse,
    BenchmarkRequest, BenchmarkResponse,
    BudgetSetRequest, BudgetStatusResponse,
    PromptTemplateCreate, PromptTemplateUpdate, PromptTemplateResponse,
    PromptTemplateRenderRequest, PromptTemplateRenderResponse,
    CompressionHistoryEntry, CompressionAnalytics,
    ModelCompareRequest, ModelCompareResponse,
    CompressionRuleCreate, CompressionRuleUpdate, CompressionRuleResponse,
    PromptDiffRequest, PromptDiffResponse,
    UsageQuotaSet, UsageQuotaResponse,
    # v0.9.0
    TemplateVersionResponse, TemplateVersionDiff, TemplateRollbackRequest,
    AlertRuleCreate, AlertRuleUpdate, AlertRuleResponse,
    AlertLogEntry, AlertSummary,
    ABExperimentCreate, ABExperimentResponse,
    ABTestPromptRequest, ABTestResultResponse, ABExperimentSummary,
)
from compressor import compress_prompt, estimate_tokens
from cache import (
    init_db, cache_get, cache_set, cache_list, cache_delete,
    get_cache_entry, purge_cache, get_stats, record_compression,
    get_daily_stats,
    create_model_cost, list_model_costs, get_model_cost, update_model_cost, delete_model_cost,
    create_profile, list_profiles, get_profile, update_profile, delete_profile,
    get_cache_analytics, export_daily_csv,
    estimate_cost, benchmark_profiles, set_budget, get_budget_status,
    create_prompt_template, list_prompt_templates, get_prompt_template,
    update_prompt_template, delete_prompt_template, render_prompt_template,
    list_compression_history, get_compression_analytics,
    compare_models,
    create_compression_rule, list_compression_rules, get_compression_rule,
    update_compression_rule, delete_compression_rule,
    get_active_rules, increment_rule_applied,
    prompt_diff,
    set_usage_quota, get_usage_quota, list_usage_quotas, delete_usage_quota,
    # v0.9.0 — Prompt Versioning
    list_template_versions, get_template_version, diff_template_versions, rollback_template,
    # v0.9.0 — Cost Alerts
    create_alert_rule, list_alert_rules, get_alert_rule, update_alert_rule, delete_alert_rule,
    evaluate_alerts, list_alert_log, acknowledge_alert, get_alert_summary,
    # v0.9.0 — A/B Testing
    create_ab_experiment, list_ab_experiments, get_ab_experiment,
    run_ab_test, complete_ab_experiment, delete_ab_experiment, get_ab_experiment_results,
)

DB_PATH = os.getenv("DB_PATH", "tokensaver.db")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await init_db(DB_PATH)
    yield
    await app.state.db.close()


app = FastAPI(
    title="TokenSaver",
    description=(
        "LLM API cost optimizer: prompt compression, semantic caching, batch dedup, "
        "model cost profiles, cost estimation, compression benchmarks, budget tracking, "
        "prompt templates with variable substitution and version history, compression "
        "history analytics, cross-model cost comparison, custom compression rules, "
        "prompt diff analysis, per-model usage quotas, configurable cost alerts, "
        "and compression A/B testing."
    ),
    version="0.9.0",
    lifespan=lifespan,
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/health")
async def health():
    return {"status": "ok", "version": "0.9.0"}


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

    # Fetch custom rules if enabled
    custom_rules = None
    if req.apply_rules:
        custom_rules = await get_active_rules(app.state.db)

    original_tokens = estimate_tokens(req.prompt)
    compressed, rules_applied = compress_prompt(
        req.prompt, max_ratio, preserve_code,
        strip_examples=strip_examples, strip_comments=strip_comments,
        custom_rules=custom_rules if custom_rules else None,
    )
    compressed_tokens = estimate_tokens(compressed)
    savings_pct = round((1 - compressed_tokens / max(original_tokens, 1)) * 100, 1)

    # Track which rules were applied
    if rules_applied > 0 and custom_rules:
        applied_ids = [r["id"] for r in custom_rules[:rules_applied]]
        await increment_rule_applied(app.state.db, applied_ids)

    await record_compression(
        app.state.db, original_tokens, compressed_tokens,
        profile=profile_used, prompt_preview=req.prompt[:120],
    )

    # Fire-and-forget alert evaluation
    try:
        await evaluate_alerts(app.state.db)
    except Exception:
        pass

    return CompressResponse(
        original=req.prompt,
        compressed=compressed,
        original_tokens=original_tokens,
        compressed_tokens=compressed_tokens,
        savings_pct=savings_pct,
        compression_ratio=round(compressed_tokens / max(original_tokens, 1), 3),
        profile_used=profile_used,
        rules_applied=rules_applied,
    )


# ── Compression Rules (v0.8.0) ───────────────────────────────────────────────

@app.post("/rules", response_model=CompressionRuleResponse, status_code=201)
async def add_rule(body: CompressionRuleCreate):
    """Create a custom regex-based compression rule. Applied during prompt compression."""
    try:
        return await create_compression_rule(app.state.db, body.model_dump())
    except ValueError as e:
        raise HTTPException(422, str(e))


@app.get("/rules", response_model=list[CompressionRuleResponse])
async def get_rules():
    """List all compression rules, ordered by priority (lower = applied first)."""
    return await list_compression_rules(app.state.db)


@app.get("/rules/{rule_id}", response_model=CompressionRuleResponse)
async def get_rule_detail(rule_id: int):
    rule = await get_compression_rule(app.state.db, rule_id)
    if not rule:
        raise HTTPException(404, "Rule not found")
    return rule


@app.patch("/rules/{rule_id}", response_model=CompressionRuleResponse)
async def patch_rule(rule_id: int, body: CompressionRuleUpdate):
    try:
        result = await update_compression_rule(app.state.db, rule_id, body.model_dump(exclude_unset=True))
    except ValueError as e:
        raise HTTPException(422, str(e))
    if not result:
        raise HTTPException(404, "Rule not found")
    return result


@app.delete("/rules/{rule_id}", status_code=204)
async def remove_rule(rule_id: int):
    ok = await delete_compression_rule(app.state.db, rule_id)
    if not ok:
        raise HTTPException(404, "Rule not found")


# ── Prompt Diff (v0.8.0) ─────────────────────────────────────────────────────

@app.post("/diff", response_model=PromptDiffResponse)
async def diff_prompts(body: PromptDiffRequest):
    """Compare two prompts: token counts, character lengths, and optional compressed cost analysis."""
    return await prompt_diff(app.state.db, body.prompt_a, body.prompt_b,
                             body.compress, body.profile)


# ── Usage Quotas (v0.8.0) ────────────────────────────────────────────────────

@app.put("/quotas/{model}", response_model=UsageQuotaResponse)
async def update_quota(model: str, body: UsageQuotaSet):
    """Set or update daily/monthly token quota for a specific model."""
    return await set_usage_quota(app.state.db, model,
                                 body.daily_token_limit, body.monthly_token_limit)


@app.get("/quotas", response_model=list[UsageQuotaResponse])
async def get_quotas():
    """List all per-model usage quotas with current usage stats."""
    return await list_usage_quotas(app.state.db)


@app.get("/quotas/{model}", response_model=UsageQuotaResponse)
async def get_quota_detail(model: str):
    quota = await get_usage_quota(app.state.db, model)
    if not quota:
        raise HTTPException(404, f"No quota set for model '{model}'")
    return quota


@app.delete("/quotas/{model}", status_code=204)
async def remove_quota(model: str):
    ok = await delete_usage_quota(app.state.db, model)
    if not ok:
        raise HTTPException(404, f"No quota set for model '{model}'")


# ── Cost Estimation ────────────────────────────────────────────────────────

@app.post("/estimate", response_model=CostEstimateResponse)
async def cost_estimate(body: CostEstimateRequest):
    """Estimate cost for a prompt across all registered models."""
    token_count = estimate_tokens(body.prompt)
    estimates = await estimate_cost(app.state.db, token_count, body.model)
    cheapest = min(estimates, key=lambda x: x["total_estimate_usd"])["model"] if estimates else None
    return CostEstimateResponse(
        input_tokens=token_count,
        estimates=estimates,
        cheapest_model=cheapest,
    )


# ── Compression Benchmark ──────────────────────────────────────────────────

@app.post("/benchmark", response_model=BenchmarkResponse)
async def benchmark(body: BenchmarkRequest):
    """Compress a prompt with all profiles and compare results side by side."""
    results = await benchmark_profiles(app.state.db, body.prompt)
    original_tokens = results[0]["original_tokens"] if results else estimate_tokens(body.prompt)
    best = results[0] if results else None
    return BenchmarkResponse(
        original_tokens=original_tokens,
        profiles_tested=len(results),
        results=results,
        best_profile=best["profile"] if best else "balanced",
        best_savings_pct=best["savings_pct"] if best else 0.0,
    )


# ── Budget Tracking ────────────────────────────────────────────────────────

@app.put("/budget", response_model=BudgetStatusResponse)
async def update_budget(body: BudgetSetRequest):
    """Set or update daily/monthly token budget limits."""
    return await set_budget(
        app.state.db, body.daily_token_limit, body.monthly_token_limit,
        body.alert_threshold_pct,
    )


@app.get("/budget", response_model=BudgetStatusResponse)
async def budget_status():
    """Get current budget status: usage vs limits, alerts."""
    return await get_budget_status(app.state.db)


# ── Prompt Templates ───────────────────────────────────────────────────────

@app.post("/templates", response_model=PromptTemplateResponse, status_code=201)
async def add_template(body: PromptTemplateCreate):
    """Create a reusable prompt template with {{variable}} placeholders."""
    try:
        return await create_prompt_template(app.state.db, body.model_dump())
    except ValueError as e:
        raise HTTPException(409, str(e))


@app.get("/templates", response_model=list[PromptTemplateResponse])
async def get_templates():
    return await list_prompt_templates(app.state.db)


@app.get("/templates/{tpl_id}", response_model=PromptTemplateResponse)
async def get_template_detail(tpl_id: int):
    tpl = await get_prompt_template(app.state.db, tpl_id)
    if not tpl:
        raise HTTPException(404, "Template not found")
    return tpl


@app.patch("/templates/{tpl_id}", response_model=PromptTemplateResponse)
async def patch_template(tpl_id: int, body: PromptTemplateUpdate):
    try:
        tpl = await update_prompt_template(app.state.db, tpl_id, body.model_dump(exclude_unset=True))
    except ValueError as e:
        raise HTTPException(409, str(e))
    if not tpl:
        raise HTTPException(404, "Template not found")
    return tpl


@app.delete("/templates/{tpl_id}", status_code=204)
async def remove_template(tpl_id: int):
    ok = await delete_prompt_template(app.state.db, tpl_id)
    if not ok:
        raise HTTPException(404, "Template not found")


@app.post("/templates/{tpl_id}/render", response_model=PromptTemplateRenderResponse)
async def render_template(tpl_id: int, body: PromptTemplateRenderRequest):
    """Render a template with variable substitution. Optionally compress the output."""
    result = await render_prompt_template(
        app.state.db, tpl_id, body.variables, body.compress, body.profile,
    )
    if not result:
        raise HTTPException(404, "Template not found")
    return result


# ── Template Versioning (v0.9.0) ──────────────────────────────────────────

@app.get("/templates/{tpl_id}/versions", response_model=list[TemplateVersionResponse])
async def get_template_versions(tpl_id: int):
    """List all historical versions for a template, newest first."""
    versions = await list_template_versions(app.state.db, tpl_id)
    if versions is None:
        raise HTTPException(404, "Template not found")
    return versions


@app.get("/templates/versions/{version_id}", response_model=TemplateVersionResponse)
async def get_version_detail(version_id: int):
    """Get a specific template version by its ID."""
    version = await get_template_version(app.state.db, version_id)
    if not version:
        raise HTTPException(404, "Template version not found")
    return version


@app.get("/templates/{tpl_id}/versions/diff", response_model=TemplateVersionDiff)
async def diff_versions(
    tpl_id: int,
    a: int = Query(..., ge=1, description="First version number"),
    b: int = Query(..., ge=1, description="Second version number"),
):
    """Compare two template versions side by side: token counts and previews."""
    if a == b:
        raise HTTPException(422, "Version numbers must be different")
    result = await diff_template_versions(app.state.db, tpl_id, a, b)
    if result is None:
        raise HTTPException(404, "Template or one of the versions not found")
    return result


@app.post("/templates/{tpl_id}/rollback", response_model=PromptTemplateResponse)
async def rollback_template_endpoint(tpl_id: int, body: TemplateRollbackRequest):
    """Rollback a template to a previous version. Current version is saved to history."""
    try:
        result = await rollback_template(app.state.db, tpl_id, body.version_number)
    except ValueError as e:
        raise HTTPException(422, str(e))
    if result is None:
        raise HTTPException(404, "Template not found")
    return result


# ── Cost Alerts (v0.9.0) ─────────────────────────────────────────────────────

@app.post("/alerts/rules", response_model=AlertRuleResponse, status_code=201)
async def add_alert_rule(body: AlertRuleCreate):
    """Create a configurable cost/performance alert rule."""
    try:
        return await create_alert_rule(app.state.db, body.model_dump())
    except ValueError as e:
        raise HTTPException(422, str(e))


@app.get("/alerts/rules", response_model=list[AlertRuleResponse])
async def get_alert_rules():
    """List all alert rules."""
    return await list_alert_rules(app.state.db)


@app.get("/alerts/rules/{rule_id}", response_model=AlertRuleResponse)
async def get_alert_rule_detail(rule_id: int):
    rule = await get_alert_rule(app.state.db, rule_id)
    if not rule:
        raise HTTPException(404, "Alert rule not found")
    return rule


@app.patch("/alerts/rules/{rule_id}", response_model=AlertRuleResponse)
async def patch_alert_rule(rule_id: int, body: AlertRuleUpdate):
    try:
        result = await update_alert_rule(app.state.db, rule_id, body.model_dump(exclude_unset=True))
    except ValueError as e:
        raise HTTPException(422, str(e))
    if not result:
        raise HTTPException(404, "Alert rule not found")
    return result


@app.delete("/alerts/rules/{rule_id}", status_code=204)
async def remove_alert_rule(rule_id: int):
    ok = await delete_alert_rule(app.state.db, rule_id)
    if not ok:
        raise HTTPException(404, "Alert rule not found")


@app.post("/alerts/evaluate", response_model=list[dict])
async def evaluate_alerts_endpoint():
    """Manually evaluate all enabled alert rules and return any newly triggered alerts."""
    return await evaluate_alerts(app.state.db)


@app.get("/alerts/log", response_model=list[AlertLogEntry])
async def get_alerts_log(
    rule_id: int | None = Query(None, description="Filter by alert rule ID"),
    acknowledged: bool | None = Query(None, description="Filter by acknowledgement status"),
    limit: int = Query(50, ge=1, le=500),
):
    """View alert history with optional filters."""
    return await list_alert_log(app.state.db, rule_id, acknowledged, limit)


@app.post("/alerts/{alert_id}/acknowledge", response_model=AlertLogEntry)
async def acknowledge_alert_endpoint(alert_id: int):
    """Acknowledge (dismiss) an alert log entry."""
    result = await acknowledge_alert(app.state.db, alert_id)
    if not result:
        raise HTTPException(404, "Alert log entry not found")
    return result


@app.get("/alerts/summary", response_model=AlertSummary)
async def alert_summary():
    """Get a summary of all alert rules and recent alerts."""
    return await get_alert_summary(app.state.db)


# ── Compression A/B Testing (v0.9.0) ─────────────────────────────────────────

@app.post("/ab-experiments", response_model=ABExperimentResponse, status_code=201)
async def add_ab_experiment(body: ABExperimentCreate):
    """Create a new A/B experiment to compare two compression profiles."""
    try:
        return await create_ab_experiment(app.state.db, body.model_dump())
    except ValueError as e:
        raise HTTPException(422, str(e))


@app.get("/ab-experiments", response_model=list[ABExperimentResponse])
async def get_ab_experiments():
    """List all A/B experiments."""
    return await list_ab_experiments(app.state.db)


@app.get("/ab-experiments/{exp_id}", response_model=ABExperimentResponse)
async def get_ab_experiment_detail(exp_id: int):
    exp = await get_ab_experiment(app.state.db, exp_id)
    if not exp:
        raise HTTPException(404, "A/B experiment not found")
    return exp


@app.post("/ab-experiments/{exp_id}/test", response_model=ABTestResultResponse)
async def run_ab_test_endpoint(exp_id: int, body: ABTestPromptRequest):
    """Run a single test within an A/B experiment: compress with both profiles and record winner."""
    try:
        result = await run_ab_test(app.state.db, exp_id, body.prompt)
    except ValueError as e:
        raise HTTPException(422, str(e))
    if result is None:
        raise HTTPException(404, "A/B experiment not found")
    return result


@app.post("/ab-experiments/{exp_id}/complete", response_model=ABExperimentResponse)
async def complete_ab_experiment_endpoint(exp_id: int):
    """Mark an A/B experiment as completed."""
    try:
        result = await complete_ab_experiment(app.state.db, exp_id)
    except ValueError as e:
        raise HTTPException(422, str(e))
    if result is None:
        raise HTTPException(404, "A/B experiment not found")
    return result


@app.delete("/ab-experiments/{exp_id}", status_code=204)
async def remove_ab_experiment(exp_id: int):
    ok = await delete_ab_experiment(app.state.db, exp_id)
    if not ok:
        raise HTTPException(404, "A/B experiment not found")


@app.get("/ab-experiments/{exp_id}/results", response_model=list[ABTestResultResponse])
async def get_ab_results(
    exp_id: int,
    limit: int = Query(50, ge=1, le=500),
):
    """View individual test results for an A/B experiment."""
    results = await get_ab_experiment_results(app.state.db, exp_id, limit)
    if results is None:
        raise HTTPException(404, "A/B experiment not found")
    return results


# ── Compression History ────────────────────────────────────────────────────

@app.get("/history", response_model=list[CompressionHistoryEntry])
async def compression_history(
    profile: str | None = Query(None, description="Filter by compression profile"),
    limit: int = Query(50, ge=1, le=500),
):
    """View recent compression operations with token counts and ratios."""
    return await list_compression_history(app.state.db, profile, limit)


@app.get("/history/analytics", response_model=CompressionAnalytics)
async def compression_analytics():
    """Aggregated compression analytics: averages, by-profile breakdown, daily trend."""
    return await get_compression_analytics(app.state.db)


# ── Model Comparison ───────────────────────────────────────────────────────

@app.post("/compare", response_model=ModelCompareResponse)
async def model_compare(body: ModelCompareRequest):
    """Compare costs across all registered models for a prompt. Optionally include compressed cost."""
    return await compare_models(app.state.db, body.prompt, body.compress, body.profile)


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

    # Fire-and-forget alert evaluation after batch processing
    try:
        await evaluate_alerts(app.state.db)
    except Exception:
        pass

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
