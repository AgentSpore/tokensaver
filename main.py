import os
"""TokenSaver v1.1.0 — FastAPI main application.

LLM API cost optimization platform: compression, caching, cost estimation,
benchmarking, A/B testing, templates, alerts, playground, forecasting, chains,
heatmaps, prompt versioning, cost allocation tags.
"""

from __future__ import annotations

import io
from typing import Optional

import aiosqlite
import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import pathlib

import cache
from compressor import compress_prompt, estimate_tokens
from models import (
    # Compression
    CompressRequest,
    CompressResponse,
    CompressionRecord,
    CompressionAnalytics,
    # Cache
    CacheSetRequest,
    CacheEntry,
    CacheAnalytics,
    # Profiles
    ProfileCreate,
    ProfileUpdate,
    ProfileResponse,
    # Model Costs
    ModelCostCreate,
    ModelCostUpdate,
    ModelCostResponse,
    # Statistics
    StatsResponse,
    DailyStatsEntry,
    # Budget
    BudgetConfig,
    BudgetStatus,
    # Templates
    TemplateCreate,
    TemplateUpdate,
    TemplateResponse,
    TemplateRenderRequest,
    TemplateRenderResponse,
    TemplateDiffResponse,
    TemplateVersionResponse,
    # Cost Estimation
    CostEstimateRequest,
    CostEstimateResponse,
    # Benchmarking
    BenchmarkRequest,
    BenchmarkResponse,
    # Model Comparison
    ModelComparisonRequest,
    ModelComparisonResponse,
    # Batch
    BatchRequest,
    BatchResponse,
    # Rules
    RuleCreate,
    RuleUpdate,
    RuleResponse,
    # Prompt Diff
    PromptDiffRequest,
    PromptDiffResponse,
    # Usage Quotas
    QuotaCreate,
    QuotaUpdate,
    QuotaResponse,
    # Cost Alerts
    AlertRuleCreate,
    AlertRuleUpdate,
    AlertRuleResponse,
    AlertLogEntry,
    AlertSummary,
    # A/B Testing
    ExperimentCreate,
    ExperimentResponse,
    ExperimentRunRequest,
    ExperimentRunResponse,
    ExperimentResults,
    # Playground (NEW v1.0.0)
    PlaygroundSessionCreate,
    PlaygroundSessionUpdate,
    PlaygroundSessionResponse,
    PlaygroundRunRequest,
    PlaygroundRunResponse,
    # Cost Forecasting (NEW v1.0.0)
    CostForecastResponse,
    CostBreakdownEntry,
    # Compression Chains (NEW v1.0.0)
    ChainCreate,
    ChainUpdate,
    ChainResponse,
    ChainRunRequest,
    ChainRunResponse,
    # Token Usage Heatmap (NEW v1.1.0)
    HeatmapResponse,
    PeakAnalysis,
    # Prompt Versioning (NEW v1.1.0)
    PromptVersionCreate,
    PromptVersionUpdate,
    PromptVersionResponse,
    PromptVersionDiff,
    # Cost Allocation Tags (NEW v1.1.0)
    CostTagCreate,
    CostTagUpdate,
    CostTagResponse,
    CostTagAllocation,
)

# ── Application ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="TokenSaver",
    version="1.1.0",
    description="LLM API cost optimization platform — compression, caching, cost analytics.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Static Files ──────────────────────────────────────────────────────────────

_static_dir = pathlib.Path(__file__).parent / "static"
if _static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")


@app.get("/", include_in_schema=False)
async def serve_ui():
    index = _static_dir / "index.html"
    if index.exists():
        return FileResponse(str(index), media_type="text/html")
    return {"message": "TokenSaver API", "docs": "/docs"}


@app.on_event("startup")
async def startup() -> None:
    db = await aiosqlite.connect(os.environ.get("DB_PATH", "tokensaver.db"))
    await cache.init_db(db)
    app.state.db = db


@app.on_event("shutdown")
async def shutdown() -> None:
    await app.state.db.close()


# ── Health ────────────────────────────────────────────────────────────────────


@app.get("/health", tags=["health"])
async def health() -> dict:
    return {"status": "ok", "version": "1.1.0"}


# ── Compression ───────────────────────────────────────────────────────────────


@app.post("/compress", response_model=CompressResponse, tags=["compression"])
async def compress_endpoint(request: Request, body: CompressRequest) -> CompressResponse:
    db = request.app.state.db
    try:
        result = await cache.compress(db, body.prompt, body.profile)
        await cache.record_compression(
            db,
            result["original_tokens"],
            result["compressed_tokens"],
            result["ratio"],
            result["profile"],
            rules_applied=result["rules_applied"],
        )
        return CompressResponse(**result)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── Compression History ───────────────────────────────────────────────────────


@app.get("/history", response_model=list[CompressionRecord], tags=["compression"])
async def get_history(
    request: Request,
    limit: int = Query(50, ge=1, le=500),
    profile: Optional[str] = None,
) -> list[CompressionRecord]:
    db = request.app.state.db
    try:
        rows = await cache.compression_history(db, limit=limit, profile=profile)
        return [CompressionRecord(**r) for r in rows]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/history/analytics", response_model=CompressionAnalytics, tags=["compression"])
async def get_history_analytics(request: Request) -> CompressionAnalytics:
    db = request.app.state.db
    try:
        result = await cache.compression_analytics(db)
        return CompressionAnalytics(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── Compression Rules ─────────────────────────────────────────────────────────


@app.post("/rules", response_model=RuleResponse, status_code=201, tags=["rules"])
async def create_rule(request: Request, body: RuleCreate) -> RuleResponse:
    db = request.app.state.db
    try:
        result = await cache.create_rule(db, body.model_dump())
        return RuleResponse(**result)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/rules", response_model=list[RuleResponse], tags=["rules"])
async def list_rules(request: Request) -> list[RuleResponse]:
    db = request.app.state.db
    try:
        rows = await cache.list_rules(db)
        return [RuleResponse(**r) for r in rows]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/rules/{rule_id}", response_model=RuleResponse, tags=["rules"])
async def get_rule(request: Request, rule_id: int) -> RuleResponse:
    db = request.app.state.db
    try:
        result = await cache.get_rule(db, rule_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Rule not found")
        return RuleResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.patch("/rules/{rule_id}", response_model=RuleResponse, tags=["rules"])
async def update_rule(request: Request, rule_id: int, body: RuleUpdate) -> RuleResponse:
    db = request.app.state.db
    try:
        result = await cache.update_rule(db, rule_id, body.model_dump(exclude_none=True))
        if result is None:
            raise HTTPException(status_code=404, detail="Rule not found")
        return RuleResponse(**result)
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.delete("/rules/{rule_id}", status_code=204, tags=["rules"])
async def delete_rule(request: Request, rule_id: int) -> Response:
    db = request.app.state.db
    try:
        deleted = await cache.delete_rule(db, rule_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Rule not found")
        return Response(status_code=204)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── Prompt Diff ───────────────────────────────────────────────────────────────


@app.post("/diff", response_model=PromptDiffResponse, tags=["diff"])
async def diff_prompts(request: Request, body: PromptDiffRequest) -> PromptDiffResponse:
    try:
        result = await cache.prompt_diff(body.prompt_a, body.prompt_b)
        return PromptDiffResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── Usage Quotas ──────────────────────────────────────────────────────────────


@app.put("/quotas/{model}", response_model=QuotaResponse, status_code=201, tags=["quotas"])
async def upsert_quota(request: Request, model: str, body: QuotaCreate) -> QuotaResponse:
    db = request.app.state.db
    try:
        existing = await cache.get_quota_by_model(db, model)
        if existing:
            updated = await cache.update_quota(db, existing["id"], body.model_dump(exclude_none=True))
            return QuotaResponse(**updated)
        result = await cache.create_quota(db, {**body.model_dump(), "model": model})
        return QuotaResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/quotas", response_model=list[QuotaResponse], tags=["quotas"])
async def list_quotas(request: Request) -> list[QuotaResponse]:
    db = request.app.state.db
    try:
        rows = await cache.list_quotas(db)
        return [QuotaResponse(**r) for r in rows]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/quotas/{model}", response_model=QuotaResponse, tags=["quotas"])
async def get_quota(request: Request, model: str) -> QuotaResponse:
    db = request.app.state.db
    try:
        result = await cache.get_quota_by_model(db, model)
        if result is None:
            raise HTTPException(status_code=404, detail="Quota not found")
        return QuotaResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.delete("/quotas/{model}", status_code=204, tags=["quotas"])
async def delete_quota(request: Request, model: str) -> Response:
    db = request.app.state.db
    try:
        existing = await cache.get_quota_by_model(db, model)
        if not existing:
            raise HTTPException(status_code=404, detail="Quota not found")
        deleted = await cache.delete_quota(db, existing["id"])
        if not deleted:
            raise HTTPException(status_code=404, detail="Quota not found")
        return Response(status_code=204)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── Cost Estimation ───────────────────────────────────────────────────────────


@app.post("/estimate", response_model=CostEstimateResponse, tags=["cost"])
async def estimate_cost(request: Request, body: CostEstimateRequest) -> CostEstimateResponse:
    db = request.app.state.db
    try:
        result = await cache.estimate_cost(db, body.prompt, body.max_output_tokens)
        return CostEstimateResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── Benchmarking ──────────────────────────────────────────────────────────────


@app.post("/benchmark", response_model=BenchmarkResponse, tags=["benchmark"])
async def run_benchmark(request: Request, body: BenchmarkRequest) -> BenchmarkResponse:
    db = request.app.state.db
    try:
        result = await cache.benchmark(db, body.prompt)
        return BenchmarkResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── Budget ────────────────────────────────────────────────────────────────────


@app.put("/budget", response_model=BudgetStatus, tags=["budget"])
async def set_budget(request: Request, body: BudgetConfig) -> BudgetStatus:
    db = request.app.state.db
    try:
        result = await cache.set_budget(db, body.model_dump())
        return BudgetStatus(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/budget", response_model=BudgetStatus, tags=["budget"])
async def get_budget(request: Request) -> BudgetStatus:
    db = request.app.state.db
    try:
        result = await cache.get_budget(db)
        return BudgetStatus(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── Profiles ──────────────────────────────────────────────────────────────────


@app.post("/profiles", response_model=ProfileResponse, status_code=201, tags=["profiles"])
async def create_profile(request: Request, body: ProfileCreate) -> ProfileResponse:
    db = request.app.state.db
    try:
        result = await cache.create_profile(db, body.model_dump())
        return ProfileResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/profiles", response_model=list[ProfileResponse], tags=["profiles"])
async def list_profiles(request: Request) -> list[ProfileResponse]:
    db = request.app.state.db
    try:
        rows = await cache.list_profiles(db)
        return [ProfileResponse(**r) for r in rows]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/profiles/{name}", response_model=ProfileResponse, tags=["profiles"])
async def get_profile(request: Request, name: str) -> ProfileResponse:
    db = request.app.state.db
    try:
        result = await cache.get_profile(db, name)
        if result is None:
            raise HTTPException(status_code=404, detail="Profile not found")
        return ProfileResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.patch("/profiles/{name}", response_model=ProfileResponse, tags=["profiles"])
async def update_profile(request: Request, name: str, body: ProfileUpdate) -> ProfileResponse:
    db = request.app.state.db
    try:
        result = await cache.update_profile(db, name, body.model_dump(exclude_none=True))
        if result is None:
            raise HTTPException(status_code=404, detail="Profile not found or is builtin")
        return ProfileResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.delete("/profiles/{name}", status_code=204, tags=["profiles"])
async def delete_profile(request: Request, name: str) -> Response:
    db = request.app.state.db
    try:
        deleted = await cache.delete_profile(db, name)
        if not deleted:
            raise HTTPException(status_code=404, detail="Profile not found or is builtin")
        return Response(status_code=204)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── Model Costs ───────────────────────────────────────────────────────────────


@app.post("/models", response_model=ModelCostResponse, status_code=201, tags=["models"])
async def create_model_cost(request: Request, body: ModelCostCreate) -> ModelCostResponse:
    db = request.app.state.db
    try:
        result = await cache.create_model_cost(db, body.model_dump())
        return ModelCostResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/models", response_model=list[ModelCostResponse], tags=["models"])
async def list_model_costs(request: Request) -> list[ModelCostResponse]:
    db = request.app.state.db
    try:
        rows = await cache.list_model_costs(db)
        return [ModelCostResponse(**r) for r in rows]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/models/{name}", response_model=ModelCostResponse, tags=["models"])
async def get_model_cost(request: Request, name: str) -> ModelCostResponse:
    db = request.app.state.db
    try:
        result = await cache.get_model_cost(db, name)
        if result is None:
            raise HTTPException(status_code=404, detail="Model not found")
        return ModelCostResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.patch("/models/{name}", response_model=ModelCostResponse, tags=["models"])
async def update_model_cost(request: Request, name: str, body: ModelCostUpdate) -> ModelCostResponse:
    db = request.app.state.db
    try:
        result = await cache.update_model_cost(db, name, body.model_dump(exclude_none=True))
        if result is None:
            raise HTTPException(status_code=404, detail="Model not found")
        return ModelCostResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.delete("/models/{name}", status_code=204, tags=["models"])
async def delete_model_cost(request: Request, name: str) -> Response:
    db = request.app.state.db
    try:
        deleted = await cache.delete_model_cost(db, name)
        if not deleted:
            raise HTTPException(status_code=404, detail="Model not found")
        return Response(status_code=204)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── Templates ─────────────────────────────────────────────────────────────────


@app.post("/templates", response_model=TemplateResponse, status_code=201, tags=["templates"])
async def create_template(request: Request, body: TemplateCreate) -> TemplateResponse:
    db = request.app.state.db
    try:
        result = await cache.create_template(db, body.model_dump())
        return TemplateResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/templates", response_model=list[TemplateResponse], tags=["templates"])
async def list_templates(
    request: Request,
    tag: Optional[str] = None,
) -> list[TemplateResponse]:
    db = request.app.state.db
    try:
        rows = await cache.list_templates(db, tag=tag)
        return [TemplateResponse(**r) for r in rows]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/templates/{template_id}", response_model=TemplateResponse, tags=["templates"])
async def get_template(request: Request, template_id: int) -> TemplateResponse:
    db = request.app.state.db
    try:
        result = await cache.get_template(db, template_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Template not found")
        return TemplateResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.patch("/templates/{template_id}", response_model=TemplateResponse, tags=["templates"])
async def update_template(
    request: Request, template_id: int, body: TemplateUpdate
) -> TemplateResponse:
    db = request.app.state.db
    try:
        result = await cache.update_template(db, template_id, body.model_dump(exclude_none=True))
        if result is None:
            raise HTTPException(status_code=404, detail="Template not found")
        return TemplateResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.delete("/templates/{template_id}", status_code=204, tags=["templates"])
async def delete_template(request: Request, template_id: int) -> Response:
    db = request.app.state.db
    try:
        deleted = await cache.delete_template(db, template_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Template not found")
        return Response(status_code=204)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post(
    "/templates/{template_id}/render",
    response_model=TemplateRenderResponse,
    tags=["templates"],
)
async def render_template(
    request: Request, template_id: int, body: TemplateRenderRequest
) -> TemplateRenderResponse:
    db = request.app.state.db
    try:
        result = await cache.render_template(db, template_id, body.variables)
        if result is None:
            raise HTTPException(status_code=404, detail="Template not found")
        return TemplateRenderResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── Template Versioning ───────────────────────────────────────────────────────


@app.get(
    "/templates/{template_id}/versions",
    response_model=list[TemplateVersionResponse],
    tags=["templates"],
)
async def get_template_versions(
    request: Request, template_id: int
) -> list[TemplateVersionResponse]:
    db = request.app.state.db
    try:
        # Ensure template exists
        t = await cache.get_template(db, template_id)
        if t is None:
            raise HTTPException(status_code=404, detail="Template not found")
        rows = await cache.template_versions(db, template_id)
        return [TemplateVersionResponse(**r) for r in rows]
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get(
    "/templates/versions/{version_id}",
    response_model=TemplateVersionResponse,
    tags=["templates"],
)
async def get_template_version_by_id(
    request: Request, version_id: int
) -> TemplateVersionResponse:
    db = request.app.state.db
    try:
        rows = await db.execute_fetchall(
            "SELECT * FROM template_versions WHERE id = ?", (version_id,)
        )
        if not rows:
            raise HTTPException(status_code=404, detail="Template version not found")
        return TemplateVersionResponse(**dict(rows[0]))
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get(
    "/templates/{template_id}/versions/diff",
    response_model=TemplateDiffResponse,
    tags=["templates"],
)
async def diff_template_versions(
    request: Request,
    template_id: int,
    v_a: int = Query(..., description="First version number"),
    v_b: int = Query(..., description="Second version number"),
) -> TemplateDiffResponse:
    db = request.app.state.db
    try:
        result = await cache.template_diff(db, template_id, v_a, v_b)
        if result is None:
            raise HTTPException(
                status_code=404, detail="Template or version(s) not found"
            )
        return TemplateDiffResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post(
    "/templates/{template_id}/rollback",
    response_model=TemplateResponse,
    tags=["templates"],
)
async def rollback_template(
    request: Request,
    template_id: int,
    version: int = Query(..., description="Version number to roll back to"),
) -> TemplateResponse:
    db = request.app.state.db
    try:
        result = await cache.template_rollback(db, template_id, version)
        if result is None:
            raise HTTPException(
                status_code=404, detail="Template or target version not found"
            )
        return TemplateResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── Cost Alerts ───────────────────────────────────────────────────────────────


@app.post(
    "/alerts/rules",
    response_model=AlertRuleResponse,
    status_code=201,
    tags=["alerts"],
)
async def create_alert_rule(request: Request, body: AlertRuleCreate) -> AlertRuleResponse:
    db = request.app.state.db
    try:
        result = await cache.create_alert_rule(db, body.model_dump())
        return AlertRuleResponse(**result)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/alerts/rules", response_model=list[AlertRuleResponse], tags=["alerts"])
async def list_alert_rules(request: Request) -> list[AlertRuleResponse]:
    db = request.app.state.db
    try:
        rows = await cache.list_alert_rules(db)
        return [AlertRuleResponse(**r) for r in rows]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/alerts/rules/{rule_id}", response_model=AlertRuleResponse, tags=["alerts"])
async def get_alert_rule(request: Request, rule_id: int) -> AlertRuleResponse:
    db = request.app.state.db
    try:
        result = await cache.get_alert_rule(db, rule_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Alert rule not found")
        return AlertRuleResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.patch("/alerts/rules/{rule_id}", response_model=AlertRuleResponse, tags=["alerts"])
async def update_alert_rule(
    request: Request, rule_id: int, body: AlertRuleUpdate
) -> AlertRuleResponse:
    db = request.app.state.db
    try:
        result = await cache.update_alert_rule(db, rule_id, body.model_dump(exclude_none=True))
        if result is None:
            raise HTTPException(status_code=404, detail="Alert rule not found")
        return AlertRuleResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.delete("/alerts/rules/{rule_id}", status_code=204, tags=["alerts"])
async def delete_alert_rule(request: Request, rule_id: int) -> Response:
    db = request.app.state.db
    try:
        deleted = await cache.delete_alert_rule(db, rule_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Alert rule not found")
        return Response(status_code=204)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/alerts/evaluate", tags=["alerts"])
async def evaluate_alerts(request: Request) -> list[dict]:
    db = request.app.state.db
    try:
        triggered = await cache.evaluate_alerts(db)
        return triggered
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/alerts/log", response_model=list[AlertLogEntry], tags=["alerts"])
async def get_alert_log(
    request: Request,
    limit: int = Query(50, ge=1, le=500),
    acknowledged: Optional[bool] = None,
) -> list[AlertLogEntry]:
    db = request.app.state.db
    try:
        rows = await cache.alert_log(db, limit=limit, acknowledged=acknowledged)
        return [AlertLogEntry(**r) for r in rows]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/alerts/{alert_id}/acknowledge", tags=["alerts"])
async def acknowledge_alert(request: Request, alert_id: int) -> dict:
    db = request.app.state.db
    try:
        ok = await cache.acknowledge_alert(db, alert_id)
        if not ok:
            raise HTTPException(status_code=404, detail="Alert not found")
        return {"acknowledged": True}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/alerts/summary", response_model=AlertSummary, tags=["alerts"])
async def get_alert_summary(request: Request) -> AlertSummary:
    db = request.app.state.db
    try:
        result = await cache.alert_summary(db)
        return AlertSummary(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── A/B Testing ───────────────────────────────────────────────────────────────


@app.post(
    "/ab-experiments",
    response_model=ExperimentResponse,
    status_code=201,
    tags=["ab-testing"],
)
async def create_experiment(request: Request, body: ExperimentCreate) -> ExperimentResponse:
    db = request.app.state.db
    try:
        result = await cache.create_experiment(db, body.model_dump())
        return ExperimentResponse(**result)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/ab-experiments", response_model=list[ExperimentResponse], tags=["ab-testing"])
async def list_experiments(request: Request) -> list[ExperimentResponse]:
    db = request.app.state.db
    try:
        rows = await cache.list_experiments(db)
        return [ExperimentResponse(**r) for r in rows]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get(
    "/ab-experiments/{exp_id}",
    response_model=ExperimentResponse,
    tags=["ab-testing"],
)
async def get_experiment(request: Request, exp_id: int) -> ExperimentResponse:
    db = request.app.state.db
    try:
        result = await cache.get_experiment(db, exp_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Experiment not found")
        return ExperimentResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post(
    "/ab-experiments/{exp_id}/test",
    response_model=ExperimentRunResponse,
    tags=["ab-testing"],
)
async def run_experiment(
    request: Request, exp_id: int, body: ExperimentRunRequest
) -> ExperimentRunResponse:
    db = request.app.state.db
    try:
        result = await cache.run_experiment(db, exp_id, body.prompt)
        if result is None:
            raise HTTPException(
                status_code=404,
                detail="Experiment not found or already completed",
            )
        return ExperimentRunResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post(
    "/ab-experiments/{exp_id}/complete",
    response_model=ExperimentResults,
    tags=["ab-testing"],
)
async def complete_experiment(request: Request, exp_id: int) -> ExperimentResults:
    db = request.app.state.db
    try:
        result = await cache.complete_experiment(db, exp_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Experiment not found")
        return ExperimentResults(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.delete("/ab-experiments/{exp_id}", status_code=204, tags=["ab-testing"])
async def delete_experiment(request: Request, exp_id: int) -> Response:
    db = request.app.state.db
    try:
        deleted = await cache.delete_experiment(db, exp_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Experiment not found")
        return Response(status_code=204)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get(
    "/ab-experiments/{exp_id}/results",
    response_model=ExperimentResults,
    tags=["ab-testing"],
)
async def get_experiment_results(request: Request, exp_id: int) -> ExperimentResults:
    db = request.app.state.db
    try:
        result = await cache.experiment_results(db, exp_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Experiment not found")
        return ExperimentResults(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── Model Comparison ──────────────────────────────────────────────────────────


@app.post("/compare", response_model=ModelComparisonResponse, tags=["cost"])
async def compare_models(
    request: Request, body: ModelComparisonRequest
) -> ModelComparisonResponse:
    db = request.app.state.db
    try:
        result = await cache.compare_models(
            db, body.prompt, body.max_output_tokens, body.profile
        )
        return ModelComparisonResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── Cache ─────────────────────────────────────────────────────────────────────


@app.get("/cache", response_model=list[CacheEntry], tags=["cache"])
async def list_cache(
    request: Request,
    model: Optional[str] = None,
    limit: int = Query(50, ge=1, le=500),
) -> list[CacheEntry]:
    db = request.app.state.db
    try:
        rows = await cache.cache_list(db, model=model, limit=limit)
        return [CacheEntry(**r) for r in rows]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/cache/analytics", response_model=CacheAnalytics, tags=["cache"])
async def get_cache_analytics(request: Request) -> CacheAnalytics:
    db = request.app.state.db
    try:
        result = await cache.cache_analytics(db)
        return CacheAnalytics(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/cache/{cache_hash}", response_model=CacheEntry, tags=["cache"])
async def get_cache_by_hash(request: Request, cache_hash: str) -> CacheEntry:
    db = request.app.state.db
    try:
        rows = await db.execute_fetchall(
            "SELECT * FROM cache WHERE prompt_hash = ?", (cache_hash,)
        )
        if not rows:
            raise HTTPException(status_code=404, detail="Cache entry not found")
        return CacheEntry(**dict(rows[0]))
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/cache/lookup", response_model=Optional[CacheEntry], tags=["cache"])
async def cache_lookup(request: Request, body: CacheSetRequest) -> Optional[CacheEntry]:
    db = request.app.state.db
    try:
        result = await cache.cache_get(db, body.prompt, body.model)
        if result is None:
            return None
        return CacheEntry(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/cache/store", response_model=CacheEntry, status_code=201, tags=["cache"])
async def cache_store(request: Request, body: CacheSetRequest) -> CacheEntry:
    db = request.app.state.db
    try:
        result = await cache.cache_set(db, body.prompt, body.response, body.model, body.ttl)
        return CacheEntry(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/cache/purge", tags=["cache"])
async def purge_cache(
    request: Request,
    model: Optional[str] = None,
) -> dict:
    db = request.app.state.db
    try:
        count = await cache.cache_purge(db, model=model)
        return {"deleted": count}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.delete("/cache/{cache_hash}", status_code=204, tags=["cache"])
async def delete_cache_entry(request: Request, cache_hash: str) -> Response:
    db = request.app.state.db
    try:
        rows = await db.execute_fetchall(
            "SELECT id FROM cache WHERE prompt_hash = ?", (cache_hash,)
        )
        if not rows:
            raise HTTPException(status_code=404, detail="Cache entry not found")
        entry_id = dict(rows[0])["id"]
        deleted = await cache.cache_delete(db, entry_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Cache entry not found")
        return Response(status_code=204)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── Batch Processing ──────────────────────────────────────────────────────────


@app.post("/batch", response_model=BatchResponse, tags=["batch"])
async def batch_process(request: Request, body: BatchRequest) -> BatchResponse:
    db = request.app.state.db
    try:
        prompts = [p.model_dump() for p in body.prompts]
        result = await cache.batch_process(db, prompts, body.profile, body.use_cache)
        return BatchResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── Statistics ────────────────────────────────────────────────────────────────


@app.get("/stats", response_model=StatsResponse, tags=["stats"])
async def get_stats(request: Request) -> StatsResponse:
    db = request.app.state.db
    try:
        result = await cache.get_stats(db)
        return StatsResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/stats/daily", response_model=list[DailyStatsEntry], tags=["stats"])
async def get_daily_stats(
    request: Request,
    days: int = Query(30, ge=1, le=365),
) -> list[DailyStatsEntry]:
    db = request.app.state.db
    try:
        rows = await cache.daily_stats(db, days=days)
        return [DailyStatsEntry(**r) for r in rows]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/stats/daily/csv", tags=["stats"])
async def get_daily_stats_csv(request: Request) -> StreamingResponse:
    db = request.app.state.db
    try:
        csv_data = await cache.export_csv(db)
        return StreamingResponse(
            io.StringIO(csv_data),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=daily_stats.csv"},
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ══════════════════════════════════════════════════════════════════════════════
# NEW v1.0.0: Prompt Playground
# ══════════════════════════════════════════════════════════════════════════════


@app.post(
    "/playground/sessions",
    response_model=PlaygroundSessionResponse,
    status_code=201,
    tags=["playground"],
)
async def create_playground_session(
    request: Request, body: PlaygroundSessionCreate
) -> PlaygroundSessionResponse:
    db = request.app.state.db
    try:
        result = await cache.create_playground_session(db, body.model_dump())
        return PlaygroundSessionResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get(
    "/playground/sessions",
    response_model=list[PlaygroundSessionResponse],
    tags=["playground"],
)
async def list_playground_sessions(
    request: Request,
) -> list[PlaygroundSessionResponse]:
    db = request.app.state.db
    try:
        rows = await cache.list_playground_sessions(db)
        return [PlaygroundSessionResponse(**r) for r in rows]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get(
    "/playground/sessions/{session_id}",
    response_model=PlaygroundSessionResponse,
    tags=["playground"],
)
async def get_playground_session(
    request: Request, session_id: int
) -> PlaygroundSessionResponse:
    db = request.app.state.db
    try:
        result = await cache.get_playground_session(db, session_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Playground session not found")
        return PlaygroundSessionResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.patch(
    "/playground/sessions/{session_id}",
    response_model=PlaygroundSessionResponse,
    tags=["playground"],
)
async def update_playground_session(
    request: Request, session_id: int, body: PlaygroundSessionUpdate
) -> PlaygroundSessionResponse:
    db = request.app.state.db
    try:
        result = await cache.update_playground_session(
            db, session_id, body.model_dump(exclude_none=True)
        )
        if result is None:
            raise HTTPException(status_code=404, detail="Playground session not found")
        return PlaygroundSessionResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.delete("/playground/sessions/{session_id}", status_code=204, tags=["playground"])
async def delete_playground_session(request: Request, session_id: int) -> Response:
    db = request.app.state.db
    try:
        deleted = await cache.delete_playground_session(db, session_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Playground session not found")
        return Response(status_code=204)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post(
    "/playground/sessions/{session_id}/run",
    response_model=PlaygroundRunResponse,
    tags=["playground"],
)
async def run_playground_session(
    request: Request, session_id: int, body: PlaygroundRunRequest
) -> PlaygroundRunResponse:
    db = request.app.state.db
    try:
        result = await cache.run_playground(db, session_id, body.model_dump())
        if result is None:
            raise HTTPException(status_code=404, detail="Playground session not found")
        return PlaygroundRunResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get(
    "/playground/sessions/{session_id}/runs",
    response_model=list[PlaygroundRunResponse],
    tags=["playground"],
)
async def list_playground_runs(
    request: Request,
    session_id: int,
    limit: int = Query(50, ge=1, le=500),
) -> list[PlaygroundRunResponse]:
    db = request.app.state.db
    try:
        # Verify session exists
        session = await cache.get_playground_session(db, session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Playground session not found")
        rows = await cache.list_playground_runs(db, session_id, limit=limit)
        return [PlaygroundRunResponse(**r) for r in rows]
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get(
    "/playground/runs/{run_id}",
    response_model=PlaygroundRunResponse,
    tags=["playground"],
)
async def get_playground_run(request: Request, run_id: int) -> PlaygroundRunResponse:
    db = request.app.state.db
    try:
        result = await cache.get_playground_run(db, run_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Playground run not found")
        return PlaygroundRunResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.delete("/playground/runs/{run_id}", status_code=204, tags=["playground"])
async def delete_playground_run(request: Request, run_id: int) -> Response:
    db = request.app.state.db
    try:
        deleted = await cache.delete_playground_run(db, run_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Playground run not found")
        return Response(status_code=204)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ══════════════════════════════════════════════════════════════════════════════
# NEW v1.0.0: Cost Forecasting
# ══════════════════════════════════════════════════════════════════════════════


@app.get("/forecast", response_model=CostForecastResponse, tags=["forecast"])
async def get_forecast(request: Request) -> CostForecastResponse:
    db = request.app.state.db
    try:
        result = await cache.get_cost_forecast(db)
        return CostForecastResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get(
    "/forecast/breakdown",
    response_model=list[CostBreakdownEntry],
    tags=["forecast"],
)
async def get_forecast_breakdown(
    request: Request,
    days: int = Query(30, ge=1, le=365),
) -> list[CostBreakdownEntry]:
    db = request.app.state.db
    try:
        rows = await cache.get_cost_breakdown(db, days=days)
        return [CostBreakdownEntry(**r) for r in rows]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ══════════════════════════════════════════════════════════════════════════════
# NEW v1.0.0: Compression Chains
# ══════════════════════════════════════════════════════════════════════════════


@app.post(
    "/chains",
    response_model=ChainResponse,
    status_code=201,
    tags=["chains"],
)
async def create_chain(request: Request, body: ChainCreate) -> ChainResponse:
    db = request.app.state.db
    try:
        result = await cache.create_chain(db, body.model_dump())
        return ChainResponse(**result)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/chains", response_model=list[ChainResponse], tags=["chains"])
async def list_chains(request: Request) -> list[ChainResponse]:
    db = request.app.state.db
    try:
        rows = await cache.list_chains(db)
        return [ChainResponse(**r) for r in rows]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/chains/{chain_id}", response_model=ChainResponse, tags=["chains"])
async def get_chain(request: Request, chain_id: int) -> ChainResponse:
    db = request.app.state.db
    try:
        result = await cache.get_chain(db, chain_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Chain not found")
        return ChainResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.patch("/chains/{chain_id}", response_model=ChainResponse, tags=["chains"])
async def update_chain(request: Request, chain_id: int, body: ChainUpdate) -> ChainResponse:
    db = request.app.state.db
    try:
        result = await cache.update_chain(db, chain_id, body.model_dump(exclude_none=True))
        if result is None:
            raise HTTPException(status_code=404, detail="Chain not found")
        return ChainResponse(**result)
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.delete("/chains/{chain_id}", status_code=204, tags=["chains"])
async def delete_chain(request: Request, chain_id: int) -> Response:
    db = request.app.state.db
    try:
        deleted = await cache.delete_chain(db, chain_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Chain not found")
        return Response(status_code=204)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/chains/{chain_id}/run", response_model=ChainRunResponse, tags=["chains"])
async def run_chain(
    request: Request, chain_id: int, body: ChainRunRequest
) -> ChainRunResponse:
    db = request.app.state.db
    try:
        result = await cache.run_chain(db, chain_id, body.prompt)
        if result is None:
            raise HTTPException(status_code=404, detail="Chain not found")
        return ChainRunResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/chains/find-optimal", response_model=ChainRunResponse, tags=["chains"])
async def find_optimal_chain(
    request: Request,
    prompt: str = Query(..., description="Prompt text to optimize"),
    max_steps: int = Query(3, ge=2, le=6, description="Maximum chain length to try"),
) -> ChainRunResponse:
    db = request.app.state.db
    try:
        result = await cache.find_optimal_chain(db, prompt, max_steps=max_steps)
        return ChainRunResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ══════════════════════════════════════════════════════════════════════════════
# NEW v1.1.0: Token Usage Heatmap
# ══════════════════════════════════════════════════════════════════════════════


@app.get("/heatmap", response_model=HeatmapResponse, tags=["heatmap"])
async def get_usage_heatmap(
    request: Request,
    days: int = Query(7, ge=1, le=90),
    model: Optional[str] = None,
) -> HeatmapResponse:
    db = request.app.state.db
    try:
        result = await cache.get_usage_heatmap(db, days=days, model=model)
        return HeatmapResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/heatmap/peaks", response_model=PeakAnalysis, tags=["heatmap"])
async def get_peak_analysis(
    request: Request,
    days: int = Query(7, ge=1, le=90),
) -> PeakAnalysis:
    db = request.app.state.db
    try:
        result = await cache.get_peak_analysis(db, days=days)
        return PeakAnalysis(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ══════════════════════════════════════════════════════════════════════════════
# NEW v1.1.0: Prompt Versioning
# ══════════════════════════════════════════════════════════════════════════════

# Register non-parameterized routes BEFORE parameterized ones

@app.get("/prompts/diff", response_model=PromptVersionDiff, tags=["prompts"])
async def diff_prompt_versions(
    request: Request,
    version_a: int = Query(..., description="First prompt version ID"),
    version_b: int = Query(..., description="Second prompt version ID"),
) -> PromptVersionDiff:
    db = request.app.state.db
    try:
        result = await cache.diff_prompt_versions(db, version_a, version_b)
        if result is None:
            raise HTTPException(status_code=404, detail="Prompt version(s) not found")
        return PromptVersionDiff(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post(
    "/prompts",
    response_model=PromptVersionResponse,
    status_code=201,
    tags=["prompts"],
)
async def create_prompt_version(
    request: Request, body: PromptVersionCreate
) -> PromptVersionResponse:
    db = request.app.state.db
    try:
        result = await cache.create_prompt_version(db, body.model_dump())
        return PromptVersionResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/prompts", response_model=list[PromptVersionResponse], tags=["prompts"])
async def list_prompt_versions(
    request: Request,
    name: Optional[str] = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> list[PromptVersionResponse]:
    db = request.app.state.db
    try:
        rows = await cache.list_prompt_versions(db, name=name, limit=limit, offset=offset)
        return [PromptVersionResponse(**r) for r in rows]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get(
    "/prompts/{prompt_id}",
    response_model=PromptVersionResponse,
    tags=["prompts"],
)
async def get_prompt_version(request: Request, prompt_id: int) -> PromptVersionResponse:
    db = request.app.state.db
    try:
        result = await cache.get_prompt_version(db, prompt_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Prompt version not found")
        return PromptVersionResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.put(
    "/prompts/{prompt_id}",
    response_model=PromptVersionResponse,
    tags=["prompts"],
)
async def update_prompt_version(
    request: Request, prompt_id: int, body: PromptVersionUpdate
) -> PromptVersionResponse:
    db = request.app.state.db
    try:
        result = await cache.update_prompt_version(
            db, prompt_id, body.model_dump(exclude_none=True)
        )
        if result is None:
            raise HTTPException(status_code=404, detail="Prompt version not found")
        return PromptVersionResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.delete("/prompts/{prompt_id}", status_code=204, tags=["prompts"])
async def delete_prompt_version(request: Request, prompt_id: int) -> Response:
    db = request.app.state.db
    try:
        deleted = await cache.delete_prompt_version(db, prompt_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Prompt version not found")
        return Response(status_code=204)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get(
    "/prompts/{prompt_id}/history",
    response_model=list[PromptVersionResponse],
    tags=["prompts"],
)
async def list_prompt_history(
    request: Request, prompt_id: int
) -> list[PromptVersionResponse]:
    db = request.app.state.db
    try:
        rows = await cache.list_prompt_history(db, prompt_id)
        if rows is None:
            raise HTTPException(status_code=404, detail="Prompt version not found")
        return [PromptVersionResponse(**r) for r in rows]
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post(
    "/prompts/{prompt_id}/use",
    response_model=PromptVersionResponse,
    tags=["prompts"],
)
async def use_prompt_version(request: Request, prompt_id: int) -> PromptVersionResponse:
    db = request.app.state.db
    try:
        result = await cache.use_prompt_version(db, prompt_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Prompt version not found")
        return PromptVersionResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ══════════════════════════════════════════════════════════════════════════════
# NEW v1.1.0: Cost Allocation Tags
# ══════════════════════════════════════════════════════════════════════════════

# Register non-parameterized routes BEFORE parameterized ones

@app.get("/cost-tags/breakdown", response_model=CostTagAllocation, tags=["cost-tags"])
async def get_cost_tag_breakdown(
    request: Request,
    from_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
) -> CostTagAllocation:
    db = request.app.state.db
    try:
        result = await cache.get_cost_tag_breakdown(db, from_date=from_date, to_date=to_date)
        return CostTagAllocation(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post(
    "/cost-tags",
    response_model=CostTagResponse,
    status_code=201,
    tags=["cost-tags"],
)
async def create_cost_tag(request: Request, body: CostTagCreate) -> CostTagResponse:
    db = request.app.state.db
    try:
        result = await cache.create_cost_tag(db, body.model_dump())
        return CostTagResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/cost-tags", response_model=list[CostTagResponse], tags=["cost-tags"])
async def list_cost_tags(request: Request) -> list[CostTagResponse]:
    db = request.app.state.db
    try:
        rows = await cache.list_cost_tags(db)
        return [CostTagResponse(**r) for r in rows]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get(
    "/cost-tags/{tag_id}",
    response_model=CostTagResponse,
    tags=["cost-tags"],
)
async def get_cost_tag(request: Request, tag_id: int) -> CostTagResponse:
    db = request.app.state.db
    try:
        result = await cache.get_cost_tag(db, tag_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Cost tag not found")
        return CostTagResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.patch(
    "/cost-tags/{tag_id}",
    response_model=CostTagResponse,
    tags=["cost-tags"],
)
async def update_cost_tag(
    request: Request, tag_id: int, body: CostTagUpdate
) -> CostTagResponse:
    db = request.app.state.db
    try:
        result = await cache.update_cost_tag(db, tag_id, body.model_dump(exclude_none=True))
        if result is None:
            raise HTTPException(status_code=404, detail="Cost tag not found")
        return CostTagResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.delete("/cost-tags/{tag_id}", status_code=204, tags=["cost-tags"])
async def delete_cost_tag(request: Request, tag_id: int) -> Response:
    db = request.app.state.db
    try:
        deleted = await cache.delete_cost_tag(db, tag_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Cost tag not found")
        return Response(status_code=204)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/cost-tags/{tag_id}/allocate", status_code=201, tags=["cost-tags"])
async def allocate_cost_to_tag(
    request: Request,
    tag_id: int,
    compression_id: int = Query(..., description="Compression record ID to allocate"),
) -> dict:
    db = request.app.state.db
    try:
        result = await cache.allocate_cost(db, tag_id, compression_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Cost tag or compression not found")
        return result
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
