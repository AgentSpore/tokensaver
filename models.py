"""TokenSaver v1.1.0 — Pydantic models for LLM cost optimization."""

from __future__ import annotations

from typing import Optional
from pydantic import BaseModel, Field


# ── Compression ──────────────────────────────────────────────────────────────

class CompressRequest(BaseModel):
    prompt: str = Field(description="Prompt text to compress")
    profile: str = Field("balanced", description="Compression profile name")


class CompressResponse(BaseModel):
    original: str
    compressed: str
    original_tokens: int
    compressed_tokens: int
    ratio: float
    profile: str
    rules_applied: int


class CompressionRecord(BaseModel):
    id: int
    original_tokens: int
    compressed_tokens: int
    ratio: float
    profile: str
    model: Optional[str]
    rules_applied: int
    created_at: str


class CompressionAnalytics(BaseModel):
    total_compressions: int
    total_tokens_saved: int
    avg_ratio: float
    best_ratio: float
    worst_ratio: float
    by_profile: dict[str, dict]
    tokens_saved_per_day: list[dict]


# ── Cache ────────────────────────────────────────────────────────────────────

class CacheSetRequest(BaseModel):
    prompt: str = Field(description="Prompt text (used as cache key)")
    response: str = Field(description="LLM response to cache")
    model: str = Field("default", description="Model name for scoping")
    ttl: Optional[int] = Field(None, description="TTL in seconds (null = no expiry)")


class CacheEntry(BaseModel):
    id: int
    prompt_hash: str
    prompt_preview: str
    model: str
    response: str
    hit_count: int
    ttl: Optional[int]
    created_at: str
    last_hit_at: Optional[str]
    expires_at: Optional[str]


class CacheAnalytics(BaseModel):
    total_entries: int
    total_hits: int
    hit_rate: float
    total_size_bytes: int
    top_entries: list[dict]
    by_model: dict[str, dict]


# ── Profiles ─────────────────────────────────────────────────────────────────

class ProfileCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    description: Optional[str] = None
    remove_filler: bool = True
    remove_duplicates: bool = True
    shorten_sentences: bool = False
    aggressiveness: float = Field(0.5, ge=0.0, le=1.0)


class ProfileUpdate(BaseModel):
    description: Optional[str] = None
    remove_filler: Optional[bool] = None
    remove_duplicates: Optional[bool] = None
    shorten_sentences: Optional[bool] = None
    aggressiveness: Optional[float] = Field(None, ge=0.0, le=1.0)


class ProfileResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    remove_filler: bool
    remove_duplicates: bool
    shorten_sentences: bool
    aggressiveness: float
    is_builtin: bool
    times_used: int
    created_at: str


# ── Model Costs ──────────────────────────────────────────────────────────────

class ModelCostCreate(BaseModel):
    model: str = Field(min_length=1, max_length=200)
    input_cost_per_1k: float = Field(ge=0, description="Cost per 1K input tokens in USD")
    output_cost_per_1k: float = Field(ge=0, description="Cost per 1K output tokens in USD")


class ModelCostUpdate(BaseModel):
    input_cost_per_1k: Optional[float] = Field(None, ge=0)
    output_cost_per_1k: Optional[float] = Field(None, ge=0)


class ModelCostResponse(BaseModel):
    id: int
    model: str
    input_cost_per_1k: float
    output_cost_per_1k: float
    created_at: str


# ── Statistics & Daily Log ───────────────────────────────────────────────────

class StatsResponse(BaseModel):
    total_requests: int
    total_tokens_in: int
    total_tokens_out: int
    total_tokens_saved: int
    total_cost: float
    total_savings: float
    cache_hits: int
    cache_misses: int
    cache_hit_rate: float
    avg_compression_ratio: float
    top_models: list[dict]


class DailyStatsEntry(BaseModel):
    date: str
    model: str
    requests: int
    tokens_in: int
    tokens_out: int
    tokens_saved: int
    cost: float
    cache_hits: int
    cache_misses: int


# ── Budget ───────────────────────────────────────────────────────────────────

class BudgetConfig(BaseModel):
    daily_limit: Optional[float] = Field(None, ge=0, description="Daily cost limit in USD")
    monthly_limit: Optional[float] = Field(None, ge=0, description="Monthly cost limit in USD")


class BudgetStatus(BaseModel):
    daily_limit: Optional[float]
    monthly_limit: Optional[float]
    daily_spent: float
    monthly_spent: float
    daily_remaining: Optional[float]
    monthly_remaining: Optional[float]
    over_budget: bool
    daily_pct: Optional[float]
    monthly_pct: Optional[float]


# ── Templates ────────────────────────────────────────────────────────────────

class TemplateCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    content: str = Field(min_length=1, description="Template with {{variable}} placeholders")
    description: Optional[str] = None
    tags: Optional[list[str]] = None


class TemplateUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=200)
    content: Optional[str] = Field(None, min_length=1)
    description: Optional[str] = None
    tags: Optional[list[str]] = None


class TemplateResponse(BaseModel):
    id: int
    name: str
    content: str
    description: Optional[str]
    tags: list[str]
    version: int
    times_rendered: int
    created_at: str
    updated_at: str


class TemplateRenderRequest(BaseModel):
    variables: dict[str, str] = Field(description="Variables to substitute into the template")


class TemplateRenderResponse(BaseModel):
    rendered: str
    original_tokens: int
    rendered_tokens: int
    variables_used: list[str]
    variables_missing: list[str]


class TemplateDiffResponse(BaseModel):
    template_id: int
    version_a: int
    version_b: int
    diff: list[str]


class TemplateVersionResponse(BaseModel):
    id: int
    template_id: int
    version: int
    content: str
    created_at: str


# ── Cost Estimation ──────────────────────────────────────────────────────────

class CostEstimateRequest(BaseModel):
    prompt: str
    max_output_tokens: int = Field(500, ge=1)


class CostEstimateEntry(BaseModel):
    model: str
    input_tokens: int
    output_tokens: int
    input_cost: float
    output_cost: float
    total_cost: float
    compressed_input_tokens: Optional[int]
    compressed_total_cost: Optional[float]
    savings: Optional[float]


class CostEstimateResponse(BaseModel):
    estimates: list[CostEstimateEntry]
    cheapest: str
    most_expensive: str


# ── Benchmarking ─────────────────────────────────────────────────────────────

class BenchmarkRequest(BaseModel):
    prompt: str


class BenchmarkEntry(BaseModel):
    profile: str
    original_tokens: int
    compressed_tokens: int
    ratio: float
    rules_applied: int
    estimated_cost: Optional[float]


class BenchmarkResponse(BaseModel):
    results: list[BenchmarkEntry]
    best_profile: str
    best_ratio: float


# ── Model Comparison ─────────────────────────────────────────────────────────

class ModelComparisonRequest(BaseModel):
    prompt: str
    max_output_tokens: int = Field(500, ge=1)
    profile: Optional[str] = Field(None, description="Profile for compression estimate")


class ModelComparisonEntry(BaseModel):
    model: str
    input_cost: float
    output_cost: float
    total_cost: float
    with_compression: Optional[float]
    savings_pct: Optional[float]


class ModelComparisonResponse(BaseModel):
    entries: list[ModelComparisonEntry]
    recommended: str
    recommendation_reason: str


# ── Batch Processing ─────────────────────────────────────────────────────────

class BatchPrompt(BaseModel):
    prompt: str
    model: str = "default"


class BatchRequest(BaseModel):
    prompts: list[BatchPrompt] = Field(min_length=1)
    profile: str = "balanced"
    use_cache: bool = True


class BatchResultEntry(BaseModel):
    index: int
    prompt_preview: str
    original_tokens: int
    compressed_tokens: int
    ratio: float
    cache_hit: bool
    deduplicated: bool
    mock_response: Optional[str]


class BatchResponse(BaseModel):
    total: int
    deduplicated: int
    cache_hits: int
    results: list[BatchResultEntry]
    total_tokens_saved: int
    total_estimated_cost: float


# ── Compression Rules ────────────────────────────────────────────────────────

class RuleCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    pattern: str = Field(description="Regex pattern to match")
    replacement: str = Field(description="Replacement string")
    priority: int = Field(0, description="Higher = applied first")
    enabled: bool = True
    description: Optional[str] = None


class RuleUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=200)
    pattern: Optional[str] = None
    replacement: Optional[str] = None
    priority: Optional[int] = None
    enabled: Optional[bool] = None
    description: Optional[str] = None


class RuleResponse(BaseModel):
    id: int
    name: str
    pattern: str
    replacement: str
    priority: int
    enabled: bool
    description: Optional[str]
    times_applied: int
    created_at: str


# ── Prompt Diff ──────────────────────────────────────────────────────────────

class PromptDiffRequest(BaseModel):
    prompt_a: str
    prompt_b: str


class PromptDiffResponse(BaseModel):
    tokens_a: int
    tokens_b: int
    token_diff: int
    diff_lines: list[str]
    similarity: float


# ── Usage Quotas ─────────────────────────────────────────────────────────────

class QuotaCreate(BaseModel):
    model: str
    daily_limit: Optional[int] = Field(None, ge=0, description="Max tokens per day")
    monthly_limit: Optional[int] = Field(None, ge=0, description="Max tokens per month")


class QuotaUpdate(BaseModel):
    daily_limit: Optional[int] = Field(None, ge=0)
    monthly_limit: Optional[int] = Field(None, ge=0)


class QuotaResponse(BaseModel):
    id: int
    model: str
    daily_limit: Optional[int]
    monthly_limit: Optional[int]
    daily_used: int
    monthly_used: int
    daily_remaining: Optional[int]
    monthly_remaining: Optional[int]
    over_quota: bool
    created_at: str


# ── Cost Alerts ──────────────────────────────────────────────────────────────

class AlertRuleCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    metric: str = Field(description="Metric: daily_cost, monthly_cost, tokens_used, cache_miss_rate")
    operator: str = Field(description="Operator: gt, gte, lt, lte, eq")
    threshold: float
    model: Optional[str] = None
    enabled: bool = True


class AlertRuleUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=200)
    metric: Optional[str] = None
    operator: Optional[str] = None
    threshold: Optional[float] = None
    model: Optional[str] = None
    enabled: Optional[bool] = None


class AlertRuleResponse(BaseModel):
    id: int
    name: str
    metric: str
    operator: str
    threshold: float
    model: Optional[str]
    enabled: bool
    times_triggered: int
    last_triggered_at: Optional[str]
    created_at: str


class AlertLogEntry(BaseModel):
    id: int
    rule_id: int
    rule_name: str
    metric: str
    current_value: float
    threshold: float
    message: str
    acknowledged: bool
    created_at: str


class AlertSummary(BaseModel):
    total_rules: int
    enabled_rules: int
    total_alerts_today: int
    total_alerts_week: int
    unacknowledged: int
    recent_alerts: list[AlertLogEntry]


# ── A/B Testing ──────────────────────────────────────────────────────────────

class ExperimentCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    description: Optional[str] = None
    profile_a: str = Field(description="First compression profile")
    profile_b: str = Field(description="Second compression profile")
    sample_size: int = Field(100, ge=2, description="Number of prompts to test")


class ExperimentResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    profile_a: str
    profile_b: str
    sample_size: int
    runs_completed: int
    status: str  # pending, running, completed
    created_at: str
    completed_at: Optional[str]


class ExperimentRunRequest(BaseModel):
    prompt: str


class ExperimentRunResponse(BaseModel):
    experiment_id: int
    run_number: int
    variant: str  # A or B
    profile: str
    original_tokens: int
    compressed_tokens: int
    ratio: float
    rules_applied: int


class ExperimentResults(BaseModel):
    experiment_id: int
    name: str
    status: str
    profile_a: str
    profile_b: str
    runs_a: int
    runs_b: int
    avg_ratio_a: float
    avg_ratio_b: float
    avg_tokens_saved_a: float
    avg_tokens_saved_b: float
    winner: Optional[str]
    confidence: Optional[float]
    detail: list[dict]


# ── Prompt Playground (NEW v1.0.0) ───────────────────────────────────────────

class PlaygroundSessionCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    description: Optional[str] = None


class PlaygroundSessionUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None


class PlaygroundSessionResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    runs_count: int
    created_at: str
    updated_at: str


class PlaygroundRunRequest(BaseModel):
    prompt: str
    profile: Optional[str] = "balanced"
    model: Optional[str] = None
    compress: bool = True
    cache_lookup: bool = True


class PlaygroundRunResponse(BaseModel):
    id: int
    session_id: int
    original_tokens: int
    compressed_tokens: int
    compression_ratio: float
    estimated_cost: Optional[float]
    cache_hit: bool
    profile_used: str
    rules_applied: int
    compressed_text: str
    created_at: str


# ── Cost Forecasting (NEW v1.0.0) ───────────────────────────────────────────

class CostForecastResponse(BaseModel):
    current_daily_avg: float
    current_weekly_total: float
    forecast_7d: float
    forecast_30d: float
    burn_rate_tokens_per_day: float
    burn_rate_cost_per_day: float
    budget_exhaustion_date: Optional[str]
    trend: str  # increasing, stable, decreasing
    trend_pct_change: float
    recommendations: list[str]


class CostBreakdownEntry(BaseModel):
    model: str
    total_tokens: int
    total_cost: float
    pct_of_total: float
    avg_daily_tokens: float
    trend: str  # increasing, stable, decreasing


# ── Compression Chains (NEW v1.0.0) ─────────────────────────────────────────

class ChainCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    description: Optional[str] = None
    steps: list[str] = Field(min_length=2, description="Profile names in order, min 2")


class ChainUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    steps: Optional[list[str]] = Field(None, min_length=2)


class ChainResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    steps: list[str]
    times_used: int
    avg_final_ratio: Optional[float]
    created_at: str


class ChainRunRequest(BaseModel):
    prompt: str


class ChainRunResponse(BaseModel):
    chain_id: int
    chain_name: str
    original_tokens: int
    final_tokens: int
    final_ratio: float
    step_results: list[dict]
    total_rules_applied: int


# ── Token Usage Heatmap (NEW v1.1.0) ────────────────────────────────────────

class HeatmapRequest(BaseModel):
    days: int = Field(default=7, ge=1, le=90)
    model: Optional[str] = None


class HeatmapCell(BaseModel):
    hour: int
    day: str
    requests: int
    tokens: int
    cost_usd: float


class HeatmapResponse(BaseModel):
    cells: list[HeatmapCell]
    peak_hour: int
    peak_day: str
    total_requests: int
    model_distribution: dict


class PeakAnalysis(BaseModel):
    peak_hours: list[dict]
    quiet_hours: list[dict]
    recommendation: str


# ── Prompt Versioning (NEW v1.1.0) ──────────────────────────────────────────

class PromptVersionCreate(BaseModel):
    name: str
    prompt_text: str
    model: Optional[str] = None
    tags: Optional[list[str]] = None
    notes: Optional[str] = None


class PromptVersionUpdate(BaseModel):
    prompt_text: Optional[str] = None
    tags: Optional[list[str]] = None
    notes: Optional[str] = None


class PromptVersionResponse(BaseModel):
    id: int
    name: str
    version: int
    prompt_text: str
    model: Optional[str]
    tags: list[str]
    notes: Optional[str]
    token_count: int
    times_used: int
    avg_cost: float
    created_at: str


class PromptVersionDiff(BaseModel):
    version_a: int
    version_b: int
    token_diff: int
    text_diff: str


# ── Cost Allocation Tags (NEW v1.1.0) ───────────────────────────────────────

class CostTagCreate(BaseModel):
    tag: str
    description: Optional[str] = None
    budget_usd: Optional[float] = Field(default=None, ge=0)


class CostTagUpdate(BaseModel):
    description: Optional[str] = None
    budget_usd: Optional[float] = Field(default=None, ge=0)


class CostTagResponse(BaseModel):
    id: int
    tag: str
    description: Optional[str]
    budget_usd: Optional[float]
    total_spent: float
    request_count: int
    created_at: str


class CostTagBreakdown(BaseModel):
    tag: str
    total_cost: float
    request_count: int
    avg_cost_per_request: float
    top_models: list[dict]
    budget_usd: Optional[float]
    budget_remaining: Optional[float]
    pct_used: Optional[float]


class CostTagAllocation(BaseModel):
    tags: list[CostTagBreakdown]
    untagged_cost: float
    total_cost: float
