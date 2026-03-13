from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional


class CompressRequest(BaseModel):
    prompt: str = Field(..., description="Original prompt text")
    max_ratio: float = Field(0.5, ge=0.1, le=1.0, description="Target compression ratio (0.5 = keep 50%)")
    preserve_code: bool = Field(True, description="Preserve code blocks verbatim")
    profile: Optional[str] = Field(None, description="Named compression profile to apply (overrides max_ratio)")
    apply_rules: bool = Field(True, description="Apply custom compression rules during compression")


class CompressResponse(BaseModel):
    original: str
    compressed: str
    original_tokens: int
    compressed_tokens: int
    savings_pct: float
    compression_ratio: float
    profile_used: Optional[str] = None
    rules_applied: int = 0


class CacheEntry(BaseModel):
    prompt_hash: str
    response: str
    model: str
    tokens_saved: int
    hits: int
    created_at: str
    last_hit: str


class BatchItem(BaseModel):
    id: str = Field(..., description="Client-supplied request ID")
    prompt: str
    model: str = Field("gpt-4o-mini")
    max_tokens: int = Field(512, ge=1, le=8192)


class BatchRequest(BaseModel):
    items: list[BatchItem] = Field(..., min_length=1, max_length=50)
    dedup: bool = Field(True, description="Deduplicate identical prompts before sending")


class BatchResultItem(BaseModel):
    id: str
    status: str
    response: Optional[str] = None
    tokens_used: int = 0
    tokens_saved: int = 0


class BatchResponse(BaseModel):
    results: list[BatchResultItem]
    total_tokens_used: int
    total_tokens_saved: int
    deduped_count: int
    cached_count: int


class ModelCostCreate(BaseModel):
    name: str = Field(..., description="Model name, e.g. gpt-4o, claude-sonnet-4-20250514")
    input_cost_per_1m: float = Field(..., ge=0, description="Cost per 1M input tokens in USD")
    output_cost_per_1m: float = Field(..., ge=0, description="Cost per 1M output tokens in USD")
    description: Optional[str] = None


class ModelCostUpdate(BaseModel):
    input_cost_per_1m: Optional[float] = Field(None, ge=0)
    output_cost_per_1m: Optional[float] = Field(None, ge=0)
    description: Optional[str] = None


class ModelCostResponse(BaseModel):
    name: str
    input_cost_per_1m: float
    output_cost_per_1m: float
    description: Optional[str]
    created_at: str


class UsageStats(BaseModel):
    total_requests: int
    total_tokens_saved: int
    total_tokens_used: int
    cache_hits: int
    cache_entries: int
    compression_requests: int
    avg_compression_ratio: float
    estimated_cost_saved_usd: float
    registered_models: int
    total_templates: int
    compression_history_entries: int
    total_rules: int
    total_quotas: int


class CachePurgeRequest(BaseModel):
    older_than_days: int = Field(30, ge=1)
    model: Optional[str] = Field(None, description="Only purge entries for a specific model")


class CachePurgeResponse(BaseModel):
    purged: int
    message: str


class DailyStatsEntry(BaseModel):
    day: str
    model: str
    compressions: int
    cache_hits: int
    cache_misses: int
    tokens_saved: int
    tokens_used: int
    estimated_cost_saved_usd: float


# ── Compression Profiles ─────────────────────────────────────────────────────

class ProfileCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=64, description="Profile name")
    max_ratio: float = Field(0.5, ge=0.1, le=1.0, description="Target compression ratio")
    preserve_code: bool = Field(True, description="Preserve code blocks")
    strip_examples: bool = Field(False, description="Remove example blocks from prompt")
    strip_comments: bool = Field(False, description="Remove code comments")
    description: Optional[str] = None


class ProfileUpdate(BaseModel):
    max_ratio: Optional[float] = Field(None, ge=0.1, le=1.0)
    preserve_code: Optional[bool] = None
    strip_examples: Optional[bool] = None
    strip_comments: Optional[bool] = None
    description: Optional[str] = None


class ProfileResponse(BaseModel):
    name: str
    max_ratio: float
    preserve_code: bool
    strip_examples: bool
    strip_comments: bool
    builtin: bool
    description: Optional[str]
    created_at: str


# ── Cache Analytics ──────────────────────────────────────────────────────────

class CacheTopEntry(BaseModel):
    prompt_hash: str
    prompt_preview: str
    model: str
    hits: int
    tokens_saved: int
    last_hit: str


class CacheModelBreakdown(BaseModel):
    model: str
    entries: int
    total_hits: int
    total_tokens_saved: int


class CacheAnalyticsResponse(BaseModel):
    total_entries: int
    total_hits: int
    overall_hit_rate: float
    avg_hits_per_entry: float
    top_entries: list[CacheTopEntry]
    model_breakdown: list[CacheModelBreakdown]


# ── Cost Estimation ──────────────────────────────────────────────────────────

class CostEstimateRequest(BaseModel):
    prompt: str = Field(..., description="Prompt text to estimate cost for")
    model: Optional[str] = Field(None, description="Specific model (or all registered)")


class CostEstimateItem(BaseModel):
    model: str
    input_tokens: int
    input_cost_usd: float
    output_cost_usd_per_1k: float
    total_estimate_usd: float


class CostEstimateResponse(BaseModel):
    input_tokens: int
    estimates: list[CostEstimateItem]
    cheapest_model: Optional[str]


# ── Compression Benchmark ───────────────────────────────────────────────────

class BenchmarkRequest(BaseModel):
    prompt: str = Field(..., description="Prompt to benchmark across all profiles")


class BenchmarkResultItem(BaseModel):
    profile: str
    builtin: bool
    max_ratio: float
    original_tokens: int
    compressed_tokens: int
    savings_pct: float
    compression_ratio: float
    compressed_preview: str


class BenchmarkResponse(BaseModel):
    original_tokens: int
    profiles_tested: int
    results: list[BenchmarkResultItem]
    best_profile: str
    best_savings_pct: float


# ── Budget Tracking ──────────────────────────────────────────────────────────

class BudgetSetRequest(BaseModel):
    daily_token_limit: Optional[int] = Field(None, ge=0, description="Daily token budget (null = unlimited)")
    monthly_token_limit: Optional[int] = Field(None, ge=0, description="Monthly token budget (null = unlimited)")
    alert_threshold_pct: float = Field(80.0, ge=0, le=100, description="Alert when usage hits this %")


class BudgetStatusResponse(BaseModel):
    daily_token_limit: Optional[int]
    monthly_token_limit: Optional[int]
    alert_threshold_pct: float
    daily_used: int
    daily_remaining: Optional[int]
    daily_pct: float
    monthly_used: int
    monthly_remaining: Optional[int]
    monthly_pct: float
    over_budget: bool
    alerts: list[str]


# ── Prompt Templates ─────────────────────────────────────────────────────────

class PromptTemplateCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, description="Template name")
    template_text: str = Field(..., min_length=1, description="Template with {{variable}} placeholders")
    description: Optional[str] = Field(None, max_length=500)


class PromptTemplateUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    template_text: Optional[str] = Field(None, min_length=1)
    description: Optional[str] = Field(None, max_length=500)


class PromptTemplateResponse(BaseModel):
    id: int
    name: str
    template_text: str
    variables: list[str]
    description: Optional[str]
    times_used: int
    created_at: str


class PromptTemplateRenderRequest(BaseModel):
    variables: dict[str, str] = Field(..., description="Variable values to substitute, e.g. {'language': 'Python'}")
    compress: bool = Field(False, description="Compress the rendered prompt")
    profile: Optional[str] = Field(None, description="Compression profile to use if compress=True")


class PromptTemplateRenderResponse(BaseModel):
    rendered: str
    rendered_tokens: int
    compressed: Optional[str] = None
    compressed_tokens: Optional[int] = None
    savings_pct: Optional[float] = None
    missing_variables: list[str]


# ── Compression History ──────────────────────────────────────────────────────

class CompressionHistoryEntry(BaseModel):
    id: int
    prompt_preview: str
    profile_used: Optional[str]
    original_tokens: int
    compressed_tokens: int
    compression_ratio: float
    savings_pct: float
    model: Optional[str]
    created_at: str


class CompressionAnalytics(BaseModel):
    total_compressions: int
    avg_ratio: float
    avg_savings_pct: float
    best_ratio: float
    worst_ratio: float
    by_profile: list[dict]
    daily_trend: list[dict]


# ── Model Comparison ─────────────────────────────────────────────────────────

class ModelCompareRequest(BaseModel):
    prompt: str = Field(..., description="Prompt to compare costs across models")
    compress: bool = Field(False, description="Also compare compressed cost")
    profile: Optional[str] = Field(None, description="Compression profile if compress=True")


class ModelCompareItem(BaseModel):
    model: str
    input_tokens: int
    input_cost_usd: float
    compressed_tokens: Optional[int] = None
    compressed_cost_usd: Optional[float] = None
    savings_usd: Optional[float] = None
    savings_pct: Optional[float] = None


class ModelCompareResponse(BaseModel):
    original_tokens: int
    compressed_tokens: Optional[int]
    models_compared: int
    results: list[ModelCompareItem]
    cheapest_model: Optional[str]
    best_savings_model: Optional[str]


# ── Compression Rules (v0.8.0) ───────────────────────────────────────────────

class CompressionRuleCreate(BaseModel):
    pattern: str = Field(..., min_length=1, max_length=500, description="Regex pattern to match")
    replacement: str = Field("", max_length=500, description="Replacement string (empty = remove)")
    priority: int = Field(50, ge=0, le=100, description="Execution order: lower = earlier (0-100)")
    description: Optional[str] = Field(None, max_length=300)


class CompressionRuleUpdate(BaseModel):
    pattern: Optional[str] = Field(None, min_length=1, max_length=500)
    replacement: Optional[str] = Field(None, max_length=500)
    priority: Optional[int] = Field(None, ge=0, le=100)
    description: Optional[str] = Field(None, max_length=300)


class CompressionRuleResponse(BaseModel):
    id: int
    pattern: str
    replacement: str
    priority: int
    description: Optional[str]
    times_applied: int
    created_at: str


# ── Prompt Diff (v0.8.0) ─────────────────────────────────────────────────────

class PromptDiffRequest(BaseModel):
    prompt_a: str = Field(..., min_length=1, description="First prompt")
    prompt_b: str = Field(..., min_length=1, description="Second prompt")
    compress: bool = Field(False, description="Also compare compressed versions")
    profile: Optional[str] = Field(None, description="Compression profile if compress=True")


class PromptDiffResponse(BaseModel):
    tokens_a: int
    tokens_b: int
    diff_tokens: int
    diff_pct: float
    cheaper: str = Field(..., description="a | b | equal")
    char_diff: int
    compressed_tokens_a: Optional[int] = None
    compressed_tokens_b: Optional[int] = None
    compressed_cheaper: Optional[str] = None
    cost_comparison: Optional[list[dict]] = None


# ── Usage Quotas (v0.8.0) ────────────────────────────────────────────────────

class UsageQuotaSet(BaseModel):
    daily_token_limit: Optional[int] = Field(None, ge=0, description="Daily token limit for this model")
    monthly_token_limit: Optional[int] = Field(None, ge=0, description="Monthly token limit for this model")


class UsageQuotaResponse(BaseModel):
    model: str
    daily_token_limit: Optional[int]
    monthly_token_limit: Optional[int]
    daily_used: int
    monthly_used: int
    daily_pct: float
    monthly_pct: float
    daily_remaining: Optional[int]
    monthly_remaining: Optional[int]
    over_quota: bool
    created_at: str
