from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional


class CompressRequest(BaseModel):
    prompt: str = Field(..., description="Original prompt text")
    max_ratio: float = Field(0.5, ge=0.1, le=1.0, description="Target compression ratio (0.5 = keep 50%)")
    preserve_code: bool = Field(True, description="Preserve code blocks verbatim")
    profile: Optional[str] = Field(None, description="Named compression profile to apply (overrides max_ratio)")


class CompressResponse(BaseModel):
    original: str
    compressed: str
    original_tokens: int
    compressed_tokens: int
    savings_pct: float
    compression_ratio: float
    profile_used: Optional[str] = None


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

class CacheAnalyticsResponse(BaseModel):
    total_entries: int
    total_hits: int
    overall_hit_rate: float
    avg_hits_per_entry: float
    top_entries: list[CacheTopEntry]
    model_breakdown: list[CacheModelBreakdown]


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
