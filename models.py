from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional


class CompressRequest(BaseModel):
    prompt: str = Field(..., description="Original prompt text")
    max_ratio: float = Field(0.5, ge=0.1, le=1.0, description="Target compression ratio (0.5 = keep 50% of tokens)")
    preserve_code: bool = Field(True, description="Preserve code blocks verbatim")


class CompressResponse(BaseModel):
    original: str
    compressed: str
    original_tokens: int
    compressed_tokens: int
    savings_pct: float
    compression_ratio: float


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
    model: str = Field("gpt-4o-mini", description="Target model identifier")
    max_tokens: int = Field(512, ge=1, le=8192)


class BatchRequest(BaseModel):
    items: list[BatchItem] = Field(..., min_length=1, max_length=50)
    dedup: bool = Field(True, description="Deduplicate identical prompts before sending")


class BatchResultItem(BaseModel):
    id: str
    status: str  # "ok" | "deduped" | "cached"
    response: Optional[str] = None
    tokens_used: int = 0
    tokens_saved: int = 0


class BatchResponse(BaseModel):
    results: list[BatchResultItem]
    total_tokens_used: int
    total_tokens_saved: int
    deduped_count: int
    cached_count: int


class UsageStats(BaseModel):
    total_requests: int
    total_tokens_saved: int
    total_tokens_used: int
    cache_hits: int
    cache_entries: int
    compression_requests: int
    avg_compression_ratio: float
    estimated_cost_saved_usd: float


class CachePurgeRequest(BaseModel):
    older_than_days: int = Field(30, ge=1, description="Remove entries not accessed in this many days")
    model: Optional[str] = Field(None, description="Only purge entries for a specific model (omit = all models)")


class CachePurgeResponse(BaseModel):
    purged: int
    message: str
