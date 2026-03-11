from __future__ import annotations
import hashlib
from datetime import datetime, timezone
import aiosqlite


SQL = """
CREATE TABLE IF NOT EXISTS cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    prompt_hash TEXT NOT NULL UNIQUE,
    prompt_preview TEXT,
    response TEXT NOT NULL,
    model TEXT NOT NULL DEFAULT '',
    tokens_saved INTEGER NOT NULL DEFAULT 0,
    hits INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    last_hit TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stats (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    total_requests INTEGER NOT NULL DEFAULT 0,
    total_tokens_saved INTEGER NOT NULL DEFAULT 0,
    total_tokens_used INTEGER NOT NULL DEFAULT 0,
    cache_hits INTEGER NOT NULL DEFAULT 0,
    compression_requests INTEGER NOT NULL DEFAULT 0,
    sum_compression_ratio REAL NOT NULL DEFAULT 0.0
);

INSERT OR IGNORE INTO stats (id) VALUES (1);

CREATE TABLE IF NOT EXISTS daily_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    day TEXT NOT NULL,
    model TEXT NOT NULL DEFAULT '',
    compressions INTEGER NOT NULL DEFAULT 0,
    cache_hits INTEGER NOT NULL DEFAULT 0,
    cache_misses INTEGER NOT NULL DEFAULT 0,
    tokens_saved INTEGER NOT NULL DEFAULT 0,
    tokens_used INTEGER NOT NULL DEFAULT 0,
    UNIQUE(day, model)
);

CREATE TABLE IF NOT EXISTS model_costs (
    name TEXT PRIMARY KEY,
    input_cost_per_1m REAL NOT NULL DEFAULT 0.15,
    output_cost_per_1m REAL NOT NULL DEFAULT 0.60,
    description TEXT,
    created_at TEXT NOT NULL
);
"""

DEFAULT_COST_PER_1M = 0.15


async def init_db(path: str) -> aiosqlite.Connection:
    db = await aiosqlite.connect(path)
    db.row_factory = aiosqlite.Row
    await db.executescript(SQL)
    await db.commit()
    return db


def _hash(prompt: str, model: str = "") -> str:
    return hashlib.sha256(f"{model}:{prompt}".encode()).hexdigest()


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


async def _get_model_cost(db: aiosqlite.Connection, model: str) -> float:
    """Get input cost per 1M tokens for a model, fallback to default."""
    if not model:
        return DEFAULT_COST_PER_1M
    rows = await db.execute_fetchall("SELECT input_cost_per_1m FROM model_costs WHERE name = ?", (model,))
    if rows:
        return rows[0]["input_cost_per_1m"]
    return DEFAULT_COST_PER_1M


async def _bump_daily(db: aiosqlite.Connection, model: str, **increments: int):
    day = _today()
    m = model or ""
    await db.execute(
        """INSERT INTO daily_log (day, model, compressions, cache_hits, cache_misses, tokens_saved, tokens_used)
           VALUES (?, ?, 0, 0, 0, 0, 0)
           ON CONFLICT(day, model) DO NOTHING""",
        (day, m),
    )
    sets = ", ".join(f"{k} = {k} + ?" for k in increments)
    vals = list(increments.values()) + [day, m]
    await db.execute(f"UPDATE daily_log SET {sets} WHERE day = ? AND model = ?", vals)


# ── Model Costs ──────────────────────────────────────────────────────────────

async def create_model_cost(db: aiosqlite.Connection, data: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    try:
        await db.execute(
            "INSERT INTO model_costs (name, input_cost_per_1m, output_cost_per_1m, description, created_at) VALUES (?, ?, ?, ?, ?)",
            (data["name"], data["input_cost_per_1m"], data["output_cost_per_1m"], data.get("description"), now),
        )
    except aiosqlite.IntegrityError:
        raise ValueError(f"Model '{data['name']}' already registered")
    await db.commit()
    return await get_model_cost(db, data["name"])


async def list_model_costs(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("SELECT * FROM model_costs ORDER BY name ASC")
    return [_model_cost_row(r) for r in rows]


async def get_model_cost(db: aiosqlite.Connection, name: str) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM model_costs WHERE name = ?", (name,))
    return _model_cost_row(rows[0]) if rows else None


async def update_model_cost(db: aiosqlite.Connection, name: str, updates: dict) -> dict | None:
    fields = {k: v for k, v in updates.items() if v is not None}
    if not fields:
        return await get_model_cost(db, name)
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [name]
    cur = await db.execute(f"UPDATE model_costs SET {set_clause} WHERE name = ?", values)
    await db.commit()
    if cur.rowcount == 0:
        return None
    return await get_model_cost(db, name)


async def delete_model_cost(db: aiosqlite.Connection, name: str) -> bool:
    cur = await db.execute("DELETE FROM model_costs WHERE name = ?", (name,))
    await db.commit()
    return cur.rowcount > 0


def _model_cost_row(r: aiosqlite.Row) -> dict:
    return {
        "name": r["name"], "input_cost_per_1m": r["input_cost_per_1m"],
        "output_cost_per_1m": r["output_cost_per_1m"],
        "description": r["description"], "created_at": r["created_at"],
    }


# ── Cache ────────────────────────────────────────────────────────────────────

async def cache_get(db: aiosqlite.Connection, prompt: str, model: str = "") -> dict | None:
    h = _hash(prompt, model)
    rows = await db.execute_fetchall("SELECT * FROM cache WHERE prompt_hash = ?", (h,))
    if not rows:
        await _bump_daily(db, model, cache_misses=1)
        await db.commit()
        return None
    row = rows[0]
    now = datetime.now(timezone.utc).isoformat()
    await db.execute("UPDATE cache SET hits = hits + 1, last_hit = ? WHERE prompt_hash = ?", (now, h))
    await db.execute("UPDATE stats SET cache_hits = cache_hits + 1 WHERE id = 1")
    await _bump_daily(db, model, cache_hits=1, tokens_saved=row["tokens_saved"])
    await db.commit()
    return {
        "prompt_hash": row["prompt_hash"],
        "response": row["response"],
        "model": row["model"],
        "tokens_saved": row["tokens_saved"],
        "hits": row["hits"] + 1,
        "created_at": row["created_at"],
        "last_hit": now,
    }


async def cache_set(db: aiosqlite.Connection, prompt: str, model: str, response: str, tokens_used: int) -> str:
    h = _hash(prompt, model)
    now = datetime.now(timezone.utc).isoformat()
    preview = prompt[:120]
    await db.execute(
        """INSERT INTO cache (prompt_hash, prompt_preview, response, model, tokens_saved, hits, created_at, last_hit)
           VALUES (?, ?, ?, ?, ?, 0, ?, ?)
           ON CONFLICT(prompt_hash) DO NOTHING""",
        (h, preview, response, model, tokens_used, now, now),
    )
    await db.execute("UPDATE stats SET total_tokens_used = total_tokens_used + ? WHERE id = 1", (tokens_used,))
    await _bump_daily(db, model, tokens_used=tokens_used)
    await db.commit()
    return h


async def cache_list(db: aiosqlite.Connection, limit: int = 50) -> list[dict]:
    rows = await db.execute_fetchall(
        "SELECT * FROM cache ORDER BY hits DESC, last_hit DESC LIMIT ?", (limit,)
    )
    return [{
        "prompt_hash": r["prompt_hash"][:16],
        "prompt_preview": r["prompt_preview"],
        "model": r["model"],
        "tokens_saved": r["tokens_saved"],
        "hits": r["hits"],
        "created_at": r["created_at"],
        "last_hit": r["last_hit"],
    } for r in rows]


async def cache_delete(db: aiosqlite.Connection, prompt_hash: str) -> bool:
    await db.execute("DELETE FROM cache WHERE prompt_hash LIKE ?", (prompt_hash + "%",))
    await db.commit()
    return True


async def get_stats(db: aiosqlite.Connection) -> dict:
    rows = await db.execute_fetchall("SELECT * FROM stats WHERE id = 1")
    if not rows:
        return {}
    s = rows[0]
    cache_rows = await db.execute_fetchall("SELECT COUNT(*) as cnt FROM cache")
    cache_count = cache_rows[0]["cnt"] if cache_rows else 0
    model_rows = await db.execute_fetchall("SELECT COUNT(*) as cnt FROM model_costs")
    model_count = model_rows[0]["cnt"] if model_rows else 0
    avg_ratio = (
        round(s["sum_compression_ratio"] / s["compression_requests"], 3)
        if s["compression_requests"] > 0 else 1.0
    )
    # Use model-aware cost calculation
    estimated_saved = await _calculate_total_cost_saved(db, s["total_tokens_saved"])
    return {
        "total_requests": s["total_requests"],
        "total_tokens_saved": s["total_tokens_saved"],
        "total_tokens_used": s["total_tokens_used"],
        "cache_hits": s["cache_hits"],
        "cache_entries": cache_count,
        "compression_requests": s["compression_requests"],
        "avg_compression_ratio": avg_ratio,
        "estimated_cost_saved_usd": estimated_saved,
        "registered_models": model_count,
    }


async def _calculate_total_cost_saved(db: aiosqlite.Connection, total_tokens: int) -> float:
    """Calculate cost using per-model rates from cache entries, fallback to default."""
    rows = await db.execute_fetchall(
        "SELECT model, SUM(tokens_saved * (hits + 1)) as total_saved FROM cache GROUP BY model"
    )
    if not rows:
        return round(total_tokens * DEFAULT_COST_PER_1M / 1_000_000, 6)
    total_cost = 0.0
    for r in rows:
        model = r["model"] or ""
        saved = r["total_saved"] or 0
        cost_per_1m = await _get_model_cost(db, model)
        total_cost += saved * cost_per_1m / 1_000_000
    return round(total_cost, 6)


async def record_compression(db: aiosqlite.Connection, original_tokens: int, compressed_tokens: int):
    ratio = compressed_tokens / max(original_tokens, 1)
    saved = original_tokens - compressed_tokens
    await db.execute(
        """UPDATE stats SET
           compression_requests = compression_requests + 1,
           total_tokens_saved = total_tokens_saved + ?,
           sum_compression_ratio = sum_compression_ratio + ?,
           total_requests = total_requests + 1
           WHERE id = 1""",
        (saved, ratio),
    )
    await _bump_daily(db, "", compressions=1, tokens_saved=saved)
    await db.commit()


async def purge_cache(
    db: aiosqlite.Connection,
    older_than_days: int = 30,
    model: str | None = None,
) -> int:
    cutoff = f"datetime('now', '-{older_than_days} days')"
    q = f"DELETE FROM cache WHERE last_hit < {cutoff}"
    params: list = []
    if model is not None:
        q += " AND model = ?"
        params.append(model)
    cur = await db.execute(q, params)
    await db.commit()
    return cur.rowcount


async def get_cache_entry(db: aiosqlite.Connection, prompt_hash_prefix: str) -> dict | None:
    rows = await db.execute_fetchall(
        "SELECT * FROM cache WHERE prompt_hash LIKE ?", (prompt_hash_prefix + "%",)
    )
    if not rows:
        return None
    r = rows[0]
    return {
        "prompt_hash": r["prompt_hash"][:16],
        "prompt_preview": r["prompt_preview"],
        "response": r["response"],
        "model": r["model"],
        "tokens_saved": r["tokens_saved"],
        "hits": r["hits"],
        "created_at": r["created_at"],
        "last_hit": r["last_hit"],
    }


async def get_daily_stats(
    db: aiosqlite.Connection,
    days: int = 30,
    model: str | None = None,
) -> list[dict]:
    q = "SELECT * FROM daily_log WHERE day >= date('now', ?)"
    params: list = [f"-{days} days"]
    if model:
        q += " AND model = ?"
        params.append(model)
    q += " ORDER BY day DESC, model ASC"
    rows = await db.execute_fetchall(q, params)
    result = []
    for r in rows:
        tokens_saved = r["tokens_saved"]
        m = r["model"] or ""
        cost_per_1m = await _get_model_cost(db, m)
        estimated_usd = round(tokens_saved * cost_per_1m / 1_000_000, 6)
        result.append({
            "day": r["day"],
            "model": r["model"] or "(all)",
            "compressions": r["compressions"],
            "cache_hits": r["cache_hits"],
            "cache_misses": r["cache_misses"],
            "tokens_saved": tokens_saved,
            "tokens_used": r["tokens_used"],
            "estimated_cost_saved_usd": estimated_usd,
        })
    return result
