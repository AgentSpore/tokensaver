from __future__ import annotations
import csv
import hashlib
import io
import re
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

CREATE TABLE IF NOT EXISTS profiles (
    name TEXT PRIMARY KEY,
    max_ratio REAL NOT NULL DEFAULT 0.5,
    preserve_code INTEGER NOT NULL DEFAULT 1,
    strip_examples INTEGER NOT NULL DEFAULT 0,
    strip_comments INTEGER NOT NULL DEFAULT 0,
    builtin INTEGER NOT NULL DEFAULT 0,
    description TEXT,
    created_at TEXT NOT NULL
);
"""

BUILTIN_PROFILES = [
    ("aggressive", 0.3, 1, 1, 1, 1, "Maximum compression: low ratio, strip examples and comments", "2026-01-01T00:00:00+00:00"),
    ("balanced", 0.5, 1, 0, 0, 1, "Default balanced compression", "2026-01-01T00:00:00+00:00"),
    ("minimal", 0.8, 1, 0, 0, 1, "Light compression, preserves most content", "2026-01-01T00:00:00+00:00"),
]

DEFAULT_COST_PER_1M = 0.15


async def init_db(path: str) -> aiosqlite.Connection:
    db = await aiosqlite.connect(path)
    db.row_factory = aiosqlite.Row
    await db.executescript(SQL)
    for p in BUILTIN_PROFILES:
        await db.execute(
            """INSERT OR IGNORE INTO profiles
               (name, max_ratio, preserve_code, strip_examples, strip_comments, builtin, description, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            p,
        )
    await _migrate_budgets(db)
    await _migrate_prompt_templates(db)
    await _migrate_compression_history(db)
    await _migrate_compression_rules(db)
    await _migrate_usage_quotas(db)
    await db.commit()
    return db


async def _migrate_budgets(db: aiosqlite.Connection):
    await db.execute("""
        CREATE TABLE IF NOT EXISTS budgets (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            daily_token_limit INTEGER,
            monthly_token_limit INTEGER,
            alert_threshold_pct REAL NOT NULL DEFAULT 80.0,
            updated_at TEXT NOT NULL
        )
    """)


async def _migrate_prompt_templates(db: aiosqlite.Connection):
    await db.execute("""
        CREATE TABLE IF NOT EXISTS prompt_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            template_text TEXT NOT NULL,
            variables TEXT NOT NULL DEFAULT '[]',
            description TEXT,
            times_used INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        )
    """)


async def _migrate_compression_history(db: aiosqlite.Connection):
    await db.execute("""
        CREATE TABLE IF NOT EXISTS compression_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prompt_preview TEXT,
            profile_used TEXT,
            original_tokens INTEGER NOT NULL,
            compressed_tokens INTEGER NOT NULL,
            compression_ratio REAL NOT NULL,
            savings_pct REAL NOT NULL,
            model TEXT,
            created_at TEXT NOT NULL
        )
    """)
    await db.execute("CREATE INDEX IF NOT EXISTS idx_comp_hist_profile ON compression_history(profile_used)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_comp_hist_date ON compression_history(created_at)")


async def _migrate_compression_rules(db: aiosqlite.Connection):
    await db.execute("""
        CREATE TABLE IF NOT EXISTS compression_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern TEXT NOT NULL,
            replacement TEXT NOT NULL DEFAULT '',
            priority INTEGER NOT NULL DEFAULT 50,
            description TEXT,
            times_applied INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        )
    """)
    await db.execute("CREATE INDEX IF NOT EXISTS idx_rules_priority ON compression_rules(priority)")


async def _migrate_usage_quotas(db: aiosqlite.Connection):
    await db.execute("""
        CREATE TABLE IF NOT EXISTS usage_quotas (
            model TEXT PRIMARY KEY,
            daily_token_limit INTEGER,
            monthly_token_limit INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)


def _hash(prompt: str, model: str = "") -> str:
    return hashlib.sha256(f"{model}:{prompt}".encode()).hexdigest()


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _this_month() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")


async def _get_model_cost(db: aiosqlite.Connection, model: str) -> float:
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


# ── Compression Rules (v0.8.0) ──────────────────────────────────────────────

async def create_compression_rule(db: aiosqlite.Connection, data: dict) -> dict:
    import re as _re
    try:
        _re.compile(data["pattern"])
    except _re.error as e:
        raise ValueError(f"Invalid regex pattern: {e}")
    now = datetime.now(timezone.utc).isoformat()
    cur = await db.execute(
        "INSERT INTO compression_rules (pattern, replacement, priority, description, created_at) VALUES (?, ?, ?, ?, ?)",
        (data["pattern"], data.get("replacement", ""), data.get("priority", 50), data.get("description"), now),
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM compression_rules WHERE id = ?", (cur.lastrowid,))
    return _rule_row(rows[0])


async def list_compression_rules(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("SELECT * FROM compression_rules ORDER BY priority ASC, id ASC")
    return [_rule_row(r) for r in rows]


async def get_compression_rule(db: aiosqlite.Connection, rule_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM compression_rules WHERE id = ?", (rule_id,))
    return _rule_row(rows[0]) if rows else None


async def update_compression_rule(db: aiosqlite.Connection, rule_id: int, updates: dict) -> dict | None:
    existing = await get_compression_rule(db, rule_id)
    if not existing:
        return None
    fields = {k: v for k, v in updates.items() if v is not None}
    if not fields:
        return existing
    if "pattern" in fields:
        import re as _re
        try:
            _re.compile(fields["pattern"])
        except _re.error as e:
            raise ValueError(f"Invalid regex pattern: {e}")
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [rule_id]
    cur = await db.execute(f"UPDATE compression_rules SET {set_clause} WHERE id = ?", values)
    await db.commit()
    if cur.rowcount == 0:
        return None
    return await get_compression_rule(db, rule_id)


async def delete_compression_rule(db: aiosqlite.Connection, rule_id: int) -> bool:
    cur = await db.execute("DELETE FROM compression_rules WHERE id = ?", (rule_id,))
    await db.commit()
    return cur.rowcount > 0


async def get_active_rules(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("SELECT * FROM compression_rules ORDER BY priority ASC")
    return [{"pattern": r["pattern"], "replacement": r["replacement"], "priority": r["priority"], "id": r["id"]} for r in rows]


async def increment_rule_applied(db: aiosqlite.Connection, rule_ids: list[int]):
    if not rule_ids:
        return
    placeholders = ",".join("?" for _ in rule_ids)
    await db.execute(
        f"UPDATE compression_rules SET times_applied = times_applied + 1 WHERE id IN ({placeholders})",
        rule_ids,
    )
    await db.commit()


def _rule_row(r: aiosqlite.Row) -> dict:
    return {
        "id": r["id"], "pattern": r["pattern"], "replacement": r["replacement"],
        "priority": r["priority"], "description": r["description"],
        "times_applied": r["times_applied"], "created_at": r["created_at"],
    }


# ── Prompt Diff (v0.8.0) ────────────────────────────────────────────────────

async def prompt_diff(db: aiosqlite.Connection, prompt_a: str, prompt_b: str,
                      compress: bool = False, profile_name: str | None = None) -> dict:
    from compressor import estimate_tokens, compress_prompt

    tokens_a = estimate_tokens(prompt_a)
    tokens_b = estimate_tokens(prompt_b)
    diff_tokens = abs(tokens_a - tokens_b)
    diff_pct = round(diff_tokens / max(tokens_a, tokens_b, 1) * 100, 1)

    if tokens_a < tokens_b:
        cheaper = "a"
    elif tokens_b < tokens_a:
        cheaper = "b"
    else:
        cheaper = "equal"

    result = {
        "tokens_a": tokens_a,
        "tokens_b": tokens_b,
        "diff_tokens": diff_tokens,
        "diff_pct": diff_pct,
        "cheaper": cheaper,
        "char_diff": abs(len(prompt_a) - len(prompt_b)),
        "compressed_tokens_a": None,
        "compressed_tokens_b": None,
        "compressed_cheaper": None,
        "cost_comparison": None,
    }

    if compress:
        max_ratio = 0.5
        preserve_code = True
        strip_examples = False
        strip_comments = False
        if profile_name:
            p = await get_profile(db, profile_name)
            if p:
                max_ratio = p["max_ratio"]
                preserve_code = p["preserve_code"]
                strip_examples = p["strip_examples"]
                strip_comments = p["strip_comments"]
        comp_a, _ = compress_prompt(prompt_a, max_ratio, preserve_code,
                                    strip_examples=strip_examples, strip_comments=strip_comments)
        comp_b, _ = compress_prompt(prompt_b, max_ratio, preserve_code,
                                    strip_examples=strip_examples, strip_comments=strip_comments)
        ct_a = estimate_tokens(comp_a)
        ct_b = estimate_tokens(comp_b)
        result["compressed_tokens_a"] = ct_a
        result["compressed_tokens_b"] = ct_b
        if ct_a < ct_b:
            result["compressed_cheaper"] = "a"
        elif ct_b < ct_a:
            result["compressed_cheaper"] = "b"
        else:
            result["compressed_cheaper"] = "equal"

        # Cost comparison across models
        models = await list_model_costs(db)
        if models:
            cost_cmp = []
            for m in models:
                cost_a = round(ct_a * m["input_cost_per_1m"] / 1_000_000, 6)
                cost_b = round(ct_b * m["input_cost_per_1m"] / 1_000_000, 6)
                cost_cmp.append({
                    "model": m["name"],
                    "cost_a_usd": cost_a,
                    "cost_b_usd": cost_b,
                    "diff_usd": round(abs(cost_a - cost_b), 6),
                })
            result["cost_comparison"] = cost_cmp

    return result


# ── Usage Quotas (v0.8.0) ───────────────────────────────────────────────────

async def set_usage_quota(db: aiosqlite.Connection, model: str,
                          daily_limit: int | None, monthly_limit: int | None) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        """INSERT INTO usage_quotas (model, daily_token_limit, monthly_token_limit, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(model) DO UPDATE SET
             daily_token_limit = excluded.daily_token_limit,
             monthly_token_limit = excluded.monthly_token_limit,
             updated_at = excluded.updated_at""",
        (model, daily_limit, monthly_limit, now, now),
    )
    await db.commit()
    return await get_usage_quota(db, model)


async def get_usage_quota(db: aiosqlite.Connection, model: str) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM usage_quotas WHERE model = ?", (model,))
    if not rows:
        return None
    r = rows[0]
    daily_limit = r["daily_token_limit"]
    monthly_limit = r["monthly_token_limit"]

    today = _today()
    daily_rows = await db.execute_fetchall(
        "SELECT COALESCE(SUM(tokens_used), 0) as used FROM daily_log WHERE day = ? AND model = ?",
        (today, model),
    )
    daily_used = daily_rows[0]["used"] if daily_rows else 0

    month_prefix = _this_month()
    monthly_rows = await db.execute_fetchall(
        "SELECT COALESCE(SUM(tokens_used), 0) as used FROM daily_log WHERE day LIKE ? AND model = ?",
        (month_prefix + "%", model),
    )
    monthly_used = monthly_rows[0]["used"] if monthly_rows else 0

    daily_pct = round(daily_used / daily_limit * 100, 1) if daily_limit else 0.0
    monthly_pct = round(monthly_used / monthly_limit * 100, 1) if monthly_limit else 0.0

    return {
        "model": model,
        "daily_token_limit": daily_limit,
        "monthly_token_limit": monthly_limit,
        "daily_used": daily_used,
        "monthly_used": monthly_used,
        "daily_pct": daily_pct,
        "monthly_pct": monthly_pct,
        "daily_remaining": (daily_limit - daily_used) if daily_limit else None,
        "monthly_remaining": (monthly_limit - monthly_used) if monthly_limit else None,
        "over_quota": (daily_limit is not None and daily_used >= daily_limit) or
                      (monthly_limit is not None and monthly_used >= monthly_limit),
        "created_at": r["created_at"],
    }


async def list_usage_quotas(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("SELECT model FROM usage_quotas ORDER BY model ASC")
    results = []
    for r in rows:
        q = await get_usage_quota(db, r["model"])
        if q:
            results.append(q)
    return results


async def delete_usage_quota(db: aiosqlite.Connection, model: str) -> bool:
    cur = await db.execute("DELETE FROM usage_quotas WHERE model = ?", (model,))
    await db.commit()
    return cur.rowcount > 0


# ── Compression Profiles ─────────────────────────────────────────────────────

async def create_profile(db: aiosqlite.Connection, data: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    try:
        await db.execute(
            """INSERT INTO profiles (name, max_ratio, preserve_code, strip_examples, strip_comments, builtin, description, created_at)
               VALUES (?, ?, ?, ?, ?, 0, ?, ?)""",
            (data["name"], data["max_ratio"], int(data.get("preserve_code", True)),
             int(data.get("strip_examples", False)), int(data.get("strip_comments", False)),
             data.get("description"), now),
        )
    except aiosqlite.IntegrityError:
        raise ValueError(f"Profile '{data['name']}' already exists")
    await db.commit()
    return await get_profile(db, data["name"])


async def list_profiles(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("SELECT * FROM profiles ORDER BY builtin DESC, name ASC")
    return [_profile_row(r) for r in rows]


async def get_profile(db: aiosqlite.Connection, name: str) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM profiles WHERE name = ?", (name,))
    return _profile_row(rows[0]) if rows else None


async def update_profile(db: aiosqlite.Connection, name: str, updates: dict) -> dict | None:
    existing = await get_profile(db, name)
    if not existing:
        return None
    if existing["builtin"]:
        raise ValueError("Cannot modify built-in profiles")
    fields = {k: v for k, v in updates.items() if v is not None}
    if not fields:
        return existing
    bool_fields = {"preserve_code", "strip_examples", "strip_comments"}
    for bf in bool_fields:
        if bf in fields:
            fields[bf] = int(fields[bf])
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [name]
    cur = await db.execute(f"UPDATE profiles SET {set_clause} WHERE name = ?", values)
    await db.commit()
    if cur.rowcount == 0:
        return None
    return await get_profile(db, name)


async def delete_profile(db: aiosqlite.Connection, name: str) -> bool:
    existing = await get_profile(db, name)
    if not existing:
        return False
    if existing["builtin"]:
        raise ValueError("Cannot delete built-in profiles")
    cur = await db.execute("DELETE FROM profiles WHERE name = ? AND builtin = 0", (name,))
    await db.commit()
    return cur.rowcount > 0


def _profile_row(r: aiosqlite.Row) -> dict:
    return {
        "name": r["name"],
        "max_ratio": r["max_ratio"],
        "preserve_code": bool(r["preserve_code"]),
        "strip_examples": bool(r["strip_examples"]),
        "strip_comments": bool(r["strip_comments"]),
        "builtin": bool(r["builtin"]),
        "description": r["description"],
        "created_at": r["created_at"],
    }


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
    tpl_rows = await db.execute_fetchall("SELECT COUNT(*) as cnt FROM prompt_templates")
    tpl_count = tpl_rows[0]["cnt"] if tpl_rows else 0
    hist_rows = await db.execute_fetchall("SELECT COUNT(*) as cnt FROM compression_history")
    hist_count = hist_rows[0]["cnt"] if hist_rows else 0
    rule_rows = await db.execute_fetchall("SELECT COUNT(*) as cnt FROM compression_rules")
    rule_count = rule_rows[0]["cnt"] if rule_rows else 0
    quota_rows = await db.execute_fetchall("SELECT COUNT(*) as cnt FROM usage_quotas")
    quota_count = quota_rows[0]["cnt"] if quota_rows else 0
    avg_ratio = (
        round(s["sum_compression_ratio"] / s["compression_requests"], 3)
        if s["compression_requests"] > 0 else 1.0
    )
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
        "total_templates": tpl_count,
        "compression_history_entries": hist_count,
        "total_rules": rule_count,
        "total_quotas": quota_count,
    }


async def _calculate_total_cost_saved(db: aiosqlite.Connection, total_tokens: int) -> float:
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


async def record_compression(db: aiosqlite.Connection, original_tokens: int,
                              compressed_tokens: int, profile: str | None = None,
                              prompt_preview: str | None = None, model: str | None = None):
    ratio = compressed_tokens / max(original_tokens, 1)
    saved = original_tokens - compressed_tokens
    savings_pct = round((1 - ratio) * 100, 1)
    await db.execute(
        """UPDATE stats SET
           compression_requests = compression_requests + 1,
           total_tokens_saved = total_tokens_saved + ?,
           sum_compression_ratio = sum_compression_ratio + ?,
           total_requests = total_requests + 1
           WHERE id = 1""",
        (saved, ratio),
    )
    await _bump_daily(db, model or "", compressions=1, tokens_saved=saved)

    # Record in compression history
    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        """INSERT INTO compression_history
           (prompt_preview, profile_used, original_tokens, compressed_tokens, compression_ratio, savings_pct, model, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (prompt_preview or "", profile, original_tokens, compressed_tokens, round(ratio, 3), savings_pct, model, now),
    )
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


# ── Cache Analytics ──────────────────────────────────────────────────────────

async def get_cache_analytics(db: aiosqlite.Connection, top_n: int = 10) -> dict:
    total_rows = await db.execute_fetchall("SELECT COUNT(*) as cnt, COALESCE(SUM(hits), 0) as total_hits FROM cache")
    total_entries = total_rows[0]["cnt"]
    total_hits = total_rows[0]["total_hits"]
    stats_rows = await db.execute_fetchall("SELECT cache_hits FROM stats WHERE id = 1")
    all_lookups = (stats_rows[0]["cache_hits"] if stats_rows else 0) + total_entries
    hit_rate = round(total_hits / max(all_lookups, 1) * 100, 1)
    avg_hits = round(total_hits / max(total_entries, 1), 2)
    top_rows = await db.execute_fetchall(
        "SELECT prompt_hash, prompt_preview, model, hits, tokens_saved, last_hit FROM cache ORDER BY hits DESC LIMIT ?",
        (top_n,),
    )
    top_entries = [{
        "prompt_hash": r["prompt_hash"][:16],
        "prompt_preview": r["prompt_preview"] or "",
        "model": r["model"],
        "hits": r["hits"],
        "tokens_saved": r["tokens_saved"],
        "last_hit": r["last_hit"],
    } for r in top_rows]
    model_rows = await db.execute_fetchall(
        "SELECT model, COUNT(*) as entries, COALESCE(SUM(hits), 0) as total_hits, COALESCE(SUM(tokens_saved), 0) as total_saved FROM cache GROUP BY model ORDER BY total_hits DESC"
    )
    model_breakdown = [{
        "model": r["model"] or "(default)",
        "entries": r["entries"],
        "total_hits": r["total_hits"],
        "total_tokens_saved": r["total_saved"],
    } for r in model_rows]
    return {
        "total_entries": total_entries,
        "total_hits": total_hits,
        "overall_hit_rate": hit_rate,
        "avg_hits_per_entry": avg_hits,
        "top_entries": top_entries,
        "model_breakdown": model_breakdown,
    }


# ── CSV Export ───────────────────────────────────────────────────────────────

async def export_daily_csv(db: aiosqlite.Connection, days: int = 90, model: str | None = None) -> str:
    rows = await get_daily_stats(db, days, model)
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=[
        "day", "model", "compressions", "cache_hits", "cache_misses",
        "tokens_saved", "tokens_used", "estimated_cost_saved_usd",
    ])
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


# ── Cost Estimation ──────────────────────────────────────────────────────────

async def estimate_cost(db: aiosqlite.Connection, token_count: int,
                        model: str | None = None) -> list[dict]:
    if model:
        rows = await db.execute_fetchall("SELECT * FROM model_costs WHERE name = ?", (model,))
    else:
        rows = await db.execute_fetchall("SELECT * FROM model_costs ORDER BY input_cost_per_1m ASC")
    if not rows:
        default_cost = round(token_count * DEFAULT_COST_PER_1M / 1_000_000, 6)
        return [{
            "model": "(default)",
            "input_tokens": token_count,
            "input_cost_usd": default_cost,
            "output_cost_usd_per_1k": round(0.60 / 1000, 6),
            "total_estimate_usd": default_cost,
        }]
    results = []
    for r in rows:
        input_cost = round(token_count * r["input_cost_per_1m"] / 1_000_000, 6)
        output_cost_1k = round(r["output_cost_per_1m"] / 1000, 6)
        results.append({
            "model": r["name"],
            "input_tokens": token_count,
            "input_cost_usd": input_cost,
            "output_cost_usd_per_1k": output_cost_1k,
            "total_estimate_usd": input_cost,
        })
    return results


# ── Compression Benchmark ────────────────────────────────────────────────────

async def benchmark_profiles(db: aiosqlite.Connection, prompt: str) -> list[dict]:
    from compressor import compress_prompt, estimate_tokens

    profiles = await list_profiles(db)
    original_tokens = estimate_tokens(prompt)
    results = []
    for p in profiles:
        compressed, _ = compress_prompt(
            prompt, p["max_ratio"], p["preserve_code"],
            strip_examples=p["strip_examples"],
            strip_comments=p["strip_comments"],
        )
        compressed_tokens = estimate_tokens(compressed)
        savings_pct = round((1 - compressed_tokens / max(original_tokens, 1)) * 100, 1)
        results.append({
            "profile": p["name"],
            "builtin": p["builtin"],
            "max_ratio": p["max_ratio"],
            "original_tokens": original_tokens,
            "compressed_tokens": compressed_tokens,
            "savings_pct": savings_pct,
            "compression_ratio": round(compressed_tokens / max(original_tokens, 1), 3),
            "compressed_preview": compressed[:200],
        })
    results.sort(key=lambda x: x["savings_pct"], reverse=True)
    return results


# ── Budget Tracking ──────────────────────────────────────────────────────────

async def set_budget(db: aiosqlite.Connection, daily_limit: int | None,
                     monthly_limit: int | None, alert_threshold: float = 80.0) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        """INSERT INTO budgets (id, daily_token_limit, monthly_token_limit, alert_threshold_pct, updated_at)
           VALUES (1, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
             daily_token_limit = excluded.daily_token_limit,
             monthly_token_limit = excluded.monthly_token_limit,
             alert_threshold_pct = excluded.alert_threshold_pct,
             updated_at = excluded.updated_at""",
        (daily_limit, monthly_limit, alert_threshold, now),
    )
    await db.commit()
    return await get_budget_status(db)


async def get_budget_status(db: aiosqlite.Connection) -> dict:
    budget_rows = await db.execute_fetchall("SELECT * FROM budgets WHERE id = 1")
    daily_limit = None
    monthly_limit = None
    alert_threshold = 80.0
    if budget_rows:
        b = budget_rows[0]
        daily_limit = b["daily_token_limit"]
        monthly_limit = b["monthly_token_limit"]
        alert_threshold = b["alert_threshold_pct"]

    today = _today()
    daily_rows = await db.execute_fetchall(
        "SELECT COALESCE(SUM(tokens_used), 0) as used FROM daily_log WHERE day = ?", (today,)
    )
    daily_used = daily_rows[0]["used"] if daily_rows else 0

    month_prefix = _this_month()
    monthly_rows = await db.execute_fetchall(
        "SELECT COALESCE(SUM(tokens_used), 0) as used FROM daily_log WHERE day LIKE ?",
        (month_prefix + "%",),
    )
    monthly_used = monthly_rows[0]["used"] if monthly_rows else 0

    daily_pct = round(daily_used / daily_limit * 100, 1) if daily_limit else 0.0
    monthly_pct = round(monthly_used / monthly_limit * 100, 1) if monthly_limit else 0.0
    daily_remaining = (daily_limit - daily_used) if daily_limit else None
    monthly_remaining = (monthly_limit - monthly_used) if monthly_limit else None

    alerts = []
    if daily_limit and daily_pct >= alert_threshold:
        alerts.append(f"Daily usage at {daily_pct}% ({daily_used}/{daily_limit} tokens)")
    if monthly_limit and monthly_pct >= alert_threshold:
        alerts.append(f"Monthly usage at {monthly_pct}% ({monthly_used}/{monthly_limit} tokens)")
    if daily_limit and daily_used >= daily_limit:
        alerts.append("DAILY BUDGET EXCEEDED")
    if monthly_limit and monthly_used >= monthly_limit:
        alerts.append("MONTHLY BUDGET EXCEEDED")

    return {
        "daily_token_limit": daily_limit,
        "monthly_token_limit": monthly_limit,
        "alert_threshold_pct": alert_threshold,
        "daily_used": daily_used,
        "daily_remaining": daily_remaining,
        "daily_pct": daily_pct,
        "monthly_used": monthly_used,
        "monthly_remaining": monthly_remaining,
        "monthly_pct": monthly_pct,
        "over_budget": (daily_limit is not None and daily_used >= daily_limit) or
                       (monthly_limit is not None and monthly_used >= monthly_limit),
        "alerts": alerts,
    }


# ── Prompt Templates ─────────────────────────────────────────────────────────

def _extract_variables(template_text: str) -> list[str]:
    return sorted(set(re.findall(r"\{\{(\w+)\}\}", template_text)))


async def create_prompt_template(db: aiosqlite.Connection, data: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    variables = _extract_variables(data["template_text"])
    import json
    try:
        cur = await db.execute(
            "INSERT INTO prompt_templates (name, template_text, variables, description, created_at) VALUES (?, ?, ?, ?, ?)",
            (data["name"], data["template_text"], json.dumps(variables), data.get("description"), now),
        )
    except aiosqlite.IntegrityError:
        raise ValueError(f"Template '{data['name']}' already exists")
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM prompt_templates WHERE id = ?", (cur.lastrowid,))
    return _prompt_template_row(rows[0])


async def list_prompt_templates(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("SELECT * FROM prompt_templates ORDER BY times_used DESC, name ASC")
    return [_prompt_template_row(r) for r in rows]


async def get_prompt_template(db: aiosqlite.Connection, tpl_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM prompt_templates WHERE id = ?", (tpl_id,))
    return _prompt_template_row(rows[0]) if rows else None


async def update_prompt_template(db: aiosqlite.Connection, tpl_id: int, updates: dict) -> dict | None:
    existing = await get_prompt_template(db, tpl_id)
    if not existing:
        return None
    fields = {k: v for k, v in updates.items() if v is not None}
    if not fields:
        return existing
    if "template_text" in fields:
        import json
        fields["variables"] = json.dumps(_extract_variables(fields["template_text"]))
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [tpl_id]
    try:
        cur = await db.execute(f"UPDATE prompt_templates SET {set_clause} WHERE id = ?", values)
    except aiosqlite.IntegrityError:
        raise ValueError("Template with that name already exists")
    await db.commit()
    if cur.rowcount == 0:
        return None
    return await get_prompt_template(db, tpl_id)


async def delete_prompt_template(db: aiosqlite.Connection, tpl_id: int) -> bool:
    cur = await db.execute("DELETE FROM prompt_templates WHERE id = ?", (tpl_id,))
    await db.commit()
    return cur.rowcount > 0


async def render_prompt_template(db: aiosqlite.Connection, tpl_id: int,
                                  variables: dict[str, str],
                                  compress: bool = False,
                                  profile_name: str | None = None) -> dict | None:
    tpl = await get_prompt_template(db, tpl_id)
    if not tpl:
        return None

    rendered = tpl["template_text"]
    missing = []
    for var in tpl["variables"]:
        placeholder = "{{" + var + "}}"
        if var in variables:
            rendered = rendered.replace(placeholder, variables[var])
        else:
            missing.append(var)

    from compressor import estimate_tokens, compress_prompt
    rendered_tokens = estimate_tokens(rendered)

    result = {
        "rendered": rendered,
        "rendered_tokens": rendered_tokens,
        "compressed": None,
        "compressed_tokens": None,
        "savings_pct": None,
        "missing_variables": missing,
    }

    if compress:
        max_ratio = 0.5
        preserve_code = True
        strip_examples = False
        strip_comments = False
        if profile_name:
            p = await get_profile(db, profile_name)
            if p:
                max_ratio = p["max_ratio"]
                preserve_code = p["preserve_code"]
                strip_examples = p["strip_examples"]
                strip_comments = p["strip_comments"]
        compressed, _ = compress_prompt(rendered, max_ratio, preserve_code,
                                        strip_examples=strip_examples, strip_comments=strip_comments)
        compressed_tokens = estimate_tokens(compressed)
        result["compressed"] = compressed
        result["compressed_tokens"] = compressed_tokens
        result["savings_pct"] = round((1 - compressed_tokens / max(rendered_tokens, 1)) * 100, 1)

    # Increment usage counter
    await db.execute("UPDATE prompt_templates SET times_used = times_used + 1 WHERE id = ?", (tpl_id,))
    await db.commit()

    return result


def _prompt_template_row(r: aiosqlite.Row) -> dict:
    import json
    try:
        variables = json.loads(r["variables"])
    except (Exception,):
        variables = []
    return {
        "id": r["id"], "name": r["name"], "template_text": r["template_text"],
        "variables": variables, "description": r["description"],
        "times_used": r["times_used"], "created_at": r["created_at"],
    }


# ── Compression History ──────────────────────────────────────────────────────

async def list_compression_history(db: aiosqlite.Connection, profile: str | None = None,
                                    limit: int = 50) -> list[dict]:
    q = "SELECT * FROM compression_history WHERE 1=1"
    params: list = []
    if profile:
        q += " AND profile_used = ?"
        params.append(profile)
    q += f" ORDER BY created_at DESC LIMIT {limit}"
    rows = await db.execute_fetchall(q, params)
    return [_comp_hist_row(r) for r in rows]


async def get_compression_analytics(db: aiosqlite.Connection) -> dict:
    total_rows = await db.execute_fetchall("SELECT COUNT(*) as cnt FROM compression_history")
    total = total_rows[0]["cnt"] if total_rows else 0
    if total == 0:
        return {
            "total_compressions": 0, "avg_ratio": 1.0, "avg_savings_pct": 0.0,
            "best_ratio": 1.0, "worst_ratio": 1.0, "by_profile": [], "daily_trend": [],
        }

    agg = await db.execute_fetchall(
        "SELECT AVG(compression_ratio) as avg_r, AVG(savings_pct) as avg_s, MIN(compression_ratio) as best_r, MAX(compression_ratio) as worst_r FROM compression_history"
    )
    a = agg[0]

    # By profile
    profile_rows = await db.execute_fetchall(
        """SELECT COALESCE(profile_used, '(none)') as profile,
           COUNT(*) as count, AVG(compression_ratio) as avg_ratio,
           AVG(savings_pct) as avg_savings, SUM(original_tokens - compressed_tokens) as total_saved
           FROM compression_history GROUP BY profile_used ORDER BY count DESC"""
    )
    by_profile = [{
        "profile": r["profile"], "count": r["count"],
        "avg_ratio": round(r["avg_ratio"], 3),
        "avg_savings_pct": round(r["avg_savings"], 1),
        "total_tokens_saved": r["total_saved"] or 0,
    } for r in profile_rows]

    # Daily trend (last 30 days)
    trend_rows = await db.execute_fetchall(
        """SELECT date(created_at) as day, COUNT(*) as count,
           AVG(compression_ratio) as avg_ratio, AVG(savings_pct) as avg_savings
           FROM compression_history
           WHERE created_at >= date('now', '-30 days')
           GROUP BY date(created_at) ORDER BY day ASC"""
    )
    daily_trend = [{
        "day": r["day"], "compressions": r["count"],
        "avg_ratio": round(r["avg_ratio"], 3),
        "avg_savings_pct": round(r["avg_savings"], 1),
    } for r in trend_rows]

    return {
        "total_compressions": total,
        "avg_ratio": round(a["avg_r"], 3),
        "avg_savings_pct": round(a["avg_s"], 1),
        "best_ratio": round(a["best_r"], 3),
        "worst_ratio": round(a["worst_r"], 3),
        "by_profile": by_profile,
        "daily_trend": daily_trend,
    }


def _comp_hist_row(r: aiosqlite.Row) -> dict:
    return {
        "id": r["id"],
        "prompt_preview": r["prompt_preview"] or "",
        "profile_used": r["profile_used"],
        "original_tokens": r["original_tokens"],
        "compressed_tokens": r["compressed_tokens"],
        "compression_ratio": r["compression_ratio"],
        "savings_pct": r["savings_pct"],
        "model": r["model"],
        "created_at": r["created_at"],
    }


# ── Model Comparison ─────────────────────────────────────────────────────────

async def compare_models(db: aiosqlite.Connection, prompt: str,
                          compress: bool = False, profile_name: str | None = None) -> dict:
    from compressor import estimate_tokens, compress_prompt

    original_tokens = estimate_tokens(prompt)
    compressed_tokens = None

    if compress:
        max_ratio = 0.5
        preserve_code = True
        strip_examples = False
        strip_comments = False
        if profile_name:
            p = await get_profile(db, profile_name)
            if p:
                max_ratio = p["max_ratio"]
                preserve_code = p["preserve_code"]
                strip_examples = p["strip_examples"]
                strip_comments = p["strip_comments"]
        compressed, _ = compress_prompt(prompt, max_ratio, preserve_code,
                                        strip_examples=strip_examples, strip_comments=strip_comments)
        compressed_tokens = estimate_tokens(compressed)

    models = await list_model_costs(db)
    if not models:
        orig_cost = round(original_tokens * DEFAULT_COST_PER_1M / 1_000_000, 6)
        item = {
            "model": "(default)",
            "input_tokens": original_tokens,
            "input_cost_usd": orig_cost,
            "compressed_tokens": compressed_tokens,
            "compressed_cost_usd": round(compressed_tokens * DEFAULT_COST_PER_1M / 1_000_000, 6) if compressed_tokens else None,
            "savings_usd": round((original_tokens - compressed_tokens) * DEFAULT_COST_PER_1M / 1_000_000, 6) if compressed_tokens else None,
            "savings_pct": round((1 - compressed_tokens / max(original_tokens, 1)) * 100, 1) if compressed_tokens else None,
        }
        return {
            "original_tokens": original_tokens,
            "compressed_tokens": compressed_tokens,
            "models_compared": 1,
            "results": [item],
            "cheapest_model": "(default)",
            "best_savings_model": "(default)" if compressed_tokens else None,
        }

    results = []
    for m in models:
        orig_cost = round(original_tokens * m["input_cost_per_1m"] / 1_000_000, 6)
        comp_cost = round(compressed_tokens * m["input_cost_per_1m"] / 1_000_000, 6) if compressed_tokens else None
        savings = round(orig_cost - comp_cost, 6) if comp_cost is not None else None
        savings_pct_val = round((1 - compressed_tokens / max(original_tokens, 1)) * 100, 1) if compressed_tokens else None
        results.append({
            "model": m["name"],
            "input_tokens": original_tokens,
            "input_cost_usd": orig_cost,
            "compressed_tokens": compressed_tokens,
            "compressed_cost_usd": comp_cost,
            "savings_usd": savings,
            "savings_pct": savings_pct_val,
        })

    results.sort(key=lambda x: x["input_cost_usd"])
    cheapest = results[0]["model"] if results else None
    best_savings = max(results, key=lambda x: x["savings_usd"] or 0)["model"] if compressed_tokens and results else None

    return {
        "original_tokens": original_tokens,
        "compressed_tokens": compressed_tokens,
        "models_compared": len(results),
        "results": results,
        "cheapest_model": cheapest,
        "best_savings_model": best_savings,
    }
