from __future__ import annotations
import csv
import hashlib
import io
import json
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

VALID_CONDITION_TYPES = {"spend_exceeds", "hit_rate_below", "tokens_exceed", "compression_ratio_below"}
VALID_PERIODS = {"daily", "weekly", "monthly"}
VALID_AB_STATUSES = {"running", "completed"}


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
    await _migrate_template_versions(db)
    await _migrate_alert_rules(db)
    await _migrate_ab_experiments(db)
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
            version INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        )
    """)
    # Add version column if missing (migration from v0.8.0)
    try:
        await db.execute("ALTER TABLE prompt_templates ADD COLUMN version INTEGER NOT NULL DEFAULT 1")
    except Exception:
        pass


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


# ── v0.9.0 Migrations ───────────────────────────────────────────────────────

async def _migrate_template_versions(db: aiosqlite.Connection):
    await db.execute("""
        CREATE TABLE IF NOT EXISTS template_versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            template_id INTEGER NOT NULL,
            version_number INTEGER NOT NULL,
            template_text TEXT NOT NULL,
            variables TEXT NOT NULL DEFAULT '[]',
            change_description TEXT,
            tokens_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            FOREIGN KEY (template_id) REFERENCES prompt_templates(id) ON DELETE CASCADE
        )
    """)
    await db.execute("CREATE INDEX IF NOT EXISTS idx_tpl_ver_template ON template_versions(template_id)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_tpl_ver_number ON template_versions(template_id, version_number)")


async def _migrate_alert_rules(db: aiosqlite.Connection):
    await db.execute("""
        CREATE TABLE IF NOT EXISTS alert_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            condition_type TEXT NOT NULL,
            threshold REAL NOT NULL,
            period TEXT NOT NULL DEFAULT 'daily',
            is_enabled INTEGER NOT NULL DEFAULT 1,
            times_triggered INTEGER NOT NULL DEFAULT 0,
            last_triggered_at TEXT,
            created_at TEXT NOT NULL
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS alert_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_id INTEGER NOT NULL,
            rule_name TEXT NOT NULL,
            condition_type TEXT NOT NULL,
            threshold REAL NOT NULL,
            actual_value REAL NOT NULL,
            message TEXT NOT NULL,
            is_acknowledged INTEGER NOT NULL DEFAULT 0,
            triggered_at TEXT NOT NULL,
            acknowledged_at TEXT,
            FOREIGN KEY (rule_id) REFERENCES alert_rules(id) ON DELETE CASCADE
        )
    """)
    await db.execute("CREATE INDEX IF NOT EXISTS idx_alert_log_rule ON alert_log(rule_id)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_alert_log_ack ON alert_log(is_acknowledged)")


async def _migrate_ab_experiments(db: aiosqlite.Connection):
    await db.execute("""
        CREATE TABLE IF NOT EXISTS ab_experiments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            profile_a TEXT NOT NULL,
            profile_b TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'running',
            tests_count INTEGER NOT NULL DEFAULT 0,
            profile_a_wins INTEGER NOT NULL DEFAULT 0,
            profile_b_wins INTEGER NOT NULL DEFAULT 0,
            ties INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            completed_at TEXT
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS ab_experiment_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            experiment_id INTEGER NOT NULL,
            prompt_preview TEXT,
            profile_a_tokens INTEGER NOT NULL,
            profile_b_tokens INTEGER NOT NULL,
            profile_a_ratio REAL NOT NULL,
            profile_b_ratio REAL NOT NULL,
            winner TEXT NOT NULL,
            tested_at TEXT NOT NULL,
            FOREIGN KEY (experiment_id) REFERENCES ab_experiments(id) ON DELETE CASCADE
        )
    """)
    await db.execute("CREATE INDEX IF NOT EXISTS idx_ab_results_exp ON ab_experiment_results(experiment_id)")


# ── Helpers ──────────────────────────────────────────────────────────────────

def _hash(prompt: str, model: str = "") -> str:
    return hashlib.sha256(f"{model}:{prompt}".encode()).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


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
    now = _now()
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
    now = _now()
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
    now = _now()
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
    now = _now()
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
    now = _now()
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
    now = _now()
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
    alert_rows = await db.execute_fetchall("SELECT COUNT(*) as cnt FROM alert_rules")
    alert_count = alert_rows[0]["cnt"] if alert_rows else 0
    ab_rows = await db.execute_fetchall("SELECT COUNT(*) as cnt FROM ab_experiments")
    ab_count = ab_rows[0]["cnt"] if ab_rows else 0
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
        "total_alert_rules": alert_count,
        "total_ab_experiments": ab_count,
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
    now = _now()
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
    now = _now()
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
    now = _now()
    variables = _extract_variables(data["template_text"])
    try:
        cur = await db.execute(
            "INSERT INTO prompt_templates (name, template_text, variables, description, version, created_at) VALUES (?, ?, ?, ?, 1, ?)",
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

    # Extract change_description before building field updates
    change_description = updates.pop("change_description", None)

    fields = {k: v for k, v in updates.items() if v is not None}
    if not fields:
        return existing

    # If template_text is being changed, save old version first
    if "template_text" in fields:
        from compressor import estimate_tokens
        old_tokens = estimate_tokens(existing["template_text"])
        old_variables = existing["variables"]
        current_version = existing.get("version", 1)

        # Save the OLD version to template_versions
        now = _now()
        await db.execute(
            """INSERT INTO template_versions
               (template_id, version_number, template_text, variables, change_description, tokens_count, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (tpl_id, current_version, existing["template_text"],
             json.dumps(old_variables), change_description, old_tokens, now),
        )

        # Update variables and bump version
        fields["variables"] = json.dumps(_extract_variables(fields["template_text"]))
        fields["version"] = current_version + 1

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
    # Also delete associated versions
    await db.execute("DELETE FROM template_versions WHERE template_id = ?", (tpl_id,))
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
    try:
        variables = json.loads(r["variables"])
    except (Exception,):
        variables = []
    # Handle version column gracefully for older schemas
    try:
        version = r["version"]
    except (IndexError, KeyError):
        version = 1
    return {
        "id": r["id"], "name": r["name"], "template_text": r["template_text"],
        "variables": variables, "description": r["description"],
        "times_used": r["times_used"], "version": version,
        "created_at": r["created_at"],
    }


# ── Template Versioning (v0.9.0) ────────────────────────────────────────────

async def list_template_versions(db: aiosqlite.Connection, template_id: int) -> list[dict]:
    # Verify template exists
    tpl = await get_prompt_template(db, template_id)
    if not tpl:
        return None  # Signals 404 from the endpoint
    rows = await db.execute_fetchall(
        "SELECT * FROM template_versions WHERE template_id = ? ORDER BY version_number DESC",
        (template_id,),
    )
    return [_template_version_row(r) for r in rows]


async def get_template_version(db: aiosqlite.Connection, version_id: int) -> dict | None:
    rows = await db.execute_fetchall(
        "SELECT * FROM template_versions WHERE id = ?", (version_id,),
    )
    return _template_version_row(rows[0]) if rows else None


async def diff_template_versions(db: aiosqlite.Connection, template_id: int,
                                  version_a: int, version_b: int) -> dict | None:
    from compressor import estimate_tokens

    # Verify template exists
    tpl = await get_prompt_template(db, template_id)
    if not tpl:
        return None

    # Fetch both versions; also consider the current live version
    ver_a_data = await _get_version_or_current(db, template_id, version_a, tpl)
    ver_b_data = await _get_version_or_current(db, template_id, version_b, tpl)

    if ver_a_data is None or ver_b_data is None:
        return None

    tokens_a = estimate_tokens(ver_a_data["template_text"])
    tokens_b = estimate_tokens(ver_b_data["template_text"])
    token_diff = tokens_b - tokens_a
    token_change_pct = round(token_diff / max(tokens_a, 1) * 100, 1)

    return {
        "version_a": version_a,
        "version_b": version_b,
        "text_a_preview": ver_a_data["template_text"][:200],
        "text_b_preview": ver_b_data["template_text"][:200],
        "tokens_a": tokens_a,
        "tokens_b": tokens_b,
        "token_diff": token_diff,
        "token_change_pct": token_change_pct,
    }


async def _get_version_or_current(db: aiosqlite.Connection, template_id: int,
                                    version_number: int, current_tpl: dict) -> dict | None:
    """Get a specific version of a template. If version_number matches the current
    version, return the live template data instead of looking in the history table."""
    if version_number == current_tpl.get("version", 1):
        return {"template_text": current_tpl["template_text"]}
    rows = await db.execute_fetchall(
        "SELECT * FROM template_versions WHERE template_id = ? AND version_number = ?",
        (template_id, version_number),
    )
    if not rows:
        return None
    return {"template_text": rows[0]["template_text"]}


async def rollback_template(db: aiosqlite.Connection, template_id: int, version_number: int) -> dict | None:
    """Rollback a template to a specific historical version. Saves the current version
    to history before restoring."""
    tpl = await get_prompt_template(db, template_id)
    if not tpl:
        return None

    current_version = tpl.get("version", 1)

    # Cannot rollback to the current version
    if version_number == current_version:
        raise ValueError(f"Template is already at version {version_number}")

    # Fetch the target version
    rows = await db.execute_fetchall(
        "SELECT * FROM template_versions WHERE template_id = ? AND version_number = ?",
        (template_id, version_number),
    )
    if not rows:
        raise ValueError(f"Version {version_number} not found for this template")

    target = rows[0]

    # Save current as a new version entry before rollback
    from compressor import estimate_tokens
    now = _now()
    old_tokens = estimate_tokens(tpl["template_text"])
    await db.execute(
        """INSERT INTO template_versions
           (template_id, version_number, template_text, variables, change_description, tokens_count, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (template_id, current_version, tpl["template_text"],
         json.dumps(tpl["variables"]), f"Before rollback to v{version_number}", old_tokens, now),
    )

    # Restore the target version's content
    new_version = current_version + 1
    new_variables = _extract_variables(target["template_text"])
    await db.execute(
        "UPDATE prompt_templates SET template_text = ?, variables = ?, version = ? WHERE id = ?",
        (target["template_text"], json.dumps(new_variables), new_version, template_id),
    )
    await db.commit()

    return await get_prompt_template(db, template_id)


def _template_version_row(r: aiosqlite.Row) -> dict:
    try:
        variables = json.loads(r["variables"])
    except (Exception,):
        variables = []
    return {
        "id": r["id"],
        "template_id": r["template_id"],
        "version_number": r["version_number"],
        "template_text": r["template_text"],
        "variables": variables,
        "change_description": r["change_description"],
        "tokens_count": r["tokens_count"],
        "created_at": r["created_at"],
    }


# ── Cost Alerts (v0.9.0) ────────────────────────────────────────────────────

async def create_alert_rule(db: aiosqlite.Connection, data: dict) -> dict:
    condition = data["condition_type"]
    if condition not in VALID_CONDITION_TYPES:
        raise ValueError(f"Invalid condition_type: {condition}. Must be one of: {', '.join(sorted(VALID_CONDITION_TYPES))}")
    period = data.get("period", "daily")
    if period not in VALID_PERIODS:
        raise ValueError(f"Invalid period: {period}. Must be one of: {', '.join(sorted(VALID_PERIODS))}")
    now = _now()
    try:
        cur = await db.execute(
            """INSERT INTO alert_rules (name, condition_type, threshold, period, is_enabled, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (data["name"], condition, data["threshold"], period,
             int(data.get("is_enabled", True)), now),
        )
    except aiosqlite.IntegrityError:
        raise ValueError(f"Alert rule '{data['name']}' already exists")
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM alert_rules WHERE id = ?", (cur.lastrowid,))
    return _alert_rule_row(rows[0])


async def list_alert_rules(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("SELECT * FROM alert_rules ORDER BY created_at DESC")
    return [_alert_rule_row(r) for r in rows]


async def get_alert_rule(db: aiosqlite.Connection, rule_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM alert_rules WHERE id = ?", (rule_id,))
    return _alert_rule_row(rows[0]) if rows else None


async def update_alert_rule(db: aiosqlite.Connection, rule_id: int, updates: dict) -> dict | None:
    existing = await get_alert_rule(db, rule_id)
    if not existing:
        return None
    fields = {k: v for k, v in updates.items() if v is not None}
    if not fields:
        return existing
    if "period" in fields and fields["period"] not in VALID_PERIODS:
        raise ValueError(f"Invalid period: {fields['period']}. Must be one of: {', '.join(sorted(VALID_PERIODS))}")
    if "is_enabled" in fields:
        fields["is_enabled"] = int(fields["is_enabled"])
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [rule_id]
    try:
        cur = await db.execute(f"UPDATE alert_rules SET {set_clause} WHERE id = ?", values)
    except aiosqlite.IntegrityError:
        raise ValueError("Alert rule with that name already exists")
    await db.commit()
    if cur.rowcount == 0:
        return None
    return await get_alert_rule(db, rule_id)


async def delete_alert_rule(db: aiosqlite.Connection, rule_id: int) -> bool:
    # Also clean up associated log entries
    await db.execute("DELETE FROM alert_log WHERE rule_id = ?", (rule_id,))
    cur = await db.execute("DELETE FROM alert_rules WHERE id = ?", (rule_id,))
    await db.commit()
    return cur.rowcount > 0


async def evaluate_alerts(db: aiosqlite.Connection) -> list[dict]:
    """Evaluate all enabled alert rules against current data.
    Returns list of newly triggered alerts."""
    rows = await db.execute_fetchall(
        "SELECT * FROM alert_rules WHERE is_enabled = 1"
    )
    if not rows:
        return []

    triggered = []
    now = _now()

    for rule in rows:
        condition = rule["condition_type"]
        threshold = rule["threshold"]
        period = rule["period"]
        actual_value = await _compute_alert_metric(db, condition, period)

        fired = False
        if condition == "spend_exceeds":
            fired = actual_value > threshold
        elif condition == "hit_rate_below":
            fired = actual_value < threshold
        elif condition == "tokens_exceed":
            fired = actual_value > threshold
        elif condition == "compression_ratio_below":
            fired = actual_value < threshold

        if fired:
            message = _format_alert_message(rule["name"], condition, threshold, actual_value, period)
            await db.execute(
                """INSERT INTO alert_log (rule_id, rule_name, condition_type, threshold, actual_value, message, triggered_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (rule["id"], rule["name"], condition, threshold, actual_value, message, now),
            )
            await db.execute(
                "UPDATE alert_rules SET times_triggered = times_triggered + 1, last_triggered_at = ? WHERE id = ?",
                (now, rule["id"]),
            )
            triggered.append({
                "rule_id": rule["id"],
                "rule_name": rule["name"],
                "condition_type": condition,
                "threshold": threshold,
                "actual_value": round(actual_value, 4),
                "message": message,
                "triggered_at": now,
            })

    if triggered:
        await db.commit()

    return triggered


async def _compute_alert_metric(db: aiosqlite.Connection, condition: str, period: str) -> float:
    """Compute the actual metric value for a given condition and period."""
    if condition == "spend_exceeds":
        tokens_used = await _get_period_tokens_used(db, period)
        # Estimate cost based on default rate
        return tokens_used * DEFAULT_COST_PER_1M / 1_000_000

    elif condition == "tokens_exceed":
        return float(await _get_period_tokens_used(db, period))

    elif condition == "hit_rate_below":
        hits, misses = await _get_period_cache_stats(db, period)
        total = hits + misses
        if total == 0:
            return 100.0  # No data = no alert
        return round(hits / total * 100, 2)

    elif condition == "compression_ratio_below":
        return await _get_period_avg_compression_ratio(db, period)

    return 0.0


async def _get_period_tokens_used(db: aiosqlite.Connection, period: str) -> int:
    if period == "daily":
        day_filter = _today()
        rows = await db.execute_fetchall(
            "SELECT COALESCE(SUM(tokens_used), 0) as total FROM daily_log WHERE day = ?", (day_filter,)
        )
    elif period == "weekly":
        rows = await db.execute_fetchall(
            "SELECT COALESCE(SUM(tokens_used), 0) as total FROM daily_log WHERE day >= date('now', '-7 days')"
        )
    else:  # monthly
        month_prefix = _this_month()
        rows = await db.execute_fetchall(
            "SELECT COALESCE(SUM(tokens_used), 0) as total FROM daily_log WHERE day LIKE ?",
            (month_prefix + "%",),
        )
    return rows[0]["total"] if rows else 0


async def _get_period_cache_stats(db: aiosqlite.Connection, period: str) -> tuple[int, int]:
    if period == "daily":
        day_filter = _today()
        rows = await db.execute_fetchall(
            "SELECT COALESCE(SUM(cache_hits), 0) as hits, COALESCE(SUM(cache_misses), 0) as misses FROM daily_log WHERE day = ?",
            (day_filter,),
        )
    elif period == "weekly":
        rows = await db.execute_fetchall(
            "SELECT COALESCE(SUM(cache_hits), 0) as hits, COALESCE(SUM(cache_misses), 0) as misses FROM daily_log WHERE day >= date('now', '-7 days')"
        )
    else:  # monthly
        month_prefix = _this_month()
        rows = await db.execute_fetchall(
            "SELECT COALESCE(SUM(cache_hits), 0) as hits, COALESCE(SUM(cache_misses), 0) as misses FROM daily_log WHERE day LIKE ?",
            (month_prefix + "%",),
        )
    if rows:
        return rows[0]["hits"], rows[0]["misses"]
    return 0, 0


async def _get_period_avg_compression_ratio(db: aiosqlite.Connection, period: str) -> float:
    if period == "daily":
        day_filter = _today()
        rows = await db.execute_fetchall(
            "SELECT AVG(compression_ratio) as avg_r FROM compression_history WHERE date(created_at) = ?",
            (day_filter,),
        )
    elif period == "weekly":
        rows = await db.execute_fetchall(
            "SELECT AVG(compression_ratio) as avg_r FROM compression_history WHERE created_at >= date('now', '-7 days')"
        )
    else:  # monthly
        month_prefix = _this_month()
        rows = await db.execute_fetchall(
            "SELECT AVG(compression_ratio) as avg_r FROM compression_history WHERE created_at LIKE ?",
            (month_prefix + "%",),
        )
    if rows and rows[0]["avg_r"] is not None:
        return round(rows[0]["avg_r"], 4)
    return 1.0  # No data = ratio 1.0 (no compression)


def _format_alert_message(name: str, condition: str, threshold: float, actual: float, period: str) -> str:
    if condition == "spend_exceeds":
        return f"[{name}] {period.capitalize()} spend ${actual:.4f} exceeds threshold ${threshold:.4f}"
    elif condition == "hit_rate_below":
        return f"[{name}] {period.capitalize()} cache hit rate {actual:.1f}% is below threshold {threshold:.1f}%"
    elif condition == "tokens_exceed":
        return f"[{name}] {period.capitalize()} token usage {int(actual)} exceeds threshold {int(threshold)}"
    elif condition == "compression_ratio_below":
        return f"[{name}] {period.capitalize()} avg compression ratio {actual:.3f} is below threshold {threshold:.3f}"
    return f"[{name}] Alert triggered: {condition} (actual={actual}, threshold={threshold})"


async def list_alert_log(db: aiosqlite.Connection, rule_id: int | None = None,
                          acknowledged: bool | None = None, limit: int = 50) -> list[dict]:
    q = "SELECT * FROM alert_log WHERE 1=1"
    params: list = []
    if rule_id is not None:
        q += " AND rule_id = ?"
        params.append(rule_id)
    if acknowledged is not None:
        q += " AND is_acknowledged = ?"
        params.append(int(acknowledged))
    q += f" ORDER BY triggered_at DESC LIMIT {limit}"
    rows = await db.execute_fetchall(q, params)
    return [_alert_log_row(r) for r in rows]


async def acknowledge_alert(db: aiosqlite.Connection, alert_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM alert_log WHERE id = ?", (alert_id,))
    if not rows:
        return None
    if rows[0]["is_acknowledged"]:
        return _alert_log_row(rows[0])
    now = _now()
    await db.execute(
        "UPDATE alert_log SET is_acknowledged = 1, acknowledged_at = ? WHERE id = ?",
        (now, alert_id),
    )
    await db.commit()
    updated = await db.execute_fetchall("SELECT * FROM alert_log WHERE id = ?", (alert_id,))
    return _alert_log_row(updated[0])


async def get_alert_summary(db: aiosqlite.Connection) -> dict:
    total_rules_rows = await db.execute_fetchall("SELECT COUNT(*) as cnt FROM alert_rules")
    total_rules = total_rules_rows[0]["cnt"] if total_rules_rows else 0
    active_rules_rows = await db.execute_fetchall("SELECT COUNT(*) as cnt FROM alert_rules WHERE is_enabled = 1")
    active_rules = active_rules_rows[0]["cnt"] if active_rules_rows else 0
    total_alerts_rows = await db.execute_fetchall("SELECT COUNT(*) as cnt FROM alert_log")
    total_alerts = total_alerts_rows[0]["cnt"] if total_alerts_rows else 0
    unack_rows = await db.execute_fetchall("SELECT COUNT(*) as cnt FROM alert_log WHERE is_acknowledged = 0")
    unacknowledged = unack_rows[0]["cnt"] if unack_rows else 0
    recent_rows = await db.execute_fetchall(
        "SELECT * FROM alert_log ORDER BY triggered_at DESC LIMIT 10"
    )
    recent_alerts = [_alert_log_row(r) for r in recent_rows]
    return {
        "total_rules": total_rules,
        "active_rules": active_rules,
        "total_alerts": total_alerts,
        "unacknowledged": unacknowledged,
        "recent_alerts": recent_alerts,
    }


def _alert_rule_row(r: aiosqlite.Row) -> dict:
    return {
        "id": r["id"],
        "name": r["name"],
        "condition_type": r["condition_type"],
        "threshold": r["threshold"],
        "period": r["period"],
        "is_enabled": bool(r["is_enabled"]),
        "times_triggered": r["times_triggered"],
        "last_triggered_at": r["last_triggered_at"],
        "created_at": r["created_at"],
    }


def _alert_log_row(r: aiosqlite.Row) -> dict:
    return {
        "id": r["id"],
        "rule_id": r["rule_id"],
        "rule_name": r["rule_name"],
        "condition_type": r["condition_type"],
        "threshold": r["threshold"],
        "actual_value": r["actual_value"],
        "message": r["message"],
        "is_acknowledged": bool(r["is_acknowledged"]),
        "triggered_at": r["triggered_at"],
        "acknowledged_at": r["acknowledged_at"],
    }


# ── Compression A/B Testing (v0.9.0) ────────────────────────────────────────

async def create_ab_experiment(db: aiosqlite.Connection, data: dict) -> dict:
    profile_a_name = data["profile_a"]
    profile_b_name = data["profile_b"]

    # Validate both profiles exist
    pa = await get_profile(db, profile_a_name)
    if not pa:
        raise ValueError(f"Profile '{profile_a_name}' not found")
    pb = await get_profile(db, profile_b_name)
    if not pb:
        raise ValueError(f"Profile '{profile_b_name}' not found")

    if profile_a_name == profile_b_name:
        raise ValueError("profile_a and profile_b must be different")

    now = _now()
    cur = await db.execute(
        """INSERT INTO ab_experiments (name, profile_a, profile_b, status, created_at)
           VALUES (?, ?, ?, 'running', ?)""",
        (data["name"], profile_a_name, profile_b_name, now),
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM ab_experiments WHERE id = ?", (cur.lastrowid,))
    return _ab_experiment_row(rows[0])


async def list_ab_experiments(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("SELECT * FROM ab_experiments ORDER BY created_at DESC")
    return [_ab_experiment_row(r) for r in rows]


async def get_ab_experiment(db: aiosqlite.Connection, experiment_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM ab_experiments WHERE id = ?", (experiment_id,))
    return _ab_experiment_row(rows[0]) if rows else None


async def run_ab_test(db: aiosqlite.Connection, experiment_id: int, prompt: str) -> dict | None:
    """Run a single A/B test: compress the prompt with both profiles and record the result."""
    from compressor import compress_prompt, estimate_tokens

    exp = await get_ab_experiment(db, experiment_id)
    if not exp:
        return None

    if exp["status"] != "running":
        raise ValueError("Experiment is not running; cannot add more tests")

    # Fetch both profiles
    pa = await get_profile(db, exp["profile_a"])
    pb = await get_profile(db, exp["profile_b"])
    if not pa or not pb:
        raise ValueError("One or both profiles no longer exist")

    original_tokens = estimate_tokens(prompt)

    # Compress with profile A
    comp_a, _ = compress_prompt(
        prompt, pa["max_ratio"], pa["preserve_code"],
        strip_examples=pa["strip_examples"], strip_comments=pa["strip_comments"],
    )
    tokens_a = estimate_tokens(comp_a)
    ratio_a = round(tokens_a / max(original_tokens, 1), 3)

    # Compress with profile B
    comp_b, _ = compress_prompt(
        prompt, pb["max_ratio"], pb["preserve_code"],
        strip_examples=pb["strip_examples"], strip_comments=pb["strip_comments"],
    )
    tokens_b = estimate_tokens(comp_b)
    ratio_b = round(tokens_b / max(original_tokens, 1), 3)

    # Determine winner (lower tokens = better compression = winner)
    if tokens_a < tokens_b:
        winner = "a"
    elif tokens_b < tokens_a:
        winner = "b"
    else:
        winner = "tie"

    now = _now()
    cur = await db.execute(
        """INSERT INTO ab_experiment_results
           (experiment_id, prompt_preview, profile_a_tokens, profile_b_tokens,
            profile_a_ratio, profile_b_ratio, winner, tested_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (experiment_id, prompt[:120], tokens_a, tokens_b, ratio_a, ratio_b, winner, now),
    )

    # Update experiment counters
    win_update = ""
    if winner == "a":
        win_update = ", profile_a_wins = profile_a_wins + 1"
    elif winner == "b":
        win_update = ", profile_b_wins = profile_b_wins + 1"
    else:
        win_update = ", ties = ties + 1"
    await db.execute(
        f"UPDATE ab_experiments SET tests_count = tests_count + 1{win_update} WHERE id = ?",
        (experiment_id,),
    )
    await db.commit()

    result_rows = await db.execute_fetchall(
        "SELECT * FROM ab_experiment_results WHERE id = ?", (cur.lastrowid,),
    )
    return _ab_test_result_row(result_rows[0])


async def complete_ab_experiment(db: aiosqlite.Connection, experiment_id: int) -> dict | None:
    exp = await get_ab_experiment(db, experiment_id)
    if not exp:
        return None
    if exp["status"] == "completed":
        raise ValueError("Experiment is already completed")
    if exp["tests_count"] == 0:
        raise ValueError("Cannot complete an experiment with no tests")

    now = _now()
    await db.execute(
        "UPDATE ab_experiments SET status = 'completed', completed_at = ? WHERE id = ?",
        (now, experiment_id),
    )
    await db.commit()
    return await get_ab_experiment(db, experiment_id)


async def delete_ab_experiment(db: aiosqlite.Connection, experiment_id: int) -> bool:
    await db.execute("DELETE FROM ab_experiment_results WHERE experiment_id = ?", (experiment_id,))
    cur = await db.execute("DELETE FROM ab_experiments WHERE id = ?", (experiment_id,))
    await db.commit()
    return cur.rowcount > 0


async def get_ab_experiment_results(db: aiosqlite.Connection, experiment_id: int,
                                     limit: int = 50) -> list[dict] | None:
    exp = await get_ab_experiment(db, experiment_id)
    if not exp:
        return None
    rows = await db.execute_fetchall(
        "SELECT * FROM ab_experiment_results WHERE experiment_id = ? ORDER BY tested_at DESC LIMIT ?",
        (experiment_id, limit),
    )
    return [_ab_test_result_row(r) for r in rows]


def _ab_experiment_row(r: aiosqlite.Row) -> dict:
    tests = r["tests_count"]
    a_wins = r["profile_a_wins"]
    b_wins = r["profile_b_wins"]
    return {
        "id": r["id"],
        "name": r["name"],
        "profile_a": r["profile_a"],
        "profile_b": r["profile_b"],
        "status": r["status"],
        "tests_count": tests,
        "profile_a_wins": a_wins,
        "profile_b_wins": b_wins,
        "ties": r["ties"],
        "win_rate_a": round(a_wins / max(tests, 1) * 100, 1),
        "win_rate_b": round(b_wins / max(tests, 1) * 100, 1),
        "created_at": r["created_at"],
        "completed_at": r["completed_at"],
    }


def _ab_test_result_row(r: aiosqlite.Row) -> dict:
    return {
        "id": r["id"],
        "experiment_id": r["experiment_id"],
        "prompt_preview": r["prompt_preview"] or "",
        "profile_a_tokens": r["profile_a_tokens"],
        "profile_b_tokens": r["profile_b_tokens"],
        "profile_a_ratio": r["profile_a_ratio"],
        "profile_b_ratio": r["profile_b_ratio"],
        "winner": r["winner"],
        "tested_at": r["tested_at"],
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
