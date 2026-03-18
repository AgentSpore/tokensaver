"""TokenSaver v1.1.0 — Database & business logic (aiosqlite).

All async functions for: compression, cache, profiles, model costs,
statistics, budget, templates, cost estimation, benchmarking,
model comparison, batch processing, compression rules, prompt diff,
usage quotas, cost alerts, A/B testing, prompt playground,
cost forecasting, compression chains, usage heatmaps, prompt versions,
and cost tags.
"""

from __future__ import annotations

import hashlib
import json
import re
import difflib
import itertools
from datetime import datetime, timedelta
from typing import Any, Optional

import aiosqlite

from compressor import compress_prompt, estimate_tokens


# ── Helpers ──────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.utcnow().isoformat()


def _today() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


def _this_month() -> str:
    return datetime.utcnow().strftime("%Y-%m")


def _hash(prompt: str, model: str = "default") -> str:
    return hashlib.sha256(f"{model}:{prompt}".encode()).hexdigest()


def _row_to_dict(row: aiosqlite.Row) -> dict:
    return dict(row)


async def _bump_daily(
    db: aiosqlite.Connection,
    model: str,
    *,
    requests: int = 0,
    tokens_in: int = 0,
    tokens_out: int = 0,
    tokens_saved: int = 0,
    cost: float = 0.0,
    cache_hits: int = 0,
    cache_misses: int = 0,
) -> None:
    """Increment daily_log counters for today + model."""
    today = _today()
    await db.execute(
        """INSERT INTO daily_log (date, model, requests, tokens_in, tokens_out,
                                  tokens_saved, cost, cache_hits, cache_misses)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(date, model) DO UPDATE SET
               requests    = requests    + excluded.requests,
               tokens_in   = tokens_in   + excluded.tokens_in,
               tokens_out  = tokens_out  + excluded.tokens_out,
               tokens_saved= tokens_saved+ excluded.tokens_saved,
               cost        = cost        + excluded.cost,
               cache_hits  = cache_hits  + excluded.cache_hits,
               cache_misses= cache_misses+ excluded.cache_misses""",
        (today, model, requests, tokens_in, tokens_out, tokens_saved, cost,
         cache_hits, cache_misses),
    )
    await db.commit()


# ── Database Initialisation ─────────────────────────────────────────────────

async def init_db(db: aiosqlite.Connection) -> None:
    """Create all tables, indices, and seed builtin profiles."""
    db.row_factory = aiosqlite.Row

    await db.executescript("""
    -- Compression history
    CREATE TABLE IF NOT EXISTS compression_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        original_tokens INTEGER NOT NULL,
        compressed_tokens INTEGER NOT NULL,
        ratio REAL NOT NULL,
        profile TEXT NOT NULL DEFAULT 'balanced',
        model TEXT,
        rules_applied INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
    );

    -- Prompt cache
    CREATE TABLE IF NOT EXISTS cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        prompt_hash TEXT NOT NULL UNIQUE,
        prompt_preview TEXT NOT NULL,
        model TEXT NOT NULL DEFAULT 'default',
        response TEXT NOT NULL,
        hit_count INTEGER NOT NULL DEFAULT 0,
        ttl INTEGER,
        created_at TEXT NOT NULL,
        last_hit_at TEXT,
        expires_at TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_cache_hash ON cache(prompt_hash);
    CREATE INDEX IF NOT EXISTS idx_cache_model ON cache(model);

    -- Compression profiles
    CREATE TABLE IF NOT EXISTS profiles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        description TEXT,
        remove_filler INTEGER NOT NULL DEFAULT 1,
        remove_duplicates INTEGER NOT NULL DEFAULT 1,
        shorten_sentences INTEGER NOT NULL DEFAULT 0,
        aggressiveness REAL NOT NULL DEFAULT 0.5,
        is_builtin INTEGER NOT NULL DEFAULT 0,
        times_used INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
    );

    -- Model pricing
    CREATE TABLE IF NOT EXISTS model_costs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        model TEXT NOT NULL UNIQUE,
        input_cost_per_1k REAL NOT NULL DEFAULT 0,
        output_cost_per_1k REAL NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
    );

    -- Daily usage log
    CREATE TABLE IF NOT EXISTS daily_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        model TEXT NOT NULL DEFAULT 'default',
        requests INTEGER NOT NULL DEFAULT 0,
        tokens_in INTEGER NOT NULL DEFAULT 0,
        tokens_out INTEGER NOT NULL DEFAULT 0,
        tokens_saved INTEGER NOT NULL DEFAULT 0,
        cost REAL NOT NULL DEFAULT 0,
        cache_hits INTEGER NOT NULL DEFAULT 0,
        cache_misses INTEGER NOT NULL DEFAULT 0,
        UNIQUE(date, model)
    );
    CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_log(date);

    -- Budget config (singleton-ish)
    CREATE TABLE IF NOT EXISTS budget (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        daily_limit REAL,
        monthly_limit REAL
    );
    INSERT OR IGNORE INTO budget (id) VALUES (1);

    -- Templates
    CREATE TABLE IF NOT EXISTS templates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        content TEXT NOT NULL,
        description TEXT,
        tags TEXT NOT NULL DEFAULT '[]',
        version INTEGER NOT NULL DEFAULT 1,
        times_rendered INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );

    -- Template version history
    CREATE TABLE IF NOT EXISTS template_versions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        template_id INTEGER NOT NULL,
        version INTEGER NOT NULL,
        content TEXT NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY (template_id) REFERENCES templates(id)
    );

    -- Compression rules (regex-based)
    CREATE TABLE IF NOT EXISTS compression_rules (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        pattern TEXT NOT NULL,
        replacement TEXT NOT NULL,
        priority INTEGER NOT NULL DEFAULT 0,
        enabled INTEGER NOT NULL DEFAULT 1,
        description TEXT,
        times_applied INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
    );

    -- Usage quotas
    CREATE TABLE IF NOT EXISTS usage_quotas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        model TEXT NOT NULL UNIQUE,
        daily_limit INTEGER,
        monthly_limit INTEGER,
        created_at TEXT NOT NULL
    );

    -- Cost alert rules
    CREATE TABLE IF NOT EXISTS alert_rules (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        metric TEXT NOT NULL,
        operator TEXT NOT NULL,
        threshold REAL NOT NULL,
        model TEXT,
        enabled INTEGER NOT NULL DEFAULT 1,
        times_triggered INTEGER NOT NULL DEFAULT 0,
        last_triggered_at TEXT,
        created_at TEXT NOT NULL
    );

    -- Cost alert log
    CREATE TABLE IF NOT EXISTS alert_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        rule_id INTEGER NOT NULL,
        rule_name TEXT NOT NULL,
        metric TEXT NOT NULL,
        current_value REAL NOT NULL,
        threshold REAL NOT NULL,
        message TEXT NOT NULL,
        acknowledged INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        FOREIGN KEY (rule_id) REFERENCES alert_rules(id)
    );

    -- A/B testing experiments
    CREATE TABLE IF NOT EXISTS experiments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        description TEXT,
        profile_a TEXT NOT NULL,
        profile_b TEXT NOT NULL,
        sample_size INTEGER NOT NULL DEFAULT 100,
        runs_completed INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'pending',
        created_at TEXT NOT NULL,
        completed_at TEXT
    );

    -- A/B testing runs
    CREATE TABLE IF NOT EXISTS experiment_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        experiment_id INTEGER NOT NULL,
        run_number INTEGER NOT NULL,
        variant TEXT NOT NULL,
        profile TEXT NOT NULL,
        original_tokens INTEGER NOT NULL,
        compressed_tokens INTEGER NOT NULL,
        ratio REAL NOT NULL,
        rules_applied INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        FOREIGN KEY (experiment_id) REFERENCES experiments(id)
    );

    -- Playground sessions (NEW v1.0.0)
    CREATE TABLE IF NOT EXISTS playground_sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        description TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );

    -- Playground runs (NEW v1.0.0)
    CREATE TABLE IF NOT EXISTS playground_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id INTEGER NOT NULL,
        original_tokens INTEGER NOT NULL,
        compressed_tokens INTEGER NOT NULL,
        compression_ratio REAL NOT NULL,
        estimated_cost REAL,
        cache_hit INTEGER NOT NULL DEFAULT 0,
        profile_used TEXT NOT NULL,
        rules_applied INTEGER NOT NULL DEFAULT 0,
        compressed_text TEXT NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY (session_id) REFERENCES playground_sessions(id)
    );

    -- Compression chains (NEW v1.0.0)
    CREATE TABLE IF NOT EXISTS compression_chains (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        description TEXT,
        times_used INTEGER NOT NULL DEFAULT 0,
        avg_final_ratio REAL,
        created_at TEXT NOT NULL
    );

    -- Chain steps (NEW v1.0.0)
    CREATE TABLE IF NOT EXISTS chain_steps (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chain_id INTEGER NOT NULL,
        step_order INTEGER NOT NULL,
        profile_name TEXT NOT NULL,
        FOREIGN KEY (chain_id) REFERENCES compression_chains(id)
    );

    -- Usage heatmap (NEW v1.1.0)
    CREATE TABLE IF NOT EXISTS usage_heatmap (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hour INTEGER NOT NULL,
        date TEXT NOT NULL,
        model TEXT NOT NULL DEFAULT 'default',
        requests INTEGER NOT NULL DEFAULT 0,
        tokens INTEGER NOT NULL DEFAULT 0,
        cost_usd REAL NOT NULL DEFAULT 0.0,
        UNIQUE(hour, date, model)
    );

    -- Prompt versions (NEW v1.1.0)
    CREATE TABLE IF NOT EXISTS prompt_versions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        version INTEGER NOT NULL DEFAULT 1,
        prompt_text TEXT NOT NULL,
        model TEXT,
        tags TEXT NOT NULL DEFAULT '[]',
        notes TEXT,
        token_count INTEGER NOT NULL DEFAULT 0,
        times_used INTEGER NOT NULL DEFAULT 0,
        total_cost REAL NOT NULL DEFAULT 0.0,
        created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_prompt_versions_name ON prompt_versions(name);

    -- Cost tags (NEW v1.1.0)
    CREATE TABLE IF NOT EXISTS cost_tags (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tag TEXT UNIQUE NOT NULL,
        description TEXT,
        budget_usd REAL,
        created_at TEXT NOT NULL
    );

    -- Cost tag usage (NEW v1.1.0)
    CREATE TABLE IF NOT EXISTS cost_tag_usage (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tag_id INTEGER NOT NULL REFERENCES cost_tags(id),
        compression_id INTEGER REFERENCES compression_log(id),
        cost_usd REAL NOT NULL DEFAULT 0.0,
        model TEXT,
        created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_cost_tag_usage_tag ON cost_tag_usage(tag_id);
    """)

    # Seed builtin profiles
    now = _now()
    builtins = [
        ("aggressive", "Maximum compression — remove filler, deduplicate, shorten", 1, 1, 1, 0.9, 1),
        ("balanced", "Balance between readability and savings", 1, 1, 0, 0.5, 1),
        ("minimal", "Light touch — preserve most of the original text", 0, 0, 0, 0.1, 1),
    ]
    for name, desc, rf, rd, ss, agg, bi in builtins:
        await db.execute(
            """INSERT OR IGNORE INTO profiles
               (name, description, remove_filler, remove_duplicates,
                shorten_sentences, aggressiveness, is_builtin, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (name, desc, rf, rd, ss, agg, bi, now),
        )
    await db.commit()


# ── Compression ──────────────────────────────────────────────────────────────

async def compress(db: aiosqlite.Connection, prompt: str, profile: str = "balanced") -> dict:
    """Compress a prompt using a profile + custom rules."""
    # Load profile settings
    row = await db.execute_fetchall(
        "SELECT * FROM profiles WHERE name = ?", (profile,)
    )
    profile_settings = _row_to_dict(row[0]) if row else None

    # Apply custom compression rules first
    text = prompt
    rules_applied = 0
    rules = await db.execute_fetchall(
        "SELECT * FROM compression_rules WHERE enabled = 1 ORDER BY priority DESC"
    )
    for r in rules:
        r = _row_to_dict(r)
        try:
            new_text = re.sub(r["pattern"], r["replacement"], text)
            if new_text != text:
                rules_applied += 1
                await db.execute(
                    "UPDATE compression_rules SET times_applied = times_applied + 1 WHERE id = ?",
                    (r["id"],),
                )
                text = new_text
        except re.error:
            pass

    # Compress with compressor
    aggressiveness = profile_settings["aggressiveness"] if profile_settings else 0.5
    compressed, _ = compress_prompt(text, max_ratio=aggressiveness)

    original_tokens = estimate_tokens(prompt)
    compressed_tokens = estimate_tokens(compressed)
    ratio = round(compressed_tokens / original_tokens, 4) if original_tokens > 0 else 1.0

    # Bump profile usage
    if profile_settings:
        await db.execute(
            "UPDATE profiles SET times_used = times_used + 1 WHERE name = ?", (profile,)
        )

    await db.commit()

    return {
        "original": prompt,
        "compressed": compressed,
        "original_tokens": original_tokens,
        "compressed_tokens": compressed_tokens,
        "ratio": ratio,
        "profile": profile,
        "rules_applied": rules_applied,
    }


async def record_compression(
    db: aiosqlite.Connection,
    original_tokens: int,
    compressed_tokens: int,
    ratio: float,
    profile: str = "balanced",
    model: Optional[str] = None,
    rules_applied: int = 0,
) -> dict:
    """Save a compression event to the log."""
    now = _now()
    cur = await db.execute(
        """INSERT INTO compression_log
           (original_tokens, compressed_tokens, ratio, profile, model, rules_applied, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (original_tokens, compressed_tokens, ratio, profile, model, rules_applied, now),
    )
    await db.commit()
    tokens_saved = original_tokens - compressed_tokens
    await _bump_daily(db, model or "default", requests=1,
                      tokens_in=original_tokens, tokens_saved=tokens_saved)
    return {"id": cur.lastrowid, "recorded": True}


async def compression_history(
    db: aiosqlite.Connection, limit: int = 50, profile: Optional[str] = None
) -> list[dict]:
    """Return recent compression log entries."""
    if profile:
        rows = await db.execute_fetchall(
            "SELECT * FROM compression_log WHERE profile = ? ORDER BY id DESC LIMIT ?",
            (profile, limit),
        )
    else:
        rows = await db.execute_fetchall(
            "SELECT * FROM compression_log ORDER BY id DESC LIMIT ?", (limit,)
        )
    return [_row_to_dict(r) for r in rows]


async def compression_analytics(db: aiosqlite.Connection) -> dict:
    """Aggregate analytics across all compressions."""
    rows = await db.execute_fetchall("SELECT * FROM compression_log")
    if not rows:
        return {
            "total_compressions": 0, "total_tokens_saved": 0,
            "avg_ratio": 0.0, "best_ratio": 0.0, "worst_ratio": 0.0,
            "by_profile": {}, "tokens_saved_per_day": [],
        }

    entries = [_row_to_dict(r) for r in rows]
    ratios = [e["ratio"] for e in entries]
    total_saved = sum(e["original_tokens"] - e["compressed_tokens"] for e in entries)

    by_profile: dict[str, dict] = {}
    for e in entries:
        p = e["profile"]
        if p not in by_profile:
            by_profile[p] = {"count": 0, "total_saved": 0, "ratios": []}
        by_profile[p]["count"] += 1
        by_profile[p]["total_saved"] += e["original_tokens"] - e["compressed_tokens"]
        by_profile[p]["ratios"].append(e["ratio"])

    for p in by_profile:
        rs = by_profile[p].pop("ratios")
        by_profile[p]["avg_ratio"] = round(sum(rs) / len(rs), 4) if rs else 0.0

    # Tokens saved per day
    day_map: dict[str, int] = {}
    for e in entries:
        day = e["created_at"][:10]
        day_map[day] = day_map.get(day, 0) + (e["original_tokens"] - e["compressed_tokens"])
    tokens_per_day = [{"date": d, "tokens_saved": v} for d, v in sorted(day_map.items())]

    return {
        "total_compressions": len(entries),
        "total_tokens_saved": total_saved,
        "avg_ratio": round(sum(ratios) / len(ratios), 4),
        "best_ratio": round(min(ratios), 4),
        "worst_ratio": round(max(ratios), 4),
        "by_profile": by_profile,
        "tokens_saved_per_day": tokens_per_day,
    }


# ── Cache ────────────────────────────────────────────────────────────────────

async def cache_get(db: aiosqlite.Connection, prompt: str, model: str = "default") -> Optional[dict]:
    """Look up cached response. Returns None on miss."""
    h = _hash(prompt, model)
    rows = await db.execute_fetchall(
        "SELECT * FROM cache WHERE prompt_hash = ?", (h,)
    )
    if not rows:
        await _bump_daily(db, model, cache_misses=1)
        return None
    entry = _row_to_dict(rows[0])
    # Check expiry
    if entry["expires_at"]:
        if datetime.fromisoformat(entry["expires_at"]) < datetime.utcnow():
            await db.execute("DELETE FROM cache WHERE id = ?", (entry["id"],))
            await db.commit()
            await _bump_daily(db, model, cache_misses=1)
            return None
    # Bump hit counter
    now = _now()
    await db.execute(
        "UPDATE cache SET hit_count = hit_count + 1, last_hit_at = ? WHERE id = ?",
        (now, entry["id"]),
    )
    await db.commit()
    await _bump_daily(db, model, cache_hits=1)
    entry["hit_count"] += 1
    entry["last_hit_at"] = now
    return entry


async def cache_set(
    db: aiosqlite.Connection,
    prompt: str,
    response: str,
    model: str = "default",
    ttl: Optional[int] = None,
) -> dict:
    """Store a prompt-response pair in cache."""
    h = _hash(prompt, model)
    now = _now()
    expires_at = None
    if ttl:
        expires_at = (datetime.utcnow() + timedelta(seconds=ttl)).isoformat()
    preview = prompt[:120] + ("..." if len(prompt) > 120 else "")

    # Upsert
    await db.execute(
        """INSERT INTO cache (prompt_hash, prompt_preview, model, response, ttl, created_at, expires_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(prompt_hash) DO UPDATE SET
               response = excluded.response,
               ttl = excluded.ttl,
               expires_at = excluded.expires_at,
               created_at = excluded.created_at""",
        (h, preview, model, response, ttl, now, expires_at),
    )
    await db.commit()
    rows = await db.execute_fetchall(
        "SELECT * FROM cache WHERE prompt_hash = ?", (h,)
    )
    return _row_to_dict(rows[0])


async def cache_list(db: aiosqlite.Connection, model: Optional[str] = None, limit: int = 50) -> list[dict]:
    """List cached entries."""
    if model:
        rows = await db.execute_fetchall(
            "SELECT * FROM cache WHERE model = ? ORDER BY id DESC LIMIT ?",
            (model, limit),
        )
    else:
        rows = await db.execute_fetchall(
            "SELECT * FROM cache ORDER BY id DESC LIMIT ?", (limit,)
        )
    return [_row_to_dict(r) for r in rows]


async def cache_delete(db: aiosqlite.Connection, cache_id: int) -> bool:
    """Delete a single cache entry by ID."""
    cur = await db.execute("DELETE FROM cache WHERE id = ?", (cache_id,))
    await db.commit()
    return cur.rowcount > 0


async def cache_purge(db: aiosqlite.Connection, model: Optional[str] = None) -> int:
    """Purge expired entries (or all for a model). Returns count deleted."""
    if model:
        cur = await db.execute("DELETE FROM cache WHERE model = ?", (model,))
    else:
        cur = await db.execute(
            "DELETE FROM cache WHERE expires_at IS NOT NULL AND expires_at < ?",
            (_now(),),
        )
    await db.commit()
    return cur.rowcount


async def cache_analytics(db: aiosqlite.Connection) -> dict:
    """Cache hit/miss analytics."""
    rows = await db.execute_fetchall("SELECT * FROM cache")
    entries = [_row_to_dict(r) for r in rows]
    total_entries = len(entries)
    total_hits = sum(e["hit_count"] for e in entries)
    total_size = sum(len(e["response"].encode()) for e in entries)

    by_model: dict[str, dict] = {}
    for e in entries:
        m = e["model"]
        if m not in by_model:
            by_model[m] = {"entries": 0, "hits": 0, "size_bytes": 0}
        by_model[m]["entries"] += 1
        by_model[m]["hits"] += e["hit_count"]
        by_model[m]["size_bytes"] += len(e["response"].encode())

    # Overall hit rate from daily_log
    dl = await db.execute_fetchall(
        "SELECT SUM(cache_hits) as h, SUM(cache_misses) as m FROM daily_log"
    )
    dl_row = _row_to_dict(dl[0]) if dl else {"h": 0, "m": 0}
    all_hits = dl_row["h"] or 0
    all_misses = dl_row["m"] or 0
    hit_rate = round(all_hits / (all_hits + all_misses), 4) if (all_hits + all_misses) > 0 else 0.0

    top = sorted(entries, key=lambda e: e["hit_count"], reverse=True)[:10]
    top_entries = [
        {"prompt_preview": e["prompt_preview"], "model": e["model"],
         "hit_count": e["hit_count"]}
        for e in top
    ]

    return {
        "total_entries": total_entries,
        "total_hits": total_hits,
        "hit_rate": hit_rate,
        "total_size_bytes": total_size,
        "top_entries": top_entries,
        "by_model": by_model,
    }


# ── Profiles ─────────────────────────────────────────────────────────────────

async def create_profile(db: aiosqlite.Connection, data: dict) -> dict:
    now = _now()
    cur = await db.execute(
        """INSERT INTO profiles
           (name, description, remove_filler, remove_duplicates,
            shorten_sentences, aggressiveness, is_builtin, created_at)
           VALUES (?, ?, ?, ?, ?, ?, 0, ?)""",
        (data["name"], data.get("description"), data.get("remove_filler", True),
         data.get("remove_duplicates", True), data.get("shorten_sentences", False),
         data.get("aggressiveness", 0.5), now),
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM profiles WHERE id = ?", (cur.lastrowid,))
    return _profile_dict(rows[0])


async def list_profiles(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("SELECT * FROM profiles ORDER BY name")
    return [_profile_dict(r) for r in rows]


async def get_profile(db: aiosqlite.Connection, name: str) -> Optional[dict]:
    rows = await db.execute_fetchall("SELECT * FROM profiles WHERE name = ?", (name,))
    return _profile_dict(rows[0]) if rows else None


async def update_profile(db: aiosqlite.Connection, name: str, data: dict) -> Optional[dict]:
    rows = await db.execute_fetchall("SELECT * FROM profiles WHERE name = ?", (name,))
    if not rows:
        return None
    existing = _row_to_dict(rows[0])
    if existing["is_builtin"]:
        return None  # cannot modify builtins
    sets, vals = [], []
    for col in ("description", "remove_filler", "remove_duplicates",
                "shorten_sentences", "aggressiveness"):
        if col in data and data[col] is not None:
            sets.append(f"{col} = ?")
            vals.append(data[col])
    if not sets:
        return _profile_dict(rows[0])
    vals.append(name)
    await db.execute(f"UPDATE profiles SET {', '.join(sets)} WHERE name = ?", vals)
    await db.commit()
    return await get_profile(db, name)


async def delete_profile(db: aiosqlite.Connection, name: str) -> bool:
    rows = await db.execute_fetchall("SELECT * FROM profiles WHERE name = ?", (name,))
    if not rows:
        return False
    if _row_to_dict(rows[0])["is_builtin"]:
        return False
    await db.execute("DELETE FROM profiles WHERE name = ?", (name,))
    await db.commit()
    return True


def _profile_dict(row) -> dict:
    d = _row_to_dict(row)
    d["is_builtin"] = bool(d["is_builtin"])
    d["remove_filler"] = bool(d["remove_filler"])
    d["remove_duplicates"] = bool(d["remove_duplicates"])
    d["shorten_sentences"] = bool(d["shorten_sentences"])
    return d


# ── Model Costs ──────────────────────────────────────────────────────────────

async def create_model_cost(db: aiosqlite.Connection, data: dict) -> dict:
    now = _now()
    cur = await db.execute(
        """INSERT INTO model_costs (model, input_cost_per_1k, output_cost_per_1k, created_at)
           VALUES (?, ?, ?, ?)""",
        (data["model"], data["input_cost_per_1k"], data["output_cost_per_1k"], now),
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM model_costs WHERE id = ?", (cur.lastrowid,))
    return _row_to_dict(rows[0])


async def list_model_costs(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("SELECT * FROM model_costs ORDER BY model")
    return [_row_to_dict(r) for r in rows]


async def get_model_cost(db: aiosqlite.Connection, model: str) -> Optional[dict]:
    rows = await db.execute_fetchall("SELECT * FROM model_costs WHERE model = ?", (model,))
    return _row_to_dict(rows[0]) if rows else None


async def update_model_cost(db: aiosqlite.Connection, model: str, data: dict) -> Optional[dict]:
    rows = await db.execute_fetchall("SELECT * FROM model_costs WHERE model = ?", (model,))
    if not rows:
        return None
    sets, vals = [], []
    for col in ("input_cost_per_1k", "output_cost_per_1k"):
        if col in data and data[col] is not None:
            sets.append(f"{col} = ?")
            vals.append(data[col])
    if not sets:
        return _row_to_dict(rows[0])
    vals.append(model)
    await db.execute(f"UPDATE model_costs SET {', '.join(sets)} WHERE model = ?", vals)
    await db.commit()
    return await get_model_cost(db, model)


async def delete_model_cost(db: aiosqlite.Connection, model: str) -> bool:
    cur = await db.execute("DELETE FROM model_costs WHERE model = ?", (model,))
    await db.commit()
    return cur.rowcount > 0


# ── Row Helpers (v1.1.0) ────────────────────────────────────────────────────

def _heatmap_row(row):
    return {"id": row["id"], "hour": row["hour"], "date": row["date"], "model": row["model"],
            "requests": row["requests"], "tokens": row["tokens"], "cost_usd": row["cost_usd"]}

def _prompt_version_row(row):
    return {"id": row["id"], "name": row["name"], "version": row["version"],
            "prompt_text": row["prompt_text"], "model": row["model"],
            "tags": json.loads(row["tags"]) if isinstance(row["tags"], str) else row["tags"],
            "notes": row["notes"], "token_count": row["token_count"],
            "times_used": row["times_used"],
            "avg_cost": round(row["total_cost"] / max(1, row["times_used"]), 6),
            "created_at": row["created_at"]}

def _cost_tag_row(row):
    return {"id": row["id"], "tag": row["tag"], "description": row["description"],
            "budget_usd": row["budget_usd"], "created_at": row["created_at"]}


# ── Statistics ───────────────────────────────────────────────────────────────

async def get_stats(db: aiosqlite.Connection) -> dict:
    """Aggregated statistics across daily_log."""
    rows = await db.execute_fetchall(
        """SELECT
             COALESCE(SUM(requests), 0) as total_requests,
             COALESCE(SUM(tokens_in), 0) as total_tokens_in,
             COALESCE(SUM(tokens_out), 0) as total_tokens_out,
             COALESCE(SUM(tokens_saved), 0) as total_tokens_saved,
             COALESCE(SUM(cost), 0) as total_cost,
             COALESCE(SUM(cache_hits), 0) as total_cache_hits,
             COALESCE(SUM(cache_misses), 0) as total_cache_misses
           FROM daily_log"""
    )
    s = _row_to_dict(rows[0])

    total_hits = s["total_cache_hits"]
    total_misses = s["total_cache_misses"]
    hit_rate = round(total_hits / (total_hits + total_misses), 4) if (total_hits + total_misses) > 0 else 0.0

    # Avg compression ratio
    cr = await db.execute_fetchall(
        "SELECT AVG(ratio) as avg_ratio FROM compression_log"
    )
    avg_ratio = round((_row_to_dict(cr[0])["avg_ratio"] or 0.0), 4)

    # Savings estimate
    cost_rows = await db.execute_fetchall("SELECT * FROM model_costs")
    total_savings = 0.0
    if cost_rows:
        avg_input_cost = sum(_row_to_dict(c)["input_cost_per_1k"] for c in cost_rows) / len(cost_rows)
        total_savings = round(s["total_tokens_saved"] / 1000 * avg_input_cost, 6)

    # Top models
    model_rows = await db.execute_fetchall(
        """SELECT model, SUM(requests) as reqs, SUM(tokens_in) as tin,
                  SUM(cost) as cost
           FROM daily_log GROUP BY model ORDER BY reqs DESC LIMIT 10"""
    )
    top_models = [_row_to_dict(r) for r in model_rows]

    # v1.1.0: total prompt versions & cost tags
    pv_rows = await db.execute_fetchall("SELECT COUNT(*) as c FROM prompt_versions")
    total_prompt_versions = _row_to_dict(pv_rows[0])["c"]

    ct_rows = await db.execute_fetchall("SELECT COUNT(*) as c FROM cost_tags")
    total_cost_tags = _row_to_dict(ct_rows[0])["c"]

    return {
        "total_requests": s["total_requests"],
        "total_tokens_in": s["total_tokens_in"],
        "total_tokens_out": s["total_tokens_out"],
        "total_tokens_saved": s["total_tokens_saved"],
        "total_cost": round(s["total_cost"], 6),
        "total_savings": total_savings,
        "cache_hits": total_hits,
        "cache_misses": total_misses,
        "cache_hit_rate": hit_rate,
        "avg_compression_ratio": avg_ratio,
        "top_models": top_models,
        "total_prompt_versions": total_prompt_versions,
        "total_cost_tags": total_cost_tags,
    }


async def daily_stats(db: aiosqlite.Connection, days: int = 30) -> list[dict]:
    """Daily stats for the last N days."""
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = await db.execute_fetchall(
        "SELECT * FROM daily_log WHERE date >= ? ORDER BY date DESC", (cutoff,)
    )
    return [_row_to_dict(r) for r in rows]


async def export_csv(db: aiosqlite.Connection) -> str:
    """Export daily_log as CSV string."""
    rows = await db.execute_fetchall("SELECT * FROM daily_log ORDER BY date")
    lines = ["date,model,requests,tokens_in,tokens_out,tokens_saved,cost,cache_hits,cache_misses"]
    for r in rows:
        d = _row_to_dict(r)
        lines.append(
            f"{d['date']},{d['model']},{d['requests']},{d['tokens_in']},"
            f"{d['tokens_out']},{d['tokens_saved']},{d['cost']},"
            f"{d['cache_hits']},{d['cache_misses']}"
        )
    return "\n".join(lines)


# ── Budget ───────────────────────────────────────────────────────────────────

async def set_budget(db: aiosqlite.Connection, data: dict) -> dict:
    await db.execute(
        "UPDATE budget SET daily_limit = ?, monthly_limit = ? WHERE id = 1",
        (data.get("daily_limit"), data.get("monthly_limit")),
    )
    await db.commit()
    return await get_budget(db)


async def get_budget(db: aiosqlite.Connection) -> dict:
    rows = await db.execute_fetchall("SELECT * FROM budget WHERE id = 1")
    b = _row_to_dict(rows[0])
    today = _today()
    month = _this_month()

    daily_rows = await db.execute_fetchall(
        "SELECT COALESCE(SUM(cost), 0) as spent FROM daily_log WHERE date = ?", (today,)
    )
    daily_spent = round(_row_to_dict(daily_rows[0])["spent"], 6)

    monthly_rows = await db.execute_fetchall(
        "SELECT COALESCE(SUM(cost), 0) as spent FROM daily_log WHERE date LIKE ?",
        (month + "%",),
    )
    monthly_spent = round(_row_to_dict(monthly_rows[0])["spent"], 6)

    daily_limit = b["daily_limit"]
    monthly_limit = b["monthly_limit"]

    daily_remaining = round(daily_limit - daily_spent, 6) if daily_limit is not None else None
    monthly_remaining = round(monthly_limit - monthly_spent, 6) if monthly_limit is not None else None

    over_budget = False
    if daily_limit is not None and daily_spent > daily_limit:
        over_budget = True
    if monthly_limit is not None and monthly_spent > monthly_limit:
        over_budget = True

    daily_pct = round(daily_spent / daily_limit * 100, 2) if daily_limit else None
    monthly_pct = round(monthly_spent / monthly_limit * 100, 2) if monthly_limit else None

    return {
        "daily_limit": daily_limit,
        "monthly_limit": monthly_limit,
        "daily_spent": daily_spent,
        "monthly_spent": monthly_spent,
        "daily_remaining": daily_remaining,
        "monthly_remaining": monthly_remaining,
        "over_budget": over_budget,
        "daily_pct": daily_pct,
        "monthly_pct": monthly_pct,
    }


# ── Templates ────────────────────────────────────────────────────────────────

async def create_template(db: aiosqlite.Connection, data: dict) -> dict:
    now = _now()
    tags = json.dumps(data.get("tags") or [])
    cur = await db.execute(
        """INSERT INTO templates (name, content, description, tags, version, created_at, updated_at)
           VALUES (?, ?, ?, ?, 1, ?, ?)""",
        (data["name"], data["content"], data.get("description"), tags, now, now),
    )
    tid = cur.lastrowid
    # Save version 1
    await db.execute(
        "INSERT INTO template_versions (template_id, version, content, created_at) VALUES (?, 1, ?, ?)",
        (tid, data["content"], now),
    )
    await db.commit()
    return await get_template(db, tid)


async def list_templates(db: aiosqlite.Connection, tag: Optional[str] = None) -> list[dict]:
    rows = await db.execute_fetchall("SELECT * FROM templates ORDER BY name")
    results = [_template_dict(r) for r in rows]
    if tag:
        results = [t for t in results if tag in t["tags"]]
    return results


async def get_template(db: aiosqlite.Connection, template_id: int) -> Optional[dict]:
    rows = await db.execute_fetchall("SELECT * FROM templates WHERE id = ?", (template_id,))
    return _template_dict(rows[0]) if rows else None


async def update_template(db: aiosqlite.Connection, template_id: int, data: dict) -> Optional[dict]:
    rows = await db.execute_fetchall("SELECT * FROM templates WHERE id = ?", (template_id,))
    if not rows:
        return None
    existing = _row_to_dict(rows[0])
    now = _now()
    sets, vals = ["updated_at = ?"], [now]

    if "name" in data and data["name"] is not None:
        sets.append("name = ?")
        vals.append(data["name"])
    if "description" in data:
        sets.append("description = ?")
        vals.append(data["description"])
    if "tags" in data and data["tags"] is not None:
        sets.append("tags = ?")
        vals.append(json.dumps(data["tags"]))

    new_version = False
    if "content" in data and data["content"] is not None and data["content"] != existing["content"]:
        new_ver = existing["version"] + 1
        sets.append("content = ?")
        vals.append(data["content"])
        sets.append("version = ?")
        vals.append(new_ver)
        new_version = True

    vals.append(template_id)
    await db.execute(f"UPDATE templates SET {', '.join(sets)} WHERE id = ?", vals)

    if new_version:
        await db.execute(
            "INSERT INTO template_versions (template_id, version, content, created_at) VALUES (?, ?, ?, ?)",
            (template_id, new_ver, data["content"], now),
        )

    await db.commit()
    return await get_template(db, template_id)


async def delete_template(db: aiosqlite.Connection, template_id: int) -> bool:
    cur = await db.execute("DELETE FROM templates WHERE id = ?", (template_id,))
    await db.execute("DELETE FROM template_versions WHERE template_id = ?", (template_id,))
    await db.commit()
    return cur.rowcount > 0


async def render_template(db: aiosqlite.Connection, template_id: int, variables: dict[str, str]) -> Optional[dict]:
    t = await get_template(db, template_id)
    if not t:
        return None

    content = t["content"]
    used, missing = [], []

    # Find all {{variable}} placeholders
    placeholders = re.findall(r"\{\{(\w+)\}\}", content)
    for p in set(placeholders):
        if p in variables:
            content = content.replace("{{" + p + "}}", variables[p])
            used.append(p)
        else:
            missing.append(p)

    await db.execute(
        "UPDATE templates SET times_rendered = times_rendered + 1 WHERE id = ?",
        (template_id,),
    )
    await db.commit()

    return {
        "rendered": content,
        "original_tokens": estimate_tokens(t["content"]),
        "rendered_tokens": estimate_tokens(content),
        "variables_used": sorted(set(used)),
        "variables_missing": sorted(set(missing)),
    }


async def template_versions(db: aiosqlite.Connection, template_id: int) -> list[dict]:
    rows = await db.execute_fetchall(
        "SELECT * FROM template_versions WHERE template_id = ? ORDER BY version DESC",
        (template_id,),
    )
    return [_row_to_dict(r) for r in rows]


async def template_diff(db: aiosqlite.Connection, template_id: int, v_a: int, v_b: int) -> Optional[dict]:
    rows_a = await db.execute_fetchall(
        "SELECT content FROM template_versions WHERE template_id = ? AND version = ?",
        (template_id, v_a),
    )
    rows_b = await db.execute_fetchall(
        "SELECT content FROM template_versions WHERE template_id = ? AND version = ?",
        (template_id, v_b),
    )
    if not rows_a or not rows_b:
        return None

    a_lines = _row_to_dict(rows_a[0])["content"].splitlines(keepends=True)
    b_lines = _row_to_dict(rows_b[0])["content"].splitlines(keepends=True)
    diff = list(difflib.unified_diff(a_lines, b_lines, fromfile=f"v{v_a}", tofile=f"v{v_b}"))

    return {
        "template_id": template_id,
        "version_a": v_a,
        "version_b": v_b,
        "diff": [line.rstrip("\n") for line in diff],
    }


async def template_rollback(db: aiosqlite.Connection, template_id: int, version: int) -> Optional[dict]:
    rows = await db.execute_fetchall(
        "SELECT content FROM template_versions WHERE template_id = ? AND version = ?",
        (template_id, version),
    )
    if not rows:
        return None
    old_content = _row_to_dict(rows[0])["content"]
    return await update_template(db, template_id, {"content": old_content})


def _template_dict(row) -> dict:
    d = _row_to_dict(row)
    d["tags"] = json.loads(d["tags"]) if isinstance(d["tags"], str) else d["tags"]
    return d


# ── Cost Estimation ──────────────────────────────────────────────────────────

async def estimate_cost(db: aiosqlite.Connection, prompt: str, max_output_tokens: int = 500) -> dict:
    """Estimate cost across all configured models."""
    input_tokens = estimate_tokens(prompt)

    # Also get compressed estimate
    comp = await compress(db, prompt, "balanced")
    comp_tokens = comp["compressed_tokens"]

    models = await list_model_costs(db)
    if not models:
        return {"estimates": [], "cheapest": "", "most_expensive": ""}

    estimates = []
    for m in models:
        inp_cost = round(input_tokens / 1000 * m["input_cost_per_1k"], 6)
        out_cost = round(max_output_tokens / 1000 * m["output_cost_per_1k"], 6)
        total = round(inp_cost + out_cost, 6)
        comp_inp_cost = round(comp_tokens / 1000 * m["input_cost_per_1k"], 6)
        comp_total = round(comp_inp_cost + out_cost, 6)
        savings = round(total - comp_total, 6)
        estimates.append({
            "model": m["model"],
            "input_tokens": input_tokens,
            "output_tokens": max_output_tokens,
            "input_cost": inp_cost,
            "output_cost": out_cost,
            "total_cost": total,
            "compressed_input_tokens": comp_tokens,
            "compressed_total_cost": comp_total,
            "savings": savings,
        })

    estimates.sort(key=lambda e: e["total_cost"])
    return {
        "estimates": estimates,
        "cheapest": estimates[0]["model"] if estimates else "",
        "most_expensive": estimates[-1]["model"] if estimates else "",
    }


# ── Benchmarking ─────────────────────────────────────────────────────────────

async def benchmark(db: aiosqlite.Connection, prompt: str) -> dict:
    """Test all profiles against a prompt."""
    profiles = await list_profiles(db)
    results = []
    for p in profiles:
        res = await compress(db, prompt, p["name"])
        # Optionally estimate cost using cheapest model
        est_cost = None
        models = await list_model_costs(db)
        if models:
            cheapest = min(models, key=lambda m: m["input_cost_per_1k"])
            est_cost = round(res["compressed_tokens"] / 1000 * cheapest["input_cost_per_1k"], 6)

        results.append({
            "profile": p["name"],
            "original_tokens": res["original_tokens"],
            "compressed_tokens": res["compressed_tokens"],
            "ratio": res["ratio"],
            "rules_applied": res["rules_applied"],
            "estimated_cost": est_cost,
        })

    results.sort(key=lambda r: r["ratio"])
    best = results[0] if results else None
    return {
        "results": results,
        "best_profile": best["profile"] if best else "",
        "best_ratio": best["ratio"] if best else 0.0,
    }


# ── Model Comparison ────────────────────────────────────────────────────────

async def compare_models(
    db: aiosqlite.Connection, prompt: str, max_output_tokens: int = 500,
    profile: Optional[str] = None,
) -> dict:
    """Compare costs across models, optionally with compression."""
    input_tokens = estimate_tokens(prompt)
    comp_tokens = None
    if profile:
        res = await compress(db, prompt, profile)
        comp_tokens = res["compressed_tokens"]

    models = await list_model_costs(db)
    entries = []
    for m in models:
        inp_cost = round(input_tokens / 1000 * m["input_cost_per_1k"], 6)
        out_cost = round(max_output_tokens / 1000 * m["output_cost_per_1k"], 6)
        total = round(inp_cost + out_cost, 6)
        with_comp = None
        savings_pct = None
        if comp_tokens is not None:
            comp_inp = round(comp_tokens / 1000 * m["input_cost_per_1k"], 6)
            with_comp = round(comp_inp + out_cost, 6)
            savings_pct = round((1 - with_comp / total) * 100, 2) if total > 0 else 0.0
        entries.append({
            "model": m["model"],
            "input_cost": inp_cost,
            "output_cost": out_cost,
            "total_cost": total,
            "with_compression": with_comp,
            "savings_pct": savings_pct,
        })

    entries.sort(key=lambda e: e["total_cost"])
    recommended = entries[0]["model"] if entries else ""
    reason = "Lowest total cost" if entries else "No models configured"
    if entries and comp_tokens is not None:
        best_savings = max(entries, key=lambda e: e["savings_pct"] or 0)
        if best_savings["savings_pct"] and best_savings["savings_pct"] > 10:
            reason = f"Lowest cost with {best_savings['savings_pct']}% compression savings"

    return {
        "entries": entries,
        "recommended": recommended,
        "recommendation_reason": reason,
    }


# ── Batch Processing ────────────────────────────────────────────────────────

async def batch_process(db: aiosqlite.Connection, prompts: list[dict],
                        profile: str = "balanced", use_cache: bool = True) -> dict:
    """Process multiple prompts: dedup, cache lookup, compress, mock responses."""
    seen_hashes: dict[str, int] = {}
    results = []
    total_saved = 0
    total_cost = 0.0
    cache_hit_count = 0
    dedup_count = 0

    for i, p in enumerate(prompts):
        prompt_text = p["prompt"]
        model = p.get("model", "default")
        h = _hash(prompt_text, model)

        # Dedup check
        deduplicated = False
        if h in seen_hashes:
            deduplicated = True
            dedup_count += 1

        # Cache check
        cache_hit = False
        mock_response = None
        if use_cache:
            cached = await cache_get(db, prompt_text, model)
            if cached:
                cache_hit = True
                cache_hit_count += 1
                mock_response = cached["response"]

        # Compress
        res = await compress(db, prompt_text, profile)
        saved = res["original_tokens"] - res["compressed_tokens"]
        total_saved += saved

        # Cost estimate
        mc = await get_model_cost(db, model)
        est = 0.0
        if mc:
            est = round(res["compressed_tokens"] / 1000 * mc["input_cost_per_1k"], 6)
        total_cost += est

        seen_hashes[h] = i
        results.append({
            "index": i,
            "prompt_preview": prompt_text[:80],
            "original_tokens": res["original_tokens"],
            "compressed_tokens": res["compressed_tokens"],
            "ratio": res["ratio"],
            "cache_hit": cache_hit,
            "deduplicated": deduplicated,
            "mock_response": mock_response,
        })

    return {
        "total": len(prompts),
        "deduplicated": dedup_count,
        "cache_hits": cache_hit_count,
        "results": results,
        "total_tokens_saved": total_saved,
        "total_estimated_cost": round(total_cost, 6),
    }


# ── Compression Rules ───────────────────────────────────────────────────────

async def create_rule(db: aiosqlite.Connection, data: dict) -> dict:
    now = _now()
    # Validate regex
    try:
        re.compile(data["pattern"])
    except re.error as exc:
        raise ValueError(f"Invalid regex pattern: {exc}")

    cur = await db.execute(
        """INSERT INTO compression_rules
           (name, pattern, replacement, priority, enabled, description, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (data["name"], data["pattern"], data["replacement"],
         data.get("priority", 0), int(data.get("enabled", True)),
         data.get("description"), now),
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM compression_rules WHERE id = ?", (cur.lastrowid,))
    return _rule_dict(rows[0])


async def list_rules(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("SELECT * FROM compression_rules ORDER BY priority DESC")
    return [_rule_dict(r) for r in rows]


async def get_rule(db: aiosqlite.Connection, rule_id: int) -> Optional[dict]:
    rows = await db.execute_fetchall("SELECT * FROM compression_rules WHERE id = ?", (rule_id,))
    return _rule_dict(rows[0]) if rows else None


async def update_rule(db: aiosqlite.Connection, rule_id: int, data: dict) -> Optional[dict]:
    rows = await db.execute_fetchall("SELECT * FROM compression_rules WHERE id = ?", (rule_id,))
    if not rows:
        return None
    sets, vals = [], []
    for col in ("name", "pattern", "replacement", "priority", "description"):
        if col in data and data[col] is not None:
            if col == "pattern":
                try:
                    re.compile(data[col])
                except re.error as exc:
                    raise ValueError(f"Invalid regex pattern: {exc}")
            sets.append(f"{col} = ?")
            vals.append(data[col])
    if "enabled" in data and data["enabled"] is not None:
        sets.append("enabled = ?")
        vals.append(int(data["enabled"]))
    if not sets:
        return _rule_dict(rows[0])
    vals.append(rule_id)
    await db.execute(f"UPDATE compression_rules SET {', '.join(sets)} WHERE id = ?", vals)
    await db.commit()
    return await get_rule(db, rule_id)


async def delete_rule(db: aiosqlite.Connection, rule_id: int) -> bool:
    cur = await db.execute("DELETE FROM compression_rules WHERE id = ?", (rule_id,))
    await db.commit()
    return cur.rowcount > 0


def _rule_dict(row) -> dict:
    d = _row_to_dict(row)
    d["enabled"] = bool(d["enabled"])
    return d


# ── Prompt Diff ──────────────────────────────────────────────────────────────

async def prompt_diff(prompt_a: str, prompt_b: str) -> dict:
    """Compare two prompts: token counts, diff, similarity."""
    tokens_a = estimate_tokens(prompt_a)
    tokens_b = estimate_tokens(prompt_b)

    a_lines = prompt_a.splitlines(keepends=True)
    b_lines = prompt_b.splitlines(keepends=True)
    diff = list(difflib.unified_diff(a_lines, b_lines, fromfile="prompt_a", tofile="prompt_b"))

    similarity = difflib.SequenceMatcher(None, prompt_a, prompt_b).ratio()

    return {
        "tokens_a": tokens_a,
        "tokens_b": tokens_b,
        "token_diff": abs(tokens_a - tokens_b),
        "diff_lines": [line.rstrip("\n") for line in diff],
        "similarity": round(similarity, 4),
    }


# ── Usage Quotas ─────────────────────────────────────────────────────────────

async def create_quota(db: aiosqlite.Connection, data: dict) -> dict:
    now = _now()
    cur = await db.execute(
        "INSERT INTO usage_quotas (model, daily_limit, monthly_limit, created_at) VALUES (?, ?, ?, ?)",
        (data["model"], data.get("daily_limit"), data.get("monthly_limit"), now),
    )
    await db.commit()
    return await get_quota(db, cur.lastrowid)


async def list_quotas(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("SELECT * FROM usage_quotas ORDER BY model")
    return [await _enrich_quota(db, r) for r in rows]


async def get_quota(db: aiosqlite.Connection, quota_id: int) -> Optional[dict]:
    rows = await db.execute_fetchall("SELECT * FROM usage_quotas WHERE id = ?", (quota_id,))
    if not rows:
        return None
    return await _enrich_quota(db, rows[0])


async def get_quota_by_model(db: aiosqlite.Connection, model: str) -> Optional[dict]:
    rows = await db.execute_fetchall("SELECT * FROM usage_quotas WHERE model = ?", (model,))
    if not rows:
        return None
    return await _enrich_quota(db, rows[0])


async def update_quota(db: aiosqlite.Connection, quota_id: int, data: dict) -> Optional[dict]:
    rows = await db.execute_fetchall("SELECT * FROM usage_quotas WHERE id = ?", (quota_id,))
    if not rows:
        return None
    sets, vals = [], []
    for col in ("daily_limit", "monthly_limit"):
        if col in data and data[col] is not None:
            sets.append(f"{col} = ?")
            vals.append(data[col])
    if not sets:
        return await get_quota(db, quota_id)
    vals.append(quota_id)
    await db.execute(f"UPDATE usage_quotas SET {', '.join(sets)} WHERE id = ?", vals)
    await db.commit()
    return await get_quota(db, quota_id)


async def delete_quota(db: aiosqlite.Connection, quota_id: int) -> bool:
    cur = await db.execute("DELETE FROM usage_quotas WHERE id = ?", (quota_id,))
    await db.commit()
    return cur.rowcount > 0


async def check_quota(db: aiosqlite.Connection, model: str) -> Optional[dict]:
    """Check if a model is over quota. Returns quota status or None if no quota set."""
    return await get_quota_by_model(db, model)


async def _enrich_quota(db: aiosqlite.Connection, row) -> dict:
    d = _row_to_dict(row)
    model = d["model"]
    today = _today()
    month = _this_month()

    # Daily usage
    daily = await db.execute_fetchall(
        "SELECT COALESCE(SUM(tokens_in + tokens_out), 0) as used FROM daily_log WHERE date = ? AND model = ?",
        (today, model),
    )
    daily_used = _row_to_dict(daily[0])["used"]

    # Monthly usage
    monthly = await db.execute_fetchall(
        "SELECT COALESCE(SUM(tokens_in + tokens_out), 0) as used FROM daily_log WHERE date LIKE ? AND model = ?",
        (month + "%", model),
    )
    monthly_used = _row_to_dict(monthly[0])["used"]

    d["daily_used"] = daily_used
    d["monthly_used"] = monthly_used
    d["daily_remaining"] = (d["daily_limit"] - daily_used) if d["daily_limit"] is not None else None
    d["monthly_remaining"] = (d["monthly_limit"] - monthly_used) if d["monthly_limit"] is not None else None
    d["over_quota"] = False
    if d["daily_limit"] is not None and daily_used > d["daily_limit"]:
        d["over_quota"] = True
    if d["monthly_limit"] is not None and monthly_used > d["monthly_limit"]:
        d["over_quota"] = True
    return d


# ── Cost Alerts ──────────────────────────────────────────────────────────────

async def create_alert_rule(db: aiosqlite.Connection, data: dict) -> dict:
    valid_metrics = {"daily_cost", "monthly_cost", "tokens_used", "cache_miss_rate"}
    if data["metric"] not in valid_metrics:
        raise ValueError(f"Invalid metric. Must be one of: {valid_metrics}")
    valid_ops = {"gt", "gte", "lt", "lte", "eq"}
    if data["operator"] not in valid_ops:
        raise ValueError(f"Invalid operator. Must be one of: {valid_ops}")

    now = _now()
    cur = await db.execute(
        """INSERT INTO alert_rules
           (name, metric, operator, threshold, model, enabled, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (data["name"], data["metric"], data["operator"], data["threshold"],
         data.get("model"), int(data.get("enabled", True)), now),
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM alert_rules WHERE id = ?", (cur.lastrowid,))
    return _alert_rule_dict(rows[0])


async def list_alert_rules(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("SELECT * FROM alert_rules ORDER BY name")
    return [_alert_rule_dict(r) for r in rows]


async def get_alert_rule(db: aiosqlite.Connection, rule_id: int) -> Optional[dict]:
    rows = await db.execute_fetchall("SELECT * FROM alert_rules WHERE id = ?", (rule_id,))
    return _alert_rule_dict(rows[0]) if rows else None


async def update_alert_rule(db: aiosqlite.Connection, rule_id: int, data: dict) -> Optional[dict]:
    rows = await db.execute_fetchall("SELECT * FROM alert_rules WHERE id = ?", (rule_id,))
    if not rows:
        return None
    sets, vals = [], []
    for col in ("name", "metric", "operator", "threshold", "model"):
        if col in data and data[col] is not None:
            sets.append(f"{col} = ?")
            vals.append(data[col])
    if "enabled" in data and data["enabled"] is not None:
        sets.append("enabled = ?")
        vals.append(int(data["enabled"]))
    if not sets:
        return _alert_rule_dict(rows[0])
    vals.append(rule_id)
    await db.execute(f"UPDATE alert_rules SET {', '.join(sets)} WHERE id = ?", vals)
    await db.commit()
    return await get_alert_rule(db, rule_id)


async def delete_alert_rule(db: aiosqlite.Connection, rule_id: int) -> bool:
    await db.execute("DELETE FROM alert_log WHERE rule_id = ?", (rule_id,))
    cur = await db.execute("DELETE FROM alert_rules WHERE id = ?", (rule_id,))
    await db.commit()
    return cur.rowcount > 0


async def evaluate_alerts(db: aiosqlite.Connection) -> list[dict]:
    """Evaluate all enabled alert rules against current metrics. Returns triggered alerts."""
    rules = await db.execute_fetchall("SELECT * FROM alert_rules WHERE enabled = 1")
    now = _now()
    today = _today()
    month = _this_month()
    triggered = []

    for rule_row in rules:
        rule = _alert_rule_dict(rule_row)
        metric = rule["metric"]
        model_filter = rule["model"]

        # Compute current value
        current_value = 0.0
        if metric == "daily_cost":
            if model_filter:
                r = await db.execute_fetchall(
                    "SELECT COALESCE(SUM(cost), 0) as v FROM daily_log WHERE date = ? AND model = ?",
                    (today, model_filter),
                )
            else:
                r = await db.execute_fetchall(
                    "SELECT COALESCE(SUM(cost), 0) as v FROM daily_log WHERE date = ?",
                    (today,),
                )
            current_value = _row_to_dict(r[0])["v"]
        elif metric == "monthly_cost":
            if model_filter:
                r = await db.execute_fetchall(
                    "SELECT COALESCE(SUM(cost), 0) as v FROM daily_log WHERE date LIKE ? AND model = ?",
                    (month + "%", model_filter),
                )
            else:
                r = await db.execute_fetchall(
                    "SELECT COALESCE(SUM(cost), 0) as v FROM daily_log WHERE date LIKE ?",
                    (month + "%",),
                )
            current_value = _row_to_dict(r[0])["v"]
        elif metric == "tokens_used":
            if model_filter:
                r = await db.execute_fetchall(
                    "SELECT COALESCE(SUM(tokens_in + tokens_out), 0) as v FROM daily_log WHERE date = ? AND model = ?",
                    (today, model_filter),
                )
            else:
                r = await db.execute_fetchall(
                    "SELECT COALESCE(SUM(tokens_in + tokens_out), 0) as v FROM daily_log WHERE date = ?",
                    (today,),
                )
            current_value = _row_to_dict(r[0])["v"]
        elif metric == "cache_miss_rate":
            if model_filter:
                r = await db.execute_fetchall(
                    "SELECT COALESCE(SUM(cache_hits), 0) as h, COALESCE(SUM(cache_misses), 0) as m FROM daily_log WHERE date = ? AND model = ?",
                    (today, model_filter),
                )
            else:
                r = await db.execute_fetchall(
                    "SELECT COALESCE(SUM(cache_hits), 0) as h, COALESCE(SUM(cache_misses), 0) as m FROM daily_log WHERE date = ?",
                    (today,),
                )
            row = _row_to_dict(r[0])
            total = (row["h"] or 0) + (row["m"] or 0)
            current_value = round((row["m"] or 0) / total * 100, 2) if total > 0 else 0.0

        # Check threshold
        op = rule["operator"]
        threshold = rule["threshold"]
        tripped = False
        if op == "gt" and current_value > threshold:
            tripped = True
        elif op == "gte" and current_value >= threshold:
            tripped = True
        elif op == "lt" and current_value < threshold:
            tripped = True
        elif op == "lte" and current_value <= threshold:
            tripped = True
        elif op == "eq" and current_value == threshold:
            tripped = True

        if tripped:
            msg = f"Alert '{rule['name']}': {metric} = {current_value} {op} {threshold}"
            if model_filter:
                msg += f" (model: {model_filter})"
            await db.execute(
                """INSERT INTO alert_log
                   (rule_id, rule_name, metric, current_value, threshold, message, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (rule["id"], rule["name"], metric, current_value, threshold, msg, now),
            )
            await db.execute(
                "UPDATE alert_rules SET times_triggered = times_triggered + 1, last_triggered_at = ? WHERE id = ?",
                (now, rule["id"]),
            )
            triggered.append({
                "rule_id": rule["id"],
                "rule_name": rule["name"],
                "metric": metric,
                "current_value": current_value,
                "threshold": threshold,
                "message": msg,
            })

    await db.commit()
    return triggered


async def alert_log(db: aiosqlite.Connection, limit: int = 50,
                    acknowledged: Optional[bool] = None) -> list[dict]:
    if acknowledged is not None:
        rows = await db.execute_fetchall(
            "SELECT * FROM alert_log WHERE acknowledged = ? ORDER BY id DESC LIMIT ?",
            (int(acknowledged), limit),
        )
    else:
        rows = await db.execute_fetchall(
            "SELECT * FROM alert_log ORDER BY id DESC LIMIT ?", (limit,)
        )
    return [_alert_log_dict(r) for r in rows]


async def acknowledge_alert(db: aiosqlite.Connection, alert_id: int) -> bool:
    cur = await db.execute(
        "UPDATE alert_log SET acknowledged = 1 WHERE id = ?", (alert_id,)
    )
    await db.commit()
    return cur.rowcount > 0


async def alert_summary(db: aiosqlite.Connection) -> dict:
    rules = await db.execute_fetchall("SELECT * FROM alert_rules")
    total_rules = len(rules)
    enabled_rules = sum(1 for r in rules if _row_to_dict(r)["enabled"])

    today = _today()
    week_ago = (datetime.utcnow() - timedelta(days=7)).isoformat()

    today_rows = await db.execute_fetchall(
        "SELECT COUNT(*) as c FROM alert_log WHERE created_at LIKE ?", (today + "%",)
    )
    week_rows = await db.execute_fetchall(
        "SELECT COUNT(*) as c FROM alert_log WHERE created_at >= ?", (week_ago,)
    )
    unack_rows = await db.execute_fetchall(
        "SELECT COUNT(*) as c FROM alert_log WHERE acknowledged = 0"
    )

    recent = await alert_log(db, limit=10)

    return {
        "total_rules": total_rules,
        "enabled_rules": enabled_rules,
        "total_alerts_today": _row_to_dict(today_rows[0])["c"],
        "total_alerts_week": _row_to_dict(week_rows[0])["c"],
        "unacknowledged": _row_to_dict(unack_rows[0])["c"],
        "recent_alerts": recent,
    }


def _alert_rule_dict(row) -> dict:
    d = _row_to_dict(row)
    d["enabled"] = bool(d["enabled"])
    return d


def _alert_log_dict(row) -> dict:
    d = _row_to_dict(row)
    d["acknowledged"] = bool(d["acknowledged"])
    return d


# ── A/B Testing ──────────────────────────────────────────────────────────────

async def create_experiment(db: aiosqlite.Connection, data: dict) -> dict:
    # Validate profiles exist
    for p in (data["profile_a"], data["profile_b"]):
        if not await get_profile(db, p):
            raise ValueError(f"Profile '{p}' not found")

    now = _now()
    cur = await db.execute(
        """INSERT INTO experiments
           (name, description, profile_a, profile_b, sample_size, status, created_at)
           VALUES (?, ?, ?, ?, ?, 'pending', ?)""",
        (data["name"], data.get("description"), data["profile_a"],
         data["profile_b"], data.get("sample_size", 100), now),
    )
    await db.commit()
    return await get_experiment(db, cur.lastrowid)


async def list_experiments(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("SELECT * FROM experiments ORDER BY id DESC")
    return [_row_to_dict(r) for r in rows]


async def get_experiment(db: aiosqlite.Connection, exp_id: int) -> Optional[dict]:
    rows = await db.execute_fetchall("SELECT * FROM experiments WHERE id = ?", (exp_id,))
    return _row_to_dict(rows[0]) if rows else None


async def run_experiment(db: aiosqlite.Connection, exp_id: int, prompt: str) -> Optional[dict]:
    exp = await get_experiment(db, exp_id)
    if not exp:
        return None
    if exp["status"] == "completed":
        return None

    # Update status to running
    if exp["status"] == "pending":
        await db.execute("UPDATE experiments SET status = 'running' WHERE id = ?", (exp_id,))

    run_number = exp["runs_completed"] + 1
    # Alternate variants: odd=A, even=B
    variant = "A" if run_number % 2 == 1 else "B"
    profile = exp["profile_a"] if variant == "A" else exp["profile_b"]

    res = await compress(db, prompt, profile)
    now = _now()

    await db.execute(
        """INSERT INTO experiment_runs
           (experiment_id, run_number, variant, profile, original_tokens,
            compressed_tokens, ratio, rules_applied, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (exp_id, run_number, variant, profile, res["original_tokens"],
         res["compressed_tokens"], res["ratio"], res["rules_applied"], now),
    )

    await db.execute(
        "UPDATE experiments SET runs_completed = ? WHERE id = ?",
        (run_number, exp_id),
    )

    # Auto-complete if sample size reached
    if run_number >= exp["sample_size"]:
        await db.execute(
            "UPDATE experiments SET status = 'completed', completed_at = ? WHERE id = ?",
            (now, exp_id),
        )

    await db.commit()
    return {
        "experiment_id": exp_id,
        "run_number": run_number,
        "variant": variant,
        "profile": profile,
        "original_tokens": res["original_tokens"],
        "compressed_tokens": res["compressed_tokens"],
        "ratio": res["ratio"],
        "rules_applied": res["rules_applied"],
    }


async def complete_experiment(db: aiosqlite.Connection, exp_id: int) -> Optional[dict]:
    exp = await get_experiment(db, exp_id)
    if not exp:
        return None
    now = _now()
    await db.execute(
        "UPDATE experiments SET status = 'completed', completed_at = ? WHERE id = ?",
        (now, exp_id),
    )
    await db.commit()
    return await experiment_results(db, exp_id)


async def experiment_results(db: aiosqlite.Connection, exp_id: int) -> Optional[dict]:
    exp = await get_experiment(db, exp_id)
    if not exp:
        return None

    runs = await db.execute_fetchall(
        "SELECT * FROM experiment_runs WHERE experiment_id = ? ORDER BY run_number",
        (exp_id,),
    )
    run_list = [_row_to_dict(r) for r in runs]

    a_runs = [r for r in run_list if r["variant"] == "A"]
    b_runs = [r for r in run_list if r["variant"] == "B"]

    avg_ratio_a = round(sum(r["ratio"] for r in a_runs) / len(a_runs), 4) if a_runs else 0.0
    avg_ratio_b = round(sum(r["ratio"] for r in b_runs) / len(b_runs), 4) if b_runs else 0.0

    avg_saved_a = round(
        sum(r["original_tokens"] - r["compressed_tokens"] for r in a_runs) / len(a_runs), 2
    ) if a_runs else 0.0
    avg_saved_b = round(
        sum(r["original_tokens"] - r["compressed_tokens"] for r in b_runs) / len(b_runs), 2
    ) if b_runs else 0.0

    winner = None
    confidence = None
    if a_runs and b_runs:
        if avg_ratio_a < avg_ratio_b:
            winner = exp["profile_a"]
        elif avg_ratio_b < avg_ratio_a:
            winner = exp["profile_b"]
        else:
            winner = "tie"
        # Simple confidence: based on sample size
        total_runs = len(a_runs) + len(b_runs)
        confidence = round(min(total_runs / exp["sample_size"], 1.0) * 100, 1)

    return {
        "experiment_id": exp_id,
        "name": exp["name"],
        "status": exp["status"],
        "profile_a": exp["profile_a"],
        "profile_b": exp["profile_b"],
        "runs_a": len(a_runs),
        "runs_b": len(b_runs),
        "avg_ratio_a": avg_ratio_a,
        "avg_ratio_b": avg_ratio_b,
        "avg_tokens_saved_a": avg_saved_a,
        "avg_tokens_saved_b": avg_saved_b,
        "winner": winner,
        "confidence": confidence,
        "detail": run_list,
    }


async def delete_experiment(db: aiosqlite.Connection, exp_id: int) -> bool:
    await db.execute("DELETE FROM experiment_runs WHERE experiment_id = ?", (exp_id,))
    cur = await db.execute("DELETE FROM experiments WHERE id = ?", (exp_id,))
    await db.commit()
    return cur.rowcount > 0


# ══════════════════════════════════════════════════════════════════════════════
# NEW v1.0.0: Prompt Playground
# ══════════════════════════════════════════════════════════════════════════════

async def create_playground_session(db: aiosqlite.Connection, data: dict) -> dict:
    """Create a new playground session."""
    now = _now()
    cur = await db.execute(
        "INSERT INTO playground_sessions (name, description, created_at, updated_at) VALUES (?, ?, ?, ?)",
        (data["name"], data.get("description"), now, now),
    )
    await db.commit()
    return await get_playground_session(db, cur.lastrowid)


async def list_playground_sessions(db: aiosqlite.Connection) -> list[dict]:
    """List all playground sessions with run counts."""
    rows = await db.execute_fetchall(
        """SELECT s.*, COALESCE(cnt.c, 0) as runs_count
           FROM playground_sessions s
           LEFT JOIN (SELECT session_id, COUNT(*) as c FROM playground_runs GROUP BY session_id) cnt
             ON s.id = cnt.session_id
           ORDER BY s.updated_at DESC"""
    )
    return [_row_to_dict(r) for r in rows]


async def get_playground_session(db: aiosqlite.Connection, session_id: int) -> Optional[dict]:
    """Get a single playground session with run count."""
    rows = await db.execute_fetchall(
        """SELECT s.*, COALESCE(cnt.c, 0) as runs_count
           FROM playground_sessions s
           LEFT JOIN (SELECT session_id, COUNT(*) as c FROM playground_runs GROUP BY session_id) cnt
             ON s.id = cnt.session_id
           WHERE s.id = ?""",
        (session_id,),
    )
    return _row_to_dict(rows[0]) if rows else None


async def update_playground_session(db: aiosqlite.Connection, session_id: int, data: dict) -> Optional[dict]:
    """Update a playground session."""
    rows = await db.execute_fetchall(
        "SELECT * FROM playground_sessions WHERE id = ?", (session_id,)
    )
    if not rows:
        return None
    now = _now()
    sets, vals = ["updated_at = ?"], [now]
    if "name" in data and data["name"] is not None:
        sets.append("name = ?")
        vals.append(data["name"])
    if "description" in data:
        sets.append("description = ?")
        vals.append(data["description"])
    vals.append(session_id)
    await db.execute(
        f"UPDATE playground_sessions SET {', '.join(sets)} WHERE id = ?", vals
    )
    await db.commit()
    return await get_playground_session(db, session_id)


async def delete_playground_session(db: aiosqlite.Connection, session_id: int) -> bool:
    """Delete a playground session and cascade-delete its runs."""
    await db.execute("DELETE FROM playground_runs WHERE session_id = ?", (session_id,))
    cur = await db.execute("DELETE FROM playground_sessions WHERE id = ?", (session_id,))
    await db.commit()
    return cur.rowcount > 0


async def run_playground(db: aiosqlite.Connection, session_id: int, data: dict) -> Optional[dict]:
    """Run a prompt in the playground: compress, check cache, estimate cost, save run."""
    # Verify session exists
    session = await get_playground_session(db, session_id)
    if not session:
        return None

    prompt = data["prompt"]
    profile = data.get("profile", "balanced")
    model = data.get("model")
    do_compress = data.get("compress", True)
    do_cache = data.get("cache_lookup", True)

    # Check cache
    cache_hit = False
    if do_cache:
        cached = await cache_get(db, prompt, model or "default")
        if cached:
            cache_hit = True

    # Compress
    original_tokens = estimate_tokens(prompt)
    compressed_text = prompt
    compressed_tokens = original_tokens
    rules_applied = 0
    ratio = 1.0

    if do_compress:
        comp = await compress(db, prompt, profile)
        compressed_text = comp["compressed"]
        compressed_tokens = comp["compressed_tokens"]
        ratio = comp["ratio"]
        rules_applied = comp["rules_applied"]

    # Estimate cost
    estimated_cost = None
    if model:
        mc = await get_model_cost(db, model)
        if mc:
            estimated_cost = round(compressed_tokens / 1000 * mc["input_cost_per_1k"], 6)

    now = _now()
    cur = await db.execute(
        """INSERT INTO playground_runs
           (session_id, original_tokens, compressed_tokens, compression_ratio,
            estimated_cost, cache_hit, profile_used, rules_applied, compressed_text, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (session_id, original_tokens, compressed_tokens, ratio,
         estimated_cost, int(cache_hit), profile, rules_applied, compressed_text, now),
    )

    # Touch session updated_at
    await db.execute(
        "UPDATE playground_sessions SET updated_at = ? WHERE id = ?", (now, session_id)
    )
    await db.commit()

    return {
        "id": cur.lastrowid,
        "session_id": session_id,
        "original_tokens": original_tokens,
        "compressed_tokens": compressed_tokens,
        "compression_ratio": ratio,
        "estimated_cost": estimated_cost,
        "cache_hit": cache_hit,
        "profile_used": profile,
        "rules_applied": rules_applied,
        "compressed_text": compressed_text,
        "created_at": now,
    }


async def list_playground_runs(db: aiosqlite.Connection, session_id: int, limit: int = 50) -> list[dict]:
    """List runs for a given playground session."""
    rows = await db.execute_fetchall(
        "SELECT * FROM playground_runs WHERE session_id = ? ORDER BY id DESC LIMIT ?",
        (session_id, limit),
    )
    return [_pg_run_dict(r) for r in rows]


async def get_playground_run(db: aiosqlite.Connection, run_id: int) -> Optional[dict]:
    """Get a single playground run."""
    rows = await db.execute_fetchall(
        "SELECT * FROM playground_runs WHERE id = ?", (run_id,)
    )
    return _pg_run_dict(rows[0]) if rows else None


async def delete_playground_run(db: aiosqlite.Connection, run_id: int) -> bool:
    """Delete a single playground run."""
    cur = await db.execute("DELETE FROM playground_runs WHERE id = ?", (run_id,))
    await db.commit()
    return cur.rowcount > 0


def _pg_run_dict(row) -> dict:
    d = _row_to_dict(row)
    d["cache_hit"] = bool(d["cache_hit"])
    return d


# ══════════════════════════════════════════════════════════════════════════════
# NEW v1.0.0: Cost Forecasting
# ══════════════════════════════════════════════════════════════════════════════

async def get_cost_forecast(db: aiosqlite.Connection) -> dict:
    """Compute cost forecast from daily_log, model_costs, and budget."""
    now_dt = datetime.utcnow()
    seven_ago = (now_dt - timedelta(days=7)).strftime("%Y-%m-%d")
    fourteen_ago = (now_dt - timedelta(days=14)).strftime("%Y-%m-%d")

    # Last 7 days
    rows_7 = await db.execute_fetchall(
        "SELECT date, SUM(cost) as cost, SUM(tokens_in + tokens_out) as tokens "
        "FROM daily_log WHERE date >= ? GROUP BY date ORDER BY date",
        (seven_ago,),
    )
    last_7 = [_row_to_dict(r) for r in rows_7]

    # Previous 7 days (days 8-14 ago)
    rows_prev = await db.execute_fetchall(
        "SELECT date, SUM(cost) as cost, SUM(tokens_in + tokens_out) as tokens "
        "FROM daily_log WHERE date >= ? AND date < ? GROUP BY date ORDER BY date",
        (fourteen_ago, seven_ago),
    )
    prev_7 = [_row_to_dict(r) for r in rows_prev]

    # Compute averages
    total_cost_7 = sum(d["cost"] for d in last_7)
    total_tokens_7 = sum(d["tokens"] for d in last_7)
    days_with_data = max(len(last_7), 1)
    daily_avg_cost = round(total_cost_7 / days_with_data, 6)
    daily_avg_tokens = round(total_tokens_7 / days_with_data, 2)

    # Forecasts (linear projection)
    forecast_7d = round(daily_avg_cost * 7, 6)
    forecast_30d = round(daily_avg_cost * 30, 6)

    # Trend: compare last 7d avg vs previous 7d avg
    prev_total_cost = sum(d["cost"] for d in prev_7)
    prev_days = max(len(prev_7), 1)
    prev_daily_avg = prev_total_cost / prev_days if prev_7 else 0.0

    if prev_daily_avg > 0:
        trend_pct = round((daily_avg_cost - prev_daily_avg) / prev_daily_avg * 100, 2)
    else:
        trend_pct = 0.0 if daily_avg_cost == 0 else 100.0

    if trend_pct > 10:
        trend = "increasing"
    elif trend_pct < -10:
        trend = "decreasing"
    else:
        trend = "stable"

    # Budget exhaustion date
    budget = await get_budget(db)
    exhaustion_date = None
    if budget["monthly_limit"] is not None and daily_avg_cost > 0:
        remaining = budget["monthly_remaining"]
        if remaining is not None and remaining > 0:
            days_left = remaining / daily_avg_cost
            exhaustion_dt = now_dt + timedelta(days=days_left)
            exhaustion_date = exhaustion_dt.strftime("%Y-%m-%d")
        elif remaining is not None and remaining <= 0:
            exhaustion_date = _today()  # already exhausted

    # Recommendations
    recommendations = []

    # Check compression usage
    comp_rows = await db.execute_fetchall(
        "SELECT COUNT(*) as c FROM compression_log"
    )
    comp_count = _row_to_dict(comp_rows[0])["c"]
    stats = await get_stats(db)
    total_requests = stats["total_requests"]
    if total_requests > 0 and comp_count < total_requests * 0.5:
        recommendations.append("Enable compression on more requests to reduce token usage")

    # Check cache utilization
    hit_rate = stats["cache_hit_rate"]
    if hit_rate < 0.20 and total_requests > 10:
        recommendations.append("Cache underutilized — consider caching frequent prompts")

    # Check model concentration
    if stats["top_models"]:
        total_model_cost = sum(m.get("cost", 0) for m in stats["top_models"])
        if total_model_cost > 0:
            for m in stats["top_models"]:
                pct = (m.get("cost", 0) / total_model_cost) * 100
                if pct > 60:
                    recommendations.append(
                        f"Consider a cheaper alternative for {m['model']} "
                        f"({pct:.0f}% of total cost)"
                    )
                    break

    if trend == "increasing":
        recommendations.append("Costs are trending upward — review usage patterns")

    if not recommendations:
        recommendations.append("Cost profile looks healthy")

    return {
        "current_daily_avg": daily_avg_cost,
        "current_weekly_total": round(total_cost_7, 6),
        "forecast_7d": forecast_7d,
        "forecast_30d": forecast_30d,
        "burn_rate_tokens_per_day": daily_avg_tokens,
        "burn_rate_cost_per_day": daily_avg_cost,
        "budget_exhaustion_date": exhaustion_date,
        "trend": trend,
        "trend_pct_change": trend_pct,
        "recommendations": recommendations,
    }


async def get_cost_breakdown(db: aiosqlite.Connection, days: int = 30) -> list[dict]:
    """Per-model cost breakdown with trends over the specified period."""
    now_dt = datetime.utcnow()
    cutoff = (now_dt - timedelta(days=days)).strftime("%Y-%m-%d")
    midpoint = (now_dt - timedelta(days=days // 2)).strftime("%Y-%m-%d")

    rows = await db.execute_fetchall(
        """SELECT model,
                  SUM(tokens_in + tokens_out) as total_tokens,
                  SUM(cost) as total_cost
           FROM daily_log WHERE date >= ?
           GROUP BY model ORDER BY total_cost DESC""",
        (cutoff,),
    )
    all_entries = [_row_to_dict(r) for r in rows]

    grand_total_cost = sum(e["total_cost"] for e in all_entries) or 1.0

    result = []
    for entry in all_entries:
        model = entry["model"]
        pct = round(entry["total_cost"] / grand_total_cost * 100, 2)
        avg_daily_tokens = round(entry["total_tokens"] / max(days, 1), 2)

        # Per-model trend: first half vs second half
        first_half = await db.execute_fetchall(
            "SELECT COALESCE(SUM(cost), 0) as c FROM daily_log WHERE date >= ? AND date < ? AND model = ?",
            (cutoff, midpoint, model),
        )
        second_half = await db.execute_fetchall(
            "SELECT COALESCE(SUM(cost), 0) as c FROM daily_log WHERE date >= ? AND model = ?",
            (midpoint, model),
        )
        first_cost = _row_to_dict(first_half[0])["c"]
        second_cost = _row_to_dict(second_half[0])["c"]

        if first_cost > 0:
            change = (second_cost - first_cost) / first_cost * 100
        else:
            change = 0.0 if second_cost == 0 else 100.0

        if change > 10:
            model_trend = "increasing"
        elif change < -10:
            model_trend = "decreasing"
        else:
            model_trend = "stable"

        result.append({
            "model": model,
            "total_tokens": entry["total_tokens"],
            "total_cost": round(entry["total_cost"], 6),
            "pct_of_total": pct,
            "avg_daily_tokens": avg_daily_tokens,
            "trend": model_trend,
        })

    return result


# ══════════════════════════════════════════════════════════════════════════════
# NEW v1.0.0: Compression Chains
# ══════════════════════════════════════════════════════════════════════════════

async def create_chain(db: aiosqlite.Connection, data: dict) -> dict:
    """Create a compression chain. Validates all profile names exist and min 2 steps."""
    steps = data["steps"]
    if len(steps) < 2:
        raise ValueError("A chain requires at least 2 steps")

    # Validate all profiles exist
    for p in steps:
        profile = await get_profile(db, p)
        if not profile:
            raise ValueError(f"Profile '{p}' not found")

    now = _now()
    cur = await db.execute(
        "INSERT INTO compression_chains (name, description, created_at) VALUES (?, ?, ?)",
        (data["name"], data.get("description"), now),
    )
    chain_id = cur.lastrowid

    for idx, profile_name in enumerate(steps):
        await db.execute(
            "INSERT INTO chain_steps (chain_id, step_order, profile_name) VALUES (?, ?, ?)",
            (chain_id, idx, profile_name),
        )

    await db.commit()
    return await get_chain(db, chain_id)


async def list_chains(db: aiosqlite.Connection) -> list[dict]:
    """List all compression chains."""
    rows = await db.execute_fetchall(
        "SELECT * FROM compression_chains ORDER BY name"
    )
    result = []
    for r in rows:
        chain = _row_to_dict(r)
        steps = await _get_chain_steps(db, chain["id"])
        chain["steps"] = steps
        result.append(chain)
    return result


async def get_chain(db: aiosqlite.Connection, chain_id: int) -> Optional[dict]:
    """Get a single compression chain by ID."""
    rows = await db.execute_fetchall(
        "SELECT * FROM compression_chains WHERE id = ?", (chain_id,)
    )
    if not rows:
        return None
    chain = _row_to_dict(rows[0])
    chain["steps"] = await _get_chain_steps(db, chain_id)
    return chain


async def update_chain(db: aiosqlite.Connection, chain_id: int, data: dict) -> Optional[dict]:
    """Update a compression chain."""
    rows = await db.execute_fetchall(
        "SELECT * FROM compression_chains WHERE id = ?", (chain_id,)
    )
    if not rows:
        return None

    sets, vals = [], []
    if "name" in data and data["name"] is not None:
        sets.append("name = ?")
        vals.append(data["name"])
    if "description" in data:
        sets.append("description = ?")
        vals.append(data["description"])

    if sets:
        vals.append(chain_id)
        await db.execute(
            f"UPDATE compression_chains SET {', '.join(sets)} WHERE id = ?", vals
        )

    # Update steps if provided
    if "steps" in data and data["steps"] is not None:
        steps = data["steps"]
        if len(steps) < 2:
            raise ValueError("A chain requires at least 2 steps")
        for p in steps:
            profile = await get_profile(db, p)
            if not profile:
                raise ValueError(f"Profile '{p}' not found")
        # Replace steps
        await db.execute("DELETE FROM chain_steps WHERE chain_id = ?", (chain_id,))
        for idx, profile_name in enumerate(steps):
            await db.execute(
                "INSERT INTO chain_steps (chain_id, step_order, profile_name) VALUES (?, ?, ?)",
                (chain_id, idx, profile_name),
            )

    await db.commit()
    return await get_chain(db, chain_id)


async def delete_chain(db: aiosqlite.Connection, chain_id: int) -> bool:
    """Delete a compression chain and its steps."""
    await db.execute("DELETE FROM chain_steps WHERE chain_id = ?", (chain_id,))
    cur = await db.execute("DELETE FROM compression_chains WHERE id = ?", (chain_id,))
    await db.commit()
    return cur.rowcount > 0


async def run_chain(db: aiosqlite.Connection, chain_id: int, prompt: str) -> Optional[dict]:
    """Apply a compression chain sequentially. Each step compresses the output of the previous step."""
    chain = await get_chain(db, chain_id)
    if not chain:
        return None

    original_tokens = estimate_tokens(prompt)
    current_text = prompt
    step_results = []
    total_rules_applied = 0

    for profile_name in chain["steps"]:
        input_tokens = estimate_tokens(current_text)
        comp = await compress(db, current_text, profile_name)
        output_tokens = comp["compressed_tokens"]
        ratio = comp["ratio"]
        total_rules_applied += comp["rules_applied"]

        step_results.append({
            "profile": profile_name,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "ratio": ratio,
        })
        current_text = comp["compressed"]

    final_tokens = estimate_tokens(current_text)
    final_ratio = round(final_tokens / original_tokens, 4) if original_tokens > 0 else 1.0

    # Update chain stats
    await db.execute(
        "UPDATE compression_chains SET times_used = times_used + 1 WHERE id = ?",
        (chain_id,),
    )
    # Update running average of final_ratio
    chain_fresh = await get_chain(db, chain_id)
    times_used = chain_fresh["times_used"]
    old_avg = chain_fresh["avg_final_ratio"] or final_ratio
    new_avg = round(((old_avg * (times_used - 1)) + final_ratio) / times_used, 4)
    await db.execute(
        "UPDATE compression_chains SET avg_final_ratio = ? WHERE id = ?",
        (new_avg, chain_id),
    )
    await db.commit()

    return {
        "chain_id": chain_id,
        "chain_name": chain["name"],
        "original_tokens": original_tokens,
        "final_tokens": final_tokens,
        "final_ratio": final_ratio,
        "step_results": step_results,
        "total_rules_applied": total_rules_applied,
    }


async def find_optimal_chain(db: aiosqlite.Connection, prompt: str, max_steps: int = 3) -> dict:
    """Try all permutations of profiles (up to max_steps) and return the best chain by compression ratio."""
    profiles = await list_profiles(db)
    profile_names = [p["name"] for p in profiles]
    original_tokens = estimate_tokens(prompt)

    best: Optional[dict] = None

    # Try permutations of length 2..max_steps
    for length in range(2, min(max_steps, len(profile_names)) + 1):
        for perm in itertools.permutations(profile_names, length):
            current_text = prompt
            step_results = []
            total_rules = 0

            for profile_name in perm:
                input_tokens = estimate_tokens(current_text)
                comp = await compress(db, current_text, profile_name)
                step_results.append({
                    "profile": profile_name,
                    "input_tokens": input_tokens,
                    "output_tokens": comp["compressed_tokens"],
                    "ratio": comp["ratio"],
                })
                total_rules += comp["rules_applied"]
                current_text = comp["compressed"]

            final_tokens = estimate_tokens(current_text)
            final_ratio = round(final_tokens / original_tokens, 4) if original_tokens > 0 else 1.0

            candidate = {
                "chain_name": " -> ".join(perm),
                "steps": list(perm),
                "original_tokens": original_tokens,
                "final_tokens": final_tokens,
                "final_ratio": final_ratio,
                "step_results": step_results,
                "total_rules_applied": total_rules,
            }

            if best is None or final_ratio < best["final_ratio"]:
                best = candidate

    if best is None:
        # Fallback: not enough profiles
        return {
            "chain_name": "none",
            "steps": [],
            "original_tokens": original_tokens,
            "final_tokens": original_tokens,
            "final_ratio": 1.0,
            "step_results": [],
            "total_rules_applied": 0,
        }

    return best


async def _get_chain_steps(db: aiosqlite.Connection, chain_id: int) -> list[str]:
    """Retrieve ordered step profile names for a chain."""
    rows = await db.execute_fetchall(
        "SELECT profile_name FROM chain_steps WHERE chain_id = ? ORDER BY step_order",
        (chain_id,),
    )
    return [_row_to_dict(r)["profile_name"] for r in rows]


# --- v1.1.0 new functions below ---


# ── Usage Heatmap ─────────────────────────────────────────────────────────────


async def record_heatmap(db, model: str, tokens: int, cost_usd: float):
    """Record a usage data point for the heatmap (hour × date × model)."""
    now = datetime.utcnow()
    hour = now.hour
    date = now.strftime("%Y-%m-%d")
    r = await db.execute(
        "SELECT id, requests, tokens, cost_usd FROM usage_heatmap "
        "WHERE hour=? AND date=? AND model=?",
        (hour, date, model),
    )
    existing = await r.fetchone()
    if existing:
        await db.execute(
            "UPDATE usage_heatmap SET requests=requests+1, tokens=tokens+?, "
            "cost_usd=cost_usd+? WHERE id=?",
            (tokens, cost_usd, existing["id"]),
        )
    else:
        await db.execute(
            "INSERT INTO usage_heatmap (hour, date, model, requests, tokens, cost_usd) "
            "VALUES (?,?,?,1,?,?)",
            (hour, date, model, tokens, cost_usd),
        )
    await db.commit()


async def get_usage_heatmap(db, days: int = 7, model: str = None):
    """Return heatmap cells with peak/distribution metadata."""
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    if model:
        r = await db.execute(
            "SELECT * FROM usage_heatmap WHERE date >= ? AND model=? ORDER BY date, hour",
            (cutoff, model),
        )
    else:
        r = await db.execute(
            "SELECT * FROM usage_heatmap WHERE date >= ? ORDER BY date, hour",
            (cutoff,),
        )
    rows = await r.fetchall()
    cells = [
        {
            "hour": row["hour"],
            "day": row["date"],
            "requests": row["requests"],
            "tokens": row["tokens"],
            "cost_usd": round(row["cost_usd"], 6),
        }
        for row in rows
    ]
    peak_hour, peak_day = 0, ""
    max_requests = 0
    total_requests = 0
    model_dist: dict[str, int] = {}
    for row in rows:
        total_requests += row["requests"]
        if row["requests"] > max_requests:
            max_requests = row["requests"]
            peak_hour = row["hour"]
            peak_day = row["date"]
        m = row["model"]
        model_dist[m] = model_dist.get(m, 0) + row["requests"]
    return {
        "cells": cells,
        "peak_hour": peak_hour,
        "peak_day": peak_day,
        "total_requests": total_requests,
        "model_distribution": model_dist,
    }


async def get_peak_analysis(db, days: int = 7):
    """Analyse peak vs quiet hours over the given window."""
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    r = await db.execute(
        "SELECT hour, SUM(requests) as total FROM usage_heatmap "
        "WHERE date >= ? GROUP BY hour ORDER BY total DESC",
        (cutoff,),
    )
    rows = await r.fetchall()
    hourly = {row["hour"]: row["total"] for row in rows}  # noqa: F841
    peak_hours = [{"hour": row["hour"], "requests": row["total"]} for row in rows[:3]]
    quiet_hours = (
        [{"hour": row["hour"], "requests": row["total"]} for row in rows[-3:]]
        if len(rows) >= 3
        else []
    )
    if peak_hours:
        rec = (
            f"Peak usage at hour {peak_hours[0]['hour']}:00 UTC. "
            "Consider scheduling batch jobs during quiet hours."
        )
    else:
        rec = "Not enough data for analysis."
    return {
        "peak_hours": peak_hours,
        "quiet_hours": quiet_hours,
        "recommendation": rec,
    }


# ── Prompt Versioning ─────────────────────────────────────────────────────────


async def create_prompt_version(db, data):
    """Create the first version of a named prompt."""
    from compressor import estimate_tokens

    now = _now()
    token_count = estimate_tokens(data["prompt_text"])
    tags = json.dumps(data.get("tags", []))
    r = await db.execute(
        "INSERT INTO prompt_versions "
        "(name, version, prompt_text, model, tags, notes, token_count, created_at) "
        "VALUES (?,1,?,?,?,?,?,?)",
        (
            data["name"],
            data["prompt_text"],
            data.get("model"),
            tags,
            data.get("notes"),
            token_count,
            now,
        ),
    )
    await db.commit()
    return await get_prompt_version(db, r.lastrowid)


async def list_prompt_versions(db, name=None, limit=50, offset=0):
    """List prompt versions — all versions for a name, or latest of each."""
    if name:
        r = await db.execute(
            "SELECT * FROM prompt_versions WHERE name=? "
            "ORDER BY version DESC LIMIT ? OFFSET ?",
            (name, limit, offset),
        )
    else:
        # Get latest version of each name
        r = await db.execute(
            """
            SELECT pv.* FROM prompt_versions pv
            INNER JOIN (
                SELECT name, MAX(version) as max_v
                FROM prompt_versions GROUP BY name
            ) latest
            ON pv.name = latest.name AND pv.version = latest.max_v
            ORDER BY pv.created_at DESC LIMIT ? OFFSET ?
            """,
            (limit, offset),
        )
    return [_prompt_version_row(row) for row in await r.fetchall()]


async def get_prompt_version(db, prompt_id):
    """Fetch a single prompt version by ID."""
    r = await db.execute("SELECT * FROM prompt_versions WHERE id=?", (prompt_id,))
    row = await r.fetchone()
    return _prompt_version_row(row) if row else None


async def update_prompt_version(db, prompt_id, **kwargs):
    """Create a new version of an existing prompt (immutable versioning)."""
    from compressor import estimate_tokens

    current = await get_prompt_version(db, prompt_id)
    if not current:
        return None
    new_text = kwargs.get("prompt_text", current["prompt_text"])
    new_tags = json.dumps(kwargs.get("tags", current["tags"]))
    new_notes = kwargs.get("notes", current["notes"])
    token_count = estimate_tokens(new_text)
    now = _now()
    max_r = await db.execute(
        "SELECT MAX(version) FROM prompt_versions WHERE name=?",
        (current["name"],),
    )
    max_v = (await max_r.fetchone())[0] or 0
    r = await db.execute(
        "INSERT INTO prompt_versions "
        "(name, version, prompt_text, model, tags, notes, token_count, created_at) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (
            current["name"],
            max_v + 1,
            new_text,
            current["model"],
            new_tags,
            new_notes,
            token_count,
            now,
        ),
    )
    await db.commit()
    return await get_prompt_version(db, r.lastrowid)


async def delete_prompt_version(db, prompt_id):
    """Delete a single prompt version by ID."""
    r = await db.execute("DELETE FROM prompt_versions WHERE id=?", (prompt_id,))
    await db.commit()
    return r.rowcount > 0


async def list_prompt_history(db, prompt_id):
    """Return full version history for the prompt that owns *prompt_id*."""
    current = await get_prompt_version(db, prompt_id)
    if not current:
        return None
    r = await db.execute(
        "SELECT * FROM prompt_versions WHERE name=? ORDER BY version ASC",
        (current["name"],),
    )
    return [_prompt_version_row(row) for row in await r.fetchall()]


async def diff_prompt_versions(db, version_a_id, version_b_id):
    """Unified diff between two prompt versions."""
    a = await get_prompt_version(db, version_a_id)
    b = await get_prompt_version(db, version_b_id)
    if not a or not b:
        return None
    import difflib

    diff_lines = list(
        difflib.unified_diff(
            a["prompt_text"].splitlines(keepends=True),
            b["prompt_text"].splitlines(keepends=True),
            fromfile=f"v{a['version']}",
            tofile=f"v{b['version']}",
        )
    )
    return {
        "version_a": a["version"],
        "version_b": b["version"],
        "token_diff": b["token_count"] - a["token_count"],
        "text_diff": "".join(diff_lines),
    }


async def use_prompt_version(db, prompt_id):
    """Increment the usage counter for a prompt version."""
    pv = await get_prompt_version(db, prompt_id)
    if not pv:
        return None
    await db.execute(
        "UPDATE prompt_versions SET times_used=times_used+1 WHERE id=?",
        (prompt_id,),
    )
    await db.commit()
    return await get_prompt_version(db, prompt_id)


# ── Cost Allocation Tags ─────────────────────────────────────────────────────


async def create_cost_tag(db, data):
    """Create a new cost-allocation tag (unique name enforced)."""
    now = _now()
    try:
        r = await db.execute(
            "INSERT INTO cost_tags (tag, description, budget_usd, created_at) "
            "VALUES (?,?,?,?)",
            (data["tag"], data.get("description"), data.get("budget_usd"), now),
        )
        await db.commit()
        return await get_cost_tag(db, r.lastrowid)
    except Exception:
        raise ValueError(f"Tag '{data['tag']}' already exists")


async def list_cost_tags(db):
    """List all cost tags with aggregated spend info."""
    r = await db.execute("SELECT * FROM cost_tags ORDER BY tag ASC")
    results = []
    for row in await r.fetchall():
        tag = _cost_tag_row(row)
        agg = await db.execute(
            "SELECT COUNT(*) as cnt, COALESCE(SUM(cost_usd),0) as total "
            "FROM cost_tag_usage WHERE tag_id=?",
            (row["id"],),
        )
        agg_row = await agg.fetchone()
        tag["total_spent"] = round(agg_row["total"], 6)
        tag["request_count"] = agg_row["cnt"]
        results.append(tag)
    return results


async def get_cost_tag(db, tag_id):
    """Fetch a cost tag by ID with aggregated usage."""
    r = await db.execute("SELECT * FROM cost_tags WHERE id=?", (tag_id,))
    row = await r.fetchone()
    if not row:
        return None
    tag = _cost_tag_row(row)
    agg = await db.execute(
        "SELECT COUNT(*) as cnt, COALESCE(SUM(cost_usd),0) as total "
        "FROM cost_tag_usage WHERE tag_id=?",
        (tag_id,),
    )
    agg_row = await agg.fetchone()
    tag["total_spent"] = round(agg_row["total"], 6)
    tag["request_count"] = agg_row["cnt"]
    return tag


async def update_cost_tag(db, tag_id, **kwargs):
    """Update mutable fields (description, budget) on a cost tag."""
    tag = await get_cost_tag(db, tag_id)
    if not tag:
        return None
    sets, vals = [], []
    if "description" in kwargs:
        sets.append("description=?")
        vals.append(kwargs["description"])
    if "budget_usd" in kwargs:
        sets.append("budget_usd=?")
        vals.append(kwargs["budget_usd"])
    if not sets:
        return tag
    vals.append(tag_id)
    await db.execute(
        f"UPDATE cost_tags SET {','.join(sets)} WHERE id=?", vals
    )
    await db.commit()
    return await get_cost_tag(db, tag_id)


async def delete_cost_tag(db, tag_id):
    """Delete a cost tag and its usage records."""
    await db.execute("DELETE FROM cost_tag_usage WHERE tag_id=?", (tag_id,))
    r = await db.execute("DELETE FROM cost_tags WHERE id=?", (tag_id,))
    await db.commit()
    return r.rowcount > 0


async def allocate_cost(db, tag_id, compression_id):
    """Link a compression-log entry's cost to a tag."""
    tag = await get_cost_tag(db, tag_id)
    if not tag:
        return None
    r = await db.execute(
        "SELECT * FROM compression_log WHERE id=?", (compression_id,)
    )
    comp = await r.fetchone()
    if not comp:
        return None
    now = _now()
    cost = comp["original_cost"] if comp["original_cost"] else 0.0
    model = comp["model"] if "model" in comp.keys() else "unknown"
    await db.execute(
        "INSERT INTO cost_tag_usage "
        "(tag_id, compression_id, cost_usd, model, created_at) "
        "VALUES (?,?,?,?,?)",
        (tag_id, compression_id, cost, model, now),
    )
    await db.commit()
    return await get_cost_tag(db, tag_id)


async def get_cost_tag_breakdown(db, from_date=None, to_date=None):
    """Full breakdown of spend by tag, including untagged remainder."""
    tags = await db.execute("SELECT * FROM cost_tags ORDER BY tag")
    tag_rows = await tags.fetchall()
    breakdowns = []
    total_tagged = 0.0
    for row in tag_rows:
        clauses = ["tag_id=?"]
        vals: list[Any] = [row["id"]]
        if from_date:
            clauses.append("created_at >= ?")
            vals.append(from_date)
        if to_date:
            clauses.append("created_at <= ?")
            vals.append(to_date)
        where = " AND ".join(clauses)
        agg = await db.execute(
            f"SELECT COUNT(*) as cnt, COALESCE(SUM(cost_usd),0) as total "
            f"FROM cost_tag_usage WHERE {where}",
            vals,
        )
        agg_row = await agg.fetchone()
        total_cost = round(agg_row["total"], 6)
        total_tagged += total_cost
        req_count = agg_row["cnt"]
        # top models
        models_r = await db.execute(
            f"SELECT model, COUNT(*) as cnt, SUM(cost_usd) as cost "
            f"FROM cost_tag_usage WHERE {where} "
            f"GROUP BY model ORDER BY cost DESC LIMIT 5",
            vals,
        )
        top_models = [
            {"model": m["model"], "count": m["cnt"], "cost": round(m["cost"], 6)}
            for m in await models_r.fetchall()
        ]
        bd: dict[str, Any] = {
            "tag": row["tag"],
            "total_cost": total_cost,
            "request_count": req_count,
            "avg_cost_per_request": round(total_cost / max(1, req_count), 6),
            "top_models": top_models,
            "budget_usd": row["budget_usd"],
        }
        if row["budget_usd"]:
            bd["budget_remaining"] = round(row["budget_usd"] - total_cost, 6)
            bd["pct_used"] = (
                round(total_cost / row["budget_usd"] * 100, 1)
                if row["budget_usd"] > 0
                else 0.0
            )
        else:
            bd["budget_remaining"] = None
            bd["pct_used"] = None
        breakdowns.append(bd)
    # untagged
    total_r = await db.execute(
        "SELECT COALESCE(SUM(original_cost),0) as total FROM compression_log"
    )
    total_all = round((await total_r.fetchone())["total"], 6)
    return {
        "tags": breakdowns,
        "untagged_cost": round(total_all - total_tagged, 6),
        "total_cost": total_all,
    }
