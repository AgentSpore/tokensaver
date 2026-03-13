from __future__ import annotations
import re


def estimate_tokens(text: str) -> int:
    """Rough token estimate: ~4 chars per token (GPT-style)."""
    return max(1, len(text) // 4)


def apply_custom_rules(text: str, rules: list[dict]) -> tuple[str, int]:
    """Apply user-defined regex compression rules sorted by priority.
    Returns (transformed_text, rules_applied_count).
    """
    applied = 0
    sorted_rules = sorted(rules, key=lambda r: r["priority"])
    for rule in sorted_rules:
        try:
            new_text = re.sub(rule["pattern"], rule["replacement"], text, flags=re.IGNORECASE)
            if new_text != text:
                applied += 1
                text = new_text
        except re.error:
            continue
    return text, applied


def compress_prompt(
    prompt: str,
    max_ratio: float = 0.5,
    preserve_code: bool = True,
    strip_examples: bool = False,
    strip_comments: bool = False,
    custom_rules: list[dict] | None = None,
) -> tuple[str, int]:
    """
    Lightweight prompt compressor that:
    1. Strips redundant whitespace
    2. Removes verbose filler phrases
    3. Optionally strips examples and comments
    4. Applies custom compression rules
    5. Condenses repeated patterns
    6. Abbreviates common instructions

    Returns (compressed_text, rules_applied_count).
    """
    rules_applied = 0

    # Step 0: preserve code blocks
    code_blocks: list[str] = []
    if preserve_code:
        def _store_code(m: re.Match) -> str:
            code_blocks.append(m.group(0))
            return f"__CODE_{len(code_blocks) - 1}__"
        prompt = re.sub(r"```[\s\S]*?```", _store_code, prompt)

    # Step 1: strip examples (e.g. "Example:", "For example, ...", "e.g., ...")
    if strip_examples:
        prompt = re.sub(
            r"(?:^|\n)\s*(?:example|for example|e\.g\.)[:\s].*?(?=\n\S|\n\n|\Z)",
            "", prompt, flags=re.IGNORECASE | re.DOTALL,
        )

    # Step 2: strip code comments (// and # style)
    if strip_comments:
        prompt = re.sub(r"(?m)^\s*(?://|#)\s.*$", "", prompt)
        prompt = re.sub(r"\s+(?://|#)\s.*$", "", prompt, flags=re.MULTILINE)

    # Step 2.5: apply custom compression rules
    if custom_rules:
        prompt, rules_applied = apply_custom_rules(prompt, custom_rules)

    # Step 3: normalise whitespace
    compressed = re.sub(r"[ \t]+", " ", prompt)
    compressed = re.sub(r"\n{3,}", "\n\n", compressed)

    # Step 4: remove filler phrases
    fillers = [
        r"\bplease\b\s*",
        r"\bcould you\b\s*(please)?\s*",
        r"\bi would like you to\b\s*",
        r"\bas an ai language model[,.]?\s*",
        r"\bfeel free to\b\s*",
        r"\bit is important to note that\b\s*",
        r"\bin other words[,.]?\s*",
        r"\bbasically[,.]?\s*",
        r"\bessentially[,.]?\s*",
        r"\bto be honest[,.]?\s*",
        r"\bof course[,.]?\s*",
        r"\bcertainly[,.]?\s*",
    ]
    for pattern in fillers:
        compressed = re.sub(pattern, "", compressed, flags=re.IGNORECASE)

    # Step 5: condense repeated whitespace again after removal
    compressed = re.sub(r"[ \t]+", " ", compressed).strip()

    # Step 6: if still above ratio, truncate from middle (keep start + end context)
    original_tokens = estimate_tokens(prompt)
    target_tokens = int(original_tokens * max_ratio)
    current_tokens = estimate_tokens(compressed)
    if current_tokens > target_tokens:
        chars_target = target_tokens * 4
        keep_start = int(chars_target * 0.6)
        keep_end = int(chars_target * 0.4)
        if keep_start + keep_end < len(compressed):
            compressed = compressed[:keep_start] + " [...] " + compressed[-keep_end:]

    # Restore code blocks
    for i, block in enumerate(code_blocks):
        compressed = compressed.replace(f"__CODE_{i}__", block)

    return compressed, rules_applied
