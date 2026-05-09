"""JSON repair pipeline: PatternFixer (regex) → LLMFixer (LLM call)."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from publiminer.core.openrouter import OpenRouterClient


@dataclass
class RepairResult:
    """Outcome of one or more repair attempts."""

    success: bool
    content: str
    fix_applied: str | None = None
    fix_history: list[dict[str, Any]] = field(default_factory=list)
    error_label: str | None = None


# ---------------------------------------------------------------------------
# PatternFixer
# ---------------------------------------------------------------------------

class PatternFixer:
    """Apply a sequence of regex/string transformations to recover valid JSON."""

    _OPERATIONS: list[tuple[str, Any]] = [
        ("strip_markdown_fences", None),
        ("extract_json_object", None),
        ("strip_special_chars", None),
        ("fix_smart_quotes", None),
        ("fix_trailing_commas", None),
        ("fix_literal_newlines", None),
        ("fix_single_quoted_keys", None),
    ]

    def fix(self, raw: str) -> RepairResult:
        history: list[dict[str, Any]] = []
        current = raw

        # Try original first
        try:
            json.loads(current)
            return RepairResult(success=True, content=current, fix_applied=None, fix_history=[])
        except json.JSONDecodeError:
            pass

        for op_name, _ in self._OPERATIONS:
            fn = getattr(self, f"_{op_name}")
            transformed = fn(current)
            try:
                json.loads(transformed)
                history.append({"step": op_name, "success": True})
                return RepairResult(
                    success=True,
                    content=transformed,
                    fix_applied="pattern",
                    fix_history=history,
                )
            except json.JSONDecodeError:
                history.append({"step": op_name, "success": False})
                current = transformed  # carry forward each transformation cumulatively

        return RepairResult(
            success=False,
            content=raw,
            fix_applied=None,
            fix_history=history,
            error_label="pattern_fix_failed",
        )

    @staticmethod
    def _strip_markdown_fences(text: str) -> str:
        text = re.sub(r"^```(?:json)?\s*\n?", "", text, flags=re.MULTILINE)
        text = re.sub(r"\n?```\s*$", "", text, flags=re.MULTILINE)
        return text.strip()

    @staticmethod
    def _extract_json_object(text: str) -> str:
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            return text[start : end + 1]
        return text

    @staticmethod
    def _strip_special_chars(text: str) -> str:
        # Remove BOM and zero-width characters
        return text.lstrip("﻿").translate(
            str.maketrans("", "", "​‌‍⁠﻿")
        )

    @staticmethod
    def _fix_smart_quotes(text: str) -> str:
        return (
            text.replace("“", '"')
            .replace("”", '"')
            .replace("‘", "'")
            .replace("’", "'")
        )

    @staticmethod
    def _fix_trailing_commas(text: str) -> str:
        return re.sub(r",\s*([}\]])", r"\1", text)

    @staticmethod
    def _fix_literal_newlines(text: str) -> str:
        # Replace unescaped literal newlines inside JSON strings
        def replace_newlines(m: re.Match[str]) -> str:
            return m.group(0).replace("\n", "\\n").replace("\r", "\\r")

        return re.sub(r'"[^"\\]*(?:\\.[^"\\]*)*"', replace_newlines, text, flags=re.DOTALL)

    @staticmethod
    def _fix_single_quoted_keys(text: str) -> str:
        return re.sub(r"(?<![\\])'([^']+)'(?=\s*:)", r'"\1"', text)


# ---------------------------------------------------------------------------
# LLMFixer
# ---------------------------------------------------------------------------

class LLMFixer:
    """Ask an LLM to repair malformed JSON."""

    def __init__(self, client: OpenRouterClient, fix_model: str) -> None:
        self._client = client
        self._fix_model = fix_model

    async def fix(self, raw: str, schema_dict: dict[str, Any]) -> RepairResult:
        repaired = await self._client.fix_json(raw, schema_dict, self._fix_model)
        try:
            parsed = json.loads(repaired)
        except json.JSONDecodeError:
            return RepairResult(
                success=False,
                content=raw,
                fix_applied=None,
                fix_history=[{"step": "llm_fix", "success": False}],
                error_label="llm_fix_failed",
            )
        if "_unrecoverable" in parsed:
            return RepairResult(
                success=False,
                content=raw,
                fix_applied=None,
                fix_history=[{"step": "llm_fix", "success": False}],
                error_label="unrecoverable",
            )
        return RepairResult(
            success=True,
            content=repaired,
            fix_applied="llm",
            fix_history=[{"step": "llm_fix", "success": True}],
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def repair(
    raw: str,
    schema_dict: dict[str, Any],
    pattern_fix: bool,
    llm_fix: bool,
    client: OpenRouterClient,
    fix_model: str,
) -> RepairResult:
    """Run the repair chain: PatternFixer → LLMFixer based on config flags."""
    result = RepairResult(success=False, content=raw, error_label="no_repair_attempted")

    if pattern_fix:
        result = PatternFixer().fix(raw)
        if result.success:
            return result

    if llm_fix:
        llm_result = await LLMFixer(client, fix_model).fix(raw, schema_dict)
        history = result.fix_history + llm_result.fix_history
        if llm_result.success:
            return RepairResult(
                success=True,
                content=llm_result.content,
                fix_applied="pattern+llm" if pattern_fix else "llm",
                fix_history=history,
            )
        return RepairResult(
            success=False,
            content=raw,
            fix_applied=None,
            fix_history=history,
            error_label=llm_result.error_label,
        )

    return result
