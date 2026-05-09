"""Build OpenRouter messages for LLM extraction."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from publiminer.steps.extract.schema import ExtractConfig

SYSTEM_PROMPT = """\
You are an expert biomedical data extractor. Your task is structured information extraction from scientific papers.

EXTRACTION RULES — apply to every field, every paper:
1. Extract only what is explicitly stated. Never infer, extrapolate, or fill gaps from background knowledge.
2. Each field is independent — one field's conclusion must not anchor judgment in another.
3. Ambiguous text: choose the most defensible literal interpretation a careful reader would defend if challenged.
4. Missing, incomplete, or genuinely unclear information: return null. A null is always preferable to a guess.
5. Apply identical standards across all fields. If you would use null in one case, use null in all comparable cases."""


def build_system_message(user_instruction: str) -> dict[str, str]:
    """Return system message dict, optionally appending domain context."""
    content = SYSTEM_PROMPT
    if user_instruction and user_instruction.strip():
        content += f"\n\nDOMAIN CONTEXT:\n{user_instruction.strip()}"
    return {"role": "system", "content": content}


def build_user_message(
    paper_row: dict[str, Any],
    author_block: str,
    config: ExtractConfig,
) -> dict[str, str]:
    """Assemble the user message from title, abstract, extra columns, and author block."""
    parts: list[str] = ["PAPER:"]

    if config.include_title:
        title = (paper_row.get("title") or "").strip()
        if title:
            parts.append(f"Title: {title}")

    if config.include_abstract:
        abstract = (paper_row.get("abstract") or "").strip()
        if abstract:
            parts.append(f"Abstract: {abstract}")

    for col in config.extra_columns:
        val = paper_row.get(col)
        if val is not None:
            parts.append(f"{col}: {val}")

    if author_block:
        parts.append(f"\nAuthors:\n{author_block}")

    parts.append("\nExtract all fields from this paper according to the schema.")

    return {"role": "user", "content": "\n".join(parts)}


def build_messages(
    paper_row: dict[str, Any],
    author_block: str,
    config: ExtractConfig,
) -> list[dict[str, str]]:
    """Return [system_msg, user_msg] ready for OpenRouter."""
    return [
        build_system_message(config.user_instruction),
        build_user_message(paper_row, author_block, config),
    ]
