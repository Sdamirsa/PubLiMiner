"""Build a human-readable author block from parsed authors JSON.

The parse step stores each author as:
  {"last_name": str, "first_name": str, "initials": str, "affiliation": str,
   "is_corresponding": bool (optional), "equal_contribution": bool (optional)}

Corresponding author is detected via the structured is_corresponding flag (set by the
parse step from the "Electronic address:" PubMed marker) or by '*' in the affiliation
string (fallback for legacy records where the flag was not captured).
"""

from __future__ import annotations

import json
from typing import Any

_ROLE_WIDTH = 24  # column width for role label


def build_author_block(authors_json_str: str | None) -> str:
    """Return formatted author block string, or '' if input is null/empty/invalid."""
    if not authors_json_str:
        return ""
    try:
        authors: list[dict[str, Any]] = json.loads(authors_json_str)
    except Exception:
        return ""
    if not authors or not isinstance(authors, list):
        return ""

    roles = detect_roles(authors)
    lines: list[str] = []

    for author in roles["first"]:
        lines.append(format_author_line("First author:", author))
    for author in roles["corresponding"]:
        lines.append(format_author_line("Corresponding (*):", author))
    for author in roles["last"]:
        lines.append(format_author_line("Last author:", author))

    return "\n".join(lines)


def detect_roles(authors: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Classify authors into roles. One author may appear in multiple lists."""
    result: dict[str, list[dict[str, Any]]] = {
        "first": [],
        "last": [],
        "corresponding": [],
    }
    if not authors:
        return result

    result["first"] = [authors[0]]
    result["last"] = [authors[-1]] if len(authors) > 1 else []

    for author in authors:
        aff = author.get("affiliation", "") or ""
        name = author.get("last_name", "") or ""
        # Prefer structured flag from parse step; fall back to asterisk heuristic
        if author.get("is_corresponding") or "*" in aff or "*" in name:
            result["corresponding"].append(author)

    return result


def format_author_name(author: dict[str, Any]) -> str:
    """Return 'Smith J' format."""
    last = author.get("last_name", "").strip()
    initials = author.get("initials", "").strip()
    if last and initials:
        return f"{last} {initials}"
    return last or author.get("first_name", "").strip() or "Unknown"


def format_author_line(role_label: str, author: dict[str, Any]) -> str:
    """Return a padded role line: 'First author:         Smith J — Dept, City, Country'"""
    name = format_author_name(author)
    aff = (author.get("affiliation", "") or "").strip()
    label = role_label.ljust(_ROLE_WIDTH)
    if aff:
        return f"{label}{name} — {aff}"
    return f"{label}{name}"
