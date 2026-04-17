"""First-run setup wizard — captures email + NCBI key, scaffolds config.

The wizard is the single source of truth for what counts as a "complete"
PubLiMiner environment. Both the CLI (`publiminer setup`) and the Streamlit
UI's first-run panel call into this module.

Design:
- `.env` lives in the user's current working directory. That is the same
  directory Streamlit is launched from, so writes and reads stay in sync
  across CLI ↔ UI.
- Writes go through ``dotenv.set_key`` which uses a temp file + os.replace
  under the hood. Safe for NTFS and POSIX alike.
- No masking of the NCBI key at rest — `.env` is a plain-text config,
  marked as such. We defend it by ensuring it is in ``.gitignore``, not
  by trying to encrypt it (that's a false-security trap on Windows where
  there's no POSIX-style 0600 file mode).
- Re-running the wizard is idempotent. Existing values are shown masked,
  the user can press enter to keep them or type to overwrite.

Why not just use ``input()`` + hardcoded logic: typer/rich give us
cross-platform hidden input (via ``click`` → ``getpass`` → msvcrt on
Windows, termios on POSIX) and styled output for free. No new deps.
"""

from __future__ import annotations

import os
from importlib import resources
from pathlib import Path

import typer
from dotenv import dotenv_values, set_key
from rich.console import Console
from rich.panel import Panel

NCBI_REGISTRATION_URL = "https://account.ncbi.nlm.nih.gov/settings/"
ENV_BYPASS_VAR = "PUBLIMINER_NO_WIZARD"

# Keys the wizard manages. ``PUBMED_EMAIL`` is required (NCBI asks for it);
# ``NCBI_API_KEY`` is optional but strongly recommended.
REQUIRED_KEYS = ("PUBMED_EMAIL",)
OPTIONAL_KEYS = ("NCBI_API_KEY",)

console = Console()


# ── Predicates ─────────────────────────────────────────────────────────


def env_path(cwd: Path | None = None) -> Path:
    """Return the resolved path to ``.env`` in the given CWD (default: Path.cwd())."""
    return (cwd or Path.cwd()) / ".env"


def read_env_values(cwd: Path | None = None) -> dict[str, str | None]:
    """Parse ``.env`` into a dict. Missing file → empty dict."""
    path = env_path(cwd)
    if not path.exists():
        return {}
    return {k: v for k, v in dotenv_values(path).items() if v is not None}


def env_is_complete(cwd: Path | None = None) -> bool:
    """True iff the required keys are present AND non-empty in ``.env``.

    ``PUBMED_EMAIL`` is the only hard requirement — NCBI rejects requests
    without it. ``NCBI_API_KEY`` is recommended but not enforced here;
    users can legitimately run against the 3 req/s anonymous limit.
    """
    values = read_env_values(cwd)
    return all(values.get(key, "").strip() for key in REQUIRED_KEYS)


def wizard_should_run(cwd: Path | None = None) -> bool:
    """Auto-trigger check for CLI commands.

    Returns False if ``PUBLIMINER_NO_WIZARD=1`` is set (CI use) OR if
    the env is already complete. True otherwise — caller should invoke
    ``run_wizard()``.
    """
    if os.environ.get(ENV_BYPASS_VAR, "").strip() in ("1", "true", "yes"):
        return False
    return not env_is_complete(cwd)


# ── Writers ────────────────────────────────────────────────────────────


def _mask(value: str) -> str:
    """Obfuscate a secret for redisplay: ``abcd1234`` → ``abcd…`` (never the full key)."""
    if not value:
        return ""
    if len(value) <= 4:
        return "***"
    return f"{value[:4]}…"


def write_env(
    cwd: Path | None = None,
    *,
    email: str | None = None,
    api_key: str | None = None,
) -> Path:
    """Write ``.env`` atomically. Creates the file if missing.

    ``dotenv.set_key`` handles:
    - atomic rename via tempfile (safe on NTFS and POSIX)
    - preserving existing keys not managed by us (OPENROUTER_API_KEY etc.)
    - preserving key order and comments across rewrites
    """
    path = env_path(cwd)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)  # set_key requires file to exist

    if email is not None:
        set_key(path, "PUBMED_EMAIL", email, quote_mode="never")
    if api_key is not None:
        # Empty api_key = explicit "skip"; still record it so we don't re-ask.
        set_key(path, "NCBI_API_KEY", api_key, quote_mode="never")

    return path


def ensure_gitignored(cwd: Path | None = None) -> bool:
    """Append ``.env`` to ``.gitignore`` if it isn't already matched.

    Returns True if we made a change, False if the pattern was already present
    (or no ``.gitignore`` exists — then it's the user's call).

    Naive line match: checks for a literal ``.env`` line. Not perfect (a
    broader pattern like ``.env*`` also covers us) but the common case is
    a bare ``.env`` entry.
    """
    gi = (cwd or Path.cwd()) / ".gitignore"
    if not gi.exists():
        return False
    lines = gi.read_text(encoding="utf-8").splitlines()
    patterns = {line.strip() for line in lines if line.strip() and not line.startswith("#")}
    if ".env" in patterns or ".env*" in patterns:
        return False
    with gi.open("a", encoding="utf-8") as f:
        if lines and lines[-1] != "":
            f.write("\n")
        f.write("# Added by `publiminer setup` — contains your NCBI credentials\n.env\n")
    return True


def scaffold_yaml(cwd: Path | None = None, *, overwrite: bool = False) -> Path | None:
    """Copy the bundled starter YAML into ``<cwd>/publiminer.yaml``.

    Returns the path that was written, or None if the file already existed
    and ``overwrite`` was False. The starter intentionally uses a generic
    example query so first-run is immediately demo-able.
    """
    dest = (cwd or Path.cwd()) / "publiminer.yaml"
    if dest.exists() and not overwrite:
        return None

    # Locate the bundled template — works both from the installed wheel
    # (package-data via importlib.resources) and from a source checkout.
    try:
        template = resources.files("publiminer.templates").joinpath("publiminer.starter.yaml")
        dest.write_text(template.read_text(encoding="utf-8"), encoding="utf-8")
    except (FileNotFoundError, ModuleNotFoundError):
        # Running from a source checkout where templates/ isn't importable —
        # fall back to a minimal inline scaffold so setup never dead-ends.
        dest.write_text(_INLINE_STARTER_YAML, encoding="utf-8")

    return dest


# Fallback in case the bundled template can't be located (defense in depth —
# the package manifest should always include it, but we'd rather write
# something than raise).
_INLINE_STARTER_YAML = """\
# PubLiMiner starter config — edit this to match your research question.
# Full reference: https://github.com/sdamirsa/PubLiMiner
general:
  output_dir: output
  log_level: INFO
steps: [fetch, parse, deduplicate]
fetch:
  query: "diabetes AND machine learning"
  start_date: "2024/01/01"
  end_date: "2024/12/31"
  # email + api_key come from .env — do not put secrets here
parse:
  prepare_llm_input: true
  flag_exclusions: true
deduplicate:
  fuzzy_threshold: 90
  remove_retracted: true
"""


# ── Interactive wizard (CLI) ───────────────────────────────────────────


def run_wizard(cwd: Path | None = None, *, force: bool = False) -> None:
    """Run the interactive CLI wizard.

    Args:
        cwd: Working directory for ``.env`` + ``publiminer.yaml``. Defaults
            to ``Path.cwd()``.
        force: If True, run the wizard even when ``env_is_complete()`` — use
            this for ``publiminer setup`` invoked explicitly. The auto-trigger
            from ``publiminer ui`` / ``publiminer run`` calls with force=False.
    """
    cwd = cwd or Path.cwd()

    if not force and env_is_complete(cwd):
        console.print("[green]✓[/green] Setup already complete — skipping wizard.")
        console.print(f"  Edit [cyan]{env_path(cwd)}[/cyan] directly to change values.")
        return

    existing = read_env_values(cwd)

    # Greeting.
    console.print(
        Panel.fit(
            "[bold]Welcome to PubLiMiner![/bold]\n\n"
            "Before we start, I'll ask two quick questions and save your\n"
            "answers to [cyan].env[/cyan] in this folder. You can edit them later.\n\n"
            "Takes about 30 seconds.",
            title="🚀 First-run setup",
            border_style="green",
        )
    )

    # Email — required.
    default_email = existing.get("PUBMED_EMAIL", "")
    prompt = "Your email (NCBI requires this to identify API callers)"
    if default_email:
        console.print(f"  Current: [dim]{default_email}[/dim]")
    email = typer.prompt(prompt, default=default_email, show_default=bool(default_email))
    email = email.strip()
    while not email or "@" not in email:
        console.print(
            "[red]An email address is required (NCBI will reject requests without one).[/red]"
        )
        email = typer.prompt(prompt).strip()

    # NCBI API key explanation + prompt.
    console.print()
    console.print(
        Panel(
            "An [bold]NCBI API key[/bold] is optional but strongly recommended.\n\n"
            "• Without a key: [yellow]3 requests/second[/yellow] (fine for small queries)\n"
            "• With a key:    [green]10 requests/second[/green] (≈3× faster on large corpora)\n\n"
            f"Register free at: [cyan]{NCBI_REGISTRATION_URL}[/cyan]\n"
            "Takes ~2 minutes — sign in with Google/ORCID, then Account → API Key Management.",
            title="🔑 About the NCBI API key",
            border_style="blue",
        )
    )

    has_key = typer.confirm("Do you have an NCBI API key to paste now?", default=False)

    api_key = ""
    if has_key:
        existing_key = existing.get("NCBI_API_KEY", "")
        if existing_key:
            console.print(f"  Current: [dim]{_mask(existing_key)}[/dim] (press enter to keep)")
        raw = typer.prompt(
            "Paste your NCBI API key",
            default=existing_key,
            show_default=False,
            hide_input=True,
        ).strip()
        api_key = raw or existing_key
    else:
        console.print(
            f"[dim]  Skipped. Add it later by editing [cyan]{env_path(cwd)}[/cyan] "
            "or re-running [bold]publiminer setup[/bold].[/dim]"
        )

    # Commit everything.
    written = write_env(cwd, email=email, api_key=api_key)
    gi_changed = ensure_gitignored(cwd)

    yaml_scaffolded = None
    yaml_path = cwd / "publiminer.yaml"
    if not yaml_path.exists() and typer.confirm(
        "Create a starter publiminer.yaml with an example query?", default=True
    ):
        yaml_scaffolded = scaffold_yaml(cwd)

    # Summary.
    console.print()
    console.print(
        Panel.fit(
            f"[green]✓[/green] Saved [cyan]{written}[/cyan]\n"
            + (
                "[green]✓[/green] Added [cyan].env[/cyan] to [cyan].gitignore[/cyan]\n"
                if gi_changed
                else ""
            )
            + (
                f"[green]✓[/green] Wrote starter [cyan]{yaml_scaffolded}[/cyan]\n"
                if yaml_scaffolded
                else f"[dim]  publiminer.yaml already exists at {yaml_path}[/dim]\n"
                if yaml_path.exists()
                else ""
            )
            + "\n[bold]Next:[/bold]\n"
            + "  • [cyan]publiminer ui[/cyan]       — visual editor + runner\n"
            + "  • [cyan]publiminer run[/cyan]      — run the pipeline\n"
            + "  • [cyan]publiminer status[/cyan]   — inspect your corpus",
            title="🎉 All set",
            border_style="green",
        )
    )
