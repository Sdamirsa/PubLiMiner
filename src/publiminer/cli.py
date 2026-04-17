"""Typer-based CLI for PubLiMiner."""

from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from publiminer.utils.env import load_env

# Auto-load .env so NCBI_API_KEY / PUBMED_EMAIL work without manual export
load_env()

app = typer.Typer(name="publiminer", help="PubLiMiner — Publication Literature Miner")
console = Console()


@app.command()
def setup(
    force: bool = typer.Option(
        False,
        "--force",
        help="Run the wizard even if .env looks complete.",
    ),
) -> None:
    """First-run setup wizard — captures email + NCBI key, scaffolds a starter config.

    Run this once per project folder. ``publiminer ui`` and ``publiminer run``
    auto-invoke the wizard when they detect a missing or incomplete ``.env``;
    use this command to re-run it explicitly (e.g. to change credentials).
    """
    from publiminer.commands.setup import run_wizard

    run_wizard(force=force)


def _ensure_setup(no_setup: bool) -> None:
    """Auto-trigger the CLI wizard when ``.env`` is missing.

    Called from the top of ``ui()`` and ``run()``. Skipped when:
    - ``--no-setup`` flag is passed (scripted / advanced use)
    - ``PUBLIMINER_NO_WIZARD=1`` env var is set (CI)
    - ``.env`` already has a valid ``PUBMED_EMAIL``
    """
    if no_setup:
        return
    from publiminer.commands.setup import run_wizard, wizard_should_run

    if wizard_should_run():
        run_wizard()
        # Re-load .env so the freshly-written values are picked up by the
        # current process (dotenv.load_dotenv caches).
        load_env()


@app.command()
def ui(
    port: int = typer.Option(8501, "--port", "-p", help="Port for the Streamlit server"),
    host: str = typer.Option("localhost", "--host", "-h", help="Host address to bind"),
    no_setup: bool = typer.Option(
        False,
        "--no-setup",
        help="Skip the first-run wizard even if .env is missing.",
    ),
) -> None:
    """Launch the bundled Streamlit configuration + runner UI."""
    import importlib.resources
    import subprocess
    import sys

    try:
        import streamlit  # noqa: F401
    except ImportError:
        console.print(
            "[red]Streamlit not installed.[/red] Run: [bold]pip install 'publiminer[ui]'[/bold]"
        )
        raise typer.Exit(1) from None

    # Note: we do NOT run _ensure_setup() here. The Streamlit UI has its own
    # interactive wizard (src/publiminer/ui/setup_panel.py) which is a far
    # better first-run experience in a browser context. ``--no-setup`` is
    # still honored by the UI via the PUBLIMINER_NO_WIZARD env var below.
    import os

    env = os.environ.copy()
    if no_setup:
        env["PUBLIMINER_NO_WIZARD"] = "1"

    app_path = importlib.resources.files("publiminer.ui") / "app.py"
    console.print(f"[bold green]Launching PubLiMiner UI[/bold green] at http://{host}:{port}")
    subprocess.run(
        [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            str(app_path),
            "--server.port",
            str(port),
            "--server.address",
            host,
        ],
        check=False,
        env=env,
    )


@app.command()
def run(
    config: str | None = typer.Option(None, "--config", "-c", help="Path to publiminer.yaml"),
    output_dir: str | None = typer.Option(None, "--output", "-o", help="Output directory"),
    steps: str | None = typer.Option(None, "--steps", "-s", help="Comma-separated step list"),
    no_setup: bool = typer.Option(
        False,
        "--no-setup",
        help="Skip the first-run wizard even if .env is missing.",
    ),
) -> None:
    """Run the pipeline (or specific steps)."""
    _ensure_setup(no_setup)

    from publiminer.core.config import load_config
    from publiminer.utils.logger import setup_logger

    overrides = {}
    if output_dir:
        overrides["general"] = {"output_dir": output_dir}
    if steps:
        overrides["steps"] = [s.strip() for s in steps.split(",")]

    global_cfg = load_config(user_config_path=config, overrides=overrides or None)
    log_dir = Path(global_cfg.general.output_dir) / "logs"
    setup_logger(level=global_cfg.general.log_level, log_dir=log_dir)

    out = output_dir or global_cfg.general.output_dir

    for step_name in global_cfg.steps:
        console.print(f"[bold blue]Running step:[/bold blue] {step_name}")
        try:
            step_instance = _create_step(step_name, global_cfg, config, out)
            meta = step_instance.execute()
            console.print(f"  [green]✓[/green] {meta.status} ({meta.duration_seconds}s)")
        except Exception as e:
            console.print(f"  [red]✗[/red] {e}")
            if global_cfg.general.on_error == "fail":
                raise typer.Exit(1) from None


@app.command()
def inspect(
    step: str = typer.Argument(..., help="Step name to inspect"),
    output_dir: str = typer.Option("output", "--output", "-o", help="Output directory"),
) -> None:
    """Inspect a step's output and metadata."""
    from publiminer.core.io import load_step_meta
    from publiminer.core.spine import Spine

    meta = load_step_meta(step, output_dir)
    if meta:
        table = Table(title=f"Step: {step}")
        table.add_column("Field", style="cyan")
        table.add_column("Value")
        table.add_row("Status", meta.status)
        table.add_row("Started", meta.started_at)
        table.add_row("Duration", f"{meta.duration_seconds}s")
        table.add_row("Rows before", str(meta.rows_before))
        table.add_row("Rows after", str(meta.rows_after))
        table.add_row("Errors", str(meta.errors))
        console.print(table)
    else:
        console.print(f"[yellow]No metadata found for step: {step}[/yellow]")

    spine = Spine(output_dir)
    if spine.exists:
        info = spine.inspect()
        console.print(f"\n[bold]Parquet:[/bold] {info['rows']} rows, {info['file_size_mb']} MB")
        console.print(f"[bold]Columns:[/bold] {', '.join(info['columns'])}")


@app.command()
def status(
    output_dir: str = typer.Option("output", "--output", "-o", help="Output directory"),
) -> None:
    """Show pipeline status dashboard."""
    from publiminer.core.spine import Spine

    spine = Spine(output_dir)
    if not spine.exists:
        console.print("[yellow]No data found. Run the pipeline first.[/yellow]")
        return

    info = spine.inspect()
    console.print(f"[bold]Total papers:[/bold] {info['rows']}")
    console.print(f"[bold]File size:[/bold] {info['file_size_mb']} MB")
    console.print(f"[bold]Columns:[/bold] {len(info['columns'])}")

    table = Table(title="Column Schema")
    table.add_column("Column", style="cyan")
    table.add_column("Type")
    for col, dtype in info["schema"].items():
        table.add_row(col, dtype)
    console.print(table)


@app.command()
def import_legacy(
    source_dir: str = typer.Argument(..., help="Directory with pubmed_batch_*.json files"),
    output_dir: str = typer.Option("output", "--output", "-o", help="Output directory"),
    max_files: int | None = typer.Option(None, "--max-files", help="Max files to import"),
) -> None:
    """Import legacy AI-in-Med-Trend batch files into PubLiMiner format."""
    from publiminer.utils.legacy_import import import_legacy_data
    from publiminer.utils.logger import setup_logger

    setup_logger(level="INFO")
    console.print(f"[bold]Importing from:[/bold] {source_dir}")
    console.print(f"[bold]Output to:[/bold] {output_dir}")

    result = import_legacy_data(source_dir, output_dir, max_files)

    console.print("\n[green]Import complete![/green]")
    console.print(f"  Files processed: {result['files']}")
    console.print(f"  Total articles: {result['articles']}")
    console.print(f"  Duplicates skipped: {result['duplicates']}")


def _create_step(
    step_name: str,
    global_cfg: object,
    config_path: str | None,
    output_dir: str,
) -> object:
    """Create a step instance by name."""
    from publiminer.core.config import load_step_config

    if step_name == "fetch":
        from publiminer.steps.fetch.schema import FetchConfig
        from publiminer.steps.fetch.step import FetchStep

        step_cfg = load_step_config(step_name, FetchConfig, global_cfg, config_path)
        return FetchStep(global_cfg, step_cfg, output_dir)

    elif step_name == "parse":
        from publiminer.steps.parse.schema import ParseConfig
        from publiminer.steps.parse.step import ParseStep

        step_cfg = load_step_config(step_name, ParseConfig, global_cfg, config_path)
        return ParseStep(global_cfg, step_cfg, output_dir)

    elif step_name == "deduplicate":
        from publiminer.steps.deduplicate.schema import DeduplicateConfig
        from publiminer.steps.deduplicate.step import DeduplicateStep

        step_cfg = load_step_config(step_name, DeduplicateConfig, global_cfg, config_path)
        return DeduplicateStep(global_cfg, step_cfg, output_dir)

    else:
        raise typer.BadParameter(f"Step '{step_name}' is not yet implemented")


if __name__ == "__main__":
    app()
