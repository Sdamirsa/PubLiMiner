"""Streamlit first-run wizard — multi-step walkthrough for the UI.

Runs when ``app.py`` detects ``env_is_complete()`` is False. Uses session
state as a step counter so the user gets a guided experience (Welcome →
Email → NCBI key → Scaffold → Done) rather than a single intimidating
form.

The wizard writes through the same ``publiminer.commands.setup`` helpers
as the CLI wizard — single source of truth for ``.env`` + YAML I/O.
"""

from __future__ import annotations

from pathlib import Path

import streamlit as st

from publiminer.commands.setup import (
    NCBI_REGISTRATION_URL,
    ensure_gitignored,
    env_path,
    read_env_values,
    scaffold_yaml,
    write_env,
)

# Step identifiers in order. Stored in ``st.session_state["_setup_step"]``.
STEPS = ["welcome", "email", "api_key", "scaffold", "done"]


def _current_step() -> str:
    return st.session_state.setdefault("_setup_step", "welcome")


def _goto(step: str) -> None:
    st.session_state["_setup_step"] = step
    st.rerun()


def _progress_bar() -> None:
    """Render a thin 5-dot progress indicator along the top of the wizard."""
    step = _current_step()
    idx = STEPS.index(step)
    dots = []
    for i in range(len(STEPS)):
        if i < idx:
            dots.append("🟢")
        elif i == idx:
            dots.append("🔵")
        else:
            dots.append("⚪")
    label = step.replace("_", " ").title()
    st.caption(f"{' '.join(dots)}   Step {idx + 1} of {len(STEPS)}: **{label}**")


# ── Step renderers ─────────────────────────────────────────────────────


def _step_welcome() -> None:
    st.markdown(
        """
        ### Welcome to PubLiMiner! 👋

        Before we start mining PubMed, we need to save two small things:

        1. **Your email** — NCBI uses it to identify API callers
        2. **An NCBI API key** — optional, but 3× faster rate limits

        Everything gets saved to a local `.env` file in this folder. You can
        change it any time. The whole thing takes about **30 seconds**.
        """
    )
    st.info(
        f"💡 The `.env` file stays on your machine — it's automatically added "
        f"to `.gitignore` so you won't commit it by accident.\n\n"
        f"Location: `{env_path()}`"
    )
    if st.button("Let's go →", type="primary", width="stretch"):
        _goto("email")


def _step_email() -> None:
    st.markdown("### Step 1 — Your email")
    st.markdown(
        "NCBI requires an email address with every API request. It's used "
        "to contact you if your queries cause problems (rare, but they're "
        "serious about it)."
    )
    st.markdown(
        "**Tip:** use the email associated with your NCBI account — that's "
        "the one NCBI already knows about."
    )

    existing = read_env_values().get("PUBMED_EMAIL", "")
    email = st.text_input(
        "Email",
        value=existing,
        placeholder="you@example.com",
        help="Must include an @ sign. This is not a secret.",
    )

    col_back, col_next = st.columns([1, 1])
    with col_back:
        if st.button("← Back", width="stretch"):
            _goto("welcome")
    with col_next:
        disabled = not email or "@" not in email
        if st.button("Continue →", type="primary", width="stretch", disabled=disabled):
            st.session_state["_setup_email"] = email.strip()
            _goto("api_key")


def _step_api_key() -> None:
    st.markdown("### Step 2 — NCBI API key (optional)")
    st.markdown(
        "An NCBI API key raises your rate limit from **3 to 10 requests per "
        "second** — roughly 3× faster on large corpora. It's free and takes "
        "about 2 minutes to get."
    )

    with st.expander("📖 How to get an NCBI API key", expanded=False):
        st.markdown(
            """
            1. Click the button below to open the NCBI settings page
            2. Sign in with Google, ORCID, or your NCBI username
            3. Scroll to **API Key Management** and click **Create an API Key**
            4. Copy the key (looks like `abc123def456…`) and paste it here
            """
        )

    st.link_button(
        "🔑 Get an NCBI API key", NCBI_REGISTRATION_URL, type="secondary", width="stretch"
    )

    st.markdown("---")

    existing = read_env_values().get("NCBI_API_KEY", "")
    api_key = st.text_input(
        "API key (leave blank to skip)",
        value=existing,
        type="password",
        placeholder="Paste your key here, or skip for now",
        help="You can add this later by re-running the wizard or editing .env.",
    )

    col_back, col_skip, col_next = st.columns([1, 1, 1])
    with col_back:
        if st.button("← Back", width="stretch"):
            _goto("email")
    with col_skip:
        if st.button("Skip for now", width="stretch"):
            st.session_state["_setup_api_key"] = ""
            _goto("scaffold")
    with col_next:
        if st.button("Continue →", type="primary", width="stretch"):
            st.session_state["_setup_api_key"] = api_key.strip()
            _goto("scaffold")


def _step_scaffold() -> None:
    st.markdown("### Step 3 — Starter config")

    yaml_path = Path("publiminer.yaml")
    if yaml_path.exists():
        st.success(
            f"✅ Found existing `publiminer.yaml` at `{yaml_path.resolve()}`. We'll leave it alone."
        )
        st.session_state["_setup_scaffold"] = False
    else:
        st.info(
            "📝 **What is publiminer.yaml?**\n\n"
            "It's the pipeline's recipe file — it controls:\n"
            "- **query** — what to search PubMed for\n"
            "- **start_date / end_date** — the date range to fetch\n"
            "- **steps** — which pipeline stages to run (fetch, parse, deduplicate…)\n"
            "- **parameters** — per-step settings (fuzzy threshold, batch size, etc.)\n\n"
            "The starter template ships with a small demo query "
            "(`diabetes AND machine learning`, 2024) so you can see the pipeline "
            "work end-to-end before writing your own query. You'll edit it in the "
            "Configure tab next."
        )
        scaffold = st.checkbox(
            "Create starter publiminer.yaml",
            value=True,
            help="You can always create one later from the Configure tab.",
        )
        st.session_state["_setup_scaffold"] = scaffold

    col_back, col_next = st.columns([1, 1])
    with col_back:
        if st.button("← Back", width="stretch"):
            _goto("api_key")
    with col_next:
        if st.button("Save everything →", type="primary", width="stretch"):
            _finalize()


def _finalize() -> None:
    """Commit wizard answers to disk and advance to the 'done' step."""
    email = st.session_state.get("_setup_email", "")
    api_key = st.session_state.get("_setup_api_key", "")
    scaffold = st.session_state.get("_setup_scaffold", False)

    write_env(email=email, api_key=api_key)
    gi_changed = ensure_gitignored()
    yaml_created = scaffold_yaml() if scaffold else None

    st.session_state["_setup_result"] = {
        "env_path": str(env_path()),
        "gitignore_changed": gi_changed,
        "yaml_created": str(yaml_created) if yaml_created else None,
    }
    _goto("done")


def _step_done() -> None:
    result = st.session_state.get("_setup_result", {})

    st.balloons()
    st.markdown("### 🎉 All set!")
    st.success(f"✅ Credentials saved to `{result.get('env_path')}`")
    if result.get("gitignore_changed"):
        st.success("✅ Added `.env` to `.gitignore` so you won't commit it by accident")
    if result.get("yaml_created"):
        st.success(f"✅ Starter config written to `{result['yaml_created']}`")

    st.markdown("---")
    st.markdown("### 🧭 What to do next")
    st.markdown(
        "The UI you're about to enter has **four tabs**:\n\n"
        "| Tab | What it does |\n"
        "|---|---|\n"
        "| ⚙️ **Configure** | Edit the query, dates, and pipeline steps |\n"
        "| ▶️ **Run** | Execute the pipeline with a live progress bar |\n"
        "| 🔍 **Explore** | Filter and sample papers once you have data |\n"
        "| 📊 **Status** | Corpus size, schema, and last-run metadata |\n"
    )

    st.info(
        "💡 **Big corpora (10k+ papers)?** Configure once in the Configure tab, "
        "then let the pipeline run overnight with `publiminer run` (or schedule "
        "`run_nightly.bat` via Task Scheduler on Windows / cron on macOS/Linux). "
        "Check on it anytime with `publiminer status`."
    )

    if st.button("Open PubLiMiner →", type="primary", width="stretch"):
        # Clear all wizard state so nothing leaks into the main UI.
        for key in list(st.session_state.keys()):
            if key.startswith("_setup_"):
                del st.session_state[key]
        st.rerun()


# ── Entry point ────────────────────────────────────────────────────────


def render_setup_wizard() -> None:
    """Render the full wizard. Called from app.py when env is incomplete."""
    st.set_page_config(page_title="PubLiMiner — Setup", layout="centered", page_icon="🚀")
    st.title("📚 PubLiMiner")

    _progress_bar()
    st.markdown("")  # small spacer

    step = _current_step()
    {
        "welcome": _step_welcome,
        "email": _step_email,
        "api_key": _step_api_key,
        "scaffold": _step_scaffold,
        "done": _step_done,
    }[step]()
