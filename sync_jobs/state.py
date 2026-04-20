"""Per-job JSON state (e.g. Supabase compare watermark)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_sync_state(state_file: Path) -> dict[str, Any]:
    try:
        if not state_file.exists():
            return {}
        text = state_file.read_text(encoding="utf-8").strip()
        if not text:
            return {}
        data = json.loads(text)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_sync_state(state_file: Path, state: dict[str, Any]) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(json.dumps(state, indent=2), encoding="utf-8")


def get_supabase_compare_watermark(state_file: Path) -> str | None:
    state = load_sync_state(state_file)
    value = state.get("supabase_compare_watermark")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def set_supabase_compare_watermark(state_file: Path, value: str) -> None:
    state = load_sync_state(state_file)
    state["supabase_compare_watermark"] = value
    state["updated_at"] = utc_now_iso()
    save_sync_state(state_file, state)
