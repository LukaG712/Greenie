"""Per-Bot memory file management.

Each Bot has a memory.md file at data/bots/{bot_id}/memory.md that stores
role context, stable preferences, and summaries of prior work. This file
is included in the task context when the Bot runs.
"""

import re
from pathlib import Path
from typing import Optional

from hub.config import NAME_RE, load_config


def _bot_memory_path(bot_id: str) -> Path:
    """Return the path to a Bot's memory.md file.

    Raises ValueError if bot_id fails path-safety validation.
    """
    if not NAME_RE.match(bot_id):
        raise ValueError(f"Invalid bot ID for memory file: {bot_id}")
    config = load_config()
    data_dir = Path(config.get("data_dir", "data")).resolve()
    return data_dir / "bots" / bot_id / "memory.md"


def read_bot_memory(bot_id: str) -> str:
    """Read the per-Bot memory file content.

    Returns empty string if the file does not exist.
    """
    path = _bot_memory_path(bot_id)
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def write_bot_memory(bot_id: str, content: str) -> None:
    """Write content to the per-Bot memory file.

    Creates parent directories if they don't exist.
    """
    path = _bot_memory_path(bot_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def bot_memory_exists(bot_id: str) -> bool:
    """Check if a Bot has a memory file."""
    path = _bot_memory_path(bot_id)
    return path.exists()
