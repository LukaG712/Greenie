"""Route module: auto_review."""

from typing import Any, Dict

from fastapi import Depends

from hub.api.core import _verify_token
from hub.config import load_config, save_config


async def api_get_auto_review_settings(_auth: None = Depends(_verify_token)):
    """Get auto-review settings."""
    from hub import auto_review
    config = load_config()
    auto_review_cfg = config.get("auto_review", {})
    return {
        "enabled": auto_review_cfg.get("enabled", False),
        "patterns": auto_review_cfg.get("patterns", auto_review.get_patterns()),
    }


async def api_update_auto_review_settings(
    body: Dict[str, Any],
    _auth: None = Depends(_verify_token),
):
    """Update auto-review settings."""
    config = load_config()
    auto_review_cfg = config.get("auto_review", {})
    if "enabled" in body:
        auto_review_cfg["enabled"] = body["enabled"]
    if "patterns" in body:
        auto_review_cfg["patterns"] = body["patterns"]
    config["auto_review"] = auto_review_cfg
    from hub.config import save_config
    save_config(config)
    return {"enabled": auto_review_cfg.get("enabled", False)}


def register(app):
    """Register auto_review routes with the FastAPI app."""
    app.get("/api/auto-review/settings")(api_get_auto_review_settings)
    app.put("/api/auto-review/settings")(api_update_auto_review_settings)
