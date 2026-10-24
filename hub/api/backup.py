"""Route module: backup."""

import hashlib
import hmac
import logging
import os
import shutil
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException

from hub.api.core import _verify_token, _webhook_secrets, _webhook_secrets_lock

logger = logging.getLogger(__name__)

# Backup directory
_BACKUP_DIR = Path(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'backups'))


async def api_create_backup(_auth: None = Depends(_verify_token)):
    """Create a backup of the SQLite database.

    Returns the backup file path and size. Backups are stored in data/backups/
    with timestamps to avoid overwriting.
    """
    import sqlite3
    from datetime import datetime as dt

    _BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    # Find the source database
    from hub.memory import _DB_PATH as evershade_valley_db
    source_db = Path(evershade_valley_db)

    if not source_db.exists():
        raise HTTPException(status_code=404, detail="Database file not found")

    # Create backup filename with timestamp
    timestamp = dt.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"hub_backup_{timestamp}.db"
    backup_path = _BACKUP_DIR / backup_name

    try:
        # Use SQLite online backup for consistency
        source_conn = sqlite3.connect(str(source_db), timeout=10.0)
        backup_conn = sqlite3.connect(str(backup_path))
        source_conn.backup(backup_conn)
        backup_conn.close()
        source_conn.close()

        file_size = backup_path.stat().st_size

        logger.info(
            f"Backup created: {backup_name} ({file_size} bytes)",
            extra={"backup_file": str(backup_path), "size_bytes": file_size},
        )

        return {
            "status": "ok",
            "backup_file": backup_name,
            "path": str(backup_path),
            "size_bytes": file_size,
            "created_at": dt.now().isoformat(),
        }
    except Exception as e:
        if backup_path.exists():
            backup_path.unlink()
        logger.error(f"Backup failed: {e}")
        raise HTTPException(status_code=500, detail=f"Backup failed: {str(e)}")


async def api_list_backups(_auth: None = Depends(_verify_token)):
    """List all available database backups."""
    if not _BACKUP_DIR.exists():
        return {"backups": [], "count": 0}

    backups = []
    for f in sorted(_BACKUP_DIR.glob("hub_backup_*.db"), key=lambda p: p.stat().st_mtime, reverse=True):
        stat = f.stat()
        backups.append({
            "file": f.name,
            "size_bytes": stat.st_size,
            "created_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        })

    return {"backups": backups, "count": len(backups)}


async def api_restore_backup(backup_file: str, _auth: None = Depends(_verify_token)):
    """Restore the database from a backup file.

    WARNING: This will overwrite the current database. The service should be
    stopped before restoring, or expect data inconsistency.
    """
    import sqlite3

    backup_path = _BACKUP_DIR / backup_file
    if not backup_path.exists():
        raise HTTPException(status_code=404, detail=f"Backup file not found: {backup_file}")

    from hub.memory import _DB_PATH as evershade_valley_db
    target_db = Path(evershade_valley_db)

    # Create a safety backup of the current database before restoring
    if target_db.exists():
        safety_backup = _BACKUP_DIR / f"hub_pre_restore_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.db"
        try:
            shutil.copy2(str(target_db), str(safety_backup))
            logger.info(f"Safety backup created: {safety_backup.name}")
        except Exception:
            logger.warning("Could not create safety backup before restore")

    try:
        backup_conn = sqlite3.connect(str(backup_path))
        target_conn = sqlite3.connect(str(target_db), timeout=10.0)
        backup_conn.backup(target_conn)
        target_conn.close()
        backup_conn.close()

        logger.info(
            f"Database restored from: {backup_file}",
            extra={"backup_file": backup_file},
        )

        return {
            "status": "ok",
            "message": f"Database restored from {backup_file}",
            "restored_at": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        logger.error(f"Restore failed: {e}")
        raise HTTPException(status_code=500, detail=f"Restore failed: {str(e)}")


async def api_delete_backup(backup_file: str, _auth: None = Depends(_verify_token)):
    """Delete a backup file."""
    backup_path = _BACKUP_DIR / backup_file
    if not backup_path.exists():
        raise HTTPException(status_code=404, detail=f"Backup file not found: {backup_file}")

    try:
        backup_path.unlink()
        logger.info(f"Backup deleted: {backup_file}")
        return {"status": "ok", "message": f"Backup deleted: {backup_file}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete backup: {str(e)}")


# ─── GitHub/GitLab Webhook Endpoints ─────────────────────────────────────────

# Store for webhook secrets
_webhook_secrets: Dict[str, Dict[str, Any]] = {}
_webhook_secrets_lock = threading.Lock()


def _verify_github_signature(payload: bytes, signature: str, secret: str) -> bool:
    """Verify GitHub webhook HMAC signature."""
    import hashlib
    import hmac
    if not signature.startswith("sha256="):
        return False
    expected = "sha256=" + hmac.new(
        secret.encode(), payload, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


def _verify_gitlab_signature(payload: bytes, token: str, secret: str) -> bool:
    """Verify GitLab webhook secret token."""
    return hmac.compare_digest(token.encode(), secret.encode()) if secret else True



def register(app):
    """Register backup routes with the FastAPI app."""
    app.post("/api/backup")(api_create_backup)
    app.get("/api/backups")(api_list_backups)
    app.post("/api/backup/restore/{backup_file}")(api_restore_backup)
    app.delete("/api/backup/{backup_file}")(api_delete_backup)
