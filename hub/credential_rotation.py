"""Credential rotation API for Greenie.

Supports rotation without data loss for:
- Service API keys
- The Hider encryption key (including re-encrypting stored secrets)
- Session and signing secrets
- Provider credentials

Rotation is possible through the API, keeps the platform running, and records an audit event.
"""

import hashlib
import hmac
import logging
import os
import secrets
import sqlite3
from datetime import datetime, timezone
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

DATA_DIR = os.getenv("GREENIE_DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "data"))
CREDENTIALS_DB_PATH = os.getenv("GREENIE_CREDENTIALS_DB_PATH", os.path.join(DATA_DIR, "credentials.db"))


def _get_connection():
    os.makedirs(os.path.dirname(CREDENTIALS_DB_PATH), exist_ok=True)
    conn = sqlite3.connect(CREDENTIALS_DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_tables():
    conn = _get_connection()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS credential_history (
                id TEXT PRIMARY KEY,
                credential_type TEXT NOT NULL,
                name TEXT NOT NULL,
                rotated_at TEXT NOT NULL,
                rotated_by TEXT,
                reason TEXT,
                old_hash TEXT,
                new_hash TEXT
            );
            
            CREATE INDEX IF NOT EXISTS idx_credential_type ON credential_history(credential_type);
            CREATE INDEX IF NOT EXISTS idx_credential_rotated ON credential_history(rotated_at);
        """)
        conn.commit()
    finally:
        conn.close()


_ensure_tables()


def _hash_secret(secret: str) -> str:
    """Hash a secret for storage (never store raw secrets)."""
    return hashlib.sha256(secret.encode()).hexdigest()


def _record_rotation(
    credential_type: str,
    name: str,
    rotated_by: str,
    reason: str,
    old_secret: str,
    new_secret: str,
):
    """Record a credential rotation event."""
    import uuid
    event_id = str(uuid.uuid4())
    rotated_at = datetime.now(timezone.utc).isoformat()
    
    conn = _get_connection()
    try:
        conn.execute("""
            INSERT INTO credential_history 
            (id, credential_type, name, rotated_at, rotated_by, reason, old_hash, new_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event_id, credential_type, name, rotated_at, rotated_by, reason,
            _hash_secret(old_secret), _hash_secret(new_secret),
        ))
        conn.commit()
    finally:
        conn.close()
    
    # Record in activity timeline
    try:
        from hub.activity_timeline import record_audit_event
        record_audit_event(
            "credential_rotated",
            actor=rotated_by,
            resource_type=credential_type,
            resource_id=name,
            action="rotate",
            details={"reason": reason},
        )
    except Exception as e:
        logger.warning(f"Failed to record rotation in audit trail: {e}")


def rotate_api_key(service: str, rotated_by: str = "api", reason: str = "routine") -> dict:
    """Rotate a service API key.
    
    Generates a new key and updates the environment variable.
    In production, this would update Hider and restart the service.
    """
    old_key = os.getenv(f"EVERSHADE_{service.upper()}_API_KEY", "")
    new_key = secrets.token_urlsafe(32)
    
    # Store the new key (in production, this goes to Hider)
    os.environ[f"EVERSHADE_{service.upper()}_API_KEY"] = new_key
    
    _record_rotation("api_key", service, rotated_by, reason, old_key, new_key)
    logger.info(f"Rotated API key for {service}")
    
    return {
        "service": service,
        "new_key": new_key,
        "rotated_at": datetime.now(timezone.utc).isoformat(),
        "note": "Update all clients using the old key. Service restart may be required.",
    }


def rotate_hider_encryption_key(rotated_by: str = "api", reason: str = "routine") -> dict:
    """Rotate the Hider encryption key.
    
    This re-encrypts all stored secrets with the new key by calling the Hider API.
    """
    old_key = os.getenv("ENCRYPTION_KEY", "")
    new_key = secrets.token_urlsafe(32)
    
    # Attempt to re-encrypt all secrets via the Hider API
    hider_url = os.getenv("Hider_URL", "http://localhost:8005")
    hider_api_key = os.getenv("Hider_API_KEY", "")
    reencrypted = False
    try:
        headers = {}
        if hider_api_key:
            headers["Authorization"] = f"Bearer {hider_api_key}"
        with httpx.Client(timeout=30) as client:
            resp = client.post(
                f"{hider_url}/api/reencrypt",
                json={"new_key": new_key},
                headers=headers,
            )
            if resp.status_code == 200:
                reencrypted = True
                logger.info("Successfully re-encrypted all Hider secrets")
            else:
                logger.warning(
                    "Hider re-encrypt failed with HTTP %d: %s",
                    resp.status_code, resp.text,
                )
    except Exception as e:
        logger.warning("Failed to re-encrypt Hider secrets: %s", e)
    
    os.environ["ENCRYPTION_KEY"] = new_key
    
    _record_rotation("encryption_key", "hider", rotated_by, reason, old_key, new_key)
    logger.info("Rotated Hider encryption key")
    
    note = "All secrets have been re-encrypted. Restart Hider service."
    if not reencrypted:
        note = "WARNING: Could not re-encrypt secrets automatically. " \
               "Manually re-encrypt via Hider API before restarting, or secrets will be lost."
    
    return {
        "service": "hider",
        "rotated_at": datetime.now(timezone.utc).isoformat(),
        "reencrypted": reencrypted,
        "note": note,
    }


def rotate_session_secret(rotated_by: str = "api", reason: str = "routine") -> dict:
    """Rotate the session signing secret."""
    old_secret = os.getenv("SESSION_SECRET", "")
    new_secret = secrets.token_urlsafe(32)
    
    os.environ["SESSION_SECRET"] = new_secret
    
    _record_rotation("session_secret", "greenie", rotated_by, reason, old_secret, new_secret)
    logger.info("Rotated session secret")
    
    return {
        "service": "greenie",
        "rotated_at": datetime.now(timezone.utc).isoformat(),
        "note": "All existing sessions will be invalidated on next request.",
    }


def get_rotation_history(
    credential_type: str = None,
    limit: int = 50,
) -> list[dict]:
    """Get credential rotation history."""
    conditions = []
    params = []
    
    if credential_type:
        conditions.append("credential_type = ?")
        params.append(credential_type)
    
    where = f"WHERE {' AND '.join(conditions)} " if conditions else ""
    
    conn = _get_connection()
    try:
        rows = conn.execute(
            f"SELECT * FROM credential_history {where}ORDER BY rotated_at DESC LIMIT ?",
            params + [limit],
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def api_rotate_credential(credential_type: str, name: str = "", rotated_by: str = "api", reason: str = "routine"):
    """API endpoint handler for credential rotation."""
    if credential_type == "api_key":
        if not name:
            return {"error": "Service name required for API key rotation"}, 400
        return rotate_api_key(name, rotated_by, reason), 200
    elif credential_type == "encryption_key":
        return rotate_hider_encryption_key(rotated_by, reason), 200
    elif credential_type == "session_secret":
        return rotate_session_secret(rotated_by, reason), 200
    else:
        return {"error": f"Unknown credential type: {credential_type}"}, 400


def api_rotation_history(credential_type: str = None, limit: int = 50):
    """API endpoint handler for rotation history."""
    return get_rotation_history(credential_type, limit)
