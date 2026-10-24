"""User authentication and session management for Evershade.

Provides browser-based user authentication with:
- User registration and login
- Password hashing using bcrypt
- Secure session management with cookies
- Session expiration and revocation
- Integration with existing RBAC system

This module is separate from service-to-service API key authentication.
API keys are for service clients; user sessions are for browser users.
"""

import hashlib
import hmac
import logging
import os
import secrets
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import Cookie, Depends, HTTPException, Request, Response
from fastapi.security import OAuth2PasswordBearer

from hub.logging_config import get_logger

logger = get_logger(__name__)

# ─── Configuration ──────────────────────────────────────────────────────────

# Session settings
SESSION_MAX_AGE = int(os.environ.get("SESSION_MAX_AGE", str(8 * 3600)))  # 8 hours default
SESSION_COOKIE_NAME = os.environ.get("SESSION_COOKIE_NAME", "evershade_session")
_SESSION_SECRET_DEFAULT = "evershade-default-session-secret-change-me"
SESSION_SECRET = os.environ.get("SESSION_SECRET", _SESSION_SECRET_DEFAULT)

if SESSION_SECRET == _SESSION_SECRET_DEFAULT:
    logger.warning(
        "SESSION_SECRET is using the default value. "
        "Set the SESSION_SECRET environment variable to a strong random value for production."
    )
SESSION_SECURE = os.environ.get("SESSION_SECURE", "false").lower() == "true"
SESSION_SAME_SITE = os.environ.get("SESSION_SAME_SITE", "lax")

# Password settings
BCRYPT_ROUNDS = 12

# ─── Password hashing ───────────────────────────────────────────────────────

def hash_password(password: str) -> str:
    """Hash a password using bcrypt.
    
    Falls back to SHA-256 with salt if bcrypt is not available.
    
    Args:
        password: Plain text password to hash.
        
    Returns:
        Hashed password string.
    """
    try:
        import bcrypt
        salt = bcrypt.gensalt(rounds=BCRYPT_ROUNDS)
        hashed = bcrypt.hashpw(password.encode("utf-8"), salt)
        return hashed.decode("utf-8")
    except ImportError:
        # Fallback: SHA-256 with salt (less secure but works without bcrypt)
        salt = secrets.token_hex(16)
        hashed = hashlib.sha256((salt + password).encode("utf-8")).hexdigest()
        return f"$sha256${salt}${hashed}"


def verify_password(password: str, hashed: str) -> bool:
    """Verify a password against a hash.
    
    Args:
        password: Plain text password to verify.
        hashed: Hashed password string.
        
    Returns:
        True if password matches.
    """
    try:
        import bcrypt
        if hashed.startswith("$2b$") or hashed.startswith("$2a$"):
            return bcrypt.checkpw(password.encode("utf-8"), hashed.encode("utf-8"))
    except ImportError:
        pass
    
    # SHA-256 fallback verification
    if hashed.startswith("$sha256$"):
        _, salt, expected = hashed.split("$", 2)
        actual = hashlib.sha256((salt + password).encode("utf-8")).hexdigest()
        return hmac.compare_digest(actual, expected)
    
    return False


# ─── Session token generation ───────────────────────────────────────────────

def _generate_session_token() -> str:
    """Generate a cryptographically secure session token."""
    return secrets.token_urlsafe(32)


def _sign_session_token(session_id: str) -> str:
    """Sign a session token with HMAC for integrity.
    
    Args:
        session_id: The session ID to sign.
        
    Returns:
        Signed token string.
    """
    signature = hmac.new(
        SESSION_SECRET.encode("utf-8"),
        session_id.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"{session_id}.{signature}"


def _verify_session_token(token: str) -> Optional[str]:
    """Verify and extract session ID from a signed token.
    
    Args:
        token: Signed token string.
        
    Returns:
        Session ID if valid, None otherwise.
    """
    try:
        session_id, signature = token.rsplit(".", 1)
        expected = hmac.new(
            SESSION_SECRET.encode("utf-8"),
            session_id.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        if hmac.compare_digest(signature, expected):
            return session_id
    except (ValueError, AttributeError):
        pass
    return None


# ─── In-memory stores ───────────────────────────────────────────────────────

# User store: username -> user data
_users: Dict[str, Dict[str, Any]] = {}
_users_lock = threading.Lock()

# Session store: session_id -> session data
_sessions: Dict[str, Dict[str, Any]] = {}
_sessions_lock = threading.Lock()


def load_from_db() -> None:
    """Load users and sessions from the database into memory.

    Call this on startup to restore persisted auth data.
    """
    try:
        from hub.feature_store import init_auth_users, init_auth_sessions
        with _users_lock:
            _users.clear()
            _users.update(init_auth_users())
        with _sessions_lock:
            _sessions.clear()
            _sessions.update(init_auth_sessions())
        logger.info(f"Loaded {len(_users)} users and {len(_sessions)} sessions from database")
    except Exception as e:
        logger.warning(f"Failed to load auth data from database: {e}")


# ─── User management ────────────────────────────────────────────────────────

def create_user(
    username: str,
    password: str,
    role: str = "user",
    email: str = "",
    display_name: str = "",
) -> Dict[str, Any]:
    """Create a new user account.
    
    Args:
        username: Unique username.
        password: Plain text password (will be hashed).
        role: User role (admin, user, viewer).
        email: Optional email address.
        display_name: Optional display name.
        
    Returns:
        User data dictionary.
        
    Raises:
        ValueError: If username already exists.
    """
    if not username or not password:
        raise ValueError("Username and password are required")
    
    if len(password) < 8:
        raise ValueError("Password must be at least 8 characters")
    
    hashed = hash_password(password)
    now = datetime.now(timezone.utc).isoformat()
    
    with _users_lock:
        if username in _users:
            raise ValueError(f"User '{username}' already exists")
        
        user = {
            "id": str(uuid.uuid4()),
            "username": username,
            "password_hash": hashed,
            "role": role,
            "email": email,
            "display_name": display_name or username,
            "created_at": now,
            "updated_at": now,
            "is_active": True,
            "last_login": None,
        }
        _users[username] = user
        logger.info(f"User created: {username} (role={role})")
        
        # Persist to database
        try:
            from hub.feature_store import save_auth_user
            save_auth_user(user)
        except Exception as e:
            logger.warning(f"Failed to persist user to database: {e}")
        
        return _sanitize_user(user)


def get_user(username: str) -> Optional[Dict[str, Any]]:
    """Get user by username.
    
    Args:
        username: Username to look up.
        
    Returns:
        User data or None if not found.
    """
    with _users_lock:
        user = _users.get(username)
        return _sanitize_user(user) if user else None


def update_user(username: str, **kwargs: Any) -> Optional[Dict[str, Any]]:
    """Update user data.
    
    Args:
        username: Username to update.
        **kwargs: Fields to update (role, email, display_name, is_active).
        
    Returns:
        Updated user data or None if not found.
    """
    allowed_fields = {"role", "email", "display_name", "is_active"}
    updates = {k: v for k, v in kwargs.items() if k in allowed_fields}
    
    if not updates:
        return get_user(username)
    
    with _users_lock:
        user = _users.get(username)
        if not user:
            return None
        
        user.update(updates)
        user["updated_at"] = datetime.now(timezone.utc).isoformat()
        logger.info(f"User updated: {username} ({list(updates.keys())})")
        
        # Persist to database
        try:
            from hub.feature_store import save_auth_user
            save_auth_user(user)
        except Exception as e:
            logger.warning(f"Failed to persist user update to database: {e}")
        
        return _sanitize_user(user)


def change_password(username: str, old_password: str, new_password: str) -> bool:
    """Change a user's password.
    
    Args:
        username: Username.
        old_password: Current password.
        new_password: New password (min 8 chars).
        
    Returns:
        True if password was changed.
        
    Raises:
        ValueError: If old password is incorrect or new password is too short.
    """
    if len(new_password) < 8:
        raise ValueError("New password must be at least 8 characters")
    
    with _users_lock:
        user = _users.get(username)
        if not user:
            raise ValueError("User not found")
        
        if not verify_password(old_password, user["password_hash"]):
            raise ValueError("Current password is incorrect")
        
        user["password_hash"] = hash_password(new_password)
        user["updated_at"] = datetime.now(timezone.utc).isoformat()
        logger.info(f"Password changed for user: {username}")
        
        # Persist to database
        try:
            from hub.feature_store import save_auth_user
            save_auth_user(user)
        except Exception as e:
            logger.warning(f"Failed to persist password change to database: {e}")
        
        # Revoke all existing sessions
        _revoke_all_sessions_for_user(user["id"])
        return True


def list_users() -> List[Dict[str, Any]]:
    """List all users.
    
    Returns:
        List of user data dictionaries.
    """
    with _users_lock:
        return [_sanitize_user(u) for u in _users.values()]


def _sanitize_user(user: Dict[str, Any]) -> Dict[str, Any]:
    """Remove sensitive fields from user data.
    
    Args:
        user: User data dictionary.
        
    Returns:
        Sanitized user data without password hash.
    """
    sanitized = user.copy()
    sanitized.pop("password_hash", None)
    return sanitized


def _revoke_all_sessions_for_user(user_id: str) -> int:
    """Revoke all sessions for a user.
    
    Args:
        user_id: User ID.
        
    Returns:
        Number of sessions revoked.
    """
    count = 0
    with _sessions_lock:
        to_revoke = [
            sid for sid, sess in _sessions.items()
            if sess["user_id"] == user_id and not sess["revoked"]
        ]
        for sid in to_revoke:
            _sessions[sid]["revoked"] = True
            _sessions[sid]["revoked_at"] = datetime.now(timezone.utc).isoformat()
            count += 1
    if count > 0:
        logger.info(f"Revoked {count} sessions for user {user_id}")
    return count


# ─── Session management ─────────────────────────────────────────────────────

def create_session(user_id: str, username: str) -> str:
    """Create a new session for a user.
    
    Args:
        user_id: User ID.
        username: Username.
        
    Returns:
        Signed session token.
    """
    session_id = _generate_session_token()
    now = time.time()
    expires_at = now + SESSION_MAX_AGE
    
    session = {
        "session_id": session_id,
        "user_id": user_id,
        "username": username,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "expires_at": datetime.fromtimestamp(expires_at, tz=timezone.utc).isoformat(),
        "expires_at_ts": expires_at,
        "revoked": False,
        "revoked_at": None,
        "ip_address": None,
        "user_agent": None,
    }
    
    with _sessions_lock:
        _sessions[session_id] = session
    
    # Persist to database
    try:
        from hub.feature_store import save_auth_session
        save_auth_session(session)
    except Exception as e:
        logger.warning(f"Failed to persist session to database: {e}")
    
    logger.info(f"Session created for user: {username}")
    return _sign_session_token(session_id)


def validate_session(token: str, ip_address: str = "", user_agent: str = "") -> Optional[Dict[str, Any]]:
    """Validate a session token.
    
    Args:
        token: Signed session token.
        ip_address: Client IP address.
        user_agent: Client user agent.
        
    Returns:
        Session data if valid, None otherwise.
    """
    session_id = _verify_session_token(token)
    if not session_id:
        return None
    
    with _sessions_lock:
        session = _sessions.get(session_id)
        if not session:
            return None
        
        # Check revocation
        if session["revoked"]:
            return None
        
        # Check expiration
        if time.time() > session["expires_at_ts"]:
            return None
        
        # Update IP and user agent
        session["ip_address"] = ip_address
        session["user_agent"] = user_agent
        
        return session


def revoke_session(token: str) -> bool:
    """Revoke a session.
    
    Args:
        token: Signed session token.
        
    Returns:
        True if session was revoked.
    """
    session_id = _verify_session_token(token)
    if not session_id:
        return False
    
    with _sessions_lock:
        session = _sessions.get(session_id)
        if not session or session["revoked"]:
            return False
        
        session["revoked"] = True
        session["revoked_at"] = datetime.now(timezone.utc).isoformat()
        
        # Persist to database
        try:
            from hub.feature_store import save_auth_session
            save_auth_session(session)
        except Exception as e:
            logger.warning(f"Failed to persist session revocation to database: {e}")
        
        logger.info(f"Session revoked for user: {session['username']}")
        return True


def cleanup_expired_sessions() -> int:
    """Remove expired sessions from memory.
    
    Returns:
        Number of sessions removed.
    """
    now = time.time()
    count = 0
    with _sessions_lock:
        expired = [
            sid for sid, sess in _sessions.items()
            if now > sess["expires_at_ts"] or sess["revoked"]
        ]
        for sid in expired:
            del _sessions[sid]
            count += 1
    if count > 0:
        logger.info(f"Cleaned up {count} expired/revoked sessions")
    return count


# ─── FastAPI dependencies ───────────────────────────────────────────────────

async def get_current_user(
    session_token: Optional[str] = Cookie(None, alias=SESSION_COOKIE_NAME),
    request: Request = None,
) -> Optional[Dict[str, Any]]:
    """Get the current user from session cookie.
    
    This dependency returns None if no valid session is found,
    allowing routes to decide whether to require authentication.
    
    Args:
        session_token: Session cookie.
        request: FastAPI request.
        
    Returns:
        User data if authenticated, None otherwise.
    """
    if not session_token:
        return None
    
    session = validate_session(
        session_token,
        ip_address=request.client.host if request else "",
        user_agent=request.headers.get("user-agent", "") if request else "",
    )
    
    if not session:
        return None
    
    username = session["username"]
    with _users_lock:
        user = _users.get(username)
        if user and user.get("is_active", True):
            return _sanitize_user(user)
    
    return None


async def require_auth(
    session_token: Optional[str] = Cookie(None, alias=SESSION_COOKIE_NAME),
    request: Request = None,
) -> Dict[str, Any]:
    """Require authentication. Raises 401 if not authenticated.
    
    Args:
        session_token: Session cookie.
        request: FastAPI request.
        
    Returns:
        User data if authenticated.
        
    Raises:
        HTTPException: 401 if not authenticated.
    """
    user = await get_current_user(session_token, request)
    if not user:
        raise HTTPException(
            status_code=401,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Cookie"},
        )
    return user


async def require_admin(
    session_token: Optional[str] = Cookie(None, alias=SESSION_COOKIE_NAME),
    request: Request = None,
) -> Dict[str, Any]:
    """Require admin role. Raises 403 if not admin.
    
    Args:
        session_token: Session cookie.
        request: FastAPI request.
        
    Returns:
        User data if admin.
        
    Raises:
        HTTPException: 401 if not authenticated, 403 if not admin.
    """
    user = await require_auth(session_token, request)
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


# ─── Authentication endpoints helpers ───────────────────────────────────────

async def login(username: str, password: str, response: Response) -> Dict[str, Any]:
    """Authenticate a user and create a session.
    
    Args:
        username: Username.
        password: Plain text password.
        response: FastAPI response to set cookie.
        
    Returns:
        User data and session info.
        
    Raises:
        HTTPException: 401 if credentials are invalid.
    """
    with _users_lock:
        user = _users.get(username)
        if not user:
            # Use a dummy hash to prevent timing attacks on username enumeration
            # This ensures the verification path takes the same amount of time
            # whether the user exists or not
            dummy_hash = hash_password("dummy-password-for-timing-protection")
            verify_password(password, dummy_hash)
            raise HTTPException(status_code=401, detail="Invalid username or password")
        
        if not user.get("is_active", True):
            raise HTTPException(status_code=401, detail="Account is disabled")
        
        if not verify_password(password, user["password_hash"]):
            raise HTTPException(status_code=401, detail="Invalid username or password")
    
    # Update last login
    with _users_lock:
        user["last_login"] = datetime.now(timezone.utc).isoformat()
    
    # Create session
    token = create_session(user["id"], username)
    
    # Set secure cookie
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=token,
        max_age=SESSION_MAX_AGE,
        httponly=True,
        secure=SESSION_SECURE,
        samesite=SESSION_SAME_SITE,
        path="/",
    )
    
    logger.info(f"User logged in: {username}")
    return {
        "user": _sanitize_user(user),
        "session": {
            "expires_at": datetime.fromtimestamp(
                time.time() + SESSION_MAX_AGE,
                tz=timezone.utc,
            ).isoformat(),
        },
    }


async def logout(response: Response, session_token: Optional[str] = None) -> Dict[str, Any]:
    """Log out the current user by revoking the session.
    
    Args:
        response: FastAPI response to clear cookie.
        session_token: Session cookie.
        
    Returns:
        Success message.
    """
    if session_token:
        revoke_session(session_token)
    
    # Clear cookie
    response.delete_cookie(
        key=SESSION_COOKIE_NAME,
        path="/",
    )
    
    logger.info("User logged out")
    return {"message": "Logged out successfully"}


# ─── Initialization ─────────────────────────────────────────────────────────

def initialize_default_admin(username: str = "admin", password: str = "") -> Dict[str, Any]:
    """Create a default admin user if none exists.
    
    This is intended for fresh installations only. In production,
    use the admin API to create users.
    
    Args:
        username: Admin username.
        password: Admin password (if empty, generates a random one).
        
    Returns:
        The admin user data.
    """
    with _users_lock:
        if _users:
            logger.info("Users already exist, skipping default admin creation")
            return {}
    
    # Generate random password if not provided
    if not password:
        password = secrets.token_urlsafe(16)
        logger.warning(f"Generated random admin password: {password}")
    
    try:
        user = create_user(username, password, role="admin", display_name="Administrator")
        logger.info(f"Default admin user created: {username}")
        return user
    except ValueError as e:
        logger.warning(f"Could not create default admin: {e}")
        return {}


# ─── Persistence hooks ──────────────────────────────────────────────────────

def load_users_from_store(users_data: Dict[str, Dict[str, Any]]) -> None:
    """Load users from persistent storage (e.g., SQLite via feature_store).
    
    Args:
        users_data: Dictionary mapping usernames to user data.
    """
    with _users_lock:
        _users.clear()
        _users.update(users_data)
    logger.info(f"Loaded {len(users_data)} users from persistent storage")


def get_users_for_persistence() -> Dict[str, Dict[str, Any]]:
    """Get all users for persistent storage.
    
    Returns:
        Dictionary mapping usernames to user data (includes password hashes).
    """
    with _users_lock:
        return dict(_users)


def load_sessions_from_store(sessions_data: Dict[str, Dict[str, Any]]) -> None:
    """Load sessions from persistent storage.
    
    Args:
        sessions_data: Dictionary mapping session IDs to session data.
    """
    with _sessions_lock:
        _sessions.clear()
        _sessions.update(sessions_data)
    logger.info(f"Loaded {len(sessions_data)} sessions from persistent storage")


def get_sessions_for_persistence() -> Dict[str, Dict[str, Any]]:
    """Get all sessions for persistent storage.
    
    Returns:
        Dictionary mapping session IDs to session data.
    """
    with _sessions_lock:
        return dict(_sessions)
