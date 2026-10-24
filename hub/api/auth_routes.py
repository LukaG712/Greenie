"""Authentication API routes for Evershade.

Provides endpoints for:
- User registration and login
- Password management
- Session management
- User profile management
- Admin user management
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Cookie, Depends, HTTPException, Query, Request, Response

from hub.api.auth import (
    change_password,
    create_user,
    get_current_user,
    get_user,
    list_users,
    login,
    logout,
    require_admin,
    require_auth,
    update_user,
)
from hub.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/auth", tags=["Authentication"])


# ─── Public endpoints ───────────────────────────────────────────────────────

@router.post("/login", summary="Login with username and password")
async def login_endpoint(
    username: str = Body(..., embed=True),
    password: str = Body(..., embed=True),
    response: Response = None,
    request: Request = None,
) -> Dict[str, Any]:
    """Authenticate a user and create a session.
    
    Sets a secure HTTP-only cookie for the session.
    Returns user data and session expiration.
    """
    return await login(username, password, response)


@router.post("/logout", summary="Logout and revoke session")
async def logout_endpoint(
    response: Response,
    current_user: Optional[Dict[str, Any]] = Depends(get_current_user),
    session_token: Optional[str] = Cookie(None, alias="evershade_session"),
) -> Dict[str, Any]:
    """Log out the current user by revoking the session cookie."""
    return await logout(response, session_token)


@router.get("/me", summary="Get current user profile")
async def me_endpoint(
    current_user: Optional[Dict[str, Any]] = Depends(get_current_user),
) -> Dict[str, Any]:
    """Get the current authenticated user's profile.
    
    Returns 401 if not authenticated.
    """
    if not current_user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return {"user": current_user}


@router.put("/me", summary="Update current user profile")
async def update_me_endpoint(
    display_name: Optional[str] = Body(None),
    email: Optional[str] = Body(None),
    current_user: Dict[str, Any] = Depends(require_auth),
) -> Dict[str, Any]:
    """Update the current user's profile information."""
    updates = {}
    if display_name is not None:
        updates["display_name"] = display_name
    if email is not None:
        updates["email"] = email
    
    updated = update_user(current_user["username"], **updates)
    if not updated:
        raise HTTPException(status_code=404, detail="User not found")
    
    return {"user": updated}


@router.post("/me/password", summary="Change current user password")
async def change_my_password_endpoint(
    old_password: str = Body(..., embed=True),
    new_password: str = Body(..., embed=True),
    current_user: Dict[str, Any] = Depends(require_auth),
) -> Dict[str, Any]:
    """Change the current user's password.
    
    Revokes all existing sessions after password change.
    """
    try:
        change_password(current_user["username"], old_password, new_password)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    return {"message": "Password changed successfully, please login again"}


# ─── Admin endpoints ────────────────────────────────────────────────────────

@router.get("/users", summary="List all users (admin)")
async def list_users_endpoint(
    admin_user: Dict[str, Any] = Depends(require_admin),
) -> Dict[str, Any]:
    """List all users. Requires admin role."""
    users = list_users()
    return {"users": users, "total": len(users)}


@router.post("/users", summary="Create a new user (admin)")
async def create_user_endpoint(
    username: str = Body(..., min_length=1, embed=True),
    password: str = Body(..., min_length=8, embed=True),
    role: str = Body("user", embed=True),
    email: str = Body("", embed=True),
    display_name: str = Body("", embed=True),
    admin_user: Dict[str, Any] = Depends(require_admin),
) -> Dict[str, Any]:
    """Create a new user account. Requires admin role.
    
    Args:
        username: Unique username.
        password: Password (min 8 characters).
        role: User role (admin, user, viewer).
        email: Optional email address.
        display_name: Optional display name.
    """
    valid_roles = {"admin", "user", "viewer"}
    if role not in valid_roles:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid role. Must be one of: {', '.join(valid_roles)}"
        )
    
    try:
        user = create_user(username, password, role=role, email=email, display_name=display_name)
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    
    return {"user": user}


@router.get("/users/{username}", summary="Get user by username (admin)")
async def get_user_endpoint(
    username: str,
    admin_user: Dict[str, Any] = Depends(require_admin),
) -> Dict[str, Any]:
    """Get a specific user by username. Requires admin role."""
    user = get_user(username)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return {"user": user}


@router.put("/users/{username}", summary="Update user (admin)")
async def update_user_endpoint(
    username: str,
    role: Optional[str] = Body(None, embed=True),
    email: Optional[str] = Body(None, embed=True),
    display_name: Optional[str] = Body(None, embed=True),
    is_active: Optional[bool] = Body(None, embed=True),
    admin_user: Dict[str, Any] = Depends(require_admin),
) -> Dict[str, Any]:
    """Update a user's information. Requires admin role.
    
    Can change role, email, display name, and active status.
    """
    valid_roles = {"admin", "user", "viewer"}
    if role is not None and role not in valid_roles:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid role. Must be one of: {', '.join(valid_roles)}"
        )
    
    updates = {}
    if role is not None:
        updates["role"] = role
    if email is not None:
        updates["email"] = email
    if display_name is not None:
        updates["display_name"] = display_name
    if is_active is not None:
        updates["is_active"] = is_active
    
    updated = update_user(username, **updates)
    if not updated:
        raise HTTPException(status_code=404, detail="User not found")
    
    return {"user": updated}


@router.delete("/users/{username}", summary="Disable user (admin)")
async def disable_user_endpoint(
    username: str,
    admin_user: Dict[str, Any] = Depends(require_admin),
) -> Dict[str, Any]:
    """Disable a user account (soft delete). Requires admin role.
    
    An admin cannot disable themselves.
    """
    if username == admin_user["username"]:
        raise HTTPException(status_code=400, detail="Cannot disable your own account")
    
    updated = update_user(username, is_active=False)
    if not updated:
        raise HTTPException(status_code=404, detail="User not found")
    
    return {"message": f"User '{username}' has been disabled", "user": updated}


@router.post("/users/{username}/reset-password", summary="Reset user password (admin)")
async def reset_password_endpoint(
    username: str,
    new_password: str = Body(..., embed=True),
    admin_user: Dict[str, Any] = Depends(require_admin),
) -> Dict[str, Any]:
    """Reset a user's password. Requires admin role.
    
    Revokes all existing sessions for the user.
    The user will need to login with the new password.
    """
    if len(new_password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")
    
    from hub.api.auth import hash_password, _users_lock, _users, _revoke_all_sessions_for_user
    import secrets
    
    # Generate random password if not provided
    if not new_password:
        new_password = secrets.token_urlsafe(12)
    
    user_id = None
    with _users_lock:
        user = _users.get(username)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        user["password_hash"] = hash_password(new_password)
        user_id = user["id"]

    # Revoke all existing sessions (outside _users_lock to avoid deadlock with _sessions_lock)
    if user_id:
        _revoke_all_sessions_for_user(user_id)
    
    logger.info(f"Admin {admin_user['username']} reset password for user {username}")
    return {
        "message": f"Password for '{username}' has been reset",
    }


# ─── Session info ───────────────────────────────────────────────────────────

@router.get("/session", summary="Get current session info")
async def session_info_endpoint(
    current_user: Optional[Dict[str, Any]] = Depends(get_current_user),
) -> Dict[str, Any]:
    """Check if the current request is authenticated.
    
    Returns user data if authenticated, or a message if not.
    Useful for the frontend to check auth status.
    """
    if current_user:
        return {"authenticated": True, "user": current_user}
    return {"authenticated": False, "user": None}
